from typing import Optional
from contextlib import contextmanager
from sempy._utils._log import log
from uuid import UUID
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_item_name_and_id,
    _base_api,
    _decode_b64,
    _conv_b64,
)
import json
import sempy_labs._icons as icons
import copy
from io import BytesIO
import zipfile
import base64


class ReportWrapper:
    """
    Connects to a Power BI report and retrieves its definition.

    The ReportWrapper and all functions which depend on it require the report to be in the `PBIR <https://powerbi.microsoft.com/blog/power-bi-enhanced-report-format-pbir-in-power-bi-desktop-developer-mode-preview>`_ format.

    Parameters
    ----------
    report : str
        The name of the report.
    workspace : str | uuid.UUID
        The name or ID of the workspace in which the report resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe containing the report metadata definition files.
    """

    _report_name: str
    _report_id: str
    _workspace_name: str
    _workspace_id: str
    _readonly: bool

    @log
    def __init__(
        self,
        report: str | UUID,
        workspace: Optional[str | UUID] = None,
        readonly: bool = True,
    ):
        (self._workspace_name, self._workspace_id) = resolve_workspace_name_and_id(
            workspace
        )
        (self._report_name, self._report_id) = resolve_item_name_and_id(
            item=report, type="Report", workspace=self._workspace_id
        )
        self._readonly = readonly

        result = _base_api(
            request=f"/v1/workspaces/{self._workspace_id}/items/{self._report_id}/getDefinition",
            method="post",
            status_codes=None,
            lro_return_json=True,
        )

        def is_zip_file(data: bytes) -> bool:
            return data.startswith(b"PK\x03\x04")

        self._report_definition = {"parts": []}
        for parts in result.get("definition", {}).get("parts", []):
            path = parts.get("path")
            payload = parts.get("payload")
            decoded_bytes = base64.b64decode(payload)

            if is_zip_file(decoded_bytes):
                with zipfile.ZipFile(BytesIO(decoded_bytes)) as zip_file:
                    for filename in zip_file.namelist():
                        with zip_file.open(filename) as f:
                            content = f.read()
                            decoded_payload = json.loads(content.decode("utf-8"))
            else:
                decoded_payload = json.loads(decoded_bytes.decode("utf-8"))

            self._report_definition["parts"].append(
                {"path": path, "payload": decoded_payload}
            )

    def get(self, file_path: str, format: bool = False) -> dict | str:
        """
        Get the json content of the specified report definition file.

        Parameters
        ----------
        file_path : str
            The path of the report definition file. For example: "definition/pages/pages.json".
        format : bool, default=False
            Whether to format the JSON output.
            If True, the JSON will be pretty-printed with indentation.

        Returns
        -------
        dict | str
            The json content of the specified report definition file.
        """
        for part in self._report_definition.get("parts"):
            if part.get("path") == file_path:
                value = part.get("payload")
                if format:
                    value = json.dumps(value, indent=2)
                return value

        raise ValueError(f"File {file_path} not found in report definition.")

    def add(self, file_path: str, payload: dict):

        if not isinstance(payload, dict):
            raise ValueError("Payload must be a dictionary.")

        self._report_definition["parts"].append({"path": file_path, "payload": payload})

    def remove(self, file_path: str):

        for part in self._report_definition.get("parts"):
            if part.get("path") == file_path:
                self._report_definition["parts"].remove(part)
                # print(f"The file '{file_path}' has been removed from report definition.")
                return
        for part in self._non_report_definition.get("parts"):
            if part.get("path") == file_path:
                self._non_report_definition["parts"].remove(part)
                # print(f"The file '{file_path}' has been removed from report definition.")
                return

        raise ValueError(f"File {file_path} not found in report definition.")

    def update(self, file_path: str, payload: dict):
        if not isinstance(payload, dict):
            raise ValueError("Payload must be a dictionary.")

        for part in self._report_definition.get("parts"):
            if part.get("path") == file_path:
                part["payload"] = payload
                # print(f"The file '{file_path}' has been updated in report definition.")
                return

        raise ValueError(f"File {file_path} not found in report definition.")

    # def all_files(self):

    #    yield self._report_definition.get('parts')

    def save_changes(self):

        if self._readonly:
            print(
                f"{icons.red_dot} The connection is read-only. Set 'readonly' to False to save changes."
            )
        else:
            # Convert the report definition to base64
            new_report_definition = copy.deepcopy(self._report_definition)

            for part in new_report_definition.get("parts", []):
                if isinstance(part.get("payload"), dict):
                    part["payload"] = _conv_b64(part["payload"])  # Do I need to zip some of these?

            # Combine report and non-report definitions
            payload = {
                "definition": {
                    "parts": new_report_definition.get("parts")
                }
            }

            # Update item definition
            _base_api(
                request=f"/v1/workspaces/{self._workspace_id}/reports/{self._report_id}/updateDefinition",
                method="post",
                payload=payload,
                lro_return_status_code=True,
                status_codes=None,
            )
            print(
                f"{icons.green_dot} The report definition has been updated successfully."
            )

    def close(self):

        if not self._readonly:
            self.save_changes()

            # self._report_definition = None


@log
@contextmanager
def connect_report(
    report: str | UUID,
    readonly: bool = True,
    workspace: Optional[str | UUID] = None,
):
    """
    Connects to the report.

    Parameters
    ----------
    report : str | uuid.UUID
        Name or ID of the report.
    readonly: bool, default=True
        Whether the connection is read-only or read/write. Setting this to False enables read/write which saves the changes made back to the server.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID. Also supports Azure Analysis Services (Service Principal Authentication required).
        If connecting to Azure Analysis Services, enter the workspace parameter in the following format: 'asazure://<region>.asazure.windows.net/<server_name>'.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    typing.Iterator[TOMWrapper]
        A connection to the semantic model's Tabular Object Model.
    """

    rw = ReportWrapper(
        report=report,
        workspace=workspace,
        readonly=readonly,
    )
    try:
        yield rw
    finally:
        rw.close()
