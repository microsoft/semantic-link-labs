from typing import Optional
from contextlib import contextmanager
from sempy._utils._log import log
from uuid import UUID
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_item_name_and_id,
    _base_api,
    _conv_b64,
    _create_dataframe,
    _update_dataframe_datatypes,
)
import json
import sempy_labs._icons as icons
import copy
from io import BytesIO
import zipfile
import base64
import pandas as pd
from jsonpath_ng.ext import parse
import sempy_labs.report._report_helper as helper


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
                merged_payload = {}
                with zipfile.ZipFile(BytesIO(decoded_bytes)) as zip_file:
                    for filename in zip_file.namelist():
                        if filename.endswith(".json"):
                            with zip_file.open(filename) as f:
                                content = f.read()
                                part_data = json.loads(content.decode("utf-8"))

                                if isinstance(part_data, dict):
                                    merged_payload.update(part_data)
                                else:
                                    # For non-dict top-level json (rare), store under filename
                                    merged_payload[filename] = part_data

                self._report_definition["parts"].append(
                    {"path": path, "payload": merged_payload}
                )
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

        existing_paths = [
            part.get("path") for part in self._report_definition.get("parts")
        ]
        if file_path in existing_paths:
            raise ValueError(
                f"{icons.red_dot} Cannot add the '{file_path}' file as this file path already exists in the report definition."
            )

        self._report_definition["parts"].append({"path": file_path, "payload": payload})

    def remove(self, file_path: str):

        for part in self._report_definition.get("parts"):
            if part.get("path") == file_path:
                self._report_definition["parts"].remove(part)
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
                    part["payload"] = _conv_b64(
                        part["payload"]
                    )  # Do I need to zip some of these?

            # Combine report and non-report definitions
            payload = {"definition": {"parts": new_report_definition.get("parts")}}

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

    def list_pages(self) -> pd.DataFrame:
        """
        List all pages in the report.

        Returns
        -------
        list
            A list of page names.
        """

        columns = {
            "File Path": "str",
            "Page Name": "str",
            "Page Display Name": "str",
            "Hidden": "bool",
            "Active": "bool",
            "Width": "int",
            "Height": "int",
            "Display Option": "str",
            "Type": "str",
            "Alignment": "str",
            "Drillthrough Target Page": "bool",
            "Visual Count": "int",
            "Data Visual Count": "int",
            "Visible Visual Count": "int",
            "Page Filter Count": "int",
            "Page URL": "str",
        }
        df = _create_dataframe(columns=columns)

        pages = [
            p
            for p in self._report_definition.get("parts")
            if p.get("path").endswith("/page.json")
        ]
        page = self.get(file_path="definition/pages/pages.json")
        active_page = page.get("activePageName")

        for p in pages:
            file_path = p.get("path")
            page_prefix = file_path[0:-9]
            payload = p.get("payload")
            page_name = payload.get("name")
            height = payload.get("height")
            width = payload.get("width")

            # Alignment
            matches = parse(
                "$.objects.displayArea[0].properties.verticalAlignment.expr.Literal.Value"
            ).find(payload)
            alignment_value = (
                matches[0].value[1:-1] if matches and matches[0].value else "Top"
            )

            # Drillthrough
            matches = parse("$.filterConfig.filters[*].howCreated").find(payload)
            how_created_values = [match.value for match in matches]
            drill_through = any(value == "Drillthrough" for value in how_created_values)

            visual_count = len(
                [
                    v
                    for v in self._report_definition.get("parts")
                    if v.get("path").endswith("/visual.json")
                    and v.get("path").startswith(page_prefix)
                ]
            )

            # data_visual_count = len(
            #    dfV[(dfV["Page Name"] == page_name) & (dfV["Data Visual"])]
            # )
            # visible_visual_count = len(
            #    dfV[(dfV["Page Name"] == page_name) & (dfV["Hidden"] == False)]
            # )

            # Page Filter Count
            matches = parse("$.filterConfig.filters").find(payload)
            page_filter_count = (
                len(matches[0].value) if matches and matches[0].value else 0
            )

            # Hidden
            matches = parse("$.visibility").find(payload)
            is_hidden = any(match.value == "HiddenInViewMode" for match in matches)

            new_data = {
                "File Path": file_path,
                "Page Name": page_name,
                "Page Display Name": payload.get("displayName"),
                "Display Option": payload.get("displayOption"),
                "Height": height,
                "Width": width,
                "Hidden": is_hidden,
                "Active": True if page_name == active_page else False,
                "Type": helper.page_type_mapping.get((width, height), "Custom"),
                "Alignment": alignment_value,
                "Drillthrough Target Page": drill_through,
                "Visual Count": visual_count,
                # "Data Visual Count": data_visual_count,
                # "Visible Visual Count": visible_visual_count,
                "Page Filter Count": page_filter_count,
                "Page URL": f"{helper.get_web_url(report=self._report_name, workspace=self._workspace_id)}/{page_name}",
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

        #_update_dataframe_datatypes(dataframe=df, column_map=columns)

        return df

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
