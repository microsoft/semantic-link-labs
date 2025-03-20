from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_lakehouse_name_and_id,
    _xml_to_dict,
)
from sempy._utils._log import log
from uuid import UUID
from typing import Optional, List
import sempy_labs._icons as icons
import xml.etree.ElementTree as ET


@log
def recover_lakehouse_object(
    file_path: str,
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Recovers an object (i.e. table, file, folder) in a lakehouse from a deleted state.

    Parameters
    ----------
    file_path : str
        The file path of the object to restore. For example: "Tables/my_delta_table".
    lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (lakehouse_name, lakehouse_id) = resolve_lakehouse_name_and_id(
        lakehouse, workspace_id
    )

    blob_path_prefix = f"{lakehouse_id}/{file_path}"

    container = file_path.split("/")[0]
    if container not in ["Tables", "Files"]:
        raise ValueError(
            f"{icons.red_dot} Invalid container '{container}' within the file_path parameter. Expected 'Tables' or 'Files'."
        )

    response = _request_blob_api(
        request=f"{workspace_id}/{lakehouse_id}/{container}?restype=container&comp=list&include=deleted"
    )
    root = ET.fromstring(response.content)
    response_json = _xml_to_dict(root)
    for blob in (
        response_json.get("EnumerationResults", {}).get("Blobs", {}).get("Blob", {})
    ):
        blob_name = blob.get("Name")
        is_deleted = blob.get("Deleted", False)
        if blob_name.startswith(blob_path_prefix) and is_deleted:
            print(f"{icons.in_progress} Restoring the '{blob_name}' blob...")
            response = _request_blob_api(
                request=f"{workspace_id}/{lakehouse_id}/{file_path}?comp=undelete",
                method="put",
            )
            print(f"{icons.green_dot} The '{blob_name}' blob has been restored.")


def _request_blob_api(
    request: str,
    method: str = "get",
    payload: Optional[dict] = None,
    status_codes: int | List[int] = 200,
):

    import requests
    import notebookutils
    from sempy.fabric.exceptions import FabricHTTPException

    if isinstance(status_codes, int):
        status_codes = [status_codes]

    token = notebookutils.credentials.getToken("storage")

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "x-ms-version": "2025-05-05",
    }

    response = requests.request(
        method.upper(),
        f"https://onelake.blob.fabric.microsoft.com/{request}",
        headers=headers,
        json=payload,
    )

    if response.status_code not in status_codes:
        raise FabricHTTPException(response)
    return response
