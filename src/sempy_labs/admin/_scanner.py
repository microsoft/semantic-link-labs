from typing import Optional, List
from uuid import UUID
from sempy.fabric.exceptions import FabricHTTPException
import time
import sempy_labs._icons as icons
from sempy_labs.admin._basic_functions import list_workspaces
from sempy._utils._log import log
from sempy_labs._helper_functions import (
    _base_api,
    _is_valid_uuid,
    _build_url,
    resolve_workspace_name,
)


@log
def scan_workspaces(
    data_source_details: bool = False,
    dataset_schema: bool = False,
    dataset_expressions: bool = False,
    lineage: bool = False,
    artifact_users: bool = False,
    workspace: Optional[str | List[str] | UUID | List[UUID]] = None,
) -> dict:
    """
    Gets the scan result for the specified scan.

    This is a wrapper function for the following APIs:
        `Admin - WorkspaceInfo PostWorkspaceInfo <https://learn.microsoft.com/rest/api/power-bi/admin/workspace-info-post-workspace-info>`_.
        `Admin - WorkspaceInfo GetScanStatus <https://learn.microsoft.com/rest/api/power-bi/admin/workspace-info-get-scan-status>`_.
        `Admin - WorkspaceInfo GetScanResult <https://learn.microsoft.com/rest/api/power-bi/admin/workspace-info-get-scan-result>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    data_source_details : bool, default=False
        Whether to return dataset expressions (DAX and Mashup queries). If you set this parameter to true, you must fully enable metadata scanning in order for data to be returned. For more information, see Enable tenant settings for metadata scanning.
    dataset_schema: bool = False
        Whether to return dataset schema (tables, columns and measures). If you set this parameter to true, you must fully enable metadata scanning in order for data to be returned. For more information, see Enable tenant settings for metadata scanning.
    dataset_expressions : bool, default=False
        Whether to return data source details.
    lineage : bool, default=False
        Whether to return lineage info (upstream dataflows, tiles, data source IDs)
    artifact_users : bool, default=False
        Whether to return user details for a Power BI item (such as a report or a dashboard).
    workspace : str | List[str] | UUID | List[UUID], default=None
        The required workspace name(s) or id(s) to be scanned. It supports a limit of 100 workspaces and only IDs in GUID format.

    Returns
    -------
    dict
        A json object with the scan result.
    """

    if workspace is None:
        workspace = resolve_workspace_name()

    if isinstance(workspace, str):
        workspace = [workspace]

    if len(workspace) > 100:
        print(
            f"{icons.yellow_dot} More than 100 workspaces where provided. Truncating to the fist 100."
        )
        workspace = workspace[:100]

    workspace_list = []

    for w in workspace:
        if _is_valid_uuid(w):
            workspace_list.append(w)
        else:
            dfW = list_workspaces(workspace=w)
            workspace_list = (
                workspace_list + dfW[dfW["Name"].isin(workspace)]["Id"].tolist()
            )

    url = "/v1.0/myorg/admin/workspaces/getInfo"
    params = {}
    params["lineage"] = lineage
    params["datasourceDetails"] = data_source_details
    params["datasetSchema"] = dataset_schema
    params["datasetExpressions"] = dataset_expressions
    params["getArtifactUsers"] = artifact_users

    url = _build_url(url, params)

    payload = {"workspaces": workspace_list}

    response = _base_api(
        request=url,
        method="post",
        payload=payload,
        status_codes=202,
        client="fabric_sp",
    )

    scan_id = response.json()["id"]
    scan_status = response.json().get("status")

    while scan_status not in ["Succeeded", "Failed"]:
        time.sleep(1)
        response = _base_api(
            request=f"/v1.0/myorg/admin/workspaces/scanStatus/{scan_id}",
            client="fabric_sp",
        )
        scan_status = response.json().get("status")

    if scan_status == "Failed":
        raise FabricHTTPException(response)

    response = _base_api(
        request=f"/v1.0/myorg/admin/workspaces/scanResult/{scan_id}",
        client="fabric_sp",
    )

    return response.json()
