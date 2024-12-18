import sempy.fabric as fabric
from typing import Optional, List
from uuid import UUID
from sempy.fabric.exceptions import FabricHTTPException
import numpy as np
import time
from sempy_labs.admin._basic_functions import list_workspaces
from sempy._utils._log import log


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

    Parameters
    ----------
    data_source_details : bool, default=False
        Whether to return dataset expressions (DAX and Mashup queries). If you set this parameter to true, you must fully enable metadata scanning in order for data to be returned. For more information, see Enable tenant settings for metadata scanning.
    dataset_schema: bool = False
        Whether to return dataset schema (tables, columns and measures). If you set this parameter to true, you must fully enable metadata scanning in order for data to be returned. For more information, see Enable tenant settings for metadata scanning.
    dataset_expressions : bool, default=False
        Whether to return data source details
    lineage : bool, default=False
        Whether to return lineage info (upstream dataflows, tiles, data source IDs)
    artifact_users : bool, default=False
        Whether to return user details for a Power BI item (such as a report or a dashboard)
    workspace : str | List[str] | uuid.UUID | List[uuid.UUID], default=None
        The required workspace name(s) or id(s) to be scanned

    Returns
    -------
    dict
        A json object with the scan result.
    """
    scan_result = {
        "workspaces": [],
        "datasourceInstances": [],
        "misconfiguredDatasourceInstances": [],
    }

    client = fabric.FabricRestClient()

    if workspace is None:
        workspace = fabric.resolve_workspace_name()

    if isinstance(workspace, str):
        workspace = [workspace]

    workspace_list = []

    dfW = list_workspaces()
    workspace_list = dfW[dfW["Name"].isin(workspace)]["Id"].tolist()
    workspace_list = workspace_list + dfW[dfW["Id"].isin(workspace)]["Id"].tolist()

    workspaces = np.array(workspace_list)
    batch_size = 99
    for i in range(0, len(workspaces), batch_size):
        batch = workspaces[i : i + batch_size].tolist()
        request_body = {"workspaces": batch}

        response_clause = f"/v1.0/myorg/admin/workspaces/getInfo?lineage={lineage}&datasourceDetails={data_source_details}&datasetSchema={dataset_schema}&datasetExpressions={dataset_expressions}&getArtifactUsers={artifact_users}"
        response = client.post(response_clause, json=request_body)

        if response.status_code != 202:
            raise FabricHTTPException(response)
        scan_id = response.json()["id"]
        scan_status = response.json().get("status")
        while scan_status not in ["Succeeded", "Failed"]:
            time.sleep(1)
            response = client.get(f"/v1.0/myorg/admin/workspaces/scanStatus/{scan_id}")
            scan_status = response.json().get("status")
        if scan_status == "Failed":
            raise FabricHTTPException(response)
        response = client.get(f"/v1.0/myorg/admin/workspaces/scanResult/{scan_id}")
        if response.status_code != 200:
            raise FabricHTTPException(response)

        responseJson = response.json()

        if "workspaces" in responseJson:
            scan_result["workspaces"].extend(responseJson["workspaces"])

        if "datasourceInstances" in responseJson:
            scan_result["datasourceInstances"].extend(
                responseJson["datasourceInstances"]
            )

        if "misconfiguredDatasourceInstances" in responseJson:
            scan_result["misconfiguredDatasourceInstances"].extend(
                responseJson["misconfiguredDatasourceInstances"]
            )

    return scan_result
