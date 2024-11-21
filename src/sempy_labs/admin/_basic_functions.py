import sempy.fabric as fabric
from typing import Optional, List, Union, Tuple
from uuid import UUID
import sempy_labs._icons as icons
from sempy.fabric.exceptions import FabricHTTPException
from sempy_labs._helper_functions import (
    pagination,
    _is_valid_uuid,
)
import numpy as np
import pandas as pd
import time
import urllib.parse
from dateutil.parser import parse as dtparser
from datetime import datetime


def list_workspaces(
    top: Optional[int] = None,
    skip: Optional[int] = None,
    capacity: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
    workspace_state: Optional[str] = None,
    workspace_type: Optional[str] = None,
    **kwargs,
) -> pd.DataFrame:
    """
    Lists workspaces for the organization. This function is the admin version of list_workspaces.

    This is a wrapper function for the following API: `Workspaces - List Workspaces - REST API (Admin) <https://learn.microsoft.com/en-us/rest/api/fabric/admin/workspaces/list-workspaces>`_.

    Parameters
    ----------
    top: int, default=None
        Returns only the first N workspaces.
    skip: int, default=None
        Skip the first N workspaces.
    capacity : str | UUID, default=None
        Returns only the workspaces in the specified Capacity.
    workspace : str | UUID, default=None
        Returns the workspace with the specific name.
    workspace_state : str, default=None
        Return only the workspace with the requested state. You can find the possible states in `Workspace States <https://learn.microsoft.com/en-us/rest/api/fabric/admin/workspaces/list-workspaces?tabs=HTTP#workspacestate>`_.
    workspace_type : str, default=None
        Return only the workspace of the specific type. You can find the possible types in `Workspace Types <https://learn.microsoft.com/en-us/rest/api/fabric/admin/workspaces/list-workspaces?tabs=HTTP#workspacetype>`_.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of workspaces for the organization.
    """
    if "filter" in kwargs:
        print(
            "The 'filter' parameter has been deprecated. Please remove this parameter from the function going forward."
        )
        del kwargs["filter"]

    client = fabric.FabricRestClient()

    df = pd.DataFrame(
        columns=[
            "Id",
            "Name",
            "State",
            "Type",
            "Capacity Id",
        ]
    )

    url = f"/v1/admin/workspaces"

    params = {}

    if capacity is not None:
        params["capacityId"] = _resolve_capacity_name_and_id(capacity)[1]

    if workspace is not None and not _is_valid_uuid(workspace):
        params["name"] = workspace

    if workspace_state is not None:
        params["state"] = workspace_state

    if workspace_type is not None:
        params["type"] = workspace_type

    url_parts = list(urllib.parse.urlparse(url))
    url_parts[4] = urllib.parse.urlencode(params)
    url = urllib.parse.urlunparse(url_parts)

    response = client.get(path_or_url=url)

    if response.status_code != 200:
        raise FabricHTTPException(response)

    responsePaginated = pagination(client, response)

    workspaces = []

    for r in responsePaginated:
        workspaces = workspaces + r.get("workspaces", [])

    if len(workspaces) > 0:
        df = pd.DataFrame(workspaces)
        df.rename(
            columns={
                "id": "Id",
                "name": "Name",
                "state": "State",
                "type": "Type",
                "capacityId": "Capacity Id",
            },
            inplace=True,
        )

        df["Id"] = df["Id"].str.lower()
        df["Capacity Id"] = df["Capacity Id"].str.lower()

        if workspace is not None and _is_valid_uuid(workspace):
            df = df[df["Id"] == workspace.lower()]

    if skip is not None:
        df = df.tail(-skip)
        df.reset_index(drop=True, inplace=True)

    if top is not None:
        df = df.head(top)

    return df


def list_capacities(
    capacity: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Shows the a list of capacities and their properties. This function is the admin version.

    This is a wrapper function for the following API: `Admin - Get Capacities As Admin <https://learn.microsoft.com/rest/api/power-bi/admin/get-capacities-as-admin>`_.

    Parameters
    ----------
    capacity : str | UUID, default=None
        Capacity name or id to filter.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the capacities and their properties
    """
    client = fabric.FabricRestClient()

    df = pd.DataFrame(
        columns=["Capacity Id", "Capacity Name", "Sku", "Region", "State", "Admins"]
    )

    response = client.get("/v1.0/myorg/admin/capacities")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    for r in responses:
        for i in r.get("value", []):
            new_data = {
                "Capacity Id": i.get("id").lower(),
                "Capacity Name": i.get("displayName"),
                "Sku": i.get("sku"),
                "Region": i.get("region"),
                "State": i.get("state"),
                "Admins": [i.get("admins", [])],
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    if capacity is not None:
        if _is_valid_uuid(capacity):
            df = df[df["Capacity Id"] == capacity.lower()]
        else:
            df = df[df["Capacity Name"] == capacity]

    return df


def assign_workspaces_to_capacity(
    source_capacity: Optional[str | UUID] = None,
    target_capacity: Optional[str | UUID] = None,
    workspace: Optional[str | List[str] | UUID | List[UUID]] = None,
):
    """
    Assigns a workspace to a capacity. This function is the admin version.

    This is a wrapper function for the following API: `Admin - Capacities AssignWorkspacesToCapacity <https://learn.microsoft.com/rest/api/power-bi/admin/capacities-assign-workspaces-to-capacity>`_.

    Parameters
    ----------
    source_capacity : str | UUID, default=None
        The name of the source capacity. If the Workspace is not specified, this is parameter mandatory.
    target_capacity : str | UUID, default=None
        The name of the target capacity.
    workspace : str | List[str] | UUID | List[UUID], default=None
        The name or id of the workspace(s).
        Defaults to None which resolves to migrating all workspaces within the source capacity to the target capacity.
    """
    if target_capacity is None:
        raise ValueError(
            f"{icons.red_dot} The parameter 'target_capacity' is mandatory."
        )

    if source_capacity is None and workspace is None:
        raise ValueError(
            f"{icons.red_dot} The parameters 'source_capacity' or 'workspace' needs to be specified."
        )

    if workspace is None:
        source_capacity_id = _resolve_capacity_name_and_id(source_capacity)[1]
        dfW = list_workspaces(capacity=source_capacity_id)
        workspaces = dfW["Id"].tolist()
    else:
        if isinstance(workspace, str):
            workspace = [workspace]
        if source_capacity is None:
            dfW = list_workspaces()
        else:
            dfW = list_workspaces(capacity=source_capacity_id)
        workspaces = dfW[dfW["Name"].isin(workspace)]["Id"].tolist()
        workspaces = workspaces + dfW[dfW["Id"].isin(workspace)]["Id"].tolist()

    if len(workspace) != len(workspaces):
        raise ValueError(
            f"{icons.red_dot} Some of the workspaces provided are not valid."
        )

    target_capacity_id = _resolve_capacity_name_and_id(target_capacity)[1]

    workspaces = np.array(workspaces)
    batch_size = 999
    for i in range(0, len(workspaces), batch_size):
        batch = workspaces[i : i + batch_size].tolist()
        request_body = {
            "capacityMigrationAssignments": [
                {
                    "targetCapacityObjectId": target_capacity_id.upper(),
                    "workspacesToAssign": batch,
                }
            ]
        }

        client = fabric.FabricRestClient()

        response = client.post(
            "/v1.0/myorg/admin/capacities/AssignWorkspaces",
            json=request_body,
        )

        if response.status_code != 200:
            raise FabricHTTPException(response)
    print(
        f"{icons.green_dot} The workspaces have been assigned to the '{target_capacity}' capacity. A total of {len(workspaces)} were moved."
    )


def unassign_workspaces_from_capacity(
    workspaces: str | List[str] | UUID | List[UUID],
):
    """
    Unassigns workspace(s) from their capacity.

    This is a wrapper function for the following API: `Admin - Capacities UnassignWorkspacesFromCapacity <https://learn.microsoft.com/rest/api/power-bi/admin/capacities-unassign-workspaces-from-capacity>`_.

    Parameters
    ----------
    workspaces : str | List[str] | UUID | List[UUID]
        The Fabric workspace name(s) or id(s).
    """
    if isinstance(workspaces, str):
        workspaces = [workspaces]

    dfW = list_workspaces()
    workspacesIds = dfW[dfW["Name"].isin(workspaces)]["Id"].tolist()
    workspacesIds = workspacesIds + dfW[dfW["Id"].isin(workspaces)]["Id"].tolist()

    if len(workspacesIds) != len(workspaces):
        raise ValueError(
            f"{icons.red_dot} Some of the workspaces provided are not valid."
        )

    payload = {"workspacesToUnassign": workspacesIds}

    client = fabric.PowerBIRestClient()
    response = client.post(
        "/v1.0/myorg/admin/capacities/UnassignWorkspaces",
        json=payload,
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} A total of {len(workspacesIds)} workspaces have been unassigned."
    )


def list_tenant_settings() -> pd.DataFrame:
    """
    Lists all tenant settings.

    This is a wrapper function for the following API: `Tenants - List Tenant Settings <https://learn.microsoft.com/rest/api/fabric/admin/tenants/list-tenant-settings>`_.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the tenant settings.
    """
    client = fabric.FabricRestClient()

    response = client.get("/v1/admin/tenantsettings")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    df = pd.DataFrame(
        columns=[
            "Setting Name",
            "Title",
            "Enabled",
            "Can Specify Security Groups",
            "Tenant Setting Group",
            "Enabled Security Groups",
        ]
    )

    for i in response.json().get("tenantSettings", []):
        new_data = {
            "Setting Name": i.get("settingName"),
            "Title": i.get("title"),
            "Enabled": i.get("enabled"),
            "Can Specify Security Groups": i.get("canSpecifySecurityGroups"),
            "Tenant Setting Group": i.get("tenantSettingGroup"),
            "Enabled Security Groups": [i.get("enabledSecurityGroups", [])],
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    bool_cols = ["Enabled", "Can Specify Security Groups"]
    df[bool_cols] = df[bool_cols].astype(bool)

    return df


def list_external_data_shares() -> pd.DataFrame:
    """
    Lists external data shares in the tenant. This function is for admins.

    This is a wrapper function for the following API: `External Data Shares - List External Data Shares <https://learn.microsoft.com/rest/api/fabric/admin/external-data-shares/list-external-data-shares>`_.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of external data shares in the tenant.
    """
    df = pd.DataFrame(
        columns=[
            "External Data Share Id",
            "Paths",
            "Creater Principal Id",
            "Creater Principal Name",
            "Creater Principal Type",
            "Creater Principal UPN",
            "Recipient UPN",
            "Status",
            "Expiration Time UTC",
            "Workspace Id",
            "Item Id",
            "Invitation URL",
        ]
    )

    client = fabric.FabricRestClient()
    response = client.get("/v1/admin/items/externalDataShares")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    for i in response.json().get("value", []):
        cp = i.get("creatorPrincipal", {})
        new_data = {
            "External Data Share Id": i.get("id"),
            "Paths": [i.get("paths", [])],
            "Creater Principal Id": cp.get("id"),
            "Creater Principal Name": cp.get("displayName"),
            "Creater Principal Type": cp.get("type"),
            "Creater Principal UPN": cp.get("userDetails", {}).get("userPrincipalName"),
            "Recipient UPN": i.get("recipient", {}).get("userPrincipalName"),
            "Status": i.get("status"),
            "Expiration Time UTC": i.get("expirationTimeUtc"),
            "Workspace Id": i.get("workspaceId"),
            "Item Id": i.get("itemId"),
            "Invitation URL": i.get("invitationUrl"),
        }

        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    date_time_columns = ["Expiration Time UTC"]
    df[date_time_columns] = pd.to_datetime(df[date_time_columns])

    return df


def revoke_external_data_share(
    external_data_share_id: UUID, item_id: UUID, workspace: str | UUID
):
    """
    Revokes the specified external data share. Note: This action cannot be undone.

    This is a wrapper function for the following API: `External Data Shares - Revoke External Data Share <https://learn.microsoft.com/rest/api/fabric/admin/external-data-shares/revoke-external-data-share>`_.

    Parameters
    ----------
    external_data_share_id : UUID
        The external data share ID.
    item_id : int, default=None
        The Item ID
    workspace : str
        The Fabric workspace name or id.
    """
    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/admin/workspaces/{workspace_id}/items/{item_id}/externalDataShares/{external_data_share_id}/revoke"
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The '{external_data_share_id}' external data share for the '{item_id}' item within the '{workspace}' workspace has been revoked."
    )


def list_capacities_delegated_tenant_settings(
    return_dataframe: bool = True,
) -> pd.DataFrame | dict:
    """
    Returns list of tenant setting overrides that override at the capacities.

    This is a wrapper function for the following API: `Tenants - List Capacities Tenant Settings Overrides <https://learn.microsoft.com/rest/api/fabric/admin/tenants/list-capacities-tenant-settings-overrides>`_.

    Parameters
    ----------
    return_dataframe : bool, default=True
        If True, returns a dataframe. If False, returns a dictionary.

    Returns
    -------
    pandas.DataFrame | dict
        A pandas dataframe showing a list of tenant setting overrides that override at the capacities.
    """
    df = pd.DataFrame(
        columns=[
            "Capacity Id",
            "Setting Name",
            "Setting Title",
            "Setting Enabled",
            "Can Specify Security Groups",
            "Enabled Security Groups",
            "Tenant Setting Group",
            "Tenant Setting Properties",
            "Delegate to Workspace",
            "Delegated From",
        ]
    )

    client = fabric.FabricRestClient()
    response = client.get("/v1/admin/capacities/delegatedTenantSettingOverrides")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    if return_dataframe:
        for r in responses:
            for i in r.get("Overrides", []):
                tenant_settings = i.get("tenantSettings", [])
                for setting in tenant_settings:
                    new_data = {
                        "Capacity Id": i.get("id"),
                        "Setting Name": setting.get("settingName"),
                        "Setting Title": setting.get("title"),
                        "Setting Enabled": setting.get("enabled"),
                        "Can Specify Security Groups": setting.get(
                            "canSpecifySecurityGroups"
                        ),
                        "Enabled Security Groups": [
                            setting.get("enabledSecurityGroups", [])
                        ],
                        "Tenant Setting Group": setting.get("tenantSettingGroup"),
                        "Tenant Setting Properties": [setting.get("properties", [])],
                        "Delegate to Workspace": setting.get("delegateToWorkspace"),
                        "Delegated From": setting.get("delegatedFrom"),
                    }

                    df = pd.concat(
                        [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                    )

            bool_cols = [
                "Enabled Security Groups",
                "Can Specify Security Groups",
                "Delegate to Workspace",
            ]
            df[bool_cols] = df[bool_cols].astype(bool)

            return df
    else:
        combined_response = {
            "overrides": [],
            "continuationUri": "",
            "continuationToken": "",
        }
        for r in responses:
            combined_response["overrides"].extend(r["Overrides"])
            combined_response["continuationUri"] = r["continuationUri"]
            combined_response["continuationToken"] = r["continuationToken"]

        return combined_response


def list_modified_workspaces(
    modified_since: Optional[str] = None,
    exclude_inactive_workspaces: Optional[bool] = False,
    exclude_personal_workspaces: Optional[bool] = False,
) -> pd.DataFrame:
    """
    Gets a list of workspace IDs in the organization.

    This is a wrapper function for the following API: `Admin - WorkspaceInfo GetModifiedWorkspaces <https://learn.microsoft.com/rest/api/power-bi/admin/workspace-info-get-modified-workspaces>`_.

    Parameters
    ----------
    modified_since : str, default=None
        Last modified date (must be in ISO compliant UTC format). Example: "2024-11-02T05:51:30" or "2024-11-02T05:51:30.0000000Z".
    exclude_inactive_workspaces : bool, default=False
        Whether to exclude inactive workspaces.
    exclude_personal_workspaces : bool, default=False
        Whether to exclude personal workspaces.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of workspace IDs in the organization.
    """
    client = fabric.PowerBIRestClient()

    params = {}

    url = "/v1.0/myorg/admin/workspaces/modified"

    if modified_since is not None:
        modified_since_dt = dtparser(modified_since)
        params["modifiedSince"] = (
            f"{modified_since_dt.isoformat(timespec='microseconds')}0Z"
        )

    if exclude_inactive_workspaces is not None:
        params["excludeInActiveWorkspaces"] = exclude_inactive_workspaces

    if exclude_personal_workspaces is not None:
        params["excludePersonalWorkspaces"] = exclude_personal_workspaces

    url_parts = list(urllib.parse.urlparse(url))
    url_parts[4] = urllib.parse.urlencode(params)
    url = urllib.parse.urlunparse(url_parts)

    response = client.get(url)

    response = client.get(url)
    if response.status_code != 200:
        raise FabricHTTPException(response)

    df = pd.DataFrame(response.json()).rename(columns={"id": "Workspace Id"})

    return df


def scan_workspaces(
    data_source_details: bool = False,
    dataset_schema: bool = False,
    dataset_expressions: bool = False,
    lineage: bool = False,
    artifact_users: bool = False,
    workspace: Optional[str | List[str] | UUID | List[UUID]] = None,
) -> dict:
    """
    Get the inventory and details of the tenant.

    This is a wrapper function for the following APIs:
        `Admin - WorkspaceInfo PostWorkspaceInfo <https://learn.microsoft.com/en-gb/rest/api/power-bi/admin/workspace-info-post-workspace-info>`_.
        `Admin - WorkspaceInfo GetScanStatus <https://learn.microsoft.com/en-gb/rest/api/power-bi/admin/workspace-info-get-scan-status>`_.
        `Admin - WorkspaceInfo GetScanResult <https://learn.microsoft.com/en-gb/rest/api/power-bi/admin/workspace-info-get-scan-result>`_.

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
    workspace : str | List[str] | UUID | List[UUID], default=None
        The required workspace name(s) or id(s) to be scanned

    Returns
    -------
    dictionary
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


def list_datasets(
    top: Optional[int] = None,
    filter: Optional[str] = None,
    skip: Optional[int] = None,
) -> pd.DataFrame:
    """
    Shows a list of datasets for the organization.

    This is a wrapper function for the following API: `Admin - Datasets GetDatasetsAsAdmin <https://learn.microsoft.com/rest/api/power-bi/admin/datasets-get-datasets-as-admin>`_.

    Parameters
    ----------
    top : int, default=None
        Returns only the first n results.
    filter : str, default=None
        Returns a subset of a results based on Odata filter query parameter condition.
    skip : int, default=None
        Skips the first n results.
    token_provider : Optional[TokenProvider] = None,
        Authentication provider used to be use in the request. Supports Service Principal.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of datasets for the organization.
    """
    df = pd.DataFrame(
        columns=[
            "Dataset Id",
            "Dataset Name",
            "Web URL",
            "Add Rows API Enabled",
            "Configured By",
            "Is Refreshable",
            "Is Effective Identity Required",
            "Is Effective Identity Roles Required",
            "Target Storage Mode",
            "Created Date",
            "Content Provider Type",
            "Create Report Embed URL",
            "QnA Embed URL",
            "Upstream Datasets",
            "Users",
            "Is In Place Sharing Enabled",
            "Workspace Id",
            "Auto Sync Read Only Replicas",
            "Max Read Only Replicas",
        ]
    )

    client = fabric.PowerBIRestClient()

    params = {}
    url = "/v1.0/myorg/admin/datasets"

    if top is not None:
        params["$top"] = top

    if filter is not None:
        params["$filter"] = filter

    if skip is not None:
        params["$skip"] = skip

    url_parts = list(urllib.parse.urlparse(url))
    url_parts[4] = urllib.parse.urlencode(params)
    url = urllib.parse.urlunparse(url_parts)

    response = client.get(url)

    if response.status_code != 200:
        raise FabricHTTPException(response)

    for v in response.json().get("value", []):
        new_data = {
            "Dataset Id": v.get("id"),
            "Dataset Name": v.get("name"),
            "Web URL": v.get("webUrl"),
            "Add Rows API Enabled": v.get("addRowsAPIEnabled"),
            "Configured By": v.get("configuredBy"),
            "Is Refreshable": v.get("isRefreshable"),
            "Is Effective Identity Required": v.get("isEffectiveIdentityRequired"),
            "Is Effective Identity Roles Required": v.get(
                "isEffectiveIdentityRolesRequired"
            ),
            "Target Storage Mode": v.get("targetStorageMode"),
            "Created Date": pd.to_datetime(v.get("createdDate")),
            "Content Provider Type": v.get("contentProviderType"),
            "Create Report Embed URL": v.get("createReportEmbedURL"),
            "QnA Embed URL": v.get("qnaEmbedURL"),
            "Upstream Datasets": v.get("upstreamDatasets", []),
            "Users": v.get("users", []),
            "Is In Place Sharing Enabled": v.get("isInPlaceSharingEnabled"),
            "Workspace Id": v.get("workspaceId"),
            "Auto Sync Read Only Replicas": v.get("queryScaleOutSettings", {}).get(
                "autoSyncReadOnlyReplicas"
            ),
            "Max Read Only Replicas": v.get("queryScaleOutSettings", {}).get(
                "maxReadOnlyReplicas"
            ),
        }
        df = pd.concat([df, pd.DataFrame([new_data])], ignore_index=True)

    bool_cols = [
        "Add Rows API Enabled",
        "Is Refreshable",
        "Is Effective Identity Required",
        "Is Effective Identity Roles Required",
        "Is In Place Sharing Enabled",
        "Auto Sync Read Only Replicas",
    ]
    df[bool_cols] = df[bool_cols].astype(bool)

    df["Created Date"] = pd.to_datetime(df["Created Date"])
    df["Max Read Only Replicas"] = df["Max Read Only Replicas"].astype(int)

    return df


def list_items(
    capacity: Optional[str | UUID] = None,
    workspace: Optional[str] = None,
    state: Optional[str] = None,
    type: Optional[str] = None,
    item: Optional[str | UUID] = None,
    **kwargs,
) -> pd.DataFrame:
    """
    Shows a list of active Fabric and Power BI items.

    This is a wrapper function for the following API: `Items - List Items <https://learn.microsoft.com/rest/api/fabric/admin/items/list-items>`_.

    Parameters
    ----------
    capacity : str | UUID, default=None
        The capacity name or id.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    state : str, default=None
        The item state.
    type : str, default=None
        The item type.
    item : str | UUID, default=None
        Item id or name to filter the list.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of active Fabric and Power BI items.
    """
    if "capacity_name" in kwargs:
        print(
            "The 'capacity_name' parameter has been deprecated. Please replace this parameter with 'capacity' from the function going forward."
        )
        capacity = kwargs["capacity_name"]
        del kwargs["capacity_name"]

    df = pd.DataFrame(
        columns=[
            "Item Id",
            "Item Name",
            "Type",
            "Description",
            "State",
            "Last Updated Date",
            "Creator Principal Id",
            "Creator Principal Display Name",
            "Creator Principal Type",
            "Creator User Principal Name",
            "Workspace Id",
            "Capacity Id",
        ]
    )

    client = fabric.FabricRestClient()

    params = {}

    url = "/v1/admin/items"

    if capacity is not None:
        params["capacityId"] = _resolve_capacity_name_and_id(capacity)[1]

    if workspace is not None:
        params["workspaceId"] = _resolve_workspace_name_and_id(workspace)[1]

    if state is not None:
        params["state"] = state

    if type is not None:
        params["type"] = type

    url_parts = list(urllib.parse.urlparse(url))
    url_parts[4] = urllib.parse.urlencode(params)
    url = urllib.parse.urlunparse(url_parts)

    response = client.get(url)

    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    for r in responses:
        for v in r.get("itemEntities", []):
            new_data = {
                "Item Id": v.get("id"),
                "Type": v.get("type"),
                "Item Name": v.get("name"),
                "Description": v.get("description"),
                "State": v.get("state"),
                "Last Updated Date": v.get("lastUpdatedDate"),
                "Creator Principal Id": v.get("creatorPrincipal", {}).get("id"),
                "Creator Principal Display Name": v.get("creatorPrincipal", {}).get(
                    "displayName"
                ),
                "Creator Principal Type": v.get("creatorPrincipal", {}).get("type"),
                "Creator User Principal Name": v.get("creatorPrincipal", {})
                .get("userDetails", {})
                .get("userPrincipalName"),
                "Workspace Id": v.get("workspaceId"),
                "Capacity Id": v.get("capacityId"),
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    if item is not None:
        if _is_valid_uuid(item):
            df = df[df["Item Id"] == item]
        else:
            df = df[df["Item Name"] == item]

    return df


def list_item_access_details(
    item: str | UUID = None,
    type: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
    **kwargs,
) -> pd.DataFrame:
    """
    Returns a list of users (including groups and service principals) and lists their workspace roles.

    This is a wrapper function for the following API: `Items - List Item Access Details <https://learn.microsoft.com/rest/api/fabric/admin/items/list-item-access-details>`_.

    Parameters
    ----------
    item : str
        Name or id of the Fabric item.
    type : str, default=None
        Type of Fabric item, default=None.
    workspace : str, default=None
        The Fabric workspace name or id.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of users (including groups and service principals) and lists their workspace roles.
    """
    if "item_name" in kwargs:
        print(
            "The 'item_name' parameter has been deprecated. Please replace this parameter with 'item' from the function going forward."
        )
        item = kwargs["item_name"]
        del kwargs["item_name"]

    if item is None:
        raise ValueError(f"{icons.red_dot} The parameter 'item' is mandatory.")

    client = fabric.FabricRestClient()

    workspace_name, workspace_id = _resolve_workspace_name_and_id(workspace)
    item_name, item_id = _resolve_item_name_and_id(
        item=item, type=type, workspace=workspace_name
    )

    df = pd.DataFrame(
        columns=[
            "User Id",
            "User Name",
            "User Type",
            "User Principal Name",
            "Item Name",
            "Item Type",
            "Item Id",
            "Permissions",
            "Additional Permissions",
        ]
    )

    response = client.get(f"/v1/admin/workspaces/{workspace_id}/items/{item_id}/users")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    for v in response.json().get("accessDetails", []):
        new_data = {
            "User Id": v.get("principal", {}).get("id"),
            "User Name": v.get("principal", {}).get("displayName"),
            "User Type": v.get("principal", {}).get("type"),
            "User Principal Name": v.get("principal", {})
            .get("userDetails", {})
            .get("userPrincipalName"),
            "Item Type": v.get("itemAccessDetails", {}).get("type"),
            "Permissions": v.get("itemAccessDetails", {}).get("permissions"),
            "Additional Permissions": v.get("itemAccessDetails", {}).get(
                "additionalPermissions"
            ),
            "Item Name": item_name,
            "Item Id": item_id,
        }
        df = pd.concat([df, pd.DataFrame([new_data])], ignore_index=True)

    return df


def list_access_entities(
    user_email_address: str,
) -> pd.DataFrame:
    """
    Shows a list of permission details for Fabric and Power BI items the specified user can access.

    This is a wrapper function for the following API: `Users - List Access Entities <https://learn.microsoft.com/rest/api/fabric/admin/users/list-access-entities>`_.

    Parameters
    ----------
    user_email_address : str
        The user's email address.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of permission details for Fabric and Power BI items the specified user can access.
    """
    df = pd.DataFrame(
        columns=[
            "Item Id",
            "Item Name",
            "Item Type",
            "Permissions",
            "Additional Permissions",
        ]
    )
    client = fabric.FabricRestClient()

    response = client.get(f"/v1/admin/users/{user_email_address}/access")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    for r in responses:
        for v in r.get("accessEntities", []):
            new_data = {
                "Item Id": v.get("id"),
                "Item Name": v.get("displayName"),
                "Item Type": v.get("itemAccessDetails", {}).get("type"),
                "Permissions": v.get("itemAccessDetails", {}).get("permissions"),
                "Additional Permissions": v.get("itemAccessDetails", {}).get(
                    "additionalPermissions"
                ),
            }
            df = pd.concat([df, pd.DataFrame([new_data])], ignore_index=True)

    return df


def list_workspace_access_details(
    workspace: Optional[Union[str, UUID]] = None
) -> pd.DataFrame:
    """
    Shows a list of users (including groups and Service Principals) that have access to the specified workspace.

    This is a wrapper function for the following API: `Workspaces - List Workspace Access Details <https://learn.microsoft.com/rest/api/fabric/admin/workspaces/list-workspace-access-details>`_.

    Parameters
    ----------
    workspace : str | UUID, default=None
        The Fabric workspace name or id.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of users (including groups and Service Principals) that have access to the specified workspace.
    """
    workspace_name, workspace_id = _resolve_workspace_name_and_id(workspace)

    df = pd.DataFrame(
        columns=[
            "User Id",
            "User Name",
            "User Type",
            "Workspace Name",
            "Workspace Id",
            "Workspace Role",
        ]
    )

    client = fabric.FabricRestClient()

    response = client.get(f"/v1/admin/workspaces/{workspace_id}/users")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    for v in response.json().get("accessDetails", []):
        new_data = {
            "User Id": v.get("principal", {}).get("id"),
            "User Name": v.get("principal", {}).get("displayName"),
            "User Type": v.get("principal", {}).get("type"),
            "Workspace Name": workspace_name,
            "Workspace Id": workspace_id,
            "Workspace Role": v.get("workspaceAccessDetails", {}).get("workspaceRole"),
        }
        df = pd.concat([df, pd.DataFrame([new_data])], ignore_index=True)

    return df


def list_activity_events(
    start_time: str,
    end_time: str,
    activity_filter: Optional[str] = None,
    user_id_filter: Optional[str] = None,
    return_dataframe: Optional[bool] = True,
) -> pd.DataFrame | dict:
    """
    Shows a list of audit activity events for a tenant.

    This is a wrapper function for the following API: `Admin - Get Activity Events <https://learn.microsoft.com/rest/api/power-bi/admin/get-activity-events>`_.

    Parameters
    ----------
    start_time : str
        Start date and time of the window for audit event results. Example: "2024-09-25T07:55:00".
    end_time : str
        End date and time of the window for audit event results. Example: "2024-09-25T08:55:00".
    activity_filter : str, default=None
        Filter value for activities. Example: 'viewreport'.
    user_id_filter : str, default=None
        Email address of the user.
    return_dataframe : bool, default=True
        If True the response is a pandas.DataFrame. If False returns the original Json. Default True

    Returns
    -------
    pandas.DataFrame | dict
        A pandas dataframe or json showing a list of audit activity events for a tenant.
    """
    start_dt = dtparser(start_time)
    end_dt = dtparser(end_time)

    if not start_dt.date() == end_dt.date():
        raise ValueError(
            f"{icons.red_dot} Start and End Times must be within the same UTC day. Please refer to the documentation here: https://learn.microsoft.com/rest/api/power-bi/admin/get-activity-events#get-audit-activity-events-within-a-time-window-and-for-a-specific-activity-type-and-user-id-example"
        )

    df = pd.DataFrame(
        columns=[
            "Id",
            "Record Type",
            "Creation Time",
            "Operation",
            "Organization Id",
            "User Type",
            "User Key",
            "Workload",
            "Result Status",
            "User Id",
            "Client IP",
            "User Agent",
            "Activity",
            "Workspace Name",
            "Workspace Id",
            "Object Id",
            "Request Id",
            "Object Type",
            "Object Display Name",
            "Experience",
            "Refresh Enforcement Policy",
        ]
    )

    resposeJson = {"activityEventEntities": []}

    tic = "%27"
    space = "%20"
    client = fabric.PowerBIRestClient()

    params = {}
    url = "/v1.0/myorg/admin/activityevents"

    if start_dt is not None:
        params["startDateTime"] = f"'{start_dt.isoformat(timespec='milliseconds')}'"

    if end_dt is not None:
        params["endDateTime"] = f"'{end_dt.isoformat(timespec='milliseconds')}'"

    conditions = []

    if activity_filter is not None:
        conditions.append(f"Activity{space}eq{space}{tic}{activity_filter}{tic}")

    if user_id_filter is not None:
        conditions.append(f"UserId{space}eq{space}{tic}{user_id_filter}{tic}")

    if conditions:
        params["filder"] = f"{f'{space}and{space}'.join(conditions)}"

    url_parts = list(urllib.parse.urlparse(url))
    url_parts[4] = urllib.parse.urlencode(params)
    url = urllib.parse.urlunparse(url_parts)

    response = client.get(url)

    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    for r in responses:
        if return_dataframe:
            for i in r.get("activityEventEntities", []):
                new_data = {
                    "Id": i.get("id"),
                    "Record Type": i.get("RecordType"),
                    "Creation Time": i.get("CreationTime"),
                    "Operation": i.get("Operation"),
                    "Organization Id": i.get("OrganizationId"),
                    "User Type": i.get("UserType"),
                    "User Key": i.get("UserKey"),
                    "Workload": i.get("Workload"),
                    "Result Status": i.get("ResultStatus"),
                    "User Id": i.get("UserId"),
                    "Client IP": i.get("ClientIP"),
                    "User Agent": i.get("UserAgent"),
                    "Activity": i.get("Activity"),
                    "Workspace Name": i.get("WorkSpaceName"),
                    "Workspace Id": i.get("WorkspaceId"),
                    "Object Id": i.get("ObjectId"),
                    "Request Id": i.get("RequestId"),
                    "Object Type": i.get("ObjectType"),
                    "Object Display Name": i.get("ObjectDisplayName"),
                    "Experience": i.get("Experience"),
                    "Refresh Enforcement Policy": i.get("RefreshEnforcementPolicy"),
                }
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])],
                    ignore_index=True,
                )
        else:
            resposeJson["activityEventEntities"].extend(r.get("activityEventEntities"))

    if return_dataframe:
        df["Creation Time"] = pd.to_datetime(df["Creation Time"])
        activity_events = df
    else:
        activity_events = resposeJson

    return activity_events


def _resolve_capacity_name_and_id(
    capacity: str | UUID,
) -> Tuple[str, UUID]:

    dfC = list_capacities(capacity=capacity)
    try:
        capacity_name = dfC["Capacity Name"].iloc[0]
        capacity_id = dfC["Capacity Id"].iloc[0]
    except:
        raise ValueError(f"Capacity {capacity} not found.")

    return capacity_name, capacity_id


def _list_capacities_meta() -> pd.DataFrame:
    """
    Shows the a list of capacities and their properties. This function is the admin version.

    This is a wrapper function for the following API: `Admin - Get Capacities As Admin <https://learn.microsoft.com/rest/api/power-bi/admin/get-capacities-as-admin>`_.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the capacities and their properties
    """

    client = fabric.FabricRestClient()

    df = pd.DataFrame(
        columns=["Capacity Id", "Capacity Name", "Sku", "Region", "State", "Admins"]
    )

    response = client.get("/v1.0/myorg/admin/capacities")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    for r in responses:
        for i in r.get("value", []):
            new_data = {
                "Capacity Id": i.get("id").lower(),
                "Capacity Name": i.get("displayName"),
                "Sku": i.get("sku"),
                "Region": i.get("region"),
                "State": i.get("state"),
                "Admins": [i.get("admins", [])],
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def _resolve_workspace_name_and_id(
    workspace: str | UUID,
) -> Tuple[str, UUID]:

    dfW = list_workspaces(workspace=workspace)
    try:
        workspace_name = dfW["Name"].iloc[0]
        workspace_id = dfW["Id"].iloc[0]
    except:
        raise ValueError(f"Workspace {workspace} not found.")

    return workspace_name, workspace_id


def _resolve_item_id(
    item_name: str,
    type: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
) -> UUID:

    dfI = list_items(workspace=workspace, type=type)
    dfI_filt = dfI[dfI["Item Name"] == item_name]

    if len(dfI_filt) == 0:
        raise ValueError(
            f"The '{item_name}' {type} does not exist within the '{workspace}' workspace or is not of type '{type}'."
        )

    return dfI_filt["Item Id"].iloc[0]


def _resolve_item_name_and_id(
    item: str,
    type: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
    **kwargs,
) -> Tuple[str, UUID]:
    if "item_name" in kwargs:
        print(
            "The 'item_name' parameter has been deprecated. Please replace this parameter with 'item' from the function going forward."
        )
        item = item_name
        del kwargs["item_name"]

    dfI = list_items(workspace=workspace, type=type, item=item)

    if len(dfI) > 1:
        raise ValueError(
            f"There are more than 1 item with the name '{item}'. Please specify the 'type' and/or 'workspace' to be more precise."
        )

    try:
        item_name = dfI["Item Name"].iloc[0]
        item_id = dfI["Item Id"].iloc[0]
    except:
        raise ValueError(
            f"The '{item}' {type} does not exist within the '{workspace}' workspace or is not of type '{type}'."
        )

    return item_name, item_id
