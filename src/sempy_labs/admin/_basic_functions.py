import sempy.fabric as fabric
from typing import Optional, List, Union
from uuid import UUID
import sempy_labs._icons as icons
from sempy.fabric.exceptions import FabricHTTPException
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    pagination,
)
import numpy as np
import pandas as pd
import time


def list_workspaces(
    top: Optional[int] = 5000, skip: Optional[int] = None
) -> pd.DataFrame:
    """
    Lists workspaces for the organization. This function is the admin version of list_workspaces.

    Parameters
    ----------
    top : int, default=5000
        Returns only the first n results. This parameter is mandatory and must be in the range of 1-5000.
    skip : int, default=None
        Skips the first n results. Use with top to fetch results beyond the first 5000.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of workspaces for the organization.
    """

    df = pd.DataFrame(
        columns=[
            "Id",
            "Is Read Only",
            "Is On Dedicated Capacity",
            "Type",
            "Name",
            "Capacity Id",
            "Default Dataset Storage Format",
            "Pipeline Id",
            "Has Workspace Level Settings",
        ]
    )

    url = f"/v1.0/myorg/admin/groups?$top={top}"
    if skip is not None:
        url = f"{url}&$skip={skip}"

    client = fabric.PowerBIRestClient()
    response = client.get(url)

    if response.status_code != 200:
        raise FabricHTTPException(response)

    for v in response.json().get("value", []):
        capacity_id = v.get("capacityId")
        if capacity_id:
            capacity_id = capacity_id.lower()
        new_data = {
            "Id": v.get("id"),
            "Is Read Only": v.get("isReadOnly"),
            "Is On Dedicated Capacity": v.get("isOnDedicatedCapacity"),
            "Capacity Id": capacity_id,
            "Default Dataset Storage Format": v.get("defaultDatasetStorageFormat"),
            "Type": v.get("type"),
            "Name": v.get("name"),
            "State": v.get("state"),
            "Pipeline Id": v.get("pipelineId"),
            "Has Workspace Level Settings": v.get("hasWorkspaceLevelSettings"),
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    bool_cols = [
        "Is Read Only",
        "Is On Dedicated Capacity",
        "Has Workspace Level Settings",
    ]
    df[bool_cols] = df[bool_cols].astype(bool)

    return df


def assign_workspaces_to_capacity(
    source_capacity: str,
    target_capacity: str,
    workspace: Optional[str | List[str]] = None,
):
    """
    Assigns a workspace to a capacity. This function is the admin version.

    Parameters
    ----------
    source_capacity : str
        The name of the source capacity.
    target_capacity : str
        The name of the target capacity.
    workspace : str | List[str], default=None
        The name of the workspace(s).
        Defaults to None which resolves to migrating all workspaces within the source capacity to the target capacity.
    """

    if isinstance(workspace, str):
        workspace = [workspace]

    dfC = list_capacities()
    dfC_filt = dfC[dfC["Capacity Name"] == source_capacity]
    source_capacity_id = dfC_filt["Capacity Id"].iloc[0]

    dfC_filt = dfC[dfC["Capacity Name"] == target_capacity]
    target_capacity_id = dfC_filt["Capacity Id"].iloc[0]

    if workspace is None:
        # workspaces = fabric.list_workspaces(
        #    filter=f"capacityId eq '{source_capacity_id.upper()}'"
        # )["Id"].values
        dfW = list_workspaces()
        dfW = dfW[dfW["Capacity Id"].str.upper() == source_capacity_id.upper()]
        workspaces = dfW["Id"].tolist()
    else:
        dfW = list_workspaces()
        workspaces = dfW[dfW["Name"].isin(workspace)]["Id"].tolist()

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

        client = fabric.PowerBIRestClient()
        response = client.post(
            "/v1.0/myorg/admin/capacities/AssignWorkspaces",
            json=request_body,
        )

        if response.status_code != 200:
            raise FabricHTTPException(response)
    print(
        f"{icons.green_dot} The workspaces have been assigned to the '{target_capacity}' capacity."
    )


def list_capacities() -> pd.DataFrame:
    """
    Shows the a list of capacities and their properties. This function is the admin version.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the capacities and their properties
    """

    df = pd.DataFrame(
        columns=["Capacity Id", "Capacity Name", "Sku", "Region", "State", "Admins"]
    )

    client = fabric.PowerBIRestClient()
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


def list_tenant_settings() -> pd.DataFrame:
    """
    Lists all tenant settings.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the tenant settings.
    """

    # https://learn.microsoft.com/en-us/rest/api/fabric/admin/tenants/list-tenant-settings?tabs=HTTP

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


def _list_capacities_meta() -> pd.DataFrame:

    df = pd.DataFrame(
        columns=["Capacity Id", "Capacity Name", "Sku", "Region", "State", "Admins"]
    )

    client = fabric.PowerBIRestClient()
    try:
        response = client.get("/v1.0/myorg/admin/capacities")
    except Exception as e:
        if e.status_code not in [200, 401]:
            raise FabricHTTPException(response)
        elif e.status_code == 401:
            response = client.get("/v1.0/myorg/capacities")

    for i in response.json().get("value", []):
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


def unassign_workspaces_from_capacity(workspaces: str | List[str]):
    """
    Unassigns workspace(s) from their capacity. This function is the admin version of list_workspaces.

    Parameters
    ----------
    workspaces : str | List[str]
        The Fabric workspace name(s).
    """

    # https://learn.microsoft.com/en-us/rest/api/power-bi/admin/capacities-unassign-workspaces-from-capacity

    if isinstance(workspaces, str):
        workspaces = [workspaces]

    payload = {"workspacesToUnassign": workspaces}

    client = fabric.PowerBIRestClient()
    response = client.post(
        "/v1.0/myorg/admin/capacities/UnassignWorkspaces",
        json=payload,
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(f"{icons.green_dot} The workspaces have been unassigned.")


def list_external_data_shares():
    """
    Lists external data shares in the tenant. This function is for admins.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of external data shares in the tenant.
    """

    # https://learn.microsoft.com/en-us/rest/api/fabric/admin/external-data-shares/list-external-data-shares?tabs=HTTP

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
    external_data_share_id: UUID, item_id: UUID, workspace: str
):
    """
    Revokes the specified external data share. Note: This action cannot be undone.

    Parameters
    ----------
    external_data_share_id : UUID
        The external data share ID.
    item_id : int, default=None
        The Item ID
    workspace : str
        The Fabric workspace name.
    """

    # https://learn.microsoft.com/en-us/rest/api/fabric/admin/external-data-shares/revoke-external-data-share?tabs=HTTP

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
) -> Optional[pd.DataFrame | dict]:
    """
    Returns list of tenant setting overrides that override at the capacities.

    Parameters
    ----------
    return_dataframe : bool, default=True
        If True, returns a dataframe. If False, returns a dictionary

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of tenant setting overrides that override at the capacities.
    """

    # https://learn.microsoft.com/en-us/rest/api/fabric/admin/tenants/list-capacities-tenant-settings-overrides?tabs=HTTP

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
            combined_response["overrides"].extend(r["overrides"])
            combined_response["continuationUri"] = r["continuationUri"]
            combined_response["continuationToken"] = r["continuationToken"]

        return combined_response


def scan_workspaces(
    data_source_details: bool = False,
    dataset_schema: bool = False,
    dataset_expressions: bool = False,
    lineage: bool = False,
    artifact_users: bool = False,
    workspace: Optional[str | List[str]] = None,
) -> dict:

    if workspace is None:
        workspace = fabric.resolve_workspace_name()

    if isinstance(workspace, str):
        workspace = [workspace]

    workspace_list = []

    for w in workspace:
        workspace_list.append(fabric.resolve_workspace_id(w))

    client = fabric.PowerBIRestClient()
    request_body = {"workspaces": workspace_list}

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

    return response.json()


def list_datasets() -> pd.DataFrame:
    """
    Shows a list of datasets for the organization.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of datasets for the organization.
    """

    # https://learn.microsoft.com/en-us/rest/api/power-bi/admin/datasets-get-datasets-as-admin

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
    response = client.get("/v1.0/myorg/admin/datasets")

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


def list_item_access_details(
    item_name: str, type: str, workspace: Optional[str] = None
) -> pd.DataFrame:
    """
    Returns a list of users (including groups and service principals) and lists their workspace roles.

    Parameters
    ----------
    item_name : str
        Name of the Fabric item.
    type : str
        Type of Fabric item.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of users (including groups and service principals) and lists their workspace roles.
    """

    # https://learn.microsoft.com/en-us/rest/api/fabric/admin/items/list-item-access-details?tabs=HTTP

    workspace = fabric.resolve_workspace_name(workspace)
    workspace_id = fabric.resolve_workspace_id(workspace)
    item_id = fabric.resolve_item_id(
        item_name=item_name, type=type, workspace=workspace
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
    client = fabric.FabricRestClient()
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
    Shows a list of permission details for Fabric and PowerBI items the specified user can access.

    Parameters
    ----------
    user_email_address : str
        The user's email address.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of permission details for Fabric and PowerBI items the specified user can access.
    """

    # https://learn.microsoft.com/en-us/rest/api/fabric/admin/users/list-access-entities?tabs=HTTP

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

    Parameters
    ----------
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of users (including groups and Service Principals) that have access to the specified workspace.
    """

    # https://learn.microsoft.com/en-us/rest/api/fabric/admin/workspaces/list-workspace-access-details?tabs=HTTP

    workspace_name = fabric.resolve_workspace_name(workspace)
    workspace_id = fabric.resolve_workspace_id(workspace_name)

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


def list_items(
    capacity_name: Optional[str] = None,
    workspace: Optional[str] = None,
    state: Optional[str] = None,
    type: Optional[str] = None,
) -> pd.DataFrame:
    """
    Shows a list of active Fabric and PowerBI items.

    Parameters
    ----------
    capacity_name : str, default=None
        The capacity name.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    state : str, default=None
        The item state.
    type : str, default=None
        The item type.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of active Fabric and Power BI items.
    """

    # https://learn.microsoft.com/en-us/rest/api/fabric/admin/items/list-items?tabs=HTTP

    url = "/v1/admin/items?"

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

    if workspace is not None:
        workspace = fabric.resolve_workspace_name(workspace)
        workspace_id = fabric.resolve_workspace_id(workspace)
        url += f"workspaceId={workspace_id}&"
    if capacity_name is not None:
        dfC = list_capacities()
        dfC_filt = dfC[dfC["Capacity Name"] == capacity_name]
        if len(dfC_filt) == 0:
            raise ValueError(
                f"{icons.red_dot} The '{capacity_name}' capacity does not exist."
            )
        capacity_id = dfC_filt["Capacity Id"].iloc[0]
        url += f"capacityId={capacity_id}&"
    if state is not None:
        url += f"state={state}&"
    if type is not None:
        url += f"type={type}&"

    if url.endswith("?") or url.endswith("&"):
        url = url[:-1]

    client = fabric.FabricRestClient()
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

    return df


def list_activity_events(
    start_time: str,
    end_time: str,
    activity_filter: Optional[str] = None,
    user_id_filter: Optional[str] = None,
):
    """
    Shows a list of audit activity events for a tenant.

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

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of audit activity events for a tenant.
    """

    # https://learn.microsoft.com/en-us/rest/api/power-bi/admin/get-activity-events

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

    tic = "%27"
    space = "%20"
    client = fabric.PowerBIRestClient()
    base_url = "/v1.0/myorg/admin/activityevents"
    conditions = []

    if activity_filter is not None:
        conditions.append(f"Activity{space}eq{space}{tic}{activity_filter}{tic}")
    if user_id_filter is not None:
        conditions.append(f"UserId{space}eq{space}{tic}{user_id_filter}{tic}")

    filter_value = (
        f"&filter={f'{space}and{space}'.join(conditions)}" if conditions else ""
    )

    full_url = f"{base_url}?startDateTime={tic}{start_time}{tic}&endDateTime={tic}{end_time}{tic}{filter_value}"
    response = client.get(full_url)
    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    for r in responses:
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

    return df
