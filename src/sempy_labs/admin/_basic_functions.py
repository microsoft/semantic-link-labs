import sempy.fabric as fabric
from typing import Optional, List
from uuid import UUID
import sempy_labs._icons as icons
from sempy.fabric.exceptions import FabricHTTPException
from sempy_labs._helper_functions import resolve_workspace_name_and_id
import datetime
import numpy as np
import pandas as pd


def assign_workspaces_to_capacity(
    source_capacity: str,
    target_capacity: str,
    workspace: Optional[str | List[str]] = None,
):
    """
    Assigns a workspace to a capacity.

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

    dfC = fabric.list_capacities()
    dfC_filt = dfC[dfC["Display Name"] == source_capacity]
    source_capacity_id = dfC_filt["Id"].iloc[0]

    dfC_filt = dfC[dfC["Display Name"] == target_capacity]
    target_capacity_id = dfC_filt["Id"].iloc[0]

    if workspace is None:
        workspaces = fabric.list_workspaces(
            filter=f"capacityId eq '{source_capacity_id.upper()}'"
        )["Id"].values
    else:
        dfW = fabric.list_workspaces()
        workspaces = dfW[dfW["Name"].isin(workspace)]["Id"].values

    workspaces = np.array(workspaces)
    batch_size = 999
    for i in range(0, len(workspaces), batch_size):
        batch = workspaces[i : i + batch_size].tolist()
        batch_length = len(batch)
        start_time = datetime.datetime.now()
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
        end_time = datetime.datetime.now()
        print(
            f"Total time for assigning {str(batch_length)} workspaces is {str((end_time - start_time).total_seconds())}"
        )
    print(
        f"{icons.green_dot} The workspaces have been assigned to the '{target_capacity}' capacity."
    )


def list_capacities() -> pd.DataFrame:
    """
    Shows the a list of capacities and their properties.

    Parameters
    ----------

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


def list_capacities_delegated_tenant_settings(return_dataframe: Optional[bool] = True):

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
            "Tenant Setting Property",
            "Tenant Settging Property Value",
            "Tenant Setting Property Type",
            "Delegate to Workspace",
            "Delegated From",
        ]
    )

    client = fabric.FabricRestClient()
    response = client.get("/v1/admin/capacities/delegatedTenantSettingOverrides")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    response_json = response.json()

    if return_dataframe:
        for i in response_json.get("overrides", []):
            tenant_settings = i.get("tenantSettings", [])
            prop = tenant_settings.get("properties", [])
            new_data = {
                "Capacity Id": i.get("id"),
                "Setting Name": tenant_settings.get("settingName"),
                "Setting Title": tenant_settings.get("title"),
                "Setting Enabled": tenant_settings.get("enabled"),
                "Can Specify Security Groups": tenant_settings.get(
                    "canSpecifySecurityGroups"
                ),
                "Enabled Security Groups": [
                    tenant_settings.get("enabledSecurityGroups", [])
                ],
                "Tenant Setting Group": tenant_settings.get("tenantSettingGroup"),
                "Tenant Setting Property": prop.get("name"),
                "Tenant Setting Property Value": prop.get("value"),
                "Tenant Setting Property Type": prop.get("type"),
                "Delegate to Workspace": tenant_settings.get("delegateToWorkspace"),
                "Delegated From": tenant_settings.get("delegatedFrom"),
            }

            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

        bool_cols = [
            "Enabled Security Groups",
            "Can Specify Security Groups",
            "Delegate to Workspace",
        ]
        df[bool_cols] = df[bool_cols].astype(bool)

        return df
    else:
        return response_json
