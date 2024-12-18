import sempy.fabric as fabric
from typing import Optional, List, Union, Tuple
from uuid import UUID
import sempy_labs._icons as icons
from sempy.fabric.exceptions import FabricHTTPException
from sempy_labs._helper_functions import (
    pagination,
    _is_valid_uuid,
    _build_url,
)
import numpy as np
import pandas as pd
from dateutil.parser import parse as dtparser


def list_workspaces(
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
    capacity : str | uuid.UUID, default=None
        Returns only the workspaces in the specified Capacity.
    workspace : str | uuid.UUID, default=None
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

    if "top" in kwargs:
        print(
            "The 'top' parameter has been deprecated. Please remove this parameter from the function going forward."
        )
        del kwargs["top"]

    if "skip" in kwargs:
        print(
            "The 'skip' parameter has been deprecated. Please remove this parameter from the function going forward."
        )
        del kwargs["skip"]

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

    url = "/v1/admin/workspaces"
    params = {}

    if capacity is not None:
        params["capacityId"] = _resolve_capacity_name_and_id(capacity)[1]

    if workspace is not None and not _is_valid_uuid(workspace):
        params["name"] = workspace

    if workspace_state is not None:
        params["state"] = workspace_state

    if workspace_type is not None:
        params["type"] = workspace_type

    url = _build_url(url, params)

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

        df["Capacity Id"] = df["Capacity Id"].str.lower()

        if workspace is not None and _is_valid_uuid(workspace):
            df = df[df["Id"] == workspace.lower()]

    return df


def list_capacities(
    capacity: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Shows the a list of capacities and their properties.

    This is a wrapper function for the following API: `Admin - Get Capacities As Admin <https://learn.microsoft.com/rest/api/power-bi/admin/get-capacities-as-admin>`_.

    Parameters
    ----------
    capacity : str | uuid.UUID, default=None
        Capacity name or id to filter.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the capacities and their properties.
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
    source_capacity : str | uuid.UUID, default=None
        The name of the source capacity. If the Workspace is not specified, this is parameter mandatory.
    target_capacity : str | uuid.UUID, default=None
        The name of the target capacity.
    workspace : str | List[str] | uuid.UUID | List[uuid.UUID], default=None
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
        if isinstance(workspace, str) or isinstance(workspace, UUID):
            workspace = [workspace]
        if source_capacity is None:
            dfW = list_workspaces()
        else:
            source_capacity_id = _resolve_capacity_name_and_id(source_capacity)[1]
            dfW = list_workspaces(capacity=source_capacity_id)

        # Extract names and IDs that are mapped in dfW
        workspaces_names = dfW[dfW["Name"].isin(workspace)]["Name"].tolist()
        workspaces_ids = dfW[dfW["Id"].isin(workspace)]["Id"].tolist()

        # Combine IDs into the final workspaces list
        workspaces = workspaces_ids + dfW[dfW["Name"].isin(workspace)]["Id"].tolist()

        # Identify unmapped workspaces
        unmapped_workspaces = [
            item
            for item in workspace
            if item not in workspaces_names and item not in workspaces_ids
        ]

        if len(workspace) != len(workspaces):
            raise ValueError(
                f"{icons.red_dot} The following workspaces are invalid or not found in source capacity: {unmapped_workspaces}."
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
    workspaces : str | List[str] | uuid.UUID | List[uuid.UUID]
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

    url = _build_url(url, params)

    response = client.get(url)

    if response.status_code != 200:
        raise FabricHTTPException(response)

    df = pd.DataFrame(response.json()).rename(columns={"id": "Workspace Id"})

    return df


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

    url = _build_url(url, params)

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
    workspace : str | uuid.UUID, default=None
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
    return_dataframe: bool = True,
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
            "Is Success",
            "Activity Id",
            "Item Name",
            "Dataset Name",
            "Report Name",
            "Capacity Id",
            "Capacity Name",
            "App Name",
            "Dataset Id",
            "Report Id",
            "Artifact Id",
            "Artifact Name",
            "Report Type",
            "App Report Id",
            "Distribution Method",
            "Consumption Method",
            "Artifact Kind",
        ]
    )

    response_json = {"activityEventEntities": []}
    client = fabric.PowerBIRestClient()
    url = f"/v1.0/myorg/admin/activityevents?startDateTime='{start_time}'&endDateTime='{end_time}'"

    conditions = []
    if activity_filter is not None:
        conditions.append(f"Activity eq '{activity_filter}'")
    if user_id_filter is not None:
        conditions.append(f"UserId eq '{user_id_filter}'")

    if conditions:
        url += f"&$filter={f' and '.join(conditions)}"

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
                    "Is Success": i.get("IsSuccess"),
                    "Activity Id": i.get("ActivityId"),
                    "Item Name": i.get("ItemName"),
                    "Dataset Name": i.get("DatasetName"),
                    "Report Name": i.get("ReportName"),
                    "Capacity Id": i.get("CapacityId"),
                    "Capacity Name": i.get("CapacityName"),
                    "App Name": i.get("AppName"),
                    "Dataset Id": i.get("DatasetId"),
                    "Report Id": i.get("ReportId"),
                    "Artifact Id": i.get("ArtifactId"),
                    "Artifact Name": i.get("ArtifactName"),
                    "Report Type": i.get("ReportType"),
                    "App Report Id": i.get("AppReportId"),
                    "Distribution Method": i.get("DistributionMethod"),
                    "Consumption Method": i.get("ConsumptionMethod"),
                    "Artifact Kind": i.get("ArtifactKind"),
                }
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])],
                    ignore_index=True,
                )
        else:
            response_json["activityEventEntities"].extend(
                r.get("activityEventEntities")
            )

    if return_dataframe:
        df["Creation Time"] = pd.to_datetime(df["Creation Time"])
        return df
    else:
        return response_json


def _resolve_capacity_name_and_id(
    capacity: str | UUID,
) -> Tuple[str, UUID]:

    dfC = list_capacities(capacity=capacity)
    try:
        capacity_name = dfC["Capacity Name"].iloc[0]
        capacity_id = dfC["Capacity Id"].iloc[0]
    except Exception:
        raise ValueError(f"{icons.red_dot} The '{capacity}' capacity was not found.")

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

    if workspace is None:
        workspace_id = fabric.get_workspace_id()
        workspace_name = fabric.resolve_workspace_name(workspace_id)
    else:
        dfW = list_workspaces(workspace=workspace)
        if not dfW.empty:
            workspace_name = dfW["Name"].iloc[0]
            workspace_id = dfW["Id"].iloc[0]
        else:
            raise ValueError(
                f"{icons.red_dot} The '{workspace}' workspace was not found."
            )

    return workspace_name, workspace_id


def list_reports(
    top: Optional[int] = None, skip: Optional[int] = None, filter: Optional[str] = None
) -> pd.DataFrame:
    """
    Shows a list of reports for the organization.

    This is a wrapper function for the following API: `Admin - Reports GetReportsAsAdmin <https://learn.microsoft.com/rest/api/power-bi/admin/reports-get-reports-as-admin>`_.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of reports for the organization.
    """

    df = pd.DataFrame(
        columns=[
            "Report Id",
            "Report Name",
            "Type",
            "Web URL",
            "Embed URL",
            "Dataset Id",
            "Created Date",
            "Modified Date",
            "Created By",
            "Modified By",
            "Sensitivity Label Id",
            "Users",
            "Subscriptions",
            "Workspace Id",
            "Report Flags",
        ]
    )

    url = "/v1.0/myorg/admin/reports?"
    if top is not None:
        url += f"$top={top}&"
    if skip is not None:
        url += f"$skip={skip}&"
    if filter is not None:
        url += f"$filter={filter}&"

    url.rstrip("$").rstrip("?")

    client = fabric.PowerBIRestClient()
    response = client.get(url)

    if response.status_code != 200:
        raise FabricHTTPException(response)

    for v in response.json().get("value", []):
        new_data = {
            "Report Id": v.get("id"),
            "Report Name": v.get("name"),
            "Type": v.get("reportType"),
            "Web URL": v.get("webUrl"),
            "Embed URL": v.get("embedUrl"),
            "Dataset Id": v.get("datasetId"),
            "Created Date": v.get("createdDateTime"),
            "Modified Date": v.get("modifiedDateTime"),
            "Created By": v.get("createdBy"),
            "Modified By": v.get("modifiedBy"),
            "Sensitivity Label Id": v.get("sensitivityLabel", {}).get("labelId"),
            "Users": v.get("users"),
            "Subscriptions": v.get("subscriptions"),
            "Workspace Id": v.get("workspaceId"),
            "Report Flags": v.get("reportFlags"),
        }
        df = pd.concat([df, pd.DataFrame([new_data])], ignore_index=True)

    int_cols = ["Report Flags"]
    df[int_cols] = df[int_cols].astype(int)

    df["Created Date"] = pd.to_datetime(df["Created Date"], errors="coerce")
    df["Modified Date"] = pd.to_datetime(df["Modified Date"], errors="coerce")

    return df


def get_capacity_assignment_status(workspace: Optional[str | UUID] = None):
    """
    Gets the status of the assignment-to-capacity operation for the specified workspace.

    This is a wrapper function for the following API: `Capacities - Groups CapacityAssignmentStatus <https://learn.microsoft.com/rest/api/power-bi/capacities/groups-capacity-assignment-status>`_.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or id.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the status of the assignment-to-capacity operation for the specified workspace.
    """

    (workspace_name, workspace_id) = _resolve_workspace_name_and_id(workspace)

    df = pd.DataFrame(
        columns=[
            "Status",
            "Activity Id",
            "Start Time",
            "End Time",
            "Capacity Id",
            "Capacity Name",
        ]
    )

    client = fabric.FabricRestClient()
    response = client.get(f"/v1.0/myorg/groups/{workspace_id}/CapacityAssignmentStatus")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    v = response.json()
    capacity_id = v.get("capacityId")

    (capacity_name, capacity_id) = _resolve_capacity_name_and_id(capacity=capacity_id)

    new_data = {
        "Status": v.get("status"),
        "Activity Id": v.get("activityId"),
        "Start Time": v.get("startTime"),
        "End Time": v.get("endTime"),
        "Capacity Id": capacity_id,
        "Capacity Name": capacity_name,
    }

    df = pd.concat([df, pd.DataFrame([new_data])], ignore_index=True)

    return df
