import sempy.fabric as fabric
from typing import Optional, List, Union, Tuple
from uuid import UUID
import sempy_labs._icons as icons
from sempy_labs._helper_functions import (
    _is_valid_uuid,
    _build_url,
    _update_dataframe_datatypes,
    _base_api,
    _create_dataframe,
    get_capacity_id,
)
from sempy._utils._log import log
import numpy as np
import pandas as pd
from dateutil.parser import parse as dtparser


@log
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

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

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

    columns = {
        "Id": "string",
        "Name": "string",
        "State": "string",
        "Type": "string",
        "Capacity Id": "string",
    }
    df = _create_dataframe(columns=columns)

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

    responses = _base_api(request=url, client="fabric_sp", uses_pagination=True)
    workspaces = []

    for r in responses:
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


@log
def list_capacities(
    capacity: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Shows the a list of capacities and their properties.

    This is a wrapper function for the following API: `Admin - Get Capacities As Admin <https://learn.microsoft.com/rest/api/power-bi/admin/get-capacities-as-admin>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    capacity : str | uuid.UUID, default=None
        Capacity name or id to filter.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the capacities and their properties.
    """

    columns = {
        "Capacity Id": "string",
        "Capacity Name": "string",
        "Sku": "string",
        "Region": "string",
        "State": "string",
        "Admins": "string",
    }
    df = _create_dataframe(columns=columns)

    responses = _base_api(
        request="/v1.0/myorg/admin/capacities", client="fabric_sp", uses_pagination=True
    )

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


@log
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
        payload = {
            "capacityMigrationAssignments": [
                {
                    "targetCapacityObjectId": target_capacity_id.upper(),
                    "workspacesToAssign": batch,
                }
            ]
        }

        _base_api(
            request="/v1.0/myorg/admin/capacities/AssignWorkspaces",
            method="post",
            payload=payload,
        )

    print(
        f"{icons.green_dot} The workspaces have been assigned to the '{target_capacity}' capacity. A total of {len(workspaces)} were moved."
    )


@log
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
    _base_api(
        request="/v1.0/myorg/admin/capacities/UnassignWorkspaces",
        method="post",
        payload=payload,
    )

    print(
        f"{icons.green_dot} A total of {len(workspacesIds)} workspaces have been unassigned."
    )


@log
def list_tenant_settings() -> pd.DataFrame:
    """
    Lists all tenant settings.

    This is a wrapper function for the following API: `Tenants - List Tenant Settings <https://learn.microsoft.com/rest/api/fabric/admin/tenants/list-tenant-settings>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the tenant settings.
    """

    columns = {
        "Setting Name": "string",
        "Title": "string",
        "Enabled": "bool",
        "Can Specify Security Groups": "bool",
        "Tenant Setting Group": "string",
        "Enabled Security Groups": "string",
    }
    df = _create_dataframe(columns=columns)

    response = _base_api(request="/v1/admin/tenantsettings", client="fabric_sp")

    if 'settingName' in response.json():
        response_key = 'settingName'
    else:
        response_key = 'value'

    for i in response.json().get(response_key, []):  
        new_data = {
            "Setting Name": i.get("settingName"),
            "Title": i.get("title"),
            "Enabled": i.get("enabled"),
            "Can Specify Security Groups": i.get("canSpecifySecurityGroups"),
            "Tenant Setting Group": i.get("tenantSettingGroup"),
            "Enabled Security Groups": [i.get("enabledSecurityGroups", [])],
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


def list_capacities_delegated_tenant_settings(
    return_dataframe: bool = True,
) -> pd.DataFrame | dict:
    """
    Returns list of tenant setting overrides that override at the capacities.

    This is a wrapper function for the following API: `Tenants - List Capacities Tenant Settings Overrides <https://learn.microsoft.com/rest/api/fabric/admin/tenants/list-capacities-tenant-settings-overrides>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    return_dataframe : bool, default=True
        If True, returns a dataframe. If False, returns a dictionary.

    Returns
    -------
    pandas.DataFrame | dict
        A pandas dataframe showing a list of tenant setting overrides that override at the capacities.
    """

    columns = {
        "Capacity Id": "string",
        "Setting Name": "string",
        "Setting Title": "string",
        "Setting Enabled": "bool",
        "Can Specify Security Groups": "bool",
        "Enabled Security Groups": "string",
        "Tenant Setting Group": "string",
        "Tenant Setting Properties": "string",
        "Delegate to Workspace": "bool",
        "Delegated From": "string",
    }
    df = _create_dataframe(columns=columns)

    responses = _base_api(
        request="/v1/admin/capacities/delegatedTenantSettingOverrides",
        client="fabric_sp",
        uses_pagination=True,
    )

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

            _update_dataframe_datatypes(dataframe=df, column_map=columns)

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

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

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
    response = _base_api(request=url, client="fabric_sp")

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

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

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

    columns = {
        "Dataset Id": "string",
        "Dataset Name": "string",
        "Web URL": "string",
        "Add Rows API Enabled": "bool",
        "Configured By": "string",
        "Is Refreshable": "bool",
        "Is Effective Identity Required": "bool",
        "Is Effective Identity Roles Required": "bool",
        "Target Storage Mode": "string",
        "Created Date": "datetime",
        "Content Provider Type": "string",
        "Create Report Embed URL": "string",
        "QnA Embed URL": "string",
        "Upstream Datasets": "string",
        "Users": "string",
        "Is In Place Sharing Enabled": "bool",
        "Workspace Id": "string",
        "Auto Sync Read Only Replicas": "bool",
        "Max Read Only Replicas": "int",
    }

    df = _create_dataframe(columns=columns)

    params = {}
    url = "/v1.0/myorg/admin/datasets"

    if top is not None:
        params["$top"] = top

    if filter is not None:
        params["$filter"] = filter

    if skip is not None:
        params["$skip"] = skip

    url = _build_url(url, params)
    response = _base_api(request=url, client="fabric_sp")

    rows = []
    for v in response.json().get("value", []):
        rows.append(
            {
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
        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


def list_access_entities(
    user_email_address: str,
) -> pd.DataFrame:
    """
    Shows a list of permission details for Fabric and Power BI items the specified user can access.

    This is a wrapper function for the following API: `Users - List Access Entities <https://learn.microsoft.com/rest/api/fabric/admin/users/list-access-entities>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    user_email_address : str
        The user's email address.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of permission details for Fabric and Power BI items the specified user can access.
    """

    columns = {
        "Item Id": "string",
        "Item Name": "string",
        "Item Type": "string",
        "Permissions": "string",
        "Additional Permissions": "string",
    }
    df = _create_dataframe(columns=columns)

    responses = _base_api(
        request=f"/v1/admin/users/{user_email_address}/access",
        client="fabric_sp",
        uses_pagination=True,
    )

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
    workspace: Optional[Union[str, UUID]] = None,
) -> pd.DataFrame:
    """
    Shows a list of users (including groups and Service Principals) that have access to the specified workspace.

    This is a wrapper function for the following API: `Workspaces - List Workspace Access Details <https://learn.microsoft.com/rest/api/fabric/admin/workspaces/list-workspace-access-details>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

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
    (workspace_name, workspace_id) = _resolve_workspace_name_and_id(workspace)

    columns = {
        "User Id": "string",
        "User Name": "string",
        "User Type": "string",
        "Workspace Name": "string",
        "Workspace Id": "string",
        "Workspace Role": "string",
    }
    df = _create_dataframe(columns=columns)

    response = _base_api(
        request=f"/v1/admin/workspaces/{workspace_id}/users", client="fabric_sp"
    )

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

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

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

    columns = {
        "Id": "string",
        "Record Type": "string",
        "Creation Time": "datetime",
        "Operation": "string",
        "Organization Id": "string",
        "User Type": "string",
        "User Key": "string",
        "Workload": "string",
        "Result Status": "string",
        "User Id": "string",
        "Client IP": "string",
        "User Agent": "string",
        "Activity": "string",
        "Workspace Name": "string",
        "Workspace Id": "string",
        "Object Id": "string",
        "Request Id": "string",
        "Object Type": "string",
        "Object Display Name": "string",
        "Experience": "string",
        "Refresh Enforcement Policy": "string",
        "Is Success": "bool",
        "Activity Id": "string",
        "Item Name": "string",
        "Dataset Name": "string",
        "Report Name": "string",
        "Capacity Id": "string",
        "Capacity Name": "string",
        "App Name": "string",
        "Dataset Id": "string",
        "Report Id": "string",
        "Artifact Id": "string",
        "Artifact Name": "string",
        "Report Type": "string",
        "App Report Id": "string",
        "Distribution Method": "string",
        "Consumption Method": "string",
        "Artifact Kind": "string",
    }
    df = _create_dataframe(columns=columns)

    response_json = {"activityEventEntities": []}
    url = f"/v1.0/myorg/admin/activityevents?startDateTime='{start_time}'&endDateTime='{end_time}'"

    conditions = []
    if activity_filter is not None:
        conditions.append(f"Activity eq '{activity_filter}'")
    if user_id_filter is not None:
        conditions.append(f"UserId eq '{user_id_filter}'")

    if conditions:
        url += f"&$filter={f' and '.join(conditions)}"

    responses = _base_api(request=url, client="fabric_sp", uses_pagination=True)

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
        _update_dataframe_datatypes(dataframe=df, column_map=columns)
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

    columns = {
        "Capacity Id": "string",
        "Capacity Name": "string",
        "Sku": "string",
        "Region": "string",
        "State": "string",
        "Admins": "string",
    }
    df = _create_dataframe(columns=columns)

    responses = _base_api(
        request="/v1.0/myorg/admin/capacities", client="fabric_sp", uses_pagination=True
    )

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
    top: Optional[int] = None,
    skip: Optional[int] = None,
    filter: Optional[str] = None,
) -> pd.DataFrame:
    """
    Shows a list of reports for the organization.

    This is a wrapper function for the following API: `Admin - Reports GetReportsAsAdmin <https://learn.microsoft.com/rest/api/power-bi/admin/reports-get-reports-as-admin>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    top : int, default=None
        Returns only the first n results.
    skip : int, default=None
        Skips the first n results.
    filter : str, default=None
        Returns a subset of a results based on Odata filter query parameter condition.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of reports for the organization.
    """

    columns = {
        "Report Id": "string",
        "Report Name": "string",
        "Type": "string",
        "Web URL": "string",
        "Embed URL": "string",
        "Dataset Id": "string",
        "Created Date": "datetime_coerce",
        "Modified Date": "datetime_coerce",
        "Created By": "string",
        "Modified By": "string",
        "Sensitivity Label Id": "string",
        "Users": "string",
        "Subscriptions": "string",
        "Workspace Id": "string",
        "Report Flags": "int",
    }

    df = _create_dataframe(columns=columns)

    url = "/v1.0/myorg/admin/reports?"
    if top is not None:
        url += f"$top={top}&"
    if skip is not None:
        url += f"$skip={skip}&"
    if filter is not None:
        url += f"$filter={filter}&"

    url.rstrip("$").rstrip("?")
    response = _base_api(request=url, client="fabric_sp")
    rows = []

    for v in response.json().get("value", []):
        rows.append(
            {
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
        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


def get_capacity_assignment_status(
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Gets the status of the assignment-to-capacity operation for the specified workspace.

    This is a wrapper function for the following API: `Capacities - Groups CapacityAssignmentStatus <https://learn.microsoft.com/rest/api/power-bi/capacities/groups-capacity-assignment-status>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

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

    columns = {
        "Status": "string",
        "Activity Id": "string",
        "Start Time": "datetime",
        "End Time": "datetime",
        "Capacity Id": "string",
        "Capacity Name": "string",
    }
    df = _create_dataframe(columns=columns)

    response = _base_api(
        request=f"/v1.0/myorg/groups/{workspace_id}/CapacityAssignmentStatus",
        client="fabric_sp",
    )
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

    _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


def get_capacity_state(capacity: Optional[str | UUID] = None):
    """
    Gets the state of a capacity.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    capacity : str | uuid.UUID, default=None
        The capacity name or ID.
        Defaults to None which resolves to the capacity of the attached lakehouse
        or if no lakehouse is attached, resolves to the workspace of the notebook.

    Returns
    -------
    str
        The capacity state.
    """

    df = list_capacities()

    if capacity is None:
        capacity = get_capacity_id()
    if _is_valid_uuid(capacity):
        df_filt = df[df["Capacity Id"] == capacity]
    else:
        df_filt = df[df["Capacity Name"] == capacity]

    if df_filt.empty:
        raise ValueError(f"{icons.red_dot} The capacity '{capacity}' was not found.")

    return df_filt["State"].iloc[0]
