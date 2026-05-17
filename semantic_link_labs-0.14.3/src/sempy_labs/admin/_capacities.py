import pandas as pd
from uuid import UUID
import sempy_labs._icons as icons
from typing import Optional, Tuple
from sempy._utils._log import log
from sempy_labs._helper_functions import (
    _base_api,
    _build_url,
    _create_dataframe,
    _update_dataframe_datatypes,
    _is_valid_uuid,
    get_capacity_id,
)


@log
def patch_capacity(capacity: str | UUID, tenant_key_id: UUID):
    """
    Changes specific capacity information. Currently, this API call only supports changing the capacity's encryption key.

    This is a wrapper function for the following API: `Admin - Patch Capacity As Admin <https://learn.microsoft.com/rest/api/power-bi/admin/patch-capacity-as-admin>`_.

    Parameters
    ----------
    capacity : str | uuid.UUID
        The name or ID of the capacity.
    tenant_key_id : str
        The ID of the encryption key.
    """

    (capacity_name, capacity_id) = _resolve_capacity_name_and_id(capacity)

    payload = {
        "tenantKeyId": tenant_key_id,
    }

    _base_api(
        request=f"/v1.0/myorg/admin/capacities/{capacity_id}",
        method="patch",
        payload=payload,
    )

    print(
        f"{icons.green_dot} The capacity '{capacity_name}' has been successfully patched."
    )


@log
def _resolve_capacity_name_and_id(
    capacity: str | UUID,
) -> Tuple[str, UUID]:

    dfC = list_capacities(capacity=capacity)
    if dfC.empty:
        raise ValueError(f"{icons.red_dot} The '{capacity}' capacity was not found.")

    capacity_name = dfC["Capacity Name"].iloc[0]
    capacity_id = dfC["Capacity Id"].iloc[0]

    return capacity_name, capacity_id


@log
def _resolve_capacity_id(
    capacity: str | UUID,
) -> UUID:

    if _is_valid_uuid(capacity):
        capacity_id = capacity
    else:
        dfC = list_capacities(capacity=capacity)
        if dfC.empty:
            raise ValueError(
                f"{icons.red_dot} The '{capacity}' capacity was not found."
            )

        capacity_id = dfC["Capacity Id"].iloc[0]

    return capacity_id


@log
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

    rows = []
    for r in responses:
        for i in r.get("value", []):
            rows.append(
                {
                    "Capacity Id": i.get("id").lower(),
                    "Capacity Name": i.get("displayName"),
                    "Sku": i.get("sku"),
                    "Region": i.get("region"),
                    "State": i.get("state"),
                    "Admins": [i.get("admins", [])],
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
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
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the status of the assignment-to-capacity operation for the specified workspace.
    """
    from sempy_labs.admin._basic_functions import _resolve_workspace_name_and_id

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

    df = pd.DataFrame([new_data], columns=list(columns.keys()))
    _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
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


@log
def list_capacities(
    capacity: Optional[str | UUID] = None,
    include_tenant_key: bool = False,
) -> pd.DataFrame:
    """
    Shows the a list of capacities and their properties.

    This is a wrapper function for the following API: `Admin - Get Capacities As Admin <https://learn.microsoft.com/rest/api/power-bi/admin/get-capacities-as-admin>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    capacity : str | uuid.UUID, default=None
        The capacity name or ID to filter. If None, all capacities are returned.
    include_tenant_key : bool, default=False
        If True, obtains the `tenant key <https://learn.microsoft.com/rest/api/power-bi/admin/get-capacities-as-admin#example-with-expand-on-tenant-key>`_ properties.

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
        "Admins": "list",
        "Users": "list",
    }
    if include_tenant_key:
        columns.update(
            {
                "Tenant Key Id": "string",
                "Tenant Key Name": "string",
            }
        )
    df = _create_dataframe(columns=columns)

    url = "/v1.0/myorg/admin/capacities"
    if include_tenant_key:
        url += "?$expand=tenantKey"

    responses = _base_api(request=url, client="fabric_sp", uses_pagination=True)

    rows = []
    for r in responses:
        for i in r.get("value", []):
            row = {
                "Capacity Id": i.get("id", "").lower(),
                "Capacity Name": i.get("displayName"),
                "Sku": i.get("sku"),
                "Region": i.get("region"),
                "State": i.get("state"),
                "Admins": i.get("admins", []),
                "Users": i.get("users", []),
            }

            if include_tenant_key:
                tenant_key = i.get("tenantKey") or {}
                row.update(
                    {
                        "Tenant Key Id": tenant_key.get("id"),
                        "Tenant Key Name": tenant_key.get("name"),
                    }
                )

            rows.append(row)

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    if capacity is not None:
        if _is_valid_uuid(capacity):
            df = df[df["Capacity Id"] == capacity.lower()]
        else:
            df = df[df["Capacity Name"] == capacity]

    return df


@log
def list_capacity_users(capacity: str | UUID) -> pd.DataFrame:
    """
    Shows a list of users that have access to the specified workspace.

    This is a wrapper function for the following API: `Admin - Capacities GetCapacityUsersAsAdmin <https://learn.microsoft.com/rest/api/power-bi/admin/capacities-get-capacity-users-as-admin>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    capacity : str | uuid.UUID
        The name or ID of the capacity.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of users that have access to the specified workspace.
    """

    (_, capacity_id) = _resolve_capacity_name_and_id(capacity)

    columns = {
        "User Name": "string",
        "Email Address": "string",
        "Capacity User Access Right": "string",
        "Identifier": "string",
        "Graph Id": "string",
        "Principal Type": "string",
        "User Type": "string",
        "Profile": "string",
    }

    df = _create_dataframe(columns=columns)

    response = _base_api(
        request=f"/v1.0/myorg/admin/capacities/{capacity_id}/users", client="fabric_sp"
    )

    rows = []
    for v in response.json().get("value", []):
        rows.append(
            {
                "User Name": v.get("displayName"),
                "Email Address": v.get("emailAddress"),
                "Capacity User Access Right": v.get("capacityUserAccessRight"),
                "Identifier": v.get("identifier"),
                "Graph Id": v.get("graphId"),
                "Principal Type": v.get("principalType"),
                "User Type": v.get("userType"),
                "Profile": v.get("profile"),
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
def get_refreshables(
    top: Optional[int] = None,
    expand: Optional[str] = None,
    filter: Optional[str] = None,
    skip: Optional[int] = None,
    capacity: Optional[str | UUID] = None,
) -> pd.DataFrame | dict:
    """
    Returns a list of refreshables for the organization within a capacity.

    Power BI retains a seven-day refresh history for each dataset, up to a maximum of sixty refreshes.

    This is a wrapper function for the following API: `Admin - Get Refreshables <https://learn.microsoft.com/rest/api/power-bi/admin/get-refreshables>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    top : int, default=None
        Returns only the first n results.
    expand : str, default=None
        Accepts a comma-separated list of data types, which will be expanded inline in the response. Supports capacities and groups.
    filter : str, default=None
        Returns a subset of a results based on Odata filter query parameter condition.
    skip : int, default=None
        Skips the first n results. Use with top to fetch results beyond the first 1000.
    capacity : str | uuid.UUID, default=None
        The capacity name or ID to filter. If None, all capacities are returned.

    Returns
    -------
    pandas.DataFrame
        Returns a list of refreshables for the organization within a capacity.
    """

    columns = {
        "Workspace Id": "string",
        "Workspace Name": "string",
        "Item Id": "string",
        "Item Name": "string",
        "Item Kind": "string",
        "Capacity Id": "string",
        "Capacity Name": "string",
        "Capacity SKU": "string",
        "Refresh Count": "int",
        "Refresh Failures": "int",
        "Average Duration": "float",
        "Median Duration": "float",
        "Refreshes Per Day": "int",
        "Refresh Type": "string",
        "Start Time": "string",
        "End Time": "string",
        "Status": "string",
        "Request Id": "string",
        "Service Exception Json": "string",
        "Extended Status": "dict",
        "Refresh Attempts": "list",
        "Refresh Schedule Days": "list",
        "Refresh Schedule Times": "list",
        "Refresh Schedule Enabled": "bool",
        "Refresh Schedule Local Timezone Id": "string",
        "Refresh Schedule Notify Option": "string",
        "Configured By": "list",
    }

    df = _create_dataframe(columns=columns)

    params = {}
    url = (
        "/v1.0/myorg/admin/capacities/refreshables"
        if capacity is None
        else f"/v1.0/myorg/admin/capacities/{_resolve_capacity_id(capacity=capacity)}/refreshables"
    )

    if top is not None:
        params["$top"] = top

    if expand is not None:
        params["$expand"] = expand

    if filter is not None:
        params["$filter"] = filter

    if skip is not None:
        params["$skip"] = skip

    url = _build_url(url, params)

    responses = _base_api(request=url, client="fabric_sp")

    rows = []
    for i in responses.json().get("value", []):
        last_refresh = i.get("lastRefresh", {})
        refresh_schedule = i.get("refreshSchedule", {})
        rows.append(
            {
                "Workspace Id": i.get("group", {}).get("id"),
                "Workspace Name": i.get("group", {}).get("name"),
                "Item Id": i.get("id"),
                "Item Name": i.get("name"),
                "Item Kind": i.get("kind"),
                "Capacity Id": (
                    i.get("capacity", {}).get("id").lower()
                    if i.get("capacity", {}).get("id")
                    else None
                ),
                "Capacity Name": i.get("capacity", {}).get("displayName"),
                "Capacity SKU": i.get("capacity", {}).get("sku"),
                "Refresh Count": i.get("refreshCount", 0),
                "Refresh Failures": i.get("refreshFailures", 0),
                "Average Duration": i.get("averageDuration", 0),
                "Median Duration": i.get("medianDuration", 0),
                "Refreshes Per Day": i.get("refreshesPerDay", 0),
                "Refresh Type": last_refresh.get("refreshType"),
                "Start Time": last_refresh.get("startTime"),
                "End Time": last_refresh.get("endTime"),
                "Status": last_refresh.get("status"),
                "Request Id": last_refresh.get("requestId"),
                "Service Exception Json": last_refresh.get("serviceExceptionJson"),
                "Extended Status": last_refresh.get("extendedStatus"),
                "Refresh Attempts": last_refresh.get("refreshAttempts"),
                "Refresh Schedule Days": refresh_schedule.get("days"),
                "Refresh Schedule Times": refresh_schedule.get("times"),
                "Refresh Schedule Enabled": refresh_schedule.get("enabled"),
                "Refresh Schedule Local Timezone Id": refresh_schedule.get(
                    "localTimeZoneId"
                ),
                "Refresh Schedule Notify Option": refresh_schedule.get("notifyOption"),
                "Configured By": i.get("configuredBy"),
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df
