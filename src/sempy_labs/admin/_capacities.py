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


def _resolve_capacity_name_and_id(
    capacity: str | UUID,
) -> Tuple[str, UUID]:

    dfC = list_capacities(capacity=capacity)
    if dfC.empty:
        raise ValueError(f"{icons.red_dot} The '{capacity}' capacity was not found.")

    capacity_name = dfC["Capacity Name"].iloc[0]
    capacity_id = dfC["Capacity Id"].iloc[0]

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
        The capacity name or ID to filter. If None, all capacities are returned.

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

    (capacity_name, capacity_id) = _resolve_capacity_name_and_id(capacity)

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
    top: int = None,
    expand: str = None,
    filter: str = None,
    skip: int = None,
) -> pd.DataFrame:
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
        "refreshCount": "int",
        "refreshFailures": "int",
        "averageDuration": "decimal",
        "medianDuration": "decimal",
        "refreshesPerDay": "int",
        "refreshType": "string",
        "startTime": "string",
        "endTime": "string",
        "status": "string",
        "requestId": "string",
        "serviceExceptionJson": "dict",
        "extendedStatus": "dict",
        "refreshAttempts": "dict",
        "refreshScheduleDays": "dict",
        "refreshScheduleTimes": "dict",
        "refreshScheduleEnabled": "bool",
        "refreshScheduleLocalTimezoneId": "string",
        "refreshScheduleNotifyOption": "string",
        "configuredBy": "dict",
    }

    params = {}
    url = "/v1.0/myorg/admin/capacities/refreshables"

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

    refreshables = []

    for i in responses.json().get("value", []):
        new_data = {
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
            "refreshCount": i.get("refreshCount", 0),
            "refreshFailures": i.get("refreshFailures", 0),
            "averageDuration": i.get("averageDuration", 0),
            "medianDuration": i.get("medianDuration", 0),
            "refreshesPerDay": i.get("refreshesPerDay", 0),
            "refreshType": i.get("lastRefresh", {}).get("refreshType"),
            "startTime": i.get("lastRefresh", {}).get("startTime"),
            "endTime": i.get("lastRefresh", {}).get("endTime"),
            "status": i.get("lastRefresh", {}).get("status"),
            "requestId": i.get("lastRefresh", {}).get("requestId"),
            "serviceExceptionJson": i.get("lastRefresh", {}).get(
                "serviceExceptionJson"
            ),
            "extendedStatus": i.get("lastRefresh", {}).get("extendedStatus"),
            "refreshAttempts": i.get("lastRefresh", {}).get("refreshAttempts"),
            "refreshScheduleDays": i.get("refreshSchedule", {}).get("days"),
            "refreshScheduleTimes": i.get("refreshSchedule", {}).get("times"),
            "refreshScheduleEnabled": i.get("refreshSchedule", {}).get("enabled"),
            "refreshScheduleLocalTimezoneId": i.get("refreshSchedule", {}).get(
                "localTimeZoneId"
            ),
            "refreshScheduleNotifyOption": i.get("refreshSchedule", {}).get(
                "notifyOption"
            ),
            "configuredBy": i.get("configuredBy"),
        }

        refreshables.append(new_data)

    return pd.DataFrame(refreshables, columns=columns)
