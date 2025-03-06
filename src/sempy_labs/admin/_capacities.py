import pandas as pd
from uuid import UUID
import sempy_labs._icons as icons
from typing import Optional, Tuple
from sempy._utils._log import log
from sempy_labs._helper_functions import (
    _base_api,
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
