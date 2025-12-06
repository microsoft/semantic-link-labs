from typing import Optional, List, Union, Tuple
from uuid import UUID
import sempy_labs._icons as icons
from sempy_labs._helper_functions import (
    _is_valid_uuid,
    _build_url,
    _base_api,
    _create_dataframe,
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

    from sempy_labs.admin._capacities import _resolve_capacity_name_and_id

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
        The name or ID of the workspace(s).
        Defaults to None which resolves to migrating all workspaces within the source capacity to the target capacity.
    """
    from sempy_labs.admin._capacities import _resolve_capacity_name_and_id

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
        The workspace name(s) or ID(s).
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


@log
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
        The workspace name or ID.
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

    rows = []
    for v in response.json().get("accessDetails", []):
        rows.append(
            {
                "User Id": v.get("principal", {}).get("id"),
                "User Name": v.get("principal", {}).get("displayName"),
                "User Type": v.get("principal", {}).get("type"),
                "Workspace Name": workspace_name,
                "Workspace Id": workspace_id,
                "Workspace Role": v.get("workspaceAccessDetails", {}).get(
                    "workspaceRole"
                ),
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def _resolve_workspace_name(workspace_id: Optional[UUID] = None) -> str:
    from sempy_labs._helper_functions import _get_fabric_context_setting
    from sempy.fabric.exceptions import FabricHTTPException

    if workspace_id is None:
        workspace_id = _get_fabric_context_setting(name="trident.workspace.id")

    try:
        workspace_name = (
            _base_api(
                request=f"/v1/admin/workspaces/{workspace_id}", client="fabric_sp"
            )
            .json()
            .get("name")
        )
    except FabricHTTPException:
        raise ValueError(
            f"{icons.red_dot} The '{workspace_id}' workspace was not found."
        )
    return workspace_name


@log
def _resolve_workspace_name_and_id(
    workspace: str | UUID,
) -> Tuple[str, UUID]:

    from sempy_labs._helper_functions import _get_fabric_context_setting

    if workspace is None:
        workspace_id = _get_fabric_context_setting(name="trident.workspace.id")
        workspace_name = _resolve_workspace_name(workspace_id)
    elif _is_valid_uuid(workspace):
        workspace_id = workspace
        workspace_name = _resolve_workspace_name(workspace_id)
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


@log
def list_workspace_users(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows a list of users that have access to the specified workspace.

    This is a wrapper function for the following API: `Admin - Groups GetGroupUsersAsAdmin <https://learn.microsoft.com/rest/api/power-bi/admin/groups-get-group-users-as-admin>`_.

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
        A pandas dataframe showing a list of users that have access to the specified workspace.
    """

    (workspace_name, workspace_id) = _resolve_workspace_name_and_id(workspace)

    columns = {
        "User Name": "string",
        "Email Address": "string",
        "Group User Access Right": "string",
        "Identifier": "string",
        "Graph Id": "string",
        "Principal Type": "string",
    }

    df = _create_dataframe(columns=columns)

    url = f"/v1.0/myorg/admin/groups/{workspace_id}/users"
    response = _base_api(request=url, client="fabric_sp")

    rows = []
    for v in response.json().get("value", []):
        rows.append(
            {
                "User Name": v.get("displayName"),
                "Email Address": v.get("emailAddress"),
                "Group User Access Right": v.get("groupUserAccessRight"),
                "Identifier": v.get("identifier"),
                "Graph Id": v.get("graphId"),
                "Principal Type": v.get("principalType"),
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df
