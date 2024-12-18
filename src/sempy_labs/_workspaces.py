import sempy.fabric as fabric
import pandas as pd
import sempy_labs._icons as icons
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    pagination,
    resolve_capacity_id,
)
from sempy.fabric.exceptions import FabricHTTPException
from uuid import UUID


def delete_user_from_workspace(
    email_address: str, workspace: Optional[str | UUID] = None
):
    """
    Removes a user from a workspace.

    This is a wrapper function for the following API: `Groups - Delete User In Group <https://learn.microsoft.com/rest/api/power-bi/groups/delete-user-in-group>`_.

    Parameters
    ----------
    email_address : str
        The email address of the user.
    workspace : str | uuid.UUID, default=None
        The name or ID of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    client = fabric.PowerBIRestClient()
    response = client.delete(f"/v1.0/myorg/groups/{workspace_id}/users/{email_address}")

    if response.status_code != 200:
        raise FabricHTTPException(response)
    print(
        f"{icons.green_dot} The '{email_address}' user has been removed from accessing the '{workspace_name}' workspace."
    )


def update_workspace_user(
    email_address: str,
    role_name: str,
    principal_type: Optional[str] = "User",
    workspace: Optional[str | UUID] = None,
):
    """
    Updates a user's role within a workspace.

    This is a wrapper function for the following API: `Groups - Update Group User <https://learn.microsoft.com/rest/api/power-bi/groups/update-group-user>`_.

    Parameters
    ----------
    email_address : str
        The email address of the user.
    role_name : str
        The `role <https://learn.microsoft.com/rest/api/power-bi/groups/add-group-user#groupuseraccessright>`_ of the user within the workspace.
    principal_type : str, default='User'
        The `principal type <https://learn.microsoft.com/rest/api/power-bi/groups/add-group-user#principaltype>`_.
    workspace : str | uuid.UUID, default=None
        The name or ID of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    role_names = icons.workspace_roles
    role_name = role_name.capitalize()
    if role_name not in role_names:
        raise ValueError(
            f"{icons.red_dot} Invalid role. The 'role_name' parameter must be one of the following: {role_names}."
        )
    principal_types = icons.principal_types
    principal_type = principal_type.capitalize()
    if principal_type not in principal_types:
        raise ValueError(
            f"{icons.red_dot} Invalid princpal type. Valid options: {principal_types}."
        )

    request_body = {
        "emailAddress": email_address,
        "groupUserAccessRight": role_name,
        "principalType": principal_type,
        "identifier": email_address,
    }

    client = fabric.PowerBIRestClient()
    response = client.put(f"/v1.0/myorg/groups/{workspace_id}/users", json=request_body)

    if response.status_code != 200:
        raise FabricHTTPException(response)
    print(
        f"{icons.green_dot} The '{email_address}' user has been updated to a '{role_name}' within the '{workspace_name}' workspace."
    )


def list_workspace_users(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    A list of all the users of a workspace and their roles.

    This is a wrapper function for the following API: `Workspaces - List Workspace Role Assignments <https://learn.microsoft.com/rest/api/fabric/core/workspaces/list-workspace-role-assignments>`_.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The name or ID of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe the users of a workspace and their properties.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    df = pd.DataFrame(columns=["User Name", "Email Address", "Role", "Type", "User ID"])
    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/roleAssignments")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    for r in responses:
        for v in r.get("value", []):
            p = v.get("principal", {})
            new_data = {
                "User Name": p.get("displayName"),
                "User ID": p.get("id"),
                "Type": p.get("type"),
                "Role": v.get("role"),
                "Email Address": p.get("userDetails", {}).get("userPrincipalName"),
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def add_user_to_workspace(
    email_address: str,
    role_name: str,
    principal_type: Optional[str] = "User",
    workspace: Optional[str | UUID] = None,
):
    """
    Adds a user to a workspace.

    This is a wrapper function for the following API: `Groups - Add Group User <https://learn.microsoft.com/rest/api/power-bi/groups/add-group-user>`_.

    Parameters
    ----------
    email_address : str
        The email address of the user. Also accepts the user identifier.
    role_name : str
        The `role <https://learn.microsoft.com/rest/api/power-bi/groups/add-group-user#groupuseraccessright>`_ of the user within the workspace.
    principal_type : str, default='User'
        The `principal type <https://learn.microsoft.com/rest/api/power-bi/groups/add-group-user#principaltype>`_.
    workspace : str | uuid.UUID, default=None
        The name or ID of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    role_names = icons.workspace_roles
    role_name = role_name.capitalize()
    if role_name not in role_names:
        raise ValueError(
            f"{icons.red_dot} Invalid role. The 'role_name' parameter must be one of the following: {role_names}."
        )
    plural = "n" if role_name == "Admin" else ""
    principal_types = icons.principal_types
    principal_type = principal_type.capitalize()
    if principal_type not in principal_types:
        raise ValueError(
            f"{icons.red_dot} Invalid princpal type. Valid options: {principal_types}."
        )

    client = fabric.PowerBIRestClient()

    request_body = {
        "emailAddress": email_address,
        "groupUserAccessRight": role_name,
        "principalType": principal_type,
        "identifier": email_address,
    }

    response = client.post(
        f"/v1.0/myorg/groups/{workspace_id}/users", json=request_body
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)
    print(
        f"{icons.green_dot} The '{email_address}' user has been added as a{plural} '{role_name}' within the '{workspace_name}' workspace."
    )


def assign_workspace_to_capacity(
    capacity_name: str, workspace: Optional[str | UUID] = None
):
    """
    Assigns a workspace to a capacity.

    This is a wrapper function for the following API: `Workspaces - Assign To Capacity <https://learn.microsoft.com/rest/api/fabric/core/workspaces/assign-to-capacity>`_.

    Parameters
    ----------
    capacity_name : str
        The name of the capacity.
    workspace : str | uuid.UUID, default=None
        The name or ID of the Fabric workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    capacity_id = resolve_capacity_id(capacity_name=capacity_name)

    request_body = {"capacityId": capacity_id}

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/workspaces/{workspace_id}/assignToCapacity",
        json=request_body,
    )

    if response.status_code not in [200, 202]:
        raise FabricHTTPException(response)
    print(
        f"{icons.green_dot} The '{workspace_name}' workspace has been assigned to the '{capacity_name}' capacity."
    )


def unassign_workspace_from_capacity(workspace: Optional[str | UUID] = None):
    """
    Unassigns a workspace from its assigned capacity.

    This is a wrapper function for the following API: `Workspaces - Unassign From Capacity <https://learn.microsoft.com/rest/api/fabric/core/workspaces/unassign-from-capacity>`_.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The name or ID of the Fabric workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.post(f"/v1/workspaces/{workspace_id}/unassignFromCapacity")

    if response.status_code not in [200, 202]:
        raise FabricHTTPException(response)
    print(
        f"{icons.green_dot} The '{workspace_name}' workspace has been unassigned from its capacity."
    )


def list_workspace_role_assignments(
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Shows the members of a given workspace.

    This is a wrapper function for the following API: `Workspaces - List Workspace Role Assignments <https://learn.microsoft.com/rest/api/fabric/core/workspaces/list-workspace-role-assignments>`_.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the members of a given workspace and their roles.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    df = pd.DataFrame(columns=["User Name", "User Email", "Role Name", "Type"])

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/roleAssignments")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    for r in responses:
        for i in r.get("value", []):
            principal = i.get("principal", {})
            new_data = {
                "User Name": principal.get("displayName"),
                "Role Name": i.get("role"),
                "Type": principal.get("type"),
                "User Email": principal.get("userDetails", {}).get("userPrincipalName"),
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df
