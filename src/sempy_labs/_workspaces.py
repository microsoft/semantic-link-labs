import pandas as pd
import sempy_labs._icons as icons
from typing import Optional, Literal
from sempy_labs._helper_functions import (
    resolve_workspace_id,
    resolve_workspace_name_and_id,
    resolve_capacity_id,
    _base_api,
    _create_dataframe,
)
from uuid import UUID
from sempy._utils._log import log


@log
def delete_user_from_workspace(
    email_address: str, workspace: Optional[str | UUID] = None
):
    """
    Removes a user from a workspace.

    This is a wrapper function for the following API: `Groups - Delete User In Group <https://learn.microsoft.com/rest/api/power-bi/groups/delete-user-in-group>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

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

    _base_api(
        request=f"/v1.0/myorg/groups/{workspace_id}/users/{email_address}",
        method="delete",
        client="fabric_sp",
    )
    print(
        f"{icons.green_dot} The '{email_address}' user has been removed from accessing the '{workspace_name}' workspace."
    )


@log
def update_workspace_user(
    email_address: str,
    role_name: str,
    principal_type: Optional[str] = "User",
    workspace: Optional[str | UUID] = None,
):
    """
    Updates a user's role within a workspace.

    This is a wrapper function for the following API: `Groups - Update Group User <https://learn.microsoft.com/rest/api/power-bi/groups/update-group-user>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

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

    payload = {
        "emailAddress": email_address,
        "groupUserAccessRight": role_name,
        "principalType": principal_type,
        "identifier": email_address,
    }

    _base_api(
        request=f"/v1.0/myorg/groups/{workspace_id}/users",
        method="put",
        payload=payload,
        client="fabric_sp",
    )
    print(
        f"{icons.green_dot} The '{email_address}' user has been updated to a '{role_name}' within the '{workspace_name}' workspace."
    )


@log
def list_workspace_users(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    A list of all the users of a workspace and their roles.

    This is a wrapper function for the following API: `Workspaces - List Workspace Role Assignments <https://learn.microsoft.com/rest/api/fabric/core/workspaces/list-workspace-role-assignments>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

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

    workspace_id = resolve_workspace_id(workspace)

    columns = {
        "User Name": "string",
        "Email Address": "string",
        "Role": "string",
        "Type": "string",
        "User ID": "string",
    }
    df = _create_dataframe(columns=columns)

    responses = _base_api(
        request=f"v1/workspaces/{workspace_id}/roleAssignments",
        uses_pagination=True,
        client="fabric_sp",
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            p = v.get("principal", {})
            rows.append(
                {
                    "User Name": p.get("displayName"),
                    "User ID": p.get("id"),
                    "Type": p.get("type"),
                    "Role": v.get("role"),
                    "Email Address": p.get("userDetails", {}).get("userPrincipalName"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def add_user_to_workspace(
    email_address: str,
    role_name: str,
    principal_type: Optional[str] = "User",
    workspace: Optional[str | UUID] = None,
):
    """
    Adds a user to a workspace.

    This is a wrapper function for the following API: `Groups - Add Group User <https://learn.microsoft.com/rest/api/power-bi/groups/add-group-user>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

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

    payload = {
        "emailAddress": email_address,
        "groupUserAccessRight": role_name,
        "principalType": principal_type,
        "identifier": email_address,
    }

    _base_api(
        request=f"/v1.0/myorg/groups/{workspace_id}/users",
        method="post",
        payload=payload,
        client="fabric_sp",
    )
    print(
        f"{icons.green_dot} The '{email_address}' user has been added as a{plural} '{role_name}' within the '{workspace_name}' workspace."
    )


@log
def assign_workspace_to_capacity(
    capacity: str | UUID,
    workspace: Optional[str | UUID] = None,
    **kwargs,
):
    """
    Assigns a workspace to a capacity.

    This is a wrapper function for the following API: `Workspaces - Assign To Capacity <https://learn.microsoft.com/rest/api/fabric/core/workspaces/assign-to-capacity>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    capacity : str | uuid.UUID
        The name or ID of the capacity.
    workspace : str | uuid.UUID, default=None
        The name or ID of the Fabric workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    if "capacity_name" in kwargs:
        capacity = kwargs["capacity_name"]
        print(
            f"{icons.warning} The 'capacity_name' parameter is deprecated. Please use 'capacity' instead."
        )

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    capacity_id = resolve_capacity_id(capacity=capacity)

    payload = {"capacityId": capacity_id}

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/assignToCapacity",
        method="post",
        payload=payload,
        status_codes=[200, 202],
        client="fabric_sp",
    )
    print(
        f"{icons.green_dot} The '{workspace_name}' workspace has been assigned to the '{capacity}' capacity."
    )


@log
def unassign_workspace_from_capacity(workspace: Optional[str | UUID] = None):
    """
    Unassigns a workspace from its assigned capacity.

    This is a wrapper function for the following API: `Workspaces - Unassign From Capacity <https://learn.microsoft.com/rest/api/fabric/core/workspaces/unassign-from-capacity>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The name or ID of the Fabric workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/unassignFromCapacity",
        method="post",
        status_codes=[200, 202],
        client="fabric_sp",
    )
    print(
        f"{icons.green_dot} The '{workspace_name}' workspace has been unassigned from its capacity."
    )


@log
def list_workspace_role_assignments(
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Shows the members of a given workspace.

    This is a wrapper function for the following API: `Workspaces - List Workspace Role Assignments <https://learn.microsoft.com/rest/api/fabric/core/workspaces/list-workspace-role-assignments>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

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

    workspace_id = resolve_workspace_id(workspace)

    columns = {
        "User Name": "string",
        "User Email": "string",
        "Role Name": "string",
        "Type": "string",
    }
    df = _create_dataframe(columns=columns)

    responses = _base_api(
        request=f"v1/workspaces/{workspace_id}/roleAssignments",
        uses_pagination=True,
        client="fabric_sp",
    )

    rows = []
    for r in responses:
        for i in r.get("value", []):
            principal = i.get("principal", {})
            rows.append(
                {
                    "User Name": principal.get("displayName"),
                    "Role Name": i.get("role"),
                    "Type": principal.get("type"),
                    "User Email": principal.get("userDetails", {}).get(
                        "userPrincipalName"
                    ),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def delete_workspace(workspace: Optional[str | UUID] = None):
    """
    Deletes a workspace.

    This is a wrapper function for the following API: `Workspaces - Delete Workspace <https://learn.microsoft.com/rest/api/fabric/core/workspaces/delete-workspace>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    _base_api(
        request=f"v1/workspaces/{workspace_id}", method="delete", client="fabric_sp"
    )

    print(f"{icons.green_dot} The '{workspace_name}' workspace has been deleted.")


@log
def get_workspace_network_communication_policy(
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Returns networking communication policy for the specified workspace. This feature is currently in preview.

    This is a wrapper function for the following API: `Workspaces - Get Network Communication Policy <https://learn.microsoft.com/rest/api/fabric/core/workspaces/get-network-communication-policy>`_.

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
        A pandas dataframe showing the networking communication policy for the specified workspace.
    """

    workspace_id = resolve_workspace_id(workspace)

    columns = {
        "Inbound Public Access Rules": "string",
        "Outbound Public Access Rules": "string",
    }

    df = _create_dataframe(columns=columns)

    data = _base_api(
        request=f"/v1/workspaces/{workspace_id}/networking/communicationPolicy",
        client="fabric_sp",
    ).json()

    if data:
        df = pd.DataFrame(
            [
                {
                    "Inbound Public Access Rules": data.get("inbound", {})
                    .get("publicAccessRules", {})
                    .get("defaultAction"),
                    "Outbound Public Access Rules": data.get("outbound", {})
                    .get("publicAccessRules", {})
                    .get("defaultAction"),
                }
            ]
        )

    return df


@log
def set_workspace_network_communication_policy(
    inbound_policy: Literal["Allow", "Deny"],
    outbound_policy: Literal["Allow", "Deny"],
    workspace: Optional[str | UUID] = None,
):
    """
    Sets networking communication policy for the specified workspace. This API uses the PUT method and will overwrite all settings. Remaining policy will be set to default value if only partial policy is provided in the request body. Always run Get Network Communication Policy first and provide full policy in the request body. This feature is currently in preview.

    This is a wrapper function for the following API: `Workspaces - Set Network Communication Policy <https://learn.microsoft.com/rest/api/fabric/core/workspaces/set-network-communication-policy>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    inbound_policy : Literal['Allow', 'Deny']
        The policy for all inbound communications to a workspace.
    outbound_policy : Literal['Allow', 'Deny']
        The policy for all outbound communications to a workspace.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    inbound_policy = inbound_policy.capitalize()
    outbound_policy = outbound_policy.capitalize()

    if inbound_policy not in ["Allow", "Deny"]:
        raise ValueError(
            f"{icons.red_dot} The 'inbound_policy' must be either 'Allow' or 'Deny'."
        )
    if outbound_policy not in ["Allow", "Deny"]:
        raise ValueError(
            f"{icons.red_dot} The 'outbound_policy' must be either 'Allow' or 'Deny'."
        )

    payload = {
        "inbound": {
            "publicAccessRules": {
                "defaultAction": inbound_policy,
            }
        },
        "outbound": {
            "publicAccessRules": {
                "defaultAction": outbound_policy,
            }
        },
    }
    _base_api(
        request=f"/v1/workspaces/{workspace_id}/networking/communicationPolicy",
        client="fabric_sp",
        payload=payload,
        method="put",
    )

    print(
        f"{icons.green_dot} The networking communication policy has been updated for the '{workspace_name}' workspace."
    )


@log
def get_workspace_git_outbound_policy(workspace: Optional[str | UUID] = None) -> str:
    """
    Returns Git Outbound policy for the specified workspace.
    In cases the workspace restricts outbound policy, a workspace admin needs to allow the use of Git integration on the specified workspace.

    This is a wrapper function for the following API: `Workspaces - Get Git Outbound Policy <https://learn.microsoft.com/rest/api/fabric/core/workspaces/get-git-outbound-policy>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    str
        The Git outbound policy for the specified workspace.
    """

    workspace_id = resolve_workspace_id(workspace)
    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/networking/communicationPolicy/outbound/git",
        client="fabric_sp",
    )

    return response.json().get("defaultAction", {})


@log
def set_workspace_git_outbound_policy(
    policy: Literal["Allow", "Deny"],
    workspace: Optional[str | UUID] = None,
):
    """
    Sets Git Outbound policy for the specified workspace, when Outbound policy is set to 'Deny'.

    This is a wrapper function for the following API: `Workspaces - Set Git Outbound Policy <https://learn.microsoft.com/rest/api/fabric/core/workspaces/set-git-outbound-policy>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    policy : Literal['Allow', 'Deny']
        The policy for all Git outbound communications from a workspace.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    policy = policy.capitalize()

    if policy not in ["Allow", "Deny"]:
        raise ValueError(
            f"{icons.red_dot} The 'policy' must be either 'Allow' or 'Deny'."
        )

    payload = {
        "defaultAction": policy,
    }

    # Check current policy
    p = get_workspace_git_outbound_policy(workspace=workspace_id)
    if p == policy:
        print(
            f"{icons.info} The Git outbound policy for the '{workspace_name}' workspace is already set to '{policy}'. No changes made."
        )
        return

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/networking/communicationPolicy/outbound/git",
        client="fabric_sp",
        payload=payload,
        method="put",
    )

    print(
        f"{icons.green_dot} The Git outbound policy has been updated for the '{workspace_name}' workspace."
    )
