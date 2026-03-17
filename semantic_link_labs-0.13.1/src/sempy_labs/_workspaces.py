import pandas as pd
from typing import Optional, Literal
from uuid import UUID
from sempy._utils._log import log
import sempy_labs.workspace as wkspc


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

    wkspc.delete_user_from_workspace(email_address=email_address, workspace=workspace)


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

    wkspc.update_workspace_user(
        email_address=email_address,
        role_name=role_name,
        principal_type=principal_type,
        workspace=workspace,
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

    return wkspc.list_workspace_users(workspace=workspace)


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

    wkspc.add_user_to_workspace(
        email_address=email_address,
        role_name=role_name,
        principal_type=principal_type,
        workspace=workspace,
    )


@log
def assign_workspace_to_capacity(
    capacity: str | UUID,
    workspace: Optional[str | UUID] = None,
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

    wkspc.assign_to_capacity(capacity=capacity, workspace=workspace)


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

    wkspc.unassign_from_capacity(workspace=workspace)


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

    return wkspc.list_role_assignments(workspace=workspace)


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

    wkspc.delete_workspace(workspace=workspace)


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

    return wkspc.get_network_communication_policy(workspace=workspace)


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
    inbound_policy : typing.Literal['Allow', 'Deny']
        The policy for all inbound communications to a workspace.
    outbound_policy : typing.Literal['Allow', 'Deny']
        The policy for all outbound communications to a workspace.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    wkspc.set_network_communication_policy(
        inbound_policy=inbound_policy,
        outbound_policy=outbound_policy,
        workspace=workspace,
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

    return wkspc.get_git_outbound_policy(workspace=workspace)


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
    policy : typing.Literal['Allow', 'Deny']
        The policy for all Git outbound communications from a workspace.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    wkspc.set_git_outbound_policy(policy=policy, workspace=workspace)
