from sempy_labs._helper_functions import (
    _base_api,
    _build_url,
    _encode_user,
)
from uuid import UUID
from typing import Optional
from sempy_labs.admin._basic_functions import (
    _resolve_workspace_name_and_id,
)
import sempy_labs._icons as icons


def add_user_to_workspace(
    user: str | UUID,
    role: str = "Member",
    principal_type: str = "User",
    workspace: Optional[str | UUID] = None,
):
    """
    Grants user permissions to the specified workspace.

    This is a wrapper function for the following API: `Admin - Groups AddUserAsAdmin <https://learn.microsoft.com/rest/api/power-bi/admin/groups-add-user-as-admin>`_.

    Parameters
    ----------
    user : str | uuid.UUID
        The user identifier or email address. For service principals and groups you must use the user identifier.
    role : str, default="Member"
        The role of the user in the workspace. Options are: 'Admin', 'Contributor', 'Member', 'None', 'Viewer'.
    principal_type : str, default="User"
        The principal type of the user. Options are: 'App', 'Group', 'None', 'User'.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = _resolve_workspace_name_and_id(workspace)

    # Validation
    role = role.capitalize()
    roles = ["Admin", "Contributor", "Member", "None", "Viewer"]
    if role not in roles:
        raise ValueError(f"{icons.red_dot} Invalid role. Please choose from {roles}")
    principal_types = ["App", "Group", "None", "User"]
    if principal_type not in principal_types:
        raise ValueError(
            f"{icons.red_dot} Invalid principal type. Please choose from {principal_types}"
        )

    user = _encode_user(user)

    payload = {
        "identifier": user,  # identifier or emailAddress?
        "principalType": principal_type,
        "groupUserAccessRight": role,
    }

    _base_api(
        request=f"/v1.0/myorg/admin/groups/{workspace_id}/users",
        method="post",
        payload=payload,
    )

    print(
        f"{icons.green_dot} The '{user}' user has been added with '{role.lower()}' permissions to the '{workspace_name}' workspace."
    )


def delete_user_from_workspace(
    user: str | UUID,
    workspace: Optional[str | UUID] = None,
    is_group: Optional[bool] = None,
    profile_id: Optional[str] = None,
):
    """
    Removes user permissions from the specified workspace.

    This is a wrapper function for the following API: `Admin - Groups DeleteUserAsAdmin <https://learn.microsoft.com/rest/api/power-bi/admin/groups-delete-user-as-admin>`_.

    Parameters
    ----------
    user : str | uuid.UUID
        The user identifier or email address. For service principals and groups you must use the user identifier.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    is_group : bool, default=None
        Whether a given user is a group or not. This parameter is required when user to delete is group.
    profile_id : str, default=None
        The service principal profile ID to delete.
    """

    (workspace_name, workspace_id) = _resolve_workspace_name_and_id(workspace)

    user = _encode_user(user)
    url = f"/v1.0/myorg/admin/groups/{workspace_id}/users/{user}"

    params = {}
    if profile_id is not None:
        params["profileId"] = profile_id
    if is_group is not None:
        params["isGroup"] = is_group

    url = _build_url(url, params)

    _base_api(
        request=url,
        method="delete",
    )

    print(
        f"{icons.green_dot} The '{user}' user has been removed from the '{workspace_name}' workspace."
    )


def restore_deleted_workspace(workspace_id: UUID, name: str, email_address: str):
    """
    Restores a deleted workspace.

    This is a wrapper function for the following API: `Admin - Groups RestoreDeletedGroupAsAdmin <https://learn.microsoft.com/rest/api/power-bi/admin/groups-restore-deleted-group-as-admin>`_.

    Parameters
    ----------
    workspace_id : uuid.UUID
        The ID of the workspace to restore.
    name : str
        The name of the group to be restored
    email_address : str
        The email address of the owner of the group to be restored
    """

    payload = {
        "name": name,
        "emailAddress": email_address,
    }

    _base_api(
        request=f"/v1.0/myorg/admin/groups/{workspace_id}/restore",
        method="post",
        payload=payload,
    )

    print(
        f"{icons.green_dot} The '{workspace_id}' workspace has been restored as '{name}'."
    )
