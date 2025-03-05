from sempy_labs._helper_functions import (
    _base_api,
)
from uuid import UUID
from typing import Optional
from sempy_labs.admin._basic_functions import (
    _resolve_workspace_name_and_id,
)
import sempy_labs._icons as icons


def update_workspace(
    workspace: Optional[str | UUID] = None,
    name: Optional[str] = None,
    description: Optional[str] = None,
    # defaultDatasetStorageFormat: Optional[str] = None,
):
    """
    # https://learn.microsoft.com/en-us/rest/api/power-bi/admin/groups-update-group-as-admin#groupuser

    """

    (workspace_name, workspace_id) = _resolve_workspace_name_and_id(workspace)

    payload = {}
    if name:
        payload["name"] = name
    if description:
        payload["description"] = description

    if not payload:
        raise ValueError(f"{icons.red_dot} Nothing to update.")

    _base_api(
        request=f"/v1.0/myorg/admin/groups/{workspace_id}",
        method="patch",
        client="fabric_sp",
        payload=payload,
    )


def add_user_to_workspace(
    user: str,
    workspace: Optional[str | UUID] = None,
    role: str = "Member",
    principal_type: str = "User",
):
    """

    # https://learn.microsoft.com/en-us/rest/api/power-bi/admin/groups-add-user-as-admin
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

    payload = {
        "identifier": user,  # identifier or emailAddress?
        "principalType": principal_type,
        "groupUserAccessRight": role,
    }

    _base_api(
        request=f"/v1.0/myorg/admin/groups/{workspace_id}/users",
        method="post",
        client="fabric_sp",
        payload=payload,
    )

    print(
        f"{icons.green_dot} The '{user}' user has been added with '{role.lower()}' permissions to the '{workspace_name}' workspace."
    )
