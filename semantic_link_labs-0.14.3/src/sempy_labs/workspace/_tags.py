import sempy_labs._icons as icons
from typing import List, Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    _base_api,
    _is_valid_uuid,
)
from uuid import UUID
from sempy._utils._log import log


@log
def apply_workspace_tags(tags: str | List[str], workspace: Optional[str | UUID] = None):
    """
    Apply tags to a workspace.

    This is a wrapper function for the following API: `Workspaces - Apply Workspace Tags <https://learn.microsoft.com/rest/api/fabric/core/workspaces/apply-workspace-tags>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    tags : str | typing.List[str]
        A single tag as a string or a list of tags to apply to the workspace.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    # Ensure tags is a list
    if isinstance(tags, str):
        tags = [tags]

    for tag in tags:
        if not _is_valid_uuid(tag):
            raise ValueError(
                f"{icons.error} Invalid tag ID: '{tag}'. Tags must be valid UUIDs."
            )

    payload = {
        "tags": tags,
    }
    _base_api(
        request=f"/v1/workspaces/{workspace_id}/applyTags",
        method="post",
        payload=payload,
    )

    print(
        f"{icons.green_dot} Successfully applied tags to the '{workspace_name}' workspace."
    )


@log
def unapply_workspace_tags(
    tags: str | List[str], workspace: Optional[str | UUID] = None
):
    """
    Unapply tags from a workspace.

    This is a wrapper function for the following API: `Workspaces - Unapply Workspace Tags <https://learn.microsoft.com/rest/api/fabric/core/workspaces/unapply-workspace-tags>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    tags : str | typing.List[str]
        A single tag as a string or a list of tags to unapply from the workspace.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    # Ensure tags is a list
    if isinstance(tags, str):
        tags = [tags]

    for tag in tags:
        if not _is_valid_uuid(tag):
            raise ValueError(
                f"{icons.error} Invalid tag ID: '{tag}'. Tags must be valid UUIDs."
            )

    payload = {
        "tags": tags,
    }
    _base_api(
        request=f"/v1/workspaces/{workspace_id}/unapplyTags",
        method="post",
        payload=payload,
    )

    print(
        f"{icons.green_dot} Successfully unapplied tags from the '{workspace_name}' workspace."
    )
