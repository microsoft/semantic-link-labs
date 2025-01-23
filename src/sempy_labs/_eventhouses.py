import sempy.fabric as fabric
import pandas as pd
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    _base_api,
    _print_success,
    resolve_item_id,
)
from uuid import UUID


def create_eventhouse(
    name: str, description: Optional[str] = None, workspace: Optional[str | UUID] = None
):
    """
    Creates a Fabric eventhouse.

    This is a wrapper function for the following API: `Items - Create Eventhouse <https://learn.microsoft.com/rest/api/fabric/environment/items/create-eventhouse>`_.

    Parameters
    ----------
    name: str
        Name of the eventhouse.
    description : str, default=None
        A description of the environment.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    payload = {"displayName": name}

    if description:
        payload["description"] = description

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/eventhouses",
        method="post",
        status_codes=[201, 202],
        payload=payload,
        lro_return_status_code=True,
    )
    _print_success(
        item_name=name,
        item_type="eventhouse",
        workspace_name=workspace_name,
        action="created",
    )


def list_eventhouses(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows the eventhouses within a workspace.

    This is a wrapper function for the following API: `Items - List Eventhouses <https://learn.microsoft.com/rest/api/fabric/environment/items/list-eventhouses>`_.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the eventhouses within a workspace.
    """

    df = pd.DataFrame(columns=["Eventhouse Name", "Eventhouse Id", "Description"])

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/eventhouses", uses_pagination=True
    )

    for r in responses:
        for v in r.get("value", []):
            new_data = {
                "Eventhouse Name": v.get("displayName"),
                "Eventhouse Id": v.get("id"),
                "Description": v.get("description"),
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def delete_eventhouse(name: str, workspace: Optional[str | UUID] = None):
    """
    Deletes a Fabric eventhouse.

    This is a wrapper function for the following API: `Items - Delete Eventhouse <https://learn.microsoft.com/rest/api/fabric/environment/items/delete-eventhouse>`_.

    Parameters
    ----------
    name: str
        Name of the eventhouse.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    item_id = resolve_item_id(item=name, type="Eventhouse", workspace=workspace)

    fabric.delete_item(item_id=item_id, workspace=workspace)
    _print_success(
        item_name=name,
        item_type="eventhouse",
        workspace_name=workspace_name,
        action="deleted",
    )
