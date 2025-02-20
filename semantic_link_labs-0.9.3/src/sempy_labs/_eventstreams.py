import sempy.fabric as fabric
import pandas as pd
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    _base_api,
    _print_success,
    resolve_item_id,
    _create_dataframe,
)
from uuid import UUID


def list_eventstreams(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows the eventstreams within a workspace.

    This is a wrapper function for the following API: `Items - List Eventstreams <https://learn.microsoft.com/rest/api/fabric/environment/items/list-eventstreams>`_.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the eventstreams within a workspace.
    """

    columns = {
        "Eventstream Name": "string",
        "Eventstream Id": "string",
        "Description": "string",
    }
    df = _create_dataframe(columns=columns)

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/eventstreams", uses_pagination=True
    )

    for r in responses:
        for v in r.get("value", []):
            new_data = {
                "Eventstream Name": v.get("displayName"),
                "Eventstream Id": v.get("id"),
                "Description": v.get("description"),
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def create_eventstream(
    name: str, description: Optional[str] = None, workspace: Optional[str | UUID] = None
):
    """
    Creates a Fabric eventstream.

    This is a wrapper function for the following API: `Items - Create Eventstream <https://learn.microsoft.com/rest/api/fabric/environment/items/create-eventstream>`_.

    Parameters
    ----------
    name: str
        Name of the eventstream.
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
        request=f"/v1/workspaces/{workspace_id}/eventstreams",
        method="post",
        payload=payload,
        status_codes=[201, 202],
        lro_return_status_code=True,
    )
    _print_success(
        item_name=name,
        item_type="eventstream",
        workspace_name=workspace_name,
        action="created",
    )


def delete_eventstream(name: str | UUID, workspace: Optional[str | UUID] = None):
    """
    Deletes a Fabric eventstream.

    This is a wrapper function for the following API: `Items - Delete Eventstream <https://learn.microsoft.com/rest/api/fabric/environment/items/delete-eventstream>`_.

    Parameters
    ----------
    name: str | uuid.UUID
        Name or ID of the eventstream.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    item_id = resolve_item_id(item=name, type="Eventstream", workspace=workspace)

    fabric.delete_item(item_id=item_id, workspace=workspace)
    _print_success(
        item_name=name,
        item_type="eventstream",
        workspace_name=workspace_name,
        action="deleted",
    )
