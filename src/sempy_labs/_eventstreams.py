import sempy.fabric as fabric
import pandas as pd
import sempy_labs._icons as icons
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    lro,
    pagination,
)
from sempy.fabric.exceptions import FabricHTTPException
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

    df = pd.DataFrame(columns=["Eventstream Name", "Eventstream Id", "Description"])

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/eventstreams")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

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

    request_body = {"displayName": name}

    if description:
        request_body["description"] = description

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/workspaces/{workspace_id}/eventstreams", json=request_body
    )

    lro(client, response, status_codes=[201, 202])

    print(
        f"{icons.green_dot} The '{name}' eventstream has been created within the '{workspace_name}' workspace."
    )


def delete_eventstream(name: str, workspace: Optional[str | UUID] = None):
    """
    Deletes a Fabric eventstream.

    This is a wrapper function for the following API: `Items - Delete Eventstream <https://learn.microsoft.com/rest/api/fabric/environment/items/delete-eventstream>`_.

    Parameters
    ----------
    name: str
        Name of the eventstream.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    item_id = fabric.resolve_item_id(
        item_name=name, type="Eventstream", workspace=workspace_id
    )

    client = fabric.FabricRestClient()
    response = client.delete(f"/v1/workspaces/{workspace_id}/eventstreams/{item_id}")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The '{name}' eventstream within the '{workspace_name}' workspace has been deleted."
    )
