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


def create_eventhouse(
    name: str, description: Optional[str] = None, workspace: Optional[str] = None
):
    """
    Creates a Fabric eventhouse.

    Parameters
    ----------
    name: str
        Name of the eventhouse.
    description : str, default=None
        A description of the environment.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    request_body = {"displayName": name}

    if description:
        request_body["description"] = description

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/workspaces/{workspace_id}/eventhouses", json=request_body
    )

    lro(client, response, status_codes=[201, 202])

    print(
        f"{icons.green_dot} The '{name}' eventhouse has been created within the '{workspace}' workspace."
    )


def list_eventhouses(workspace: Optional[str] = None) -> pd.DataFrame:
    """
    Shows the eventhouses within a workspace.

    Parameters
    ----------
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the eventhouses within a workspace.
    """

    df = pd.DataFrame(columns=["Eventhouse Name", "Eventhouse Id", "Description"])

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/eventhouses")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    for r in responses:
        for v in r.get("value", []):
            new_data = {
                "Eventhouse Name": v.get("displayName"),
                "Eventhouse Id": v.get("id"),
                "Description": v.get("description"),
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def delete_eventhouse(name: str, workspace: Optional[str] = None):
    """
    Deletes a Fabric eventhouse.

    Parameters
    ----------
    name: str
        Name of the eventhouse.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    item_id = fabric.resolve_item_id(
        item_name=name, type="Eventhouse", workspace=workspace
    )

    client = fabric.FabricRestClient()
    response = client.delete(f"/v1/workspaces/{workspace_id}/eventhouses/{item_id}")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The '{name}' eventhouse within the '{workspace}' workspace has been deleted."
    )
