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
    name: str,
    description: Optional[str] = None,
    workspace: Optional[str] = None,
    token_provider: Optional[str] = None,
):
    f"""
    Creates a Fabric eventhouse.

    This is a wrapper function for the following API: `Items - Create Eventhouse <https://learn.microsoft.com/rest/api/fabric/environment/items/create-eventhouse>`_.

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
    token_provider : str, default=None
        {icons.token_provider_desc}
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    request_body = {"displayName": name}

    if description:
        request_body["description"] = description

    client = fabric.FabricRestClient(token_provider=token_provider)
    response = client.post(
        f"/v1/workspaces/{workspace_id}/eventhouses", json=request_body
    )

    lro(client, response, status_codes=[201, 202])

    print(
        f"{icons.green_dot} The '{name}' eventhouse has been created within the '{workspace}' workspace."
    )


def list_eventhouses(
    workspace: Optional[str] = None,
    token_provider: Optional[str] = None,
) -> pd.DataFrame:
    f"""
    Shows the eventhouses within a workspace.

    This is a wrapper function for the following API: `Items - List Eventhouses <https://learn.microsoft.com/rest/api/fabric/environment/items/list-eventhouses>`_.

    Parameters
    ----------
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    token_provider : str, default=None
        {icons.token_provider_desc}

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the eventhouses within a workspace.
    """

    df = pd.DataFrame(columns=["Eventhouse Name", "Eventhouse Id", "Description"])

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient(token_provider=token_provider)
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


def delete_eventhouse(
    name: str, workspace: Optional[str] = None, token_provider: Optional[str] = None
):
    f"""
    Deletes a Fabric eventhouse.

    This is a wrapper function for the following API: `Items - Delete Eventhouse <https://learn.microsoft.com/rest/api/fabric/environment/items/delete-eventhouse>`_.

    Parameters
    ----------
    name: str
        Name of the eventhouse.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    token_provider : str, default=None
        {icons.token_provider_desc}
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    item_id = fabric.resolve_item_id(
        item_name=name, type="Eventhouse", workspace=workspace
    )

    client = fabric.FabricRestClient(token_provider=token_provider)
    response = client.delete(f"/v1/workspaces/{workspace_id}/eventhouses/{item_id}")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The '{name}' eventhouse within the '{workspace}' workspace has been deleted."
    )
