import sempy.fabric as fabric
from uuid import UUID
import pandas as pd
from typing import Optional, List
import sempy_labs._icons as icons
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    pagination,
)
from sempy.fabric.exceptions import FabricHTTPException


def create_external_data_share(
    item_name: str,
    item_type: str,
    paths: str | List[str],
    recipient: str,
    workspace: Optional[str] = None,
):
    """
    Creates an external data share for a given path or list of paths in the specified item.

    Parameters
    ----------
    item_name : str
        The item name.
    item_type : str
        The `item type <https://learn.microsoft.com/rest/api/fabric/core/items/list-items?tabs=HTTP#itemtype>`_.
    paths : str | List[str]
        The path or list of paths that are to be externally shared. Currently, only a single path is supported.
    recipient : str
        The email address of the recipient.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    # https://learn.microsoft.com/en-us/rest/api/fabric/core/external-data-shares/create-external-data-share?tabs=HTTP

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)
    item_id = fabric.resolve_item_id(
        item_name=item_name, type=item_type, workspace=workspace
    )

    if isinstance(paths, str):
        paths = [paths]

    payload = {"paths": paths, "recipient": {"userPrincipalName": recipient}}

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/workspaces/{workspace_id}/items/{item_id}/externalDataShares",
        json=payload,
    )

    if response.status_code != 201:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} An external data share was created for the '{item_name}' {item_type} within the '{workspace}' workspace for the {paths} paths."
    )


def revoke_external_data_share(
    external_data_share_id: UUID,
    item_name: str,
    item_type: str,
    workspace: Optional[str] = None,
):
    """
    Revokes the specified external data share. Note: This action cannot be undone.

    Parameters
    ----------
    external_data_share_id : UUID
        The external data share ID.
    item_name : str
        The item name.
    item_type : str
        The `item type <https://learn.microsoft.com/rest/api/fabric/core/items/list-items?tabs=HTTP#itemtype>`_.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    # https://learn.microsoft.com/en-us/rest/api/fabric/core/external-data-shares/revoke-external-data-share?tabs=HTTP

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)
    item_id = fabric.resolve_item_id(
        item_name=item_name, type=item_type, workspace=workspace
    )

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/workspaces/{workspace_id}/items/{item_id}/externalDataShares/{external_data_share_id}/revoke"
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The '{external_data_share_id}' external data share for the '{item_name}' {item_type} within the '{workspace}' workspace has been revoked."
    )


def list_external_data_shares_in_item(
    item_name: str, item_type: str, workspace: Optional[str] = None
) -> pd.DataFrame:
    """
    Returns a list of the external data shares that exist for the specified item.

    Parameters
    ----------
    item_name : str
        The item name.
    item_type : str
        The `item type <https://learn.microsoft.com/rest/api/fabric/core/items/list-items?tabs=HTTP#itemtype>`_.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of the external data shares that exist for the specified item.
    """

    # https://learn.microsoft.com/en-us/rest/api/fabric/core/external-data-shares/list-external-data-shares-in-item?tabs=HTTP

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)
    item_id = fabric.resolve_item_id(
        item_name=item_name, type=item_type, workspace=workspace
    )

    client = fabric.FabricRestClient()
    response = client.get(
        f"/v1/workspaces/{workspace_id}/items/{item_id}/externalDataShares"
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)

    df = pd.DataFrame(
        columns=[
            "External Data Share Id",
            "Paths",
            "Creator Principal Id",
            "Creater Principal Type",
            "Recipient User Principal Name",
            "Status",
            "Expiration Time UTC",
            "Workspace Id",
            "Item Id",
            "Item Name",
            "Item Type",
            "Invitation URL",
        ]
    )

    responses = pagination(client, response)
    dfs = []

    for r in responses:
        for i in r.get("value", []):
            item_id = i.get("itemId")
            new_data = {
                "External Data Share Id": i.get("id"),
                "Paths": [i.get("paths")],
                "Creator Principal Id": i.get("creatorPrincipal", {}).get("id"),
                "Creator Principal Type": i.get("creatorPrincipal", {}).get("type"),
                "Recipient User Principal Name": i.get("recipient", {}).get(
                    "userPrincipalName"
                ),
                "Status": i.get("status"),
                "Expiration Time UTC": i.get("expriationTimeUtc"),
                "Workspace Id": i.get("workspaceId"),
                "Item Id": item_id,
                "Item Name": item_name,
                "Item Type": item_type,
                "Invitation URL": i.get("invitationUrl"),
            }
            dfs.append(pd.DataFrame(new_data, index=[0]))
    df = pd.concat(dfs, ignore_index=True)

    return df
