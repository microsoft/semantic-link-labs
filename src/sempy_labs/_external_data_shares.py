from uuid import UUID
import pandas as pd
from typing import Optional, List
import sempy_labs._icons as icons
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    _base_api,
    _create_dataframe,
    resolve_item_id,
    resolve_item_name_and_id,
    resolve_workspace_id,
)
from sempy._utils._log import log


@log
def create_external_data_share(
    item_name: str,
    item_type: str,
    paths: str | List[str],
    recipient: str,
    workspace: Optional[str | UUID] = None,
):
    """
    Creates an external data share for a given path or list of paths in the specified item.

    This is a wrapper function for the following API: `External Data Shares - Create External Data Share <https://learn.microsoft.com/rest/api/fabric/core/external-data-shares/create-external-data-share>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

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
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    item_id = resolve_item_id(item=item_name, type=item_type, workspace=workspace_id)

    if isinstance(paths, str):
        paths = [paths]

    payload = {"paths": paths, "recipient": {"userPrincipalName": recipient}}

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/items/{item_id}/externalDataShares",
        method="post",
        status_codes=201,
        payload=payload,
        client="fabric_sp",
    )
    print(
        f"{icons.green_dot} An external data share was created for the '{item_name}' {item_type} within the '{workspace_name}' workspace for the {paths} paths."
    )


@log
def revoke_external_data_share(
    external_data_share_id: UUID,
    item_name: str,
    item_type: str,
    workspace: Optional[str | UUID] = None,
):
    """
    Revokes the specified external data share. Note: This action cannot be undone.

    This is a wrapper function for the following API: `External Data Shares - Revoke External Data Share <https://learn.microsoft.com/rest/api/fabric/core/external-data-shares/revoke-external-data-share`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    external_data_share_id : uuid.UUID
        The external data share ID.
    item_name : str
        The item name.
    item_type : str
        The `item type <https://learn.microsoft.com/rest/api/fabric/core/items/list-items?tabs=HTTP#itemtype>`_.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    item_id = resolve_item_id(item=item_name, type=item_type, workspace=workspace_id)

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/items/{item_id}/externalDataShares/{external_data_share_id}/revoke",
        method="post",
        client="fabric_sp",
    )
    print(
        f"{icons.green_dot} The '{external_data_share_id}' external data share for the '{item_name}' {item_type} within the '{workspace_name}' workspace has been revoked."
    )


@log
def list_external_data_shares_in_item(
    item_name: str, item_type: str, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Returns a list of the external data shares that exist for the specified item.

    This is a wrapper function for the following API: `External Data Shares - List External Data Shares In Item <https://learn.microsoft.com/rest/api/fabric/core/external-data-shares/list-external-data-shares-in-item`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    item_name : str
        The item name.
    item_type : str
        The `item type <https://learn.microsoft.com/rest/api/fabric/core/items/list-items?tabs=HTTP#itemtype>`_.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of the external data shares that exist for the specified item.
    """

    workspace_id = resolve_workspace_id(workspace)
    item_id = resolve_item_id(item=item_name, type=item_type, workspace=workspace_id)

    columns = {
        "External Data Share Id": "string",
        "Paths": "string",
        "Creator Principal Id": "string",
        "Creator Principal Type": "string",
        "Recipient User Principal Name": "string",
        "Status": "string",
        "Expiration Time UTC": "string",
        "Workspace Id": "string",
        "Item Id": "string",
        "Item Name": "string",
        "Item Type": "string",
        "Invitation URL": "string",
    }
    df = _create_dataframe(columns=columns)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/items/{item_id}/externalDataShares",
        uses_pagination=True,
        client="fabric_sp",
    )

    rows = []
    for r in responses:
        for i in r.get("value", []):
            item_id = i.get("itemId")
            rows.append(
                {
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
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def delete_external_data_share(
    external_data_share_id: UUID,
    item: str | UUID,
    item_type: str,
    workspace: Optional[str | UUID] = None,
):
    """
    Deletes the specified external data share.

    This is a wrapper function for the following API: `External Data Shares Provider - Delete External Data Share <https://learn.microsoft.com/rest/api/fabric/core/external-data-shares-provider/delete-external-data-share`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    external_data_share_id : uuid.UUID
        The external data share ID.
    item : str | uuid.UUID
        The item name or ID.
    item_type : str
        The `item type <https://learn.microsoft.com/rest/api/fabric/core/items/list-items?tabs=HTTP#itemtype>`_.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (item_name, item_id) = resolve_item_name_and_id(
        item=item, type=item_type, workspace=workspace_id
    )

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/items/{item_id}/externalDataShares/{external_data_share_id}",
        method="delete",
        client="fabric_sp",
    )
    print(
        f"{icons.green_dot} The '{external_data_share_id}' external data share for the '{item_name}' {item_type} within the '{workspace_name}' workspace has been revoked."
    )
