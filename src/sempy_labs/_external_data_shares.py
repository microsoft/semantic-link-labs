from uuid import UUID
import pandas as pd
from typing import Optional, List
from sempy._utils._log import log
import sempy_labs.external_data_share as eds


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

    eds.create_external_data_share(
        item_name=item_name,
        item_type=item_type,
        paths=paths,
        recipient=recipient,
        workspace=workspace,
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

    eds.revoke_external_data_share(
        external_data_share_id=external_data_share_id,
        item_name=item_name,
        item_type=item_type,
        workspace=workspace,
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

    return eds.list_external_data_shares_in_item(
        item_name=item_name, item_type=item_type, workspace=workspace
    )


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

    eds.delete_external_data_share(
        external_data_share_id=external_data_share_id,
        item=item,
        item_type=item_type,
        workspace=workspace,
    )
