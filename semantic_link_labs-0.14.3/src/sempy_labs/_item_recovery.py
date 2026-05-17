from sempy._utils._log import log
import pandas as pd
from typing import Optional, Tuple
from sempy_labs._helper_functions import (
    _base_api,
    _create_dataframe,
    resolve_workspace_id,
    resolve_workspace_name_and_id,
    _is_valid_uuid,
)
from uuid import UUID
import sempy_labs._icons as icons


@log
def list_recoverable_items(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Returns a list of `recoverable <https://learn.microsoft.com/fabric/admin/item-recovery>`_ items (soft-deleted items which can be recovered).

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        Shows a list of recoverable items (soft-deleted items which can be recovered).
    """

    workspace_id = resolve_workspace_id(workspace)

    columns = {
        "Item Name": "str",
        "Item Id": "str",
        "Item Type": "str",
        "Description": "str",
        "Retention Expiration DateTime": "datetime",
        "Parent Item Id": "str",
        "Folder Id": "str",
    }

    df = _create_dataframe(columns=columns)
    response = _base_api(request=f"/v1/workspaces/{workspace_id}/recoverableItems")

    rows = []
    for item in response.json().get("value", []):
        rows.append(
            {
                "Item Name": item.get("displayName"),
                "Item Id": item.get("id"),
                "Item Type": item.get("type"),
                "Description": item.get("description"),
                "Retention Expiration DateTime": item.get(
                    "retentionExpirationDateTime"
                ),
                "Parent Item Id": item.get("parentItemId"),
                "Folder Id": item.get("folderId"),
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


def resolve_recoverable_item_id(
    item: str | UUID, type: Optional[str], workspace: Optional[str | UUID] = None
) -> Tuple[str, str, str, str]:

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    df = list_recoverable_items(workspace=workspace_id)
    if _is_valid_uuid(item):
        df_filt = df[df["Item Id"] == item]
    else:
        if type is None:
            raise ValueError(
                f"{icons.red_dot} If specifying the item name, you must also supply the item type."
            )
        df_filt = df[(df["Item Name"] == item) & (df["Item Type"] == type)]

    if df_filt.empty:
        raise ValueError(
            f"{icons.red_dot} The '{item}' was not found in the list of recoverable items for '{workspace_name}' workspace."
        )

    item_id = df_filt["Item Id"].iloc[0]
    item_type = df_filt["Item Type"].iloc[0]

    return (workspace_name, workspace_id, item_id, item_type)


@log
def recover_item(
    item: str | UUID, type: Optional[str] = None, workspace: Optional[str | UUID] = None
) -> dict:
    """
    Recovers a soft-deleted item in the specified workspace.

    Parameters
    ----------
    item : str | uuid.UUID
        The item name or ID. If specifying the item name, the item type is required.
    type : str, default=None
        The item `type <https://learn.microsoft.com/rest/api/fabric/core/items/list-items?tabs=HTTP#itemtype>`_. If specifying the item name as the item, the item type is required.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse

    Returns
    -------
    dict
        The API response as a dictionary.
    """

    (workspace_name, workspace_id, item_id, item_type) = resolve_recoverable_item_id(
        item=item, type=type, workspace=workspace
    )

    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/recoverableItems/{item_id}",
        method="post",
    )

    print(
        f"{icons.green_dot} The '{item}' item of type '{item_type}' within the '{workspace_name}' workspace has been recovered."
    )
    return response.json()


@log
def permanently_delete_item(
    item: str | UUID, type: Optional[str] = None, workspace: Optional[str | UUID] = None
) -> dict:
    """
    Permanently deletes a soft-deleted item in the specified workspace. This action cannot be undone.

    Parameters
    ----------
    item : str | uuid.UUID
        The item name or ID. If specifying the item name, the item type is required.
    type : str, default=None
        The item `type <https://learn.microsoft.com/rest/api/fabric/core/items/list-items?tabs=HTTP#itemtype>`_. If specifying the item name as the item, the item type is required.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    dict
        The API response as a dictionary.
    """

    (workspace_name, workspace_id, item_id, item_type) = resolve_recoverable_item_id(
        item=item, type=type, workspace=workspace
    )

    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/recoverableItems/{item_id}",
        method="delete",
    )

    print(
        f"{icons.green_dot} The '{item}' item of type '{item_type}' within the '{workspace_name}' workspace has been permanently deleted."
    )
    return response.json()
