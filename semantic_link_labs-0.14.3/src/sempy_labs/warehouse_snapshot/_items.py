from sempy_labs._helper_functions import (
    resolve_workspace_id,
    _base_api,
    _create_dataframe,
    _update_dataframe_datatypes,
    resolve_item_id,
    delete_item,
    resolve_workspace_name_and_id,
)
import pandas as pd
from typing import Optional
from uuid import UUID
from sempy._utils._log import log
from datetime import datetime
import sempy_labs._icons as icons


@log
def list_warehouse_snapshots(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Lists all warehouse snapshots for a given warehouse.

    This is a wrapper function for the following API: `Items - List Warehouse Snapshots <https://learn.microsoft.com/rest/api/fabric/warehousesnapshot/items/list-warehouse-snapshots>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas DataFrame containing the list of warehouse snapshots within the workspace.
    """

    workspace_id = resolve_workspace_id(workspace)

    columns = {
        "Warehouse Snapshot Id": "string",
        "Warehouse Snapshot Name": "string",
        "Description": "string",
        "Connection String": "string",
        "Parent Warehouse Id": "string",
        "Snapshot DateTime": "datetime",
    }

    df = _create_dataframe(columns=columns)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/warehousesnapshots",
        client="fabric_sp",
        uses_pagination=True,
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            rows.append(
                {
                    "Warehouse Snapshot Id": v.get("id"),
                    "Warehouse Snapshot Name": v.get("displayName"),
                    "Description": v.get("properties", {}).get("description"),
                    "Connection String": v.get("properties", {}).get(
                        "connectionString"
                    ),
                    "Parent Warehouse Id": v.get("properties", {}).get(
                        "parentWarehouseId"
                    ),
                    "Snapshot DateTime": v.get("properties", {}).get(
                        "snapshotDateTime"
                    ),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
def delete_warehouse_snapshot(
    warehouse_snapshot: str | UUID,
    workspace: Optional[str | UUID] = None,
):
    """
    Deletes a warehouse snapshot in the Fabric workspace.

    This is a wrapper function for the following API: `Items - Delete Warehouse Snapshot <https://learn.microsoft.com/rest/api/fabric/warehousesnapshot/items/delete-warehouse-snapshot>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).
    """
    delete_item(item=warehouse_snapshot, type="WarehouseSnapshot", workspace=workspace)


@log
def create_warehouse_snapshot(
    name: str,
    parent_warehouse: str | UUID,
    parent_warehouse_workspace: Optional[str | UUID] = None,
    warehouse_snapshot_workspace: Optional[str | UUID] = None,
    description: Optional[str] = None,
    snapshot_datetime: Optional[datetime] = None,
):
    """
    Creates a Warehouse snapshot in the specified workspace.

    This is a wrapper function for the following API: `Items - Create Warehouse Snapshot <https://learn.microsoft.com/rest/api/fabric/warehousesnapshot/items/create-warehouse-snapshot>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    name : str
        The name of the warehouse snapshot to create.
    parent_warehouse : str | uuid.UUID
        The name or ID of the parent warehouse to snapshot.
    parent_warehouse_workspace : str | uuid.UUID, default=None
        The workspace name or ID where the parent warehouse is located.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    warehouse_snapshot_workspace : str | uuid.UUID, default=None
        The workspace name or ID where the warehouse snapshot will be created.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    description : str, default=None
        A description for the warehouse snapshot.
    snapshot_datetime : datetime, default=None
        The datetime for the snapshot. If not provided, the current datetime will be used.
        Example: "2024-10-15T13:00:00Z"
    """
    parent_warehouse_workspace_id = resolve_workspace_id(parent_warehouse_workspace)
    (warehouse_snapshot_workspace_name, warehouse_snapshot_workspace_id) = (
        resolve_workspace_name_and_id(warehouse_snapshot_workspace)
    )
    parent_warehouse_id = resolve_item_id(
        item=parent_warehouse, type="Warehouse", workspace=parent_warehouse_workspace_id
    )

    payload = {
        "displayName": name,
        "creationPayload": {
            "parentWarehouseId": parent_warehouse_id,
        },
    }
    if description:
        payload["description"] = description
    if snapshot_datetime:
        payload["creationPayload"]["snapshotDateTime"] = snapshot_datetime

    _base_api(
        request=f"/v1/workspaces/{warehouse_snapshot_workspace_id}/warehousesnapshots",
        client="fabric_sp",
        method="post",
        payload=payload,
        status_codes=[201, 202],
        lro_return_status_code=True,
    )

    print(
        f"{icons.green_dot} The warehouse snapshot '{name}' has been created in the workspace '{warehouse_snapshot_workspace_name}'."
    )


@log
def update_warehouse_snapshot(
    warehouse_snapshot: str | UUID,
    name: Optional[str] = None,
    description: Optional[str] = None,
    snapshot_datetime: Optional[datetime] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Updates the properties of a warehouse snapshot in the Fabric workspace.

    This is a wrapper function for the following API: `Items - Update Warehouse Snapshot <https://learn.microsoft.com/rest/api/fabric/warehousesnapshot/items/update-warehouse-snapshot>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    warehouse_snapshot : str | uuid.UUID
        The name or ID of the warehouse snapshot to update.
    name : str, optional
        The new name for the warehouse snapshot.
    description : str, optional
        The new description for the warehouse snapshot.
    snapshot_datetime : datetime, optional
        The new snapshot datetime for the warehouse snapshot.
        Example: "2024-10-15T13:00:00Z"
    workspace : str | uuid.UUID, optional
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """
    workspace_id = resolve_workspace_id(workspace)
    warehouse_snapshot_id = resolve_item_id(
        item=warehouse_snapshot, type="WarehouseSnapshot", workspace=workspace_id
    )

    payload = {}

    if name:
        payload["displayName"] = name
    if description:
        payload["description"] = description
    if snapshot_datetime:
        payload["properties"] = {"snapshotDateTime": snapshot_datetime}

    if not payload:
        print(
            f"{icons.yellow_dot} No updates provided for warehouse snapshot '{warehouse_snapshot}'."
        )
        return

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/warehousesnapshots/{warehouse_snapshot_id}",
        client="fabric_sp",
        method="patch",
        payload=payload,
    )

    print(
        f"{icons.green_dot} The warehouse snapshot '{warehouse_snapshot}' has been updated."
    )
