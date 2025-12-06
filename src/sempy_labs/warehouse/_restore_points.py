from sempy_labs._helper_functions import (
    resolve_item_id,
    resolve_item_name_and_id,
    resolve_workspace_id,
    _base_api,
    _create_dataframe,
    _update_dataframe_datatypes,
    resolve_workspace_name_and_id,
)
import pandas as pd
from typing import Optional
from uuid import UUID
from sempy._utils._log import log
import sempy_labs._icons as icons


@log
def list_restore_points(
    warehouse: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Returns all restore points for a warehouse.

    This is a wrapper function for the following API: `Restore Points - List Restore Points <https://learn.microsoft.com/rest/api/fabric/warehouse/restore-points/list-restore-points>`_.

    Parameters
    ----------
    warehouse : str | uuid.UUID
        The Fabric warehouse name or ID.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing all restore points for a warehouse.
    """

    workspace_id = resolve_workspace_id(workspace)
    warehouse_id = resolve_item_id(
        item=warehouse, type="warehouse", workspace=workspace_id
    )

    columns = {
        "Restore Point Id": "string",
        "Restore Point Name": "string",
        "Description": "string",
        "Creation Mode": "string",
        "Event DateTime": "datetime",
        "Event Initiator Id": "string",
        "Event Initiator Display Name": "string",
        "Event Initiator Type": "string",
        "Event Initiator User Principal Name": "string",
    }
    df = _create_dataframe(columns=columns)
    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/warehouses/{warehouse_id}/restorePoints",
        uses_pagination=True,
        client="fabric_sp",
    )

    rows = []
    for r in responses:
        for v in r.get("values", []):
            cd = v.get("creationDetails", {})
            event_initiator = cd.get("eventInitiator", {})
            rows.append(
                {
                    "Restore Point Id": v.get("id"),
                    "Restore Point Name": v.get("displayName"),
                    "Description": v.get("description"),
                    "Creation Mode": v.get("creationMode"),
                    "Event DateTime": cd.get("eventDateTime"),
                    "Event Initiator Id": event_initiator.get("id"),
                    "Event Initiator Display Name": event_initiator.get("displayName"),
                    "Event Initiator Type": event_initiator.get("type"),
                    "Event Initiator User Principal Name": event_initiator.get(
                        "userDetails", {}
                    ).get("userPrincipalName"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(df=df, columns=columns)

    return df


def _resolve_restore_point_id(
    warehouse_id: UUID, restore_point: str | UUID, workspace_id: UUID
) -> str:

    df = list_restore_points(warehouse=warehouse_id, workspace=workspace_id)
    df_filt = df[
        (df["Restore Point Id"] == restore_point)
        | (df["Restore Point Name"] == restore_point)
    ]

    if df_filt.empty:
        raise ValueError(
            f"Restore Point '{restore_point}' not found in the '{warehouse_id}' warehouse within the '{workspace_id}' workspace."
        )
    return df_filt["Restore Point Id"].iloc[0]


@log
def delete_restore_point(
    warehouse: str | UUID,
    restore_point: str | UUID,
    workspace: Optional[str | UUID] = None,
):
    """
    Deletes a restore point from a warehouse.

    This is a wrapper function for the following API: `Restore Points - Delete Restore Point <https://learn.microsoft.com/rest/api/fabric/warehouse/restore-points/delete-restore-point>`_.

    Parameters
    ----------
    warehouse : str | uuid.UUID
        The Fabric warehouse name or ID.
    restore_point : str | uuid.UUID
        The restore point name or ID.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (warehouse_name, warehouse_id) = resolve_item_name_and_id(
        item=warehouse, type="Warehouse", workspace=workspace_id
    )
    restore_point_id = _resolve_restore_point_id(
        warehouse_id=warehouse_id,
        restore_point=restore_point,
        workspace_id=workspace_id,
    )

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/warehouses/{warehouse_id}/restorePoints/{restore_point_id}",
        method="delete",
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The restore point '{restore_point}' has been deleted from the '{warehouse_name}' warehouse within the '{workspace_name}' workspace."
    )


@log
def create_restore_point(
    warehouse: str | UUID,
    name: str,
    description: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Creates a restore point in a warehouse.

    This is a wrapper function for the following API: `Restore Points - Create Restore Point <https://learn.microsoft.com/rest/api/fabric/warehouse/restore-points/create-restore-point>`_.

    Parameters
    ----------
    warehouse : str | uuid.UUID
        The Fabric warehouse name or ID.
    name : str
        The name of the restore point.
    description : str, default=None
        The description of the restore point.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (warehouse_name, warehouse_id) = resolve_item_name_and_id(
        item=warehouse, type="Warehouse", workspace=workspace_id
    )

    payload = {"displayName": name}

    if description:
        payload["description"] = description

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/warehouses/{warehouse_id}/restorePoints",
        method="post",
        client="fabric_sp",
        payload=payload,
        lro_return_status_code=True,
        status_codes=[201, 202],
    )

    print(
        f"{icons.green_dot} The restore point '{name}' has been created in the '{warehouse_name}' warehouse within the '{workspace_name}' workspace."
    )


@log
def update_restore_point(
    warehouse: str | UUID,
    restore_point: str | UUID,
    name: Optional[str] = None,
    description: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Updates a restore point in a warehouse.

    This is a wrapper function for the following API: `Restore Points - Update Restore Point <https://learn.microsoft.com/rest/api/fabric/warehouse/restore-points/update-restore-point>`_.

    Parameters
    ----------
    warehouse : str | uuid.UUID
        The Fabric warehouse name or ID.
    restore_point : str | uuid.UUID
        The restore point name or ID.
    name : str, default=None
        The new name of the restore point.
    description : str, default=None
        The new description of the restore point.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (warehouse_name, warehouse_id) = resolve_item_name_and_id(
        item=warehouse, type="Warehouse", workspace=workspace_id
    )
    restore_point_id = _resolve_restore_point_id(
        warehouse_id=warehouse_id,
        restore_point=restore_point,
        workspace_id=workspace_id,
    )

    payload = {}
    if name:
        payload["displayName"] = name
    if description:
        payload["description"] = description

    if not payload:
        print(
            f"{icons.yellow_dot} No updates provided for the restore point '{restore_point}'."
        )
        return

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/warehouses/{warehouse_id}/restorePoints/{restore_point_id}",
        method="patch",
        client="fabric_sp",
        payload=payload,
    )

    print(
        f"{icons.green_dot} The restore point '{restore_point}' has been updated in the '{warehouse_name}' warehouse within the '{workspace_name}' workspace."
    )


@log
def restore_to_restore_point(
    warehouse: str | UUID,
    restore_point: str | UUID,
    workspace: Optional[str | UUID] = None,
):
    """
    Restores a warehouse in-place to the restore point specified.

    This is a wrapper function for the following API: `Restore Points - Restore To Restore Point <https://learn.microsoft.com/rest/api/fabric/warehouse/restore-points/restore-to-restore-point>`_.

    Parameters
    ----------
    warehouse : str | uuid.UUID
        The Fabric warehouse name or ID.
    restore_point : str | uuid.UUID
        The restore point name or ID.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (warehouse_name, warehouse_id) = resolve_item_name_and_id(
        item=warehouse, type="Warehouse", workspace=workspace_id
    )
    restore_point_id = _resolve_restore_point_id(
        warehouse_id=warehouse_id,
        restore_point=restore_point,
        workspace_id=workspace_id,
    )

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/warehouses/{warehouse_id}/restorePoints/{restore_point_id}/restore",
        method="post",
        client="fabric_sp",
        lro_return_status_code=True,
        status_codes=[200, 202],
    )

    print(
        f"{icons.green_dot} The warehouse '{warehouse_name}' is being restored to the restore point '{restore_point}' within the '{workspace_name}' workspace."
    )
