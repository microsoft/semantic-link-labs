import sempy.fabric as fabric
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    pagination,
    lro,
)
import pandas as pd
from typing import Optional
import sempy_labs._icons as icons
from sempy.fabric.exceptions import FabricHTTPException
from uuid import UUID


def create_warehouse(
    warehouse: str,
    description: Optional[str] = None,
    case_insensitive_collation: bool = False,
    workspace: Optional[str | UUID] = None,
):
    """
    Creates a Fabric warehouse.

    This is a wrapper function for the following API: `Items - Create Warehouse <https://learn.microsoft.com/rest/api/fabric/warehouse/items/create-warehouse>`_.

    Parameters
    ----------
    warehouse: str
        Name of the warehouse.
    description : str, default=None
        A description of the warehouse.
    case_insensitive_collation: bool, default=False
        If True, creates the warehouse with case-insensitive collation.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    request_body = {"displayName": warehouse}

    if description:
        request_body["description"] = description
    if case_insensitive_collation:
        request_body.setdefault("creationPayload", {})
        request_body["creationPayload"][
            "defaultCollation"
        ] = "Latin1_General_100_CI_AS_KS_WS_SC_UTF8"

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/workspaces/{workspace_id}/warehouses/", json=request_body
    )

    lro(client, response, status_codes=[201, 202])

    print(
        f"{icons.green_dot} The '{warehouse}' warehouse has been created within the '{workspace_name}' workspace."
    )


def list_warehouses(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows the warehouses within a workspace.

    This is a wrapper function for the following API: `Items - List Warehouses <https://learn.microsoft.com/rest/api/fabric/warehouse/items/list-warehouses>`_.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the warehouses within a workspace.
    """

    df = pd.DataFrame(
        columns=[
            "Warehouse Name",
            "Warehouse Id",
            "Description",
            "Connection Info",
            "Created Date",
            "Last Updated Time",
        ]
    )

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/warehouses")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    for r in responses:
        for v in r.get("value", []):
            prop = v.get("properties", {})

            new_data = {
                "Warehouse Name": v.get("displayName"),
                "Warehouse Id": v.get("id"),
                "Description": v.get("description"),
                "Connection Info": prop.get("connectionInfo"),
                "Created Date": prop.get("createdDate"),
                "Last Updated Time": prop.get("lastUpdatedTime"),
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def delete_warehouse(name: str, workspace: Optional[str | UUID] = None):
    """
    Deletes a Fabric warehouse.

    This is a wrapper function for the following API: `Items - Delete Warehouse <https://learn.microsoft.com/rest/api/fabric/warehouse/items/delete-warehouse>`_.

    Parameters
    ----------
    name: str
        Name of the warehouse.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    item_id = fabric.resolve_item_id(
        item_name=name, type="Warehouse", workspace=workspace_id
    )

    client = fabric.FabricRestClient()
    response = client.delete(f"/v1/workspaces/{workspace_id}/warehouses/{item_id}")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The '{name}' warehouse within the '{workspace_name}' workspace has been deleted."
    )


def get_warehouse_tables(
    warehouse: str, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Shows a list of the tables in the Fabric warehouse. This function is based on INFORMATION_SCHEMA.TABLES.

    Parameters
    ----------
    warehouse : str
        Name of the Fabric warehouse.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of the tables in the Fabric warehouse.
    """

    from sempy_labs._sql import ConnectWarehouse

    with ConnectWarehouse(warehouse=warehouse, workspace=workspace) as sql:
        df = sql.query(
            """
        SELECT TABLE_SCHEMA AS [Schema], TABLE_NAME AS [Table Name], TABLE_TYPE AS [Table Type]
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        """
        )

    return df


def get_warehouse_columns(
    warehouse: str, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Shows a list of the columns in each table within the Fabric warehouse. This function is based on INFORMATION_SCHEMA.COLUMNS.

    Parameters
    ----------
    warehouse : str
        Name of the Fabric warehouse.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of the columns in each table within the Fabric warehouse.
    """

    from sempy_labs._sql import ConnectWarehouse

    with ConnectWarehouse(warehouse=warehouse, workspace=workspace) as sql:
        df = sql.query(
            """
        SELECT t.TABLE_SCHEMA AS [Schema], t.TABLE_NAME AS [Table Name], c.COLUMN_NAME AS [Column Name], c.DATA_TYPE AS [Data Type], c.IS_NULLABLE AS [Is Nullable], c.CHARACTER_MAXIMUM_LENGTH AS [Character Max Length]
        FROM INFORMATION_SCHEMA.TABLES AS t
        LEFT JOIN INFORMATION_SCHEMA.COLUMNS AS c
        ON t.TABLE_NAME = c.TABLE_NAME
        AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
        WHERE t.TABLE_TYPE = 'BASE TABLE'
        """
        )

    return df
