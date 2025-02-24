import sempy.fabric as fabric
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    _base_api,
    _create_dataframe,
    _update_dataframe_datatypes,
)
import pandas as pd
from typing import Optional
import sempy_labs._icons as icons
from uuid import UUID

## Still debugging the creation of an sql database
# def create_warehouse(
#     warehouse: str,
#     description: Optional[str] = None,
#     case_insensitive_collation: bool = False,
#     workspace: Optional[str | UUID] = None,
# ):
#     """
#     Creates a Fabric warehouse.

#     This is a wrapper function for the following API: `Items - Create Warehouse <https://learn.microsoft.com/rest/api/fabric/warehouse/items/create-warehouse>`_.

#     Parameters
#     ----------
#     warehouse: str
#         Name of the warehouse.
#     description : str, default=None
#         A description of the warehouse.
#     case_insensitive_collation: bool, default=False
#         If True, creates the warehouse with case-insensitive collation.
#     workspace : str | uuid.UUID, default=None
#         The Fabric workspace name or ID.
#         Defaults to None which resolves to the workspace of the attached lakehouse
#         or if no lakehouse attached, resolves to the workspace of the notebook.
#     """

#     (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

#     payload = {"displayName": warehouse}

#     if description:
#         payload["description"] = description
#     if case_insensitive_collation:
#         payload.setdefault("creationPayload", {})
#         payload["creationPayload"][
#             "defaultCollation"
#         ] = "Latin1_General_100_CI_AS_KS_WS_SC_UTF8"

#     _base_api(
#         request=f"/v1/workspaces/{workspace_id}/warehouses",
#         payload=payload,
#         method="post",
#         lro_return_status_code=True,
#         status_codes=[201, 202],
#     )

#     print(
#         f"{icons.green_dot} The '{warehouse}' warehouse has been created within the '{workspace_name}' workspace."
#     )


def _list_sql_databases(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows the databses within a workspace.


    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the SQLDabatases within a workspace.
    """

    columns = {
        "SQL Database Name": "string",
        "SQL Database Id": "string",
        "Description": "string",
        "Connection Type": "string",
        "Connection Info": "string",
        "Database Name": "string",
        "Server FQDN": "string",
        "Provisioning Status": "string",
        "Created Date": "datetime",
        "Last Updated Time UTC": "datetime",
    }
    df = _create_dataframe(columns=columns)

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/sqldatabases", uses_pagination=True
    )

    for r in responses:
        for v in r.get("value", []):
            prop = v.get("properties", {})

            new_data = {
                "SQL Database Name": v.get("displayName"),
                "SQL Database Id": v.get("id"),
                "Description": v.get("description"),
                "Connection Type": v.get("type"),
                "Connection Info": prop.get("connectionInfo"),
                "Database Name": prop.get("databaseName"),
                "Server FQDN": prop.get("serverFqdn"),
                "Provisioning Status": prop.get("provisioningState"),
                "Created Date": prop.get("createdDate"),
                "Last Updated Time UTC": prop.get("lastUpdatedTimeUtc"),
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


## Still debugging the deletion of an sql database
# def delete_warehouse(name: str, workspace: Optional[str | UUID] = None):
#     """
#     Deletes a Fabric warehouse.

#     This is a wrapper function for the following API: `Items - Delete Warehouse <https://learn.microsoft.com/rest/api/fabric/warehouse/items/delete-warehouse>`_.

#     Parameters
#     ----------
#     name: str
#         Name of the warehouse.
#     workspace : str | uuid.UUID, default=None
#         The Fabric workspace name or ID.
#         Defaults to None which resolves to the workspace of the attached lakehouse
#         or if no lakehouse attached, resolves to the workspace of the notebook.
#     """

#     (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

#     item_id = fabric.resolve_item_id(
#         item_name=name, type="Warehouse", workspace=workspace_id
#     )

#     _base_api(
#         request=f"/v1/workspaces/{workspace_id}/warehouses/{item_id}", method="delete"
#     )

#     print(
#         f"{icons.green_dot} The '{name}' warehouse within the '{workspace_name}' workspace has been deleted."
#     )


def get_sql_database_tables(
    sql_database: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Shows a list of the tables in the Fabric SQLDabatse. This function is based on INFORMATION_SCHEMA.TABLES.

    Parameters
    ----------
    sql_database : str | uuid.UUID
        Name or ID of the Fabric SQLDabatase.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of the tables in the Fabric SQLDabatase.
    """

    from sempy_labs._sql import ConnectSQLDatabase

    with ConnectSQLDatabase(sql_database=sql_database, workspace=workspace) as sql:
        df = sql.query(
            """
        SELECT TABLE_SCHEMA AS [Schema], TABLE_NAME AS [Table Name], TABLE_TYPE AS [Table Type]
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        """
        )

    return df


def get_sql_database_columns(
    sql_database: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Shows a list of the columns in each table within the Fabric SQLDabatase. This function is based on INFORMATION_SCHEMA.COLUMNS.

    Parameters
    ----------
    sql_database : str | uuid.UUID
        Name or ID of the Fabric SQLDabatase.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of the columns in each table within the Fabric SQLDabatase.
    """

    from sempy_labs._sql import ConnectSQLDatabase

    with ConnectSQLDatabase(sql_database=sql_database, workspace=workspace) as sql:
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
