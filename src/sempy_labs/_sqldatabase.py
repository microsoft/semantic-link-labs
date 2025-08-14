from sempy_labs._helper_functions import (
    resolve_workspace_id,
    _base_api,
    _create_dataframe,
    _update_dataframe_datatypes,
    create_item,
    delete_item,
)
import pandas as pd
from typing import Optional
from uuid import UUID
from sempy._utils._log import log


@log
def create_sql_database(
    name: str, description: Optional[str] = None, workspace: Optional[str | UUID] = None
):
    """
    Creates a SQL database.

    This is a wrapper function for the following API: `Items - Create SQL Database <https://learn.microsoft.com/rest/api/fabric/sqldatabase/items/create-sql-database>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    name: str
        Name of the SQL database.
    description : str, default=None
        A description of the SQL database.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    create_item(
        name=name, description=description, type="SQLDatabase", workspace=workspace
    )


@log
def delete_sql_database(
    sql_database: str | UUID, workspace: Optional[str | UUID] = None
):
    """
    Deletes a SQL Database.

    This is a wrapper function for the following API: `Items - Delete SQL Database <https://learn.microsoft.com/rest/api/fabric/sqldatabase/items/delete-sql-database>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    sql_database: str | uuid.UUID
        Name of the SQL database.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    delete_item(item=sql_database, type="SQLDatabase", workspace=workspace)


@log
def list_sql_databases(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Lists all SQL databases in the Fabric workspace.

    This is a wrapper function for the following API: `Items - List SQL Databases <https://learn.microsoft.com/rest/api/fabric/sqldatabase/items/list-sql-databases>`_.

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
        A pandas dataframe showing a list of SQL databases in the Fabric workspace.
    """

    workspace_id = resolve_workspace_id(workspace)

    columns = {
        "SQL Database Name": "string",
        "SQL Database Id": "string",
        "Description": "string",
        "Connection Info": "string",
        "Database Name": "string",
        "Server FQDN": "string",
    }
    df = _create_dataframe(columns=columns)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/SQLDatabases",
        uses_pagination=True,
        client="fabric_sp",
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            prop = v.get("properties", {})
            rows.append(
                {
                    "SQL Database Name": v.get("displayName"),
                    "SQL Database Id": v.get("id"),
                    "Description": v.get("description"),
                    "Connection Info": prop.get("connectionInfo"),
                    "Database Name": prop.get("databaseName"),
                    "Server FQDN": prop.get("serverFqdn"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
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


@log
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
