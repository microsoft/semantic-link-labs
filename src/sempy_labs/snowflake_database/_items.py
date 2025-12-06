from sempy_labs._helper_functions import (
    _base_api,
    _create_dataframe,
    _update_dataframe_datatypes,
    delete_item,
    resolve_workspace_id,
)
import pandas as pd
from typing import Optional
from uuid import UUID
from sempy._utils._log import log


@log
def list_snowflake_databases(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows the snowflake databases within a workspace.

    This is a wrapper function for the following API: `Items - List Snowflake Databases <https://learn.microsoft.com/rest/api/fabric/snowflakedatabase/items/list-snowflake-databases>`_.

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
        A pandas dataframe showing the snowflake databases within a workspace.
    """

    columns = {
        "Snowflake Database Display Name": "string",
        "Snowflake Database Id": "string",
        "Description": "string",
        "Snowflake Database Name": "string",
        "OneLake Tables Path": "string",
        "Snowflake Volume Path": "string",
        "SQL Endpoint Connection String": "string",
        "SQL Endpoint Id": "string",
        "SQL Endpoint Provisioning Status": "string",
        "Default Schema": "string",
    }
    df = _create_dataframe(columns=columns)

    workspace_id = resolve_workspace_id(workspace)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/snowflakeDatabases",
        uses_pagination=True,
        client="fabric_sp",
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            prop = v.get("properties", {})

            rows.append(
                {
                    "Snowflake Database Display Name": v.get("displayName"),
                    "Snowflake Database Id": v.get("id"),
                    "Description": v.get("description"),
                    "Snowflake Database Name": prop.get("snowflakeDatabaseName"),
                    "OneLake Tables Path": prop.get("onelakeTablesPath"),
                    "Snowflake Volume Path": prop.get("snowflakeVolumePath"),
                    "SQL Endpoint Connection String": prop.get(
                        "sqlEndpointProperties", {}
                    ).get("connectionString"),
                    "SQL Endpoint Id": prop.get("sqlEndpointProperties", {}).get("id"),
                    "SQL Endpoint Provisioning Status": prop.get(
                        "sqlEndpointProperties", {}
                    ).get("provisioningStatus"),
                    "Default Schema": prop.get("defaultSchema"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
def delete_snowflake_database(
    snowflake_database: str | UUID, workspace: Optional[str | UUID] = None
):
    """
    Deletes a Fabric snowflake database.

    This is a wrapper function for the following API: `Items - Delete Snowflake Database <https://learn.microsoft.com/rest/api/fabric/snowflakedatabase/items/delete-snowflake-database>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).
    """

    delete_item(
        item=snowflake_database,
        type="SnowflakeDatabase",
        workspace=workspace,
    )
