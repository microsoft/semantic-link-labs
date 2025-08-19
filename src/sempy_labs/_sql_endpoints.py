from typing import Optional, Literal
from uuid import UUID
import pandas as pd
from sempy_labs._helper_functions import (
    _base_api,
    _create_dataframe,
    resolve_workspace_name_and_id,
    resolve_item_name_and_id,
    _update_dataframe_datatypes,
    resolve_workspace_id,
)
import sempy_labs._icons as icons
from sempy._utils._log import log


@log
def list_sql_endpoints(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows the SQL endpoints within a workspace.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the SQL endpoints within a workspace.
    """

    columns = {
        "SQL Endpoint Id": "string",
        "SQL Endpoint Name": "string",
        "Description": "string",
    }
    df = _create_dataframe(columns=columns)

    workspace_id = resolve_workspace_id(workspace)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/sqlEndpoints", uses_pagination=True
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            rows.append(
                {
                    "SQL Endpoint Id": v.get("id"),
                    "SQL Endpoint Name": v.get("displayName"),
                    "Description": v.get("description"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def refresh_sql_endpoint_metadata(
    item: str | UUID,
    type: Literal["Lakehouse", "MirroredDatabase"],
    workspace: Optional[str | UUID] = None,
    tables: dict[str, list[str]] = None,
) -> pd.DataFrame:
    """
    Refreshes the metadata of a SQL endpoint.

    This is a wrapper function for the following API: `Items - Refresh Sql Endpoint Metadata <https://learn.microsoft.com/rest/api/fabric/sqlendpoint/items/refresh-sql-endpoint-metadata>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    item : str | uuid.UUID
        The name or ID of the item (Lakehouse or MirroredDatabase).
    type : Literal['Lakehouse', 'MirroredDatabase']
        The type of the item. Must be 'Lakehouse' or 'MirroredDatabase'.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    tables : dict[str, list[str]], default=None
        A dictionary where the keys are schema names and the values are lists of table names.
        If empty, all table metadata will be refreshed.

        Example:
        {
            "dbo": ["DimDate", "DimGeography"],
            "sls": ["FactSales", "FactBudget"],
        }

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the status of the metadata refresh operation.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    (item_name, item_id) = resolve_item_name_and_id(
        item=item, type=type, workspace=workspace
    )

    if type == "Lakehouse":
        response = _base_api(
            request=f"/v1/workspaces/{workspace_id}/lakehouses/{item_id}",
            client="fabric_sp",
        )
        sql_endpoint_id = (
            response.json()
            .get("properties", {})
            .get("sqlEndpointProperties", {})
            .get("id")
        )
    elif type == "MirroredDatabase":
        response = _base_api(
            request=f"/v1/workspaces/{workspace_id}/mirroredDatabases/{item_id}",
            client="fabric_sp",
        )
        sql_endpoint_id = (
            response.json()
            .get("properties", {})
            .get("sqlEndpointProperties", {})
            .get("id")
        )
    else:
        raise ValueError("Invalid type. Must be 'Lakehouse' or 'MirroredDatabase'.")

    payload = {}
    if tables:
        payload = {
            "tableDefinitions": [
                {"schema": schema, "tableNames": tables}
                for schema, tables in tables.items()
            ]
        }

    result = _base_api(
        request=f"v1/workspaces/{workspace_id}/sqlEndpoints/{sql_endpoint_id}/refreshMetadata",
        method="post",
        client="fabric_sp",
        status_codes=[200, 202],
        lro_return_json=True,
        payload=payload,
    )

    columns = {
        "Table Name": "string",
        "Status": "string",
        "Start Time": "datetime",
        "End Time": "datetime",
        "Last Successful Sync Time": "datetime",
        "Error Code": "string",
        "Error Message": "string",
    }

    if result:
        df = pd.json_normalize(result.get("value"))

        # Extract error code and message, set to None if no error
        df["Error Code"] = df.get("error.errorCode", None)
        df["Error Message"] = df.get("error.message", None)

        # Friendly column renaming
        df.rename(
            columns={
                "tableName": "Table Name",
                "startDateTime": "Start Time",
                "endDateTime": "End Time",
                "status": "Status",
                "lastSuccessfulSyncDateTime": "Last Successful Sync Time",
            },
            inplace=True,
        )

        # Drop the original 'error' column if present
        df.drop(columns=[col for col in ["error"] if col in df.columns], inplace=True)

        # Optional: Reorder columns
        column_order = [
            "Table Name",
            "Status",
            "Start Time",
            "End Time",
            "Last Successful Sync Time",
            "Error Code",
            "Error Message",
        ]
        df = df[column_order]

        printout = f"{icons.green_dot} The metadata of the SQL endpoint for the '{item_name}' {type.lower()} within the '{workspace_name}' workspace has been refreshed"
        if tables:
            print(f"{printout} for the following tables: {tables}.")
        else:
            print(f"{printout} for all tables.")
    else:
        # If the target item has no tables to refresh the metadata for
        df = pd.DataFrame(columns=columns.keys())
        print(
            f"{icons.yellow_dot} The SQL endpoint '{item_name}' {type.lower()} within the '{workspace_name}' workspace has no tables to refresh..."
        )

    _update_dataframe_datatypes(df, columns)

    return df
