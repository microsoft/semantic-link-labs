import pandas as pd
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    _base_api,
    _create_dataframe,
    delete_item,
    create_item,
)
from uuid import UUID
import sempy_labs._icons as icons


def list_kql_databases(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows the KQL databases within a workspace.

    This is a wrapper function for the following API: `Items - List KQL Databases <https://learn.microsoft.com/rest/api/fabric/kqldatabase/items/list-kql-databases>`_.

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
        A pandas dataframe showing the KQL databases within a workspace.
    """

    columns = {
        "KQL Database Name": "string",
        "KQL Database Id": "string",
        "Description": "string",
        "Parent Eventhouse Item Id": "string",
        "Query Service URI": "string",
        "Ingestion Service URI": "string",
        "Database Type": "string",
    }
    df = _create_dataframe(columns=columns)

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    responses = _base_api(
        request=f"v1/workspaces/{workspace_id}/kqlDatabases",
        uses_pagination=True,
        client="fabric_sp",
    )

    for r in responses:
        for v in r.get("value", []):
            prop = v.get("properties", {})

            new_data = {
                "KQL Database Name": v.get("displayName"),
                "KQL Database Id": v.get("id"),
                "Description": v.get("description"),
                "Parent Eventhouse Item Id": prop.get("parentEventhouseItemId"),
                "Query Service URI": prop.get("queryServiceUri"),
                "Ingestion Service URI": prop.get("ingestionServiceUri"),
                "Database Type": prop.get("databaseType"),
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def _create_kql_database(
    name: str, description: Optional[str] = None, workspace: Optional[str | UUID] = None
):
    """
    Creates a KQL database.

    This is a wrapper function for the following API: `Items - Create KQL Database <https://learn.microsoft.com/rest/api/fabric/kqldatabase/items/create-kql-database>`_.

    Parameters
    ----------
    name: str
        Name of the KQL database.
    description : str, default=None
        A description of the environment.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    create_item(
        name=name, description=description, type="KQLDatabase", workspace=workspace
    )


def delete_kql_database(
    kql_database: str | UUID,
    workspace: Optional[str | UUID] = None,
    **kwargs,
):
    """
    Deletes a KQL database.

    This is a wrapper function for the following API: `Items - Delete KQL Database <https://learn.microsoft.com/rest/api/fabric/kqldatabase/items/delete-kql-database>`_.

    Parameters
    ----------
    kql_database: str | uuid.UUID
        Name or ID of the KQL database.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    if "name" in kwargs:
        kql_database = kwargs["name"]
        print(
            f"{icons.warning} The 'name' parameter is deprecated. Please use 'kql_database' instead."
        )

    delete_item(item=kql_database, type="KQLDatabase", workspace=workspace)
