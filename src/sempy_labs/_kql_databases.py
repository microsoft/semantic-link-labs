import sempy.fabric as fabric
import pandas as pd
import sempy_labs._icons as icons
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    lro,
    pagination,
)
from sempy.fabric.exceptions import FabricHTTPException
from uuid import UUID


def list_kql_databases(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows the KQL databases within a workspace.

    This is a wrapper function for the following API: `Items - List KQL Databases <https://learn.microsoft.com/rest/api/fabric/kqldatabase/items/list-kql-databases>`_.

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

    df = pd.DataFrame(
        columns=[
            "KQL Database Name",
            "KQL Database Id",
            "Description",
            "Parent Eventhouse Item Id",
            "Query Service URI",
            "Ingestion Service URI",
            "Database Type",
        ]
    )

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/kqlDatabases")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

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


def create_kql_database(
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

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    request_body = {"displayName": name}

    if description:
        request_body["description"] = description

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/workspaces/{workspace_id}/kqlDatabases", json=request_body
    )

    lro(client, response, status_codes=[201, 202])

    print(
        f"{icons.green_dot} The '{name}' KQL database has been created within the '{workspace_name}' workspace."
    )


def delete_kql_database(name: str, workspace: Optional[str | UUID] = None):
    """
    Deletes a KQL database.

    This is a wrapper function for the following API: `Items - Delete KQL Database <https://learn.microsoft.com/rest/api/fabric/kqldatabase/items/delete-kql-database>`_.

    Parameters
    ----------
    name: str
        Name of the KQL database.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    kql_database_id = fabric.resolve_item_id(
        item_name=name, type="KQLDatabase", workspace=workspace_id
    )

    client = fabric.FabricRestClient()
    response = client.delete(
        f"/v1/workspaces/{workspace_id}/kqlDatabases/{kql_database_id}"
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)
    print(
        f"{icons.green_dot} The '{name}' KQL database within the '{workspace_name}' workspace has been deleted."
    )
