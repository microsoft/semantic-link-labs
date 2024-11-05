import sempy.fabric as fabric
import pandas as pd
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    pagination,
    lro,
)
from sempy.fabric.exceptions import FabricHTTPException
import sempy_labs._icons as icons


def list_mirrored_databases(workspace: Optional[str] = None) -> pd.DataFrame:
    """
    Shows the mirrored databases within a workspace.

    This is a wrapper function for the following API: `Items - List Mirrored Databases <https://learn.microsoft.com/rest/api/fabric/mirroredwarehouse/items/list-mirrored-databases>`_.

    Parameters
    ----------
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the mirrored databases within a workspace.
    """

    df = pd.DataFrame(
        columns=["Mirrored Database Name", "Mirrored Database Id", "Description", "OneLake Tables Path", "SQL Endpoint Connection String", "SQL Endpoint Id", "Provisioning Status", "Default Schema"]
    )

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/mirroredDatabases")
    if response.status_code != 200:
        raise FabricHTTPException(response)
    responses = pagination(client, response)

    for r in responses:
        for v in r.get("value", []):
            prop = v.get('properties', {})
            sql = prop.get('sqlEndpointProperties', {})
            new_data = {
                "Mirrored Database Name": v.get("displayName"),
                "Mirrored Database Id": v.get("id"),
                "Description": v.get("description"),
                "OneLake Tables Path": prop.get('oneLakeTablesPath') if prop is not None else None,
                "SQL Endpoint Connection String": sql.get('connectionString') if sql is not None else None,
                "SQL Endpoint Id": sql.get('id') if sql is not None else None,
                "Provisioning Status": sql.get('provisioningStatus') if sql is not None else None,
                "Default Schema": prop.get('defaultSchema') if prop is not None else None,
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def create_mirrored_database(
    name: str, description: Optional[str] = None, workspace: Optional[str] = None
):
    """
    Creates a Fabric mirrored database.

    This is a wrapper function for the following API: `Items - Create Mirrored Database <https://learn.microsoft.com/rest/api/fabric/mlmodel/items/create-mirrored-database>`_.

    Parameters
    ----------
    name: str
        Name of the mirrored database.
    description : str, default=None
        A description of the mirrored database.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    request_body = {"displayName": name}

    if description:
        request_body["description"] = description

    client = fabric.FabricRestClient()
    response = client.post(f"/v1/workspaces/{workspace_id}/mirroredDatabases", json=request_body)

    if response.status_code != 201:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The '{name}' mirrored database has been created within the '{workspace}' workspace."
    )


def delete_mirrored_database(name: str, workspace: Optional[str] = None):
    """
    Deletes a mirrored database.

    This is a wrapper function for the following API: `Items - Delete Mirrored Database <https://learn.microsoft.com/rest/api/fabric/mlmodel/items/delete-mirrored-database>`_.

    Parameters
    ----------
    name: str
        Name of the mirrored database.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    item_id = fabric.resolve_item_id(
        item_name=name, type="MirroredDatabase", workspace=workspace
    )

    client = fabric.FabricRestClient()
    response = client.delete(f"/v1/workspaces/{workspace_id}/mirroredDatabases/{item_id}")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The '{name}' mirrored database within the '{workspace}' workspace has been deleted."
    )


def get_mirroring_status(mirrored_database: str, workspace: Optional[str] = None) -> str:

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    item_id = fabric.resolve_item_id(item_name=mirrored_database, type='MirroredDatabase', workspace=workspace)

    client = fabric.FabricRestClient()
    response = client.post(f"/v1/workspaces/{workspace_id}/mirroredDatabases/{item_id}/getMirroringStatus")

    if response.status_code != 200:
        raise FabricHTTPException(response)
    
    return response.json().get('status', {})


def get_tables_mirroring_status(mirrored_database: str, workspace: Optional[str] = None) -> str:

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    item_id = fabric.resolve_item_id(item_name=mirrored_database, type='MirroredDatabase', workspace=workspace)

    client = fabric.FabricRestClient()
    response = client.post(f"/v1/workspaces/{workspace_id}/mirroredDatabases/{item_id}/getTablesMirroringStatus")

    if response.status_code != 200:
        raise FabricHTTPException(response)
    
    return response.json().get('status', {})




def start_mirroring():



def stop_mirroring():