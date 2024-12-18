import sempy.fabric as fabric
import pandas as pd
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    pagination,
    lro,
    _decode_b64,
)
from sempy.fabric.exceptions import FabricHTTPException
import sempy_labs._icons as icons
import base64
from uuid import UUID


def list_mirrored_databases(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows the mirrored databases within a workspace.

    This is a wrapper function for the following API: `Items - List Mirrored Databases <https://learn.microsoft.com/rest/api/fabric/mirroredwarehouse/items/list-mirrored-databases>`_.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the mirrored databases within a workspace.
    """

    df = pd.DataFrame(
        columns=[
            "Mirrored Database Name",
            "Mirrored Database Id",
            "Description",
            "OneLake Tables Path",
            "SQL Endpoint Connection String",
            "SQL Endpoint Id",
            "Provisioning Status",
            "Default Schema",
        ]
    )

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/mirroredDatabases")
    if response.status_code != 200:
        raise FabricHTTPException(response)
    responses = pagination(client, response)

    for r in responses:
        for v in r.get("value", []):
            prop = v.get("properties", {})
            sql = prop.get("sqlEndpointProperties", {})
            new_data = {
                "Mirrored Database Name": v.get("displayName"),
                "Mirrored Database Id": v.get("id"),
                "Description": v.get("description"),
                "OneLake Tables Path": prop.get("oneLakeTablesPath"),
                "SQL Endpoint Connection String": sql.get("connectionString"),
                "SQL Endpoint Id": sql.get("id"),
                "Provisioning Status": sql.get("provisioningStatus"),
                "Default Schema": prop.get("defaultSchema"),
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def create_mirrored_database(
    name: str, description: Optional[str] = None, workspace: Optional[str | UUID] = None
):
    """
    Creates a Fabric mirrored database.

    This is a wrapper function for the following API: `Items - Create Mirrored Database <https://learn.microsoft.com/rest/api/fabric/mirroreddatabase/items/create-mirrored-database>`_.

    Parameters
    ----------
    name: str
        Name of the mirrored database.
    description : str, default=None
        A description of the mirrored database.
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
        f"/v1/workspaces/{workspace_id}/mirroredDatabases", json=request_body
    )

    if response.status_code != 201:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The '{name}' mirrored database has been created within the '{workspace_name}' workspace."
    )


def delete_mirrored_database(
    mirrored_database: str, workspace: Optional[str | UUID] = None
):
    """
    Deletes a mirrored database.

    This is a wrapper function for the following API: `Items - Delete Mirrored Database <https://learn.microsoft.com/rest/api/fabric/mirroreddatabase/items/delete-mirrored-database>`_.

    Parameters
    ----------
    mirrored_database: str
        Name of the mirrored database.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    item_id = fabric.resolve_item_id(
        item_name=mirrored_database, type="MirroredDatabase", workspace=workspace_id
    )

    client = fabric.FabricRestClient()
    response = client.delete(
        f"/v1/workspaces/{workspace_id}/mirroredDatabases/{item_id}"
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The '{mirrored_database}' mirrored database within the '{workspace_name}' workspace has been deleted."
    )


def get_mirroring_status(
    mirrored_database: str, workspace: Optional[str | UUID] = None
) -> str:
    """
    Get the status of the mirrored database.

    This is a wrapper function for the following API: `Mirroring - Get Mirroring Status <https://learn.microsoft.com/rest/api/fabric/mirroreddatabase/mirroring/get-mirroring-status>`_.

    Parameters
    ----------
    mirrored_database: str
        Name of the mirrored database.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    str
        The status of a mirrored database.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    item_id = fabric.resolve_item_id(
        item_name=mirrored_database, type="MirroredDatabase", workspace=workspace_id
    )

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/workspaces/{workspace_id}/mirroredDatabases/{item_id}/getMirroringStatus"
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)

    return response.json().get("status", {})


def get_tables_mirroring_status(
    mirrored_database: str, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Gets the mirroring status of the tables.

    This is a wrapper function for the following API: `Mirroring - Get Tables Mirroring Status <https://learn.microsoft.com/rest/api/fabric/mirroreddatabase/mirroring/get-tables-mirroring-status>`_.

    Parameters
    ----------
    mirrored_database: str
        Name of the mirrored database.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the mirroring status of the tables.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    item_id = fabric.resolve_item_id(
        item_name=mirrored_database, type="MirroredDatabase", workspace=workspace_id
    )

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/workspaces/{workspace_id}/mirroredDatabases/{item_id}/getTablesMirroringStatus"
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    df = pd.DataFrame(
        columns=[
            "Source Schema Name",
            "Source Table Name",
            "Status",
            "Processed Bytes",
            "Processed Rows",
            "Last Sync Date",
        ]
    )

    for r in responses:
        for v in r.get("data", []):
            m = v.get("metrics", {})
            new_data = {
                "Source Schema Name": v.get("sourceSchemaName"),
                "Source Table Name": v.get("sourceTableName"),
                "Status": v.get("status"),
                "Processed Bytes": m.get("processedBytes"),
                "Processed Rows": m.get("processedRows"),
                "Last Sync Date": m.get("lastSyncDateTime"),
            }

            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    int_cols = ["Processed Bytes", "Processed Rows"]
    df[int_cols] = df[int_cols].astype(int)
    df["Last Sync Date"] = pd.to_datetime(df["Last Sync Date"])

    return df


def start_mirroring(mirrored_database: str, workspace: Optional[str | UUID] = None):
    """
    Starts the mirroring for a database.

    This is a wrapper function for the following API: `Mirroring - Start Mirroring <https://learn.microsoft.com/rest/api/fabric/mirroreddatabase/mirroring/start-mirroring>`_.

    Parameters
    ----------
    mirrored_database: str
        Name of the mirrored database.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    item_id = fabric.resolve_item_id(
        item_name=mirrored_database, type="MirroredDatabase", workspace=workspace_id
    )

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/workspaces/{workspace_id}/mirroredDatabases/{item_id}/startMirroring"
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} Mirroring has started for the '{mirrored_database}' database within the '{workspace_name}' workspace."
    )


def stop_mirroring(mirrored_database: str, workspace: Optional[str | UUID] = None):
    """
    Stops the mirroring for a database.

    This is a wrapper function for the following API: `Mirroring - Stop Mirroring <https://learn.microsoft.com/rest/api/fabric/mirroreddatabase/mirroring/stop-mirroring>`_.

    Parameters
    ----------
    mirrored_database: str
        Name of the mirrored database.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    item_id = fabric.resolve_item_id(
        item_name=mirrored_database, type="MirroredDatabase", workspace=workspace_id
    )

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/workspaces/{workspace_id}/mirroredDatabases/{item_id}/stopMirroring"
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} Mirroring has stopped for the '{mirrored_database}' database within the '{workspace_name}' workspace."
    )


def get_mirrored_database_definition(
    mirrored_database: str, workspace: Optional[str | UUID] = None, decode: bool = True
) -> str:
    """
    Obtains the mirrored database definition.

    This is a wrapper function for the following API: `Items - Get Mirrored Database Definition <https://learn.microsoft.com/rest/api/fabric/mirroreddatabase/items/get-mirrored-database-definition>`_.

    Parameters
    ----------
    mirrored_database : str
        The name of the mirrored database.
    workspace : str | uuid.UUID, default=None
        The name or ID of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    decode : bool, default=True
        If True, decodes the mirrored database definition file into .json format.
        If False, obtains the mirrored database definition file in base64 format.

    Returns
    -------
    str
        The mirrored database definition.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    item_id = fabric.resolve_item_id(
        item_name=mirrored_database, type="MirroredDatabase", workspace=workspace_id
    )
    client = fabric.FabricRestClient()
    response = client.post(
        f"v1/workspaces/{workspace_id}/mirroredDatabases/{item_id}/getDefinition",
    )

    result = lro(client, response).json()
    df_items = pd.json_normalize(result["definition"]["parts"])
    df_items_filt = df_items[df_items["path"] == "mirroredDatabase.json"]
    payload = df_items_filt["payload"].iloc[0]

    if decode:
        result = _decode_b64(payload)
    else:
        result = payload

    return result


def update_mirrored_database_definition(
    mirrored_database: str,
    mirrored_database_content: dict,
    workspace: Optional[str | UUID] = None,
):
    """
    Updates an existing notebook with a new definition.

    Parameters
    ----------
    mirrored_database : str
        The name of the mirrored database to be created.
    mirrored_database_content : dict
        The mirrored database definition (not in Base64 format).
    workspace : str | uuid.UUID, default=None
        The name or ID of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    client = fabric.FabricRestClient()
    payload = base64.b64encode(mirrored_database_content)
    item_id = fabric.resolve_item_id(
        item_name=mirrored_database, type="MirroredDatabase", workspace=workspace_id
    )

    request_body = {
        "displayName": mirrored_database,
        "definition": {
            "format": "ipynb",
            "parts": [
                {
                    "path": "mirroredDatabase.json",
                    "payload": payload,
                    "payloadType": "InlineBase64",
                }
            ],
        },
    }

    response = client.post(
        f"v1/workspaces/{workspace_id}/mirroredDatabases/{item_id}/updateDefinition",
        json=request_body,
    )

    lro(client, response, return_status_code=True)

    print(
        f"{icons.green_dot} The '{mirrored_database}' mirrored database was updated within the '{workspace_name}' workspace."
    )
