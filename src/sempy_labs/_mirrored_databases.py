import pandas as pd
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    _update_dataframe_datatypes,
    _base_api,
    resolve_item_id,
    _create_dataframe,
    delete_item,
    create_item,
    _get_item_definition,
    resolve_workspace_id,
)
import sempy_labs._icons as icons
import base64
from uuid import UUID
from sempy._utils._log import log


@log
def list_mirrored_databases(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows the mirrored databases within a workspace.

    This is a wrapper function for the following API: `Items - List Mirrored Databases <https://learn.microsoft.com/rest/api/fabric/mirroredwarehouse/items/list-mirrored-databases>`_.

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
        A pandas dataframe showing the mirrored databases within a workspace.
    """

    columns = {
        "Mirrored Database Name": "string",
        "Mirrored Database Id": "string",
        "Description": "string",
        "OneLake Tables Path": "string",
        "SQL Endpoint Connection String": "string",
        "SQL Endpoint Id": "string",
        "Provisioning Status": "string",
        "Default Schema": "string",
    }
    df = _create_dataframe(columns=columns)

    workspace_id = resolve_workspace_id(workspace)
    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/mirroredDatabases",
        uses_pagination=True,
        client="fabric_sp",
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            prop = v.get("properties", {})
            sql = prop.get("sqlEndpointProperties", {})
            rows.append(
                {
                    "Mirrored Database Name": v.get("displayName"),
                    "Mirrored Database Id": v.get("id"),
                    "Description": v.get("description"),
                    "OneLake Tables Path": prop.get("oneLakeTablesPath"),
                    "SQL Endpoint Connection String": sql.get("connectionString"),
                    "SQL Endpoint Id": sql.get("id"),
                    "Provisioning Status": sql.get("provisioningStatus"),
                    "Default Schema": prop.get("defaultSchema"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def create_mirrored_database(
    name: str, description: Optional[str] = None, workspace: Optional[str | UUID] = None
):
    """
    Creates a Fabric mirrored database.

    This is a wrapper function for the following API: `Items - Create Mirrored Database <https://learn.microsoft.com/rest/api/fabric/mirroreddatabase/items/create-mirrored-database>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

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

    create_item(
        name=name, description=description, type="MirroredDatabase", workspace=workspace
    )


@log
def delete_mirrored_database(
    mirrored_database: str, workspace: Optional[str | UUID] = None
):
    """
    Deletes a mirrored database.

    This is a wrapper function for the following API: `Items - Delete Mirrored Database <https://learn.microsoft.com/rest/api/fabric/mirroreddatabase/items/delete-mirrored-database>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    mirrored_database: str
        Name of the mirrored database.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    delete_item(item=mirrored_database, type="MirroredDatabase", workspace=workspace)


@log
def get_mirroring_status(
    mirrored_database: str | UUID, workspace: Optional[str | UUID] = None
) -> str:
    """
    Get the status of the mirrored database.

    This is a wrapper function for the following API: `Mirroring - Get Mirroring Status <https://learn.microsoft.com/rest/api/fabric/mirroreddatabase/mirroring/get-mirroring-status>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    mirrored_database: str | uuid.UUID
        Name or ID of the mirrored database.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    str
        The status of a mirrored database.
    """

    workspace_id = resolve_workspace_id(workspace)
    item_id = resolve_item_id(
        item=mirrored_database, type="MirroredDatabase", workspace=workspace
    )
    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/mirroredDatabases/{item_id}/getMirroringStatus",
        status_codes=200,
        client="fabric_sp",
    )

    return response.json().get("status", {})


@log
def get_tables_mirroring_status(
    mirrored_database: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Gets the mirroring status of the tables.

    This is a wrapper function for the following API: `Mirroring - Get Tables Mirroring Status <https://learn.microsoft.com/rest/api/fabric/mirroreddatabase/mirroring/get-tables-mirroring-status>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    mirrored_database: str | uuid.UUID
        Name or ID of the mirrored database.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the mirroring status of the tables.
    """

    workspace_id = resolve_workspace_id(workspace)
    item_id = resolve_item_id(
        item=mirrored_database, type="MirroredDatabase", workspace=workspace
    )
    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/mirroredDatabases/{item_id}/getTablesMirroringStatus",
        method="post",
        status_codes=200,
        uses_pagination=True,
        client="fabric_sp",
    )

    columns = {
        "Source Schema Name": "string",
        "Source Table Name": "string",
        "Status": "string",
        "Processed Bytes": "int",
        "Processed Rows": "int",
        "Last Sync Date": "datetime",
    }
    df = _create_dataframe(columns=columns)

    rows = []
    for r in responses:
        for v in r.get("data", []):
            m = v.get("metrics", {})
            rows.append(
                {
                    "Source Schema Name": v.get("sourceSchemaName"),
                    "Source Table Name": v.get("sourceTableName"),
                    "Status": v.get("status"),
                    "Processed Bytes": m.get("processedBytes"),
                    "Processed Rows": m.get("processedRows"),
                    "Last Sync Date": m.get("lastSyncDateTime"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
def start_mirroring(
    mirrored_database: str | UUID, workspace: Optional[str | UUID] = None
):
    """
    Starts the mirroring for a database.

    This is a wrapper function for the following API: `Mirroring - Start Mirroring <https://learn.microsoft.com/rest/api/fabric/mirroreddatabase/mirroring/start-mirroring>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    mirrored_database: str | uuid.UUID
        Name or ID of the mirrored database.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    item_id = resolve_item_id(
        item=mirrored_database, type="MirroredDatabase", workspace=workspace
    )
    _base_api(
        request=f"/v1/workspaces/{workspace_id}/mirroredDatabases/{item_id}/startMirroring",
        method="post",
        status_codes=200,
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} Mirroring has started for the '{mirrored_database}' database within the '{workspace_name}' workspace."
    )


@log
def stop_mirroring(
    mirrored_database: str | UUID, workspace: Optional[str | UUID] = None
):
    """
    Stops the mirroring for a database.

    This is a wrapper function for the following API: `Mirroring - Stop Mirroring <https://learn.microsoft.com/rest/api/fabric/mirroreddatabase/mirroring/stop-mirroring>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    mirrored_database: str | uuid.UUID
        Name or ID of the mirrored database.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    item_id = resolve_item_id(
        item=mirrored_database, type="MirroredDatabase", workspace=workspace
    )
    _base_api(
        request=f"/v1/workspaces/{workspace_id}/mirroredDatabases/{item_id}/stopMirroring",
        method="post",
        status_codes=200,
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} Mirroring has stopped for the '{mirrored_database}' database within the '{workspace_name}' workspace."
    )


@log
def get_mirrored_database_definition(
    mirrored_database: str | UUID,
    workspace: Optional[str | UUID] = None,
    decode: bool = True,
) -> dict:
    """
    Obtains the mirrored database definition.

    This is a wrapper function for the following API: `Items - Get Mirrored Database Definition <https://learn.microsoft.com/rest/api/fabric/mirroreddatabase/items/get-mirrored-database-definition>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    mirrored_database : str | uuid.UUID
        The name or ID of the mirrored database.
    workspace : str | uuid.UUID, default=None
        The name or ID of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    decode : bool, default=True
        If True, decodes the mirrored database definition file into .json format.
        If False, obtains the mirrored database definition file in base64 format.

    Returns
    -------
    dict
        The mirrored database definition.
    """

    return _get_item_definition(
        item=mirrored_database,
        type="MirroredDatabase",
        workspace=workspace,
        return_dataframe=False,
        decode=decode,
    )


@log
def update_mirrored_database_definition(
    mirrored_database: str | UUID,
    mirrored_database_content: dict,
    workspace: Optional[str | UUID] = None,
):
    """
    Updates an existing notebook with a new definition.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    mirrored_database : str | uuid.UUID
        The name or ID of the mirrored database to be updated.
    mirrored_database_content : dict
        The mirrored database definition (not in Base64 format).
    workspace : str | uuid.UUID, default=None
        The name or ID of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    item_id = resolve_item_id(
        item=mirrored_database, type="MirroredDatabase", workspace=workspace
    )
    payload = (
        base64.b64encode(mirrored_database_content).encode("utf-8").decode("utf-8")
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

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/mirroredDatabases/{item_id}/updateDefinition",
        method="post",
        json=request_body,
        status_codes=None,
        lro_return_status_code=True,
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The '{mirrored_database}' mirrored database was updated within the '{workspace_name}' workspace."
    )
