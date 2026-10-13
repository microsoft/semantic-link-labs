import pandas as pd
from typing import Optional
from urllib.parse import quote
from sempy_labs._helper_functions import (
    _base_api,
    _create_dataframe,
    resolve_item_id,
    resolve_item_name_and_id,
    resolve_workspace_id,
    resolve_workspace_name_and_id,
)
from uuid import UUID
import sempy_labs._icons as icons
from sempy._utils._log import log

_shortcut_columns = {
    "Shortcut Name": "string",
    "Query Acceleration Enabled": "bool",
    "Source Type": "string",
    "Source Workspace Id": "string",
    "Source Item Id": "string",
    "OneLake Path": "string",
    "Connection Id": "string",
    "Location": "string",
    "Bucket": "string",
    "Subpath": "string",
}

# Maps the lower-cased source name to the key used in the API's 'target' object.
_external_sources = {
    "adlsgen2": "adlsGen2",
    "amazons3": "amazonS3",
    "azureblobstorage": "azureBlobStorage",
    "googlecloudstorage": "googleCloudStorage",
    "s3compatible": "s3Compatible",
}

_invalid_name_chars = '?:/\\"][+#%'


def _shortcut_to_row(v: dict) -> dict:

    target = v.get("target") or {}
    # The target contains a 'type' plus exactly one source-specific object (i.e. 'oneLake', 'adlsGen2').
    source = next(
        (val for key, val in target.items() if key != "type" and isinstance(val, dict)),
        {},
    )
    return {
        "Shortcut Name": v.get("name"),
        "Query Acceleration Enabled": v.get("enableQueryAcceleration"),
        "Source Type": target.get("type"),
        "Source Workspace Id": source.get("workspaceId"),
        "Source Item Id": source.get("itemId"),
        "OneLake Path": source.get("path"),
        "Connection Id": source.get("connectionId"),
        "Location": source.get("location"),
        "Bucket": source.get("bucket"),
        "Subpath": source.get("subpath"),
    }


def _validate_shortcut_name(shortcut_name: str):

    invalid = [c for c in _invalid_name_chars if c in shortcut_name]
    if invalid:
        raise ValueError(
            f"{icons.red_dot} The shortcut name cannot contain the following characters: {' '.join(_invalid_name_chars)}"
        )


def _create_table_shortcut(
    kql_database: str | UUID,
    shortcut_name: str,
    target: dict,
    enable_query_acceleration: bool,
    workspace: Optional[str | UUID],
) -> str:

    _validate_shortcut_name(shortcut_name)

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    item_name, item_id = resolve_item_name_and_id(
        item=kql_database, type="KQLDatabase", workspace=workspace_id
    )

    payload = {
        "name": shortcut_name,
        "target": target,
        "enableQueryAcceleration": enable_query_acceleration,
    }

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/kqlDatabases/{item_id}/shortcuts",
        method="post",
        payload=payload,
        status_codes=[201, 202],
        lro_return_status_code=True,
        client="fabric_sp",
    )

    return f"the '{item_name}' KQL database within the '{workspace_name}' workspace"


@log
def list_shortcuts(
    kql_database: str | UUID,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Shows the table shortcuts within a KQL database.

    This is a wrapper function for the following API: `Table Shortcuts - List Shortcuts <https://learn.microsoft.com/rest/api/fabric/kqldatabase/table-shortcuts/list-shortcuts>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    kql_database : str | uuid.UUID
        The name or ID of the KQL database.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the table shortcuts within the KQL database.
    """

    workspace_id = resolve_workspace_id(workspace)
    item_id = resolve_item_id(
        item=kql_database, type="KQLDatabase", workspace=workspace_id
    )

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/kqlDatabases/{item_id}/shortcuts",
        uses_pagination=True,
        client="fabric_sp",
    )

    rows = [_shortcut_to_row(v) for r in responses for v in r.get("value", [])]

    if not rows:
        return _create_dataframe(columns=_shortcut_columns)

    return pd.DataFrame(rows, columns=list(_shortcut_columns.keys()))


@log
def get_shortcut(
    kql_database: str | UUID,
    shortcut_name: str,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Shows the properties of a table shortcut within a KQL database.

    This is a wrapper function for the following API: `Table Shortcuts - Get Shortcut <https://learn.microsoft.com/rest/api/fabric/kqldatabase/table-shortcuts/get-shortcut>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    kql_database : str | uuid.UUID
        The name or ID of the KQL database.
    shortcut_name : str
        The name of the table shortcut.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the properties of the table shortcut.
    """

    workspace_id = resolve_workspace_id(workspace)
    item_id = resolve_item_id(
        item=kql_database, type="KQLDatabase", workspace=workspace_id
    )

    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/kqlDatabases/{item_id}/shortcuts/{quote(shortcut_name, safe='')}",
        client="fabric_sp",
    ).json()

    return pd.DataFrame(
        [_shortcut_to_row(response)], columns=list(_shortcut_columns.keys())
    )


@log
def create_shortcut_onelake(
    kql_database: str | UUID,
    shortcut_name: str,
    source_item: str | UUID,
    source_path: str,
    source_item_type: str = "Lakehouse",
    source_workspace: Optional[str | UUID] = None,
    enable_query_acceleration: bool = False,
    workspace: Optional[str | UUID] = None,
):
    """
    Creates a table shortcut within a KQL database which points to a path within a OneLake item (i.e. a lakehouse table).

    This is a wrapper function for the following API: `Table Shortcuts - Create Shortcut <https://learn.microsoft.com/rest/api/fabric/kqldatabase/table-shortcuts/create-shortcut>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    kql_database : str | uuid.UUID
        The name or ID of the KQL database in which the shortcut will be created.
    shortcut_name : str
        The name of the table shortcut. Cannot contain the following characters: ? : / \\ " ] [ + # %
    source_item : str | uuid.UUID
        The name or ID of the source item.
    source_path : str
        The full path to the target folder within the source item, relative to the root of the item (i.e. "Tables/Weather").
    source_item_type : str, default="Lakehouse"
        The type of the source item. Valid values: 'Lakehouse', 'KQLDatabase', 'Warehouse'.
    source_workspace : str | uuid.UUID, default=None
        The name or ID of the workspace in which the source item resides.
        Defaults to None which resolves to the workspace of the KQL database.
    enable_query_acceleration : bool, default=False
        If True, enables `query acceleration <https://learn.microsoft.com/fabric/real-time-intelligence/query-acceleration-overview>`_ for the shortcut.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID of the KQL database.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    valid_types = ["Lakehouse", "KQLDatabase", "Warehouse"]
    if source_item_type not in valid_types:
        raise ValueError(
            f"{icons.red_dot} Invalid source_item_type '{source_item_type}'. Valid values: {valid_types}."
        )

    source_workspace_name, source_workspace_id = resolve_workspace_name_and_id(
        source_workspace if source_workspace is not None else workspace
    )
    source_item_name, source_item_id = resolve_item_name_and_id(
        item=source_item, type=source_item_type, workspace=source_workspace_id
    )

    target = {
        "oneLake": {
            "workspaceId": source_workspace_id,
            "itemId": source_item_id,
            "path": source_path,
        }
    }

    destination = _create_table_shortcut(
        kql_database=kql_database,
        shortcut_name=shortcut_name,
        target=target,
        enable_query_acceleration=enable_query_acceleration,
        workspace=workspace,
    )

    print(
        f"{icons.green_dot} The '{shortcut_name}' table shortcut has been created in {destination}. "
        f"It points to the '{source_path}' path in the '{source_item_name}' {source_item_type} within the '{source_workspace_name}' workspace."
    )


@log
def create_shortcut(
    kql_database: str | UUID,
    shortcut_name: str,
    source: str,
    location: str,
    subpath: str,
    connection_id: str | UUID,
    bucket: Optional[str] = None,
    enable_query_acceleration: bool = False,
    workspace: Optional[str | UUID] = None,
):
    """
    Creates a table shortcut within a KQL database which points to an external storage location (i.e. ADLS Gen2, Amazon S3).

    For shortcuts to OneLake items, see :func:`create_kql_database_table_shortcut_onelake`.

    This is a wrapper function for the following API: `Table Shortcuts - Create Shortcut <https://learn.microsoft.com/rest/api/fabric/kqldatabase/table-shortcuts/create-shortcut>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    kql_database : str | uuid.UUID
        The name or ID of the KQL database in which the shortcut will be created.
    shortcut_name : str
        The name of the table shortcut. Cannot contain the following characters: ? : / \\ " ] [ + # %
    source : str
        The external source type. Valid values: 'AdlsGen2', 'AmazonS3', 'AzureBlobStorage', 'GoogleCloudStorage', 'S3Compatible' (case-insensitive).
    location : str
        The location (URI) of the external source (i.e. "https://myaccount.dfs.core.windows.net").
    subpath : str
        The container/bucket and subfolder within the external source (i.e. "/mycontainer/mysubfolder").
    connection_id : str | uuid.UUID
        The ID of the cloud connection used by the shortcut to access the external source.
    bucket : str, default=None
        The target bucket. Required (and only used) when the source is 'S3Compatible'.
    enable_query_acceleration : bool, default=False
        If True, enables `query acceleration <https://learn.microsoft.com/fabric/real-time-intelligence/query-acceleration-overview>`_ for the shortcut.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID of the KQL database.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    source_key = _external_sources.get(source.replace(" ", "").lower())
    if source_key is None:
        raise ValueError(
            f"{icons.red_dot} Invalid source '{source}'. Valid values: 'AdlsGen2', 'AmazonS3', 'AzureBlobStorage', 'GoogleCloudStorage', 'S3Compatible'."
        )
    if source_key == "s3Compatible" and not bucket:
        raise ValueError(
            f"{icons.red_dot} The 'bucket' parameter is required when the source is 'S3Compatible'."
        )

    target_properties = {
        "location": location,
        "subpath": subpath,
        "connectionId": str(connection_id),
    }
    if source_key == "s3Compatible":
        target_properties["bucket"] = bucket

    destination = _create_table_shortcut(
        kql_database=kql_database,
        shortcut_name=shortcut_name,
        target={source_key: target_properties},
        enable_query_acceleration=enable_query_acceleration,
        workspace=workspace,
    )

    print(
        f"{icons.green_dot} The '{shortcut_name}' table shortcut has been created in {destination}. "
        f"It points to '{location}' ('{subpath}')."
    )


@log
def delete_shortcut(
    kql_database: str | UUID,
    shortcut_name: str,
    workspace: Optional[str | UUID] = None,
):
    """
    Deletes a table shortcut from a KQL database.

    This is a wrapper function for the following API: `Table Shortcuts - Delete Shortcut <https://learn.microsoft.com/rest/api/fabric/kqldatabase/table-shortcuts/delete-shortcut>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    kql_database : str | uuid.UUID
        The name or ID of the KQL database.
    shortcut_name : str
        The name of the table shortcut.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    item_name, item_id = resolve_item_name_and_id(
        item=kql_database, type="KQLDatabase", workspace=workspace_id
    )

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/kqlDatabases/{item_id}/shortcuts/{quote(shortcut_name, safe='')}",
        method="delete",
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The '{shortcut_name}' table shortcut has been deleted from the '{item_name}' KQL database within the '{workspace_name}' workspace."
    )
