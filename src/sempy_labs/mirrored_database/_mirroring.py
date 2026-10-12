import pandas as pd
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_item_name_and_id,
    _update_dataframe_datatypes,
    _base_api,
    resolve_item_id,
    _create_dataframe,
    resolve_workspace_id,
)
import sempy_labs._icons as icons
from uuid import UUID
from sempy._utils._log import log


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
        The status of a mirrored database (i.e. 'Initializing', 'Initialized', 'Starting', 'Running', 'Paused', 'Stopping', 'Stopped').
        If the mirroring has an error, the error is printed as a warning.
    """

    workspace_id = resolve_workspace_id(workspace)
    item_id = resolve_item_id(
        item=mirrored_database, type="MirroredDatabase", workspace=workspace_id
    )
    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/mirroredDatabases/{item_id}/getMirroringStatus",
        method="post",
        client="fabric_sp",
    ).json()

    error = response.get("error")
    if error:
        print(
            f"{icons.warning} Mirroring error ({error.get('errorCode')}): {error.get('message')}"
        )

    return response.get("status")


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
        item=mirrored_database, type="MirroredDatabase", workspace=workspace_id
    )
    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/mirroredDatabases/{item_id}/getTablesMirroringStatus",
        method="post",
        uses_pagination=True,
        client="fabric_sp",
    )

    columns = {
        "Source Schema Name": "string",
        "Source Table Name": "string",
        "Status": "string",
        "Processed Bytes": "int_fillna",
        "Processed Rows": "int_fillna",
        "Last Sync Date": "datetime",
        "Last Sync Latency In Seconds": "int_fillna",
        "Source Object Type": "string",
        "Error Code": "string",
        "Error Message": "string",
    }
    df = _create_dataframe(columns=columns)

    rows = []
    for r in responses:
        for v in r.get("data", []):
            m = v.get("metrics") or {}
            error = v.get("error") or {}
            rows.append(
                {
                    "Source Schema Name": v.get("sourceSchemaName"),
                    "Source Table Name": v.get("sourceTableName"),
                    "Status": v.get("status"),
                    "Processed Bytes": m.get("processedBytes"),
                    "Processed Rows": m.get("processedRows"),
                    "Last Sync Date": m.get("lastSyncDateTime"),
                    "Last Sync Latency In Seconds": m.get("lastSyncLatencyInSeconds"),
                    "Source Object Type": v.get("sourceObjectType"),
                    "Error Code": error.get("errorCode"),
                    "Error Message": error.get("message"),
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

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    item_name, item_id = resolve_item_name_and_id(
        item=mirrored_database, type="MirroredDatabase", workspace=workspace_id
    )
    _base_api(
        request=f"/v1/workspaces/{workspace_id}/mirroredDatabases/{item_id}/startMirroring",
        method="post",
        status_codes=200,
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} Mirroring has started for the '{item_name}' database within the '{workspace_name}' workspace."
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

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    item_name, item_id = resolve_item_name_and_id(
        item=mirrored_database, type="MirroredDatabase", workspace=workspace_id
    )
    _base_api(
        request=f"/v1/workspaces/{workspace_id}/mirroredDatabases/{item_id}/stopMirroring",
        method="post",
        status_codes=200,
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} Mirroring has stopped for the '{item_name}' database within the '{workspace_name}' workspace."
    )
