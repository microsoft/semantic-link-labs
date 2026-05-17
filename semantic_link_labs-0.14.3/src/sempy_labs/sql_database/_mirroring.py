from sempy_labs._helper_functions import (
    resolve_item_id,
    _base_api,
    resolve_workspace_name_and_id,
)
from typing import Optional
from uuid import UUID
from sempy._utils._log import log
import sempy_labs._icons as icons


@log
def start_mirroring(sql_database: str | UUID, workspace: Optional[str | UUID] = None):
    """
    Starts data mirroring for the specified SQL Database.

    This is a wrapper function for the following API: `Mirroring - Start Mirroring <https://learn.microsoft.com/rest/api/fabric/sqldatabase/mirroring/start-mirroring>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    sql_database : str | uuid.UUID
        Name or ID of the SQL Database.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    item_id = resolve_item_id(
        item=sql_database, type="SQLDatabase", workspace=workspace_id
    )

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/sqlDatabases/{item_id}/startMirroring",
        method="post",
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The SQL Database '{sql_database}' in the '{workspace_name}' workspace is now being mirrored."
    )


@log
def stop_mirroring(sql_database: str | UUID, workspace: Optional[str | UUID] = None):
    """
    Stops data mirroring for the specified SQL Database.

    This is a wrapper function for the following API: `Mirroring - Stop Mirroring <https://learn.microsoft.com/rest/api/fabric/sqldatabase/mirroring/stop-mirroring>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    sql_database : str | uuid.UUID
        Name or ID of the SQL Database.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    item_id = resolve_item_id(
        item=sql_database, type="SQLDatabase", workspace=workspace_id
    )

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/sqlDatabases/{item_id}/stopMirroring",
        method="post",
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The SQL Database '{sql_database}' in the '{workspace_name}' workspace is no longer being mirrored."
    )
