from typing import Optional
import pandas as pd
from sempy_labs._helper_functions import (
    _base_api,
    _create_dataframe,
    resolve_workspace_id,
    resolve_item_id,
)
from uuid import UUID
from sempy._utils._log import log


@log
def get_mirroring_status(
    mirrored_catalog: str | UUID, workspace: Optional[str | UUID] = None
) -> dict:
    """
    Gets the mirroring status of a mirrored catalog.

    This is a wrapper function for the following API: `Monitoring - Mirroring Status <https://learn.microsoft.com/rest/api/fabric/mirroredcatalog/monitoring/mirroring-status>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    mirrored_catalog : str | uuid.UUID
        The name or ID of the mirrored catalog.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    dict
        A dictionary containing the mirroring status of the specified mirrored catalog.
    """
    workspace_id = resolve_workspace_id(workspace)
    mirrored_catalog_id = resolve_item_id(
        item=mirrored_catalog, type="MirroredCatalog", workspace=workspace_id
    )

    response = _base_api(
        f"/v1/workspaces/{workspace_id}/mirroredCatalogs/{mirrored_catalog_id}/mirroringStatus?beta=True",
    )

    return response.json()


@log
def list_tables_mirroring_status(
    mirrored_catalog: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Returns the per-table mirroring status for the mirrored catalog.

    This is a wrapper function for the following API: `Monitoring - Tables Mirroring Status <https://learn.microsoft.com/rest/api/fabric/mirroredcatalog/monitoring/tables-mirroring-status(beta)>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    mirrored_catalog : str | uuid.UUID
        The name or ID of the mirrored catalog.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the per-table mirroring status of the specified mirrored catalog.
    """

    workspace_id = resolve_workspace_id(workspace)
    mirrored_catalog_id = resolve_item_id(
        item=mirrored_catalog, type="MirroredCatalog", workspace=workspace_id
    )

    columns = {
        "Schema Name": "str",
        "Table Name": "str",
        "Status": "str",
        "Last Sync DateTime": "datetime",
        "Last Update DateTime": "str",
    }

    df = _create_dataframe(columns=columns)

    responses = _base_api(
        f"/v1/workspaces/{workspace_id}/mirroredCatalogs/{mirrored_catalog_id}/tablesMirroringStatus?beta=True",
        uses_pagination=True,
    )

    rows = []
    for r in responses:
        for item in r.get("value", []):
            rows.append(
                {
                    "Schema Name": item.get("schemaName"),
                    "Table Name": item.get("name"),
                    "Status": item.get("status"),
                    "Last Sync DateTime": item.get("lastSyncDateTime"),
                    "Last Update DateTime": item.get("lastUpdateDateTime"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=columns.keys())

    return df
