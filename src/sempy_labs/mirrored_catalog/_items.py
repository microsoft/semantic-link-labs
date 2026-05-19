import pandas as pd
from typing import Optional
from sempy_labs._helper_functions import (
    _base_api,
    _create_dataframe,
    resolve_workspace_id,
    _update_dataframe_datatypes,
    delete_item,
)
from uuid import UUID
from sempy._utils._log import log


@log
def list_mirrored_catalogs(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    List all mirrored catalogs in a workspace.

    This is a wrapper function for the following API: `Items - List Mirrored Catalogs <https://learn.microsoft.com/rest/api/fabric/mirroredcatalog/items/list-mirrored-catalogs>`_.

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
        A DataFrame containing the details of the mirrored catalogs in the specified workspace.
    """
    workspace_id = resolve_workspace_id(workspace)

    columns = {
        "Mirrored Catalog Name": "str",
        "Mirrored Catalog Id": "str",
        "Description": "str",
        "Source Type": "str",
        "Connection Id": "str",
        "Scope": "list",
        "OneLake Tables Path": "str",
        "SQL Endpoint Connection String": "str",
        "SQL Endpoint Id": "str",
        "SQL Endpoint Provisioning Status": "str",
    }
    df = _create_dataframe(columns=columns)
    responses = _base_api(
        f"/v1/workspaces/{workspace_id}/mirroredCatalogs", uses_pagination=True
    )

    rows = []
    for r in responses:
        for item in r.get("value", []):
            p = item.get("properties", {})
            sql = p.get("sqlEndpointProperties", {})
            rows.append(
                {
                    "Mirrored Catalog Name": item.get("name"),
                    "Mirrored Catalog Id": item.get("id"),
                    "Description": item.get("description"),
                    "Source Type": p.get("sourceType"),
                    "Connection Id": p.get("connectionId"),
                    "Scope": p.get("scope", []),
                    "OneLake Tables Path": p.get("oneLakeTablesPath"),
                    "SQL Endpoint Connection String": sql.get("connectionString"),
                    "SQL Endpoint Id": sql.get("id"),
                    "SQL Endpoint Provisioning Status": sql.get("provisioningStatus"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=columns.keys())
        _update_dataframe_datatypes(df, columns)

    return df


@log
def delete_mirrored_catalog(
    mirrored_catalog: str | UUID, workspace: Optional[str | UUID] = None
):
    """
    Deletes a mirrored catalog from a workspace.

    This is a wrapper function for the following API: `Items - Delete Mirrored Catalog <https://learn.microsoft.com/rest/api/fabric/mirroredcatalog/items/delete-mirrored-catalog>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    mirrored_catalog : str | uuid.UUID
        The name or ID of the mirrored catalog.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    delete_item(item=mirrored_catalog, type="MirroredCatalog", workspace=workspace)
