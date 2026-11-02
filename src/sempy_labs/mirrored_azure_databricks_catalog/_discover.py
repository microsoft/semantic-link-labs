from uuid import UUID
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_id,
    _base_api,
    _create_dataframe,
)
import pandas as pd
from sempy._utils._log import log


@log
def discover_catalogs(
    databricks_workspace_connection_id: UUID,
    workspace: Optional[str | UUID] = None,
    max_results: Optional[int] = None,
) -> pd.DataFrame:
    """
    Returns a list of catalogs from Unity Catalog.

    This is a wrapper function for the following API: `Databricks Metadata Discovery - Discover Catalogs <https://learn.microsoft.comrest/api/fabric/mirroredazuredatabrickscatalog/databricks-metadata-discovery/discover-catalogs>`_.

    Parameters
    ----------
    databricks_workspace_connection_id : uuid.UUID
        The ID of the Databricks workspace connection.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    max_results : int, default=None
        The maximum number of results to return. If not specified, all results are returned.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of catalogs from Unity Catalog.
    """

    workspace_id = resolve_workspace_id(workspace)

    url = f"/v1/workspaces/{workspace_id}/azuredatabricks/catalogs?databricksWorkspaceConnectionId={databricks_workspace_connection_id}"
    if max_results:
        url += f"&maxResults={max_results}"

    responses = _base_api(request=url, uses_pagination=True)

    columns = {
        "Catalog Name": "str",
        "Catalog Full Name": "str",
        "Catalog Type": "str",
        "Storage Location": "str",
    }

    df = _create_dataframe(columns=columns)

    rows = []
    for r in responses:
        for i in r.get("value", []):
            rows.append(
                {
                    "Catalog Name": i.get("name"),
                    "Catalog Full Name": i.get("fullName"),
                    "Catalog Type": i.get("catalogType"),
                    "Storage Location": i.get("storageLocation"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def discover_schemas(
    catalog: str,
    databricks_workspace_connection_id: UUID,
    workspace: Optional[str | UUID] = None,
    max_results: Optional[int] = None,
) -> pd.DataFrame:
    """
    Returns a list of schemas in the given catalog from Unity Catalog.

    This is a wrapper function for the following API: `Databricks Metadata Discovery - Discover Schemas <https://learn.microsoft.comrest/api/fabric/mirroredazuredatabrickscatalog/databricks-metadata-discovery/discover-schemas>`_.

    Parameters
    ----------
    catalog : str
        The name of the catalog.
    databricks_workspace_connection_id : uuid.UUID
        The ID of the Databricks workspace connection.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    max_results : int, default=None
        The maximum number of results to return. If not specified, all results are returned.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of schemas in the given catalog from Unity Catalog.
    """

    workspace_id = resolve_workspace_id(workspace)

    url = f"/v1/workspaces/{workspace_id}/azuredatabricks/catalogs/{catalog}/schemas?databricksWorkspaceConnectionId={databricks_workspace_connection_id}"
    if max_results:
        url += f"&maxResults={max_results}"

    responses = _base_api(request=url, uses_pagination=True)

    columns = {
        "Catalog Name": "str",
        "Schema Name": "str",
        "Schema Full Name": "str",
        "Storage Location": "str",
    }

    df = _create_dataframe(columns=columns)

    rows = []
    for r in responses:
        for i in r.get("value", []):
            rows.append(
                {
                    "Catalog Name": catalog,
                    "Schema Name": i.get("name"),
                    "Schema Full Name": i.get("fullName"),
                    "Storage Location": i.get("storageLocation"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def discover_tables(
    catalog: str,
    schema: str,
    databricks_workspace_connection_id: UUID,
    workspace: Optional[str | UUID] = None,
    max_results: Optional[int] = None,
) -> pd.DataFrame:
    """
    Returns a list of schemas in the given catalog from Unity Catalog.

    This is a wrapper function for the following API: `Databricks Metadata Discovery - Discover Tables <https://learn.microsoft.comrest/api/fabric/mirroredazuredatabrickscatalog/databricks-metadata-discovery/discover-tables>`_.

    Parameters
    ----------
    catalog : str
        The name of the catalog.
    schema : str
        The name of the schema.
    databricks_workspace_connection_id : uuid.UUID
        The ID of the Databricks workspace connection.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    max_results : int, default=None
        The maximum number of results to return. If not specified, all results are returned.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of schemas in the given catalog from Unity Catalog.
    """

    workspace_id = resolve_workspace_id(workspace)

    url = f"/v1/workspaces/{workspace_id}/azuredatabricks/catalogs/{catalog}/schemas/{schema}/tables?databricksWorkspaceConnectionId={databricks_workspace_connection_id}"
    if max_results:
        url += f"&maxResults={max_results}"

    responses = _base_api(request=url, uses_pagination=True)

    columns = {
        "Catalog Name": "str",
        "Schema Name": "str",
        "Table Name": "str",
        "Table Full Name": "str",
        "Storage Location": "str",
        "Table Type": "str",
        "Data Source Format": "str",
    }

    df = _create_dataframe(columns=columns)

    rows = []
    for r in responses:
        for i in r.get("value", []):
            rows.append(
                {
                    "Catalog Name": catalog,
                    "Schema Name": schema,
                    "Table Name": i.get("name"),
                    "Table Full Name": i.get("fullName"),
                    "Storage Location": i.get("storageLocation"),
                    "Table Type": i.get("tableType"),
                    "Data Source Format": i.get("dataSourceFormat"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df
