import requests
import pandas as pd
from sempy.fabric.exceptions import FabricHTTPException
from sempy._utils._log import log
import sempy_labs._icons as icons
from typing import Optional
from uuid import UUID
from sempy_labs._kql_databases import _resolve_cluster_uri
from sempy_labs._helper_functions import resolve_item_id


@log
def query_kusto(
    query: str,
    kql_database: str | UUID,
    workspace: Optional[str | UUID] = None,
    language: str = "kql",
) -> pd.DataFrame:
    """
    Runs a KQL query against a KQL database.

    Parameters
    ----------
    query : str
        The query (supports KQL or SQL - make sure to specify the language parameter accordingly).
    kql_database : str | uuid.UUID
        The KQL database name or ID.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    language : str, default="kql"
        The language of the query. Currently "kql' and "sql" are supported.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the result of the KQL query.
    """

    import notebookutils

    language = language.lower()
    if language not in ["kql", "sql"]:
        raise ValueError(
            f"{icons._red_dot} Invalid language '{language}'. Only 'kql' and 'sql' are supported."
        )

    cluster_uri = _resolve_cluster_uri(kql_database=kql_database, workspace=workspace)
    token = notebookutils.credentials.getToken(cluster_uri)

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    kql_database_id = resolve_item_id(
        item=kql_database, type="KQLDatabase", workspace=workspace
    )
    payload = {"db": kql_database_id, "csl": query}
    if language == "sql":
        payload["properties"] = {"Options": {"query_language": "sql"}}

    response = requests.post(
        f"{cluster_uri}/v1/rest/query",
        headers=headers,
        json=payload,
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)

    results = response.json()
    columns_info = results["Tables"][0]["Columns"]
    rows = results["Tables"][0]["Rows"]

    df = pd.DataFrame(rows, columns=[col["ColumnName"] for col in columns_info])

    return df
    # for col_info in columns_info:
    #    col_name = col_info["ColumnName"]
    #    data_type = col_info["DataType"]

    #    try:
    #        if data_type == "DateTime":
    #            df[col_name] = pd.to_datetime(df[col_name])
    #        elif data_type in ["Int64", "Int32", "Long"]:
    #            df[col_name] = (
    #                pd.to_numeric(df[col_name], errors="coerce")
    #                .fillna(0)
    #                .astype("int64")
    #            )
    #        elif data_type == "Real" or data_type == "Double":
    #            df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
    #        else:
    #            # Convert any other type to string, change as needed
    #            df[col_name] = df[col_name].astype(str)
    #    except Exception as e:
    #        print(
    #            f"{icons.yellow_dot} Could not convert column {col_name} to {data_type}, defaulting to string: {str(e)}"
    #        )
    #        df[col_name] = df[col_name].astype(str)

    return df


@log
def query_workspace_monitoring(
    query: str, workspace: Optional[str | UUID] = None, language: str = "kql"
) -> pd.DataFrame:
    """
    Runs a query against the Fabric workspace monitoring database. Workspace monitoring must be enabled on the workspace to use this function.

    Parameters
    ----------
    query : str
        The query (supports KQL or SQL - make sure to specify the language parameter accordingly).
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    language : str, default="kql"
        The language of the query. Currently "kql' and "sql" are supported.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the result of the query.
    """

    return query_kusto(
        query=query,
        kql_database="Monitoring KQL database",
        workspace=workspace,
        language=language,
    )
