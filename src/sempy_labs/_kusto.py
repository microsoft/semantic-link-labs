import requests
import pandas as pd
from sempy.fabric.exceptions import FabricHTTPException
from sempy._utils._log import log
import sempy_labs._icons as icons
from typing import Optional
from uuid import UUID
from sempy_labs._kql_databases import _resolve_cluster_uri
from sempy_labs._helper_functions import resolve_item_name_and_id


@log
def query_kusto(
    query: str,
    eventhouse: str | UUID,
    workspace: Optional[str | UUID] = None,
    language: str = "kql",
) -> pd.DataFrame:
    """
    Runs a KQL query against a KQL database.

    Parameters
    ----------
    query : str
        The KQL query.
    eventhouse : str | uuid.UUID
        The eventhouse name or ID.
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

    if language not in ["kql", "sql"]:
        raise ValueError(
            f"Invalid language '{language}'. Only 'kql' and 'sql' are supported."
        )

    cluster_uri = _resolve_cluster_uri(workspace=workspace)
    token = notebookutils.credentials.getToken(cluster_uri)

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    (eventhouse_name, eventhouse_id) = resolve_item_name_and_id(
        item=eventhouse, type="Eventhouse", workspace=workspace
    )
    payload = {"db": eventhouse_name, "csl": query}
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

    for col_info in columns_info:
        col_name = col_info["ColumnName"]
        data_type = col_info["DataType"]

        try:
            if data_type == "DateTime":
                df[col_name] = pd.to_datetime(df[col_name])
            elif data_type in ["Int64", "Int32", "Long"]:
                df[col_name] = (
                    pd.to_numeric(df[col_name], errors="coerce")
                    .fillna(0)
                    .astype("int64")
                )
            elif data_type == "Real" or data_type == "Double":
                df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
            else:
                # Convert any other type to string, change as needed
                df[col_name] = df[col_name].astype(str)
        except Exception as e:
            print(
                f"{icons.yellow_dot} Could not convert column {col_name} to {data_type}, defaulting to string: {str(e)}"
            )
            df[col_name] = df[col_name].astype(str)

    return df


def query_workspace_monitoring(
    query: str, workspace: Optional[str | UUID] = None, language: str = "kql"
) -> pd.DataFrame:
    """
    Runs a KQL query against the Fabric workspace monitoring database.

    Parameters
    ----------
    query : str
        The KQL query.
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

    return query_kusto(
        query=query,
        eventhouse="Monitoring Eventhouse",
        workspace=workspace,
        language=language,
    )
