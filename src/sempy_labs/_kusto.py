import sempy.fabric as fabric
import requests
import pandas as pd
from sempy.fabric.exceptions import FabricHTTPException
from sempy._utils._log import log
import sempy_labs._icons as icons
from typing import Optional, List
from uuid import UUID
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    create_abfss_path,
    save_as_delta_table,
)
from sempy_labs._kql_databases import _resolve_cluster_uri


@log
def query_kusto(cluster_uri: str, query: str, database: str) -> pd.DataFrame:
    """
    Shows the KQL querysets within a workspace.

    Parameters
    ----------
    cluster_uri : str
        The Query URI for the KQL database. Example: "https://guid.kusto.fabric.microsoft.com"
    query : str
        The KQL query.
    database : str
        The KQL database name.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the result of the KQL query.
    """

    import notebookutils

    token = notebookutils.credentials.getToken(cluster_uri)

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    payload = {"db": database, "csl": query}

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