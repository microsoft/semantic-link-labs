import sempy.fabric as fabric
import pandas as pd
from typing import Optional, Tuple
from sempy._utils._log import log
import time
import sempy_labs._icons as icons
from sempy_labs._helper_functions import (
    resolve_dataset_id,
    resolve_workspace_name_and_id,
)
from sempy_labs._clear_cache import clear_cache


@log
def evaluate_dax_impersonation(
    dataset: str,
    dax_query: str,
    user_name: Optional[str] = None,
    workspace: Optional[str] = None,
):
    """
    Runs a DAX query against a semantic model using the `REST API <https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/execute-queries-in-group>`_.

    Compared to evaluate_dax this allows passing the user name for impersonation.
    Note that the REST API has significant limitations compared to the XMLA endpoint.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    dax_query : str
        The DAX query.
    user_name : str
        The user name (i.e. hello@goodbye.com).
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe holding the result of the DAX query.
    """

    # https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/execute-queries-in-group

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    dataset_id = resolve_dataset_id(dataset=dataset, workspace=workspace)

    request_body = {
        "queries": [{"query": dax_query}],
        "impersonatedUserName": user_name,
    }

    client = fabric.PowerBIRestClient()
    response = client.post(
        f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/executeQueries",
        json=request_body,
    )
    data = response.json()["results"][0]["tables"]
    column_names = data[0]["rows"][0].keys()
    data_rows = [row.values() for item in data for row in item["rows"]]
    df = pd.DataFrame(data_rows, columns=column_names)

    return df


def trace_dax(
    dataset: str,
    dax_queries: dict,
    rest_time: int = 2,
    clear_cache_before_run: bool = False,
    clear_cache_before_each_query: bool = False,
    workspace: Optional[str] = None,
) -> Tuple[pd.DataFrame, dict]:
    """
    Runs a SQL Profiler trace over a set of DAX queries.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    dax_queries : dict
        The dax queries to run in a dictionary format. Here is an example:
        {
            "Sales Amount Test", """ """ EVALUATE SUMMARIZECOLUMNS("Sales Amount", [Sales Amount]) """ """,
            "Order Quantity with Product", """ """ EVALUATE SUMMARIZECOLUMNS('Product'[Color], "Order Qty", [Order Qty]) """ """,
        }
    rest_time : int, default=2
        Rest time (in seconds) between the execution of each DAX query.
    clear_cache_before_run : bool, default=False
        If True, clears the cache before running any DAX queries.
    clear_cache_before_each_query : bool, default=False
        If True, clears the cache before running each DAX query.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    Tuple[pandas.DataFrame, dict]
        A pandas dataframe showing the SQL profiler trace results of the DAX queries.
        A dictionary of the query results in pandas dataframes.
    """

    if workspace is None:
        workspace = fabric.resolve_workspace_name()

    base_cols = ["EventClass", "EventSubclass", "CurrentTime", "NTUserName", "TextData"]
    begin_cols = base_cols + ["StartTime"]
    end_cols = base_cols + ["StartTime", "EndTime", "Duration", "CpuTime", "Success"]

    event_schema = {
        "VertiPaqSEQueryBegin": begin_cols,
        "VertiPaqSEQueryEnd": end_cols,
        "VertiPaqSEQueryCacheMatch": base_cols,
        "QueryBegin": begin_cols + ["ApplicationName"],
        "QueryEnd": end_cols + ["ApplicationName"],
    }

    query_results = {}

    if clear_cache_before_run:
        clear_cache(dataset=dataset, workspace=workspace)

    # Establish trace connection
    with fabric.create_trace_connection(
        dataset=dataset, workspace=workspace
    ) as trace_connection:
        with trace_connection.create_trace(event_schema) as trace:
            trace.start()
            # Loop through DAX queries
            for i, (name, dax) in enumerate(dax_queries.items()):
                # Clear cache for each query but not if done already before the run began
                if clear_cache_before_each_query and not (
                    i == 0 and clear_cache_before_run
                ):
                    clear_cache(dataset=dataset, workspace=workspace)

                result = fabric.evaluate_dax(
                    dataset=dataset, workspace=workspace, dax_string=dax
                )
                # Add results to output
                query_results[name] = result

                time.sleep(rest_time)
                print(f"{icons.green_dot} The '{name}' query has completed.")

            df = trace.stop()
            # Allow time to collect trace results
            time.sleep(5)

            # Name queries per dictionary
            query_names = list(dax_queries.keys())
            query_begin = df["Event Class"] == "QueryBegin"
            df["Query Name"] = (query_begin).cumsum()
            df["Query Name"] = df["Query Name"].where(query_begin, None).ffill()
            df["Query Name"] = pd.to_numeric(df["Query Name"], downcast="integer")
            df["Query Name"] = df["Query Name"].map(lambda x: query_names[x - 1])

    return df, query_results
