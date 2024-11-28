import sempy
import sempy.fabric as fabric
import pandas as pd
import datetime
from typing import Optional, Tuple
from sempy._utils._log import log
import time
import sempy_labs._icons as icons
from sempy_labs._helper_functions import (
    resolve_dataset_id,
    resolve_workspace_name_and_id,
    _get_max_run_id,
    resolve_lakehouse_name,
    save_as_delta_table,
    _resolve_workspace_capacity_name_id_sku,
    format_dax_object_name,
)
from sempy_labs.lakehouse._lakehouse import lakehouse_attached
from sempy_labs._clear_cache import clear_cache
from sempy_labs.lakehouse._get_lakehouse_tables import get_lakehouse_tables
import tqdm


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


def _get_dax_query_dependencies_all(
    dataset: str,
    dax_string: str,
    workspace: Optional[str] = None,
) -> pd.DataFrame:

    from sempy_labs._model_dependencies import get_model_calc_dependencies

    if workspace is None:
        workspace = fabric.resolve_workspace_name(workspace)

    # Escape quotes in dax
    dax_string = dax_string.replace('"', '""')
    final_query = f"""
        EVALUATE
        VAR source_query = "{dax_string}"
        VAR all_dependencies = SELECTCOLUMNS(
            INFO.CALCDEPENDENCY("QUERY", source_query),
                "Referenced Object Type",[REFERENCED_OBJECT_TYPE],
                "Referenced Table", [REFERENCED_TABLE],
                "Referenced Object", [REFERENCED_OBJECT]
            )
        RETURN all_dependencies
        """
    dep = fabric.evaluate_dax(
        dataset=dataset, workspace=workspace, dax_string=final_query
    )

    # Clean up column names and values (remove outside square brackets, underscorees in object type)
    dep.columns = dep.columns.map(lambda x: x[1:-1])
    dep["Referenced Object Type"] = (
        dep["Referenced Object Type"].str.replace("_", " ").str.title()
    )
    dep

    # Dataframe df will contain the output of all dependencies of the objects used in the query
    df = dep.copy()

    cd = get_model_calc_dependencies(dataset=dataset, workspace=workspace)

    for _, r in dep.iterrows():
        ot = r["Referenced Object Type"]
        object_name = r["Referenced Object"]
        table_name = r["Referenced Table"]
        cd_filt = cd[
            (cd["Object Type"] == ot)
            & (cd["Object Name"] == object_name)
            & (cd["Table Name"] == table_name)
        ]

        # Adds in the dependencies of each object used in the query (i.e. relationship etc.)
        if len(cd_filt) > 0:
            subset = cd_filt[
                ["Referenced Object Type", "Referenced Table", "Referenced Object"]
            ]
            df = pd.concat([df, subset], ignore_index=True)

    df.columns = df.columns.map(lambda x: x.replace("Referenced ", ""))
    df = df[(~df["Object"].str.startswith("RowNumber-"))]
    # Remove duplicates
    df = df.drop_duplicates().reset_index(drop=True)

    return df


@log
def get_dax_query_dependencies(
    dataset: str,
    dax_string: str,
    put_in_memory: bool = False,
    show_vertipaq_stats: bool = True,
    workspace: Optional[str] = None,
) -> pd.DataFrame:
    """
    Obtains the columns on which a DAX query depends, including model dependencies. Shows Vertipaq statistics (i.e. Total Size, Data Size, Dictionary Size, Hierarchy Size) for easy prioritizing.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    dax_string : str
        The DAX query.
    put_in_memory : bool, default=False
        If True, ensures that the dependent columns are put into memory in order to give realistic Vertipaq stats (i.e. Total Size etc.).
    show_vertipaq_stats : bool, default=True
        If True, shows Vertipaq statistics.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the dependent columns of a given DAX query including model dependencies.
    """

    from sempy_labs._model_dependencies import get_model_calc_dependencies

    if workspace is None:
        workspace = fabric.resolve_workspace_name(workspace)

    if put_in_memory:
        show_vertipaq_stats = True

    # Escape quotes in dax
    dax_string = dax_string.replace('"', '""')
    final_query = f"""
        EVALUATE
        VAR source_query = "{dax_string}"
        VAR all_dependencies = SELECTCOLUMNS(
            INFO.CALCDEPENDENCY("QUERY", source_query),
                "Referenced Object Type",[REFERENCED_OBJECT_TYPE],
                "Referenced Table", [REFERENCED_TABLE],
                "Referenced Object", [REFERENCED_OBJECT]
            )
        RETURN all_dependencies
        """
    dep = fabric.evaluate_dax(
        dataset=dataset, workspace=workspace, dax_string=final_query
    )

    # Clean up column names and values (remove outside square brackets, underscorees in object type)
    dep.columns = dep.columns.map(lambda x: x[1:-1])
    dep["Referenced Object Type"] = (
        dep["Referenced Object Type"].str.replace("_", " ").str.title()
    )
    dep

    # Dataframe df will contain the output of all dependencies of the objects used in the query
    df = dep.copy()

    cd = get_model_calc_dependencies(dataset=dataset, workspace=workspace)

    for _, r in dep.iterrows():
        ot = r["Referenced Object Type"]
        object_name = r["Referenced Object"]
        table_name = r["Referenced Table"]
        cd_filt = cd[
            (cd["Object Type"] == ot)
            & (cd["Object Name"] == object_name)
            & (cd["Table Name"] == table_name)
        ]

        # Adds in the dependencies of each object used in the query (i.e. relationship etc.)
        if len(cd_filt) > 0:
            subset = cd_filt[
                ["Referenced Object Type", "Referenced Table", "Referenced Object"]
            ]
            df = pd.concat([df, subset], ignore_index=True)

    df.columns = df.columns.map(lambda x: x.replace("Referenced ", ""))
    # Remove duplicates
    df = df.drop_duplicates().reset_index(drop=True)
    # Only show columns and remove the rownumber column
    df = df[
        (df["Object Type"].isin(["Column", "Calc Column"]))
        & (~df["Object"].str.startswith("RowNumber-"))
    ]

    # Get vertipaq stats, filter to just the objects in the df dataframe
    if show_vertipaq_stats:
        df["Full Object"] = format_dax_object_name(df["Table"], df["Object"])
        dfC = fabric.list_columns(dataset=dataset, workspace=workspace, extended=True)
        dfC["Full Object"] = format_dax_object_name(
            dfC["Table Name"], dfC["Column Name"]
        )

        dfC_filtered = dfC[dfC["Full Object"].isin(df["Full Object"].values)][
            [
                "Table Name",
                "Column Name",
                "Total Size",
                "Data Size",
                "Dictionary Size",
                "Hierarchy Size",
                "Is Resident",
                "Full Object",
            ]
        ].reset_index(drop=True)

    if put_in_memory:
        not_in_memory = dfC_filtered[dfC_filtered["Is Resident"] == False]

        if len(not_in_memory) > 0:
            tbls = not_in_memory["Table Name"].unique()

            # Run basic query to get columns into memory; completed one table at a time (so as not to overload the capacity)
            for table_name in (bar := tqdm(tbls)):
                bar.set_description(f"Warming the '{table_name}' table...")
                css = ", ".join(
                    not_in_memory[not_in_memory["Table Name"] == table_name][
                        "Full Object"
                    ]
                    .astype(str)
                    .tolist()
                )
                dax = f"""EVALUATE TOPN(1,SUMMARIZECOLUMNS({css}))"""
                fabric.evaluate_dax(
                    dataset=dataset, dax_string=dax, workspace=workspace
                )

            # Get column stats again
            dfC = fabric.list_columns(
                dataset=dataset, workspace=workspace, extended=True
            )
            dfC["Full Object"] = format_dax_object_name(
                dfC["Table Name"], dfC["Column Name"]
            )

            dfC_filtered = dfC[dfC["Full Object"].isin(df["Full Object"].values)][
                [
                    "Table Name",
                    "Column Name",
                    "Total Size",
                    "Data Size",
                    "Dictionary Size",
                    "Hierarchy Size",
                    "Is Resident",
                    "Full Object",
                ]
            ].reset_index(drop=True)

    if show_vertipaq_stats:
        dfC_filtered.drop(["Full Object"], axis=1, inplace=True)

    return dfC_filtered


@log
def get_dax_query_memory_size(
    dataset: str, dax_string: str, workspace: Optional[str] = None
) -> int:
    """
    Obtains the total size, in bytes, used by all columns that a DAX query depends on.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    dax_string : str
        The DAX query.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    int
        The total size, in bytes, used by all columns that the DAX query depends on.
    """

    if workspace is None:
        workspace = fabric.resolve_workspace_name(workspace)

    df = get_dax_query_dependencies(
        dataset=dataset, workspace=workspace, dax_string=dax_string, put_in_memory=True
    )

    return df["Total Size"].sum()


@log
def trace_dax(
    dataset: str,
    dax_queries: dict,
    rest_time: int = 2,
    clear_cache_before_run: bool = False,
    clear_cache_before_each_query: bool = False,
    trace_vertipaq_se: bool = False,
    trace_direct_query: bool = False,
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
    trace_vertipaq_se : bool, default=False
        If True, adds the following events to the trace: VertiPaq SE Query Begin, VertiPaq SE Query End, VertiPaq SE Query Cache Match
    trace_direct_query : bool, default=False
        If True, adds the following events to the trace: Direct Query Begin, Direct Query End
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
    dq_cols = [
        "EventClass",
        "CurrentTime",
        "StartTime",
        "EndTime",
        "Duration",
        "CpuTime",
        "Success",
        "Error",
        "TextData",
    ]

    event_schema = {
        "QueryBegin": begin_cols + ["ApplicationName"],
        "QueryEnd": end_cols + ["ApplicationName"],
    }

    if trace_vertipaq_se:
        event_schema["VertiPaqSEQueryBegin"] = begin_cols
        event_schema["VertiPaqSEQueryEnd"] = end_cols
        event_schema["VertiPaqSEQueryCacheMatch"] = base_cols
    if trace_direct_query:
        event_schema["DirectQueryBegin"] = dq_cols
        event_schema["DirectQueryEnd"] = dq_cols

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


@log
def dax_perf_test(
    dataset: str,
    dax_queries: dict,
    cache_type: str = "warm",
    rest_time: int = 2,
    workspace: Optional[str] = None,
) -> Tuple[pd.DataFrame, dict]:
    """
    Runs a warm/cold cache test against a single or set of DAX queries. Valid for import-only or Direct Lake semantic models. 
    Cold-cache testing is only available for Direct Lake semantic models.

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
    cache_type : str, default="warm"
        Allows testing for 'warm' or 'cold' cache scenarios. 'Cold' cache testing is only available for Direct Lake semantic models.
    rest_time : int, default=2
        Rest time (in seconds) between the execution of each DAX query.
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
    from sempy_labs.tom import connect_semantic_model
    from sempy_labs._refresh_semantic_model import refresh_semantic_model

    sempy.fabric._client._utils._init_analysis_services()
    import Microsoft.AnalysisServices.Tabular as TOM

    if workspace is None:
        workspace = fabric.resolve_workspace_name()

    cache_type = _validate_cache_type(cache_type)

    dl_tables = []
    with connect_semantic_model(
        dataset=dataset, workspace=workspace, readonly=True
    ) as tom:
        for p in tom.all_partitions():
            if p.Mode == TOM.ModeType.DirectLake:
                dl_tables.append(p.Parent.Name)
            elif p.Mode == TOM.ModeType.DirectQuery or (
                p.Mode == TOM.ModeType.Default
                and tom.model.Model.DefaultMode == TOM.ModeType.DirectQuery
            ):
                raise ValueError(
                    f"{icons.red_dot} This testing is only for Import & Direct Lake semantic models."
                )

        if cache_type != "warm" and not tom.is_direct_lake():
            raise ValueError(
                f"{icons.red_dot} Cold cache testing is only available for Direct Lake semantic models."
            )

    base_cols = ["EventClass", "EventSubclass", "CurrentTime", "NTUserName", "TextData"]
    begin_cols = base_cols + ["StartTime"]
    end_cols = base_cols + ["StartTime", "EndTime", "Duration", "CpuTime", "Success"]

    event_schema = {
        "QueryBegin": begin_cols + ["ApplicationName"],
        "QueryEnd": end_cols + ["ApplicationName"],
    }

    event_schema["VertiPaqSEQueryBegin"] = begin_cols
    event_schema["VertiPaqSEQueryEnd"] = end_cols
    event_schema["VertiPaqSEQueryCacheMatch"] = base_cols

    query_results = {}
    evaluate_one = """ EVALUATE {1}"""

    # Establish trace connection
    with fabric.create_trace_connection(
        dataset=dataset, workspace=workspace
    ) as trace_connection:
        with trace_connection.create_trace(event_schema) as trace:
            trace.start()
            print(f"{icons.in_progress} Starting performance testing...")
            # Loop through DAX queries
            for i, (name, dax) in enumerate(dax_queries.items()):

                # Cold Cache Direct Lake
                if dl_tables and cache_type == "cold":
                    # Process Clear
                    refresh_semantic_model(
                        dataset=dataset,
                        workspace=workspace,
                        refresh_type="clearValues",
                        tables=dl_tables,
                    )
                    # Process Full
                    refresh_semantic_model(
                        dataset=dataset, workspace=workspace, refresh_type="full"
                    )
                    # Evaluate {1}
                    fabric.evaluate_dax(
                        dataset=dataset, workspace=workspace, dax_string=evaluate_one
                    )
                    # Run DAX Query
                    result = fabric.evaluate_dax(
                        dataset=dataset, workspace=workspace, dax_string=dax
                    )
                else:
                    # Run DAX Query
                    fabric.evaluate_dax(
                        dataset=dataset, workspace=workspace, dax_string=dax
                    )
                    # Clear Cache
                    clear_cache(dataset=dataset, workspace=workspace)
                    # Evaluate {1}
                    fabric.evaluate_dax(
                        dataset=dataset, workspace=workspace, dax_string=evaluate_one
                    )
                    # Run DAX Query
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

            query_names = list(dax_queries.keys())

            # DL Cold Cache
            if dl_tables and cache_type == "cold":
                # Filter out unnecessary operations
                df = df[
                    ~df["Application Name"].isin(["PowerBI", "PowerBIEIM"])
                    & (~df["Text Data"].str.startswith("EVALUATE {1}"))
                ]
                query_begin = df["Event Class"] == "QueryBegin"
                # Name queries per dictionary
                df["Query Name"] = (query_begin).cumsum()
                df["Query Name"] = df["Query Name"].where(query_begin, None).ffill()
                df["Query Name"] = pd.to_numeric(df["Query Name"], downcast="integer")
                df["Query Name"] = df["Query Name"].map(lambda x: query_names[x - 1])
            else:
                # Filter out unnecessary operations
                df = df[(~df["Text Data"].str.startswith("EVALUATE {1}"))]
                query_begin = df["Event Class"] == "QueryBegin"
                # Name queries per dictionary
                suffix = "_removeXXX"
                query_names_full = [
                    item
                    for query in query_names
                    for item in (f"{query}{suffix}", query)
                ]
                # Step 3: Assign query names by group and convert to integer
                df["Query Name"] = (query_begin).cumsum()
                df["Query Name"] = df["Query Name"].where(query_begin, None).ffill()
                df["Query Name"] = pd.to_numeric(df["Query Name"], downcast="integer")
                # Step 4: Map to full query names
                df["Query Name"] = df["Query Name"].map(
                    lambda x: query_names_full[x - 1]
                )
                df = df[~df["Query Name"].str.endswith(suffix)]

    df = df.reset_index(drop=True)

    return df, query_results


def _validate_cache_type(cache_type: str) -> str:

    cache_type = cache_type.lower()
    cache_types = ["warm", "cold"]
    if cache_type not in cache_types:
        raise ValueError(
            f"{icons.red_dot} Invalid cache type. Valid options: {cache_types}."
        )
    return cache_type


def run_benchmark(
    dataset: str,
    dax_queries: dict,
    cache_type: str = "warm",
    workspace: Optional[str] = None,
):

    from sempy_labs._documentation import save_semantic_model_metadata

    if workspace is None:
        workspace = fabric.resolve_workspace_name()

    workspace_id = fabric.resolve_workspace_id(workspace)
    capacity_id, capacity_name, sku, region = _resolve_workspace_capacity_name_id_sku(
        workspace
    )
    dataset_id = resolve_dataset_id(dataset, workspace)
    cache_type = _validate_cache_type(cache_type)

    # Get RunId
    table_name = "SLL_Measures"

    if not lakehouse_attached():
        raise ValueError(
            f"{icons.red_dot} A lakehouse must be attached to the notebook."
        )

    lakehouse_id = fabric.get_lakehouse_id()
    lakehouse_workspace = fabric.resolve_workspace_name()
    lakehouse_name = resolve_lakehouse_name(lakehouse_id, lakehouse_workspace)

    dfLT = get_lakehouse_tables(lakehouse_name, lakehouse_workspace)
    dfLT_filt = dfLT[dfLT["Table Name"] == table_name]
    if len(dfLT_filt) == 0:
        run_id = 1
    else:
        run_id = _get_max_run_id(lakehouse=lakehouse_name, table_name=table_name) + 1
    time_stamp = datetime.datetime.now()

    dfC = save_semantic_model_metadata(
        dataset=dataset, workspace=workspace, run_id=run_id, time_stamp=time_stamp
    )

    # Run and save trace data
    trace_result, query_result = dax_perf_test(
        dataset=dataset,
        workspace=workspace,
        dax_queries=dax_queries,
        cache_type=cache_type,
    )

    trace_schema = {
        "Capacity_Name": "string",
        "Capacity_Id": "string",
        "SKU": "string",
        "Region": "string",
        "Workspace_Name": "string",
        "Workspace_Id": "string",
        "Dataset_Name": "string",
        "Dataset_Id": "string",
        "Query_Name": "string",
        "Query_Text": "string",
        "Cache_Type": "string",
        "Duration": "long",
        "SE_Duration": "long",
        "SE_CPU": "long",
        "SE_Cache": "long",
        "SE_Queries": "long",
        "Column_Dependencies": "string",
        "Column_Dependencies_Size": "long",
        "Measure_Dependencies": "string",
        "Relationship_Dependencies": "string",
        "RunId": "long",
        "Timestamp": "timestamp",
    }
    df = pd.DataFrame(columns=list(trace_schema.keys()))

    for query_name in trace_result["Query Name"].unique().tolist():
        df_trace = trace_result[trace_result["Query Name"] == query_name]
        # Capture Query Text
        query_begin = df_trace[df_trace["Event Class"] == "QueryBegin"]
        query_text = query_begin["Text Data"].iloc[0]

        # Filter to only end events; filter out internal events
        df_trace = df_trace[
            (~df_trace["Event Subclass"].str.endswith("Internal"))
            & (df_trace["Event Class"].str.endswith("End"))
        ]

        # SE Cache: # of times the cache match event occurred
        se_cache = len(df_trace[df_trace["Event Class"] == "VertiPaqSEQueryCacheMatch"])

        # Total Time -> QueryEnd Duration
        total_duration = df_trace[df_trace["Event Class"] == "QueryEnd"][
            "Duration"
        ].sum()

        # SE Duration: Sum of Duration for Vertipaq End or DQEnd event
        se_duration = df_trace[
            (df_trace["Event Class"].str.endswith("End"))
            & (df_trace["Event Class"] != "QueryEnd")
        ]["Duration"].sum()

        # SE CPU: Sum of CPU for Vertipaq End or DQEnd event
        se_cpu = se_duration = df_trace[
            (df_trace["Event Class"].str.endswith("End"))
            & (df_trace["Event Class"] != "QueryEnd")
        ]["Cpu Time"].sum()

        # SE Queries: # of times the Vertipaq End or DQEnd event occurred
        se_queries = len(
            df_trace[
                (df_trace["Event Class"].str.endswith("End"))
                & (df_trace["Event Class"] != "QueryEnd")
            ]
        )

        # Collect query dependencies
        dep = _get_dax_query_dependencies_all(
            dataset=dataset,
            workspace=workspace,
            dax_string=query_text,
        )

        # Column dependencies
        filtered_dep = dep[dep["Object Type"].isin(["Column", "Calc Column"])][
            ["Table", "Object"]
        ]
        columns_used = [
            f"'{table}'[{obj}]"
            for table, obj in zip(filtered_dep["Table"], filtered_dep["Object"])
        ]
        dfC["Object"] = format_dax_object_name(dfC["Table_Name"], dfC["Column_Name"])
        dfC_filt = dfC[dfC["Object"].isin(columns_used)]
        total_size = dfC_filt["Total_Size"].sum()

        # Measure dependencies
        measures_used = dep[dep["Object Type"] == "Measure"]["Object"].tolist()

        # Relationship dependencies
        relationships_used = dep[dep["Object Type"] == "Relationship"][
            "Object"
        ].tolist()

        new_data = {
            "Capacity_Name": capacity_name,
            "Capacity_Id": capacity_id,
            "SKU": sku,
            "Region": region,
            "Workspace_Name": workspace,
            "Workspace_Id": workspace_id,
            "Dataset_Name": dataset,
            "Dataset_Id": dataset_id,
            "Query_Name": str(query_name),
            "Query_Text": query_text,
            "Cache_Type": cache_type,
            "Duration": total_duration,
            "SE_Duration": se_duration,
            "SE_CPU": se_cpu,
            "SE_Cache": se_cache,
            "SE_Queries": se_queries,
            "Column_Dependencies": str(columns_used),
            "Column_Dependencies_Size": total_size,
            "Measure_Dependencies": str(measures_used),
            "Relationship_Dependencies": str(relationships_used),
            "RunId": run_id,
            "Timestamp": time_stamp,
        }

        if df.empty:
            df = pd.DataFrame(new_data, index=[0])
        else:
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    df["Query_Text"] = df["Query_Text"].astype(str)

    save_as_delta_table(
        dataframe=df,
        delta_table_name="SLL_PerfBenchmark",
        write_mode="append",
        schema=trace_schema,
    )
