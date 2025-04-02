import sempy.fabric as fabric
import pandas as pd
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    format_dax_object_name,
    resolve_dataset_name_and_id,
    _base_api,
    generate_guid,
)
from sempy_labs._model_dependencies import get_model_calc_dependencies
from typing import Optional, List, Tuple
from sempy._utils._log import log
from uuid import UUID
from sempy_labs.directlake._warm_cache import _put_columns_into_memory
import sempy_labs._icons as icons
import time


@log
def evaluate_dax_impersonation(
    dataset: str | UUID,
    dax_query: str,
    user_name: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Runs a DAX query against a semantic model using the `REST API <https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/execute-queries-in-group>`_.

    Compared to evaluate_dax this allows passing the user name for impersonation.
    Note that the REST API has significant limitations compared to the XMLA endpoint.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    dax_query : str
        The DAX query.
    user_name : str
        The user name (i.e. hello@goodbye.com).
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe holding the result of the DAX query.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    payload = {
        "queries": [{"query": dax_query}],
        "impersonatedUserName": user_name,
    }

    response = _base_api(
        request=f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/executeQueries",
        method="post",
        payload=payload,
    )
    data = response.json()["results"][0]["tables"]

    # Get all possible column names from all rows because null columns aren't returned
    all_columns = set()
    for item in data:
        for row in item["rows"]:
            all_columns.update(row.keys())

    # Create rows with all columns, filling missing values with None
    rows = []
    for item in data:
        for row in item["rows"]:
            # Create a new row with all columns, defaulting to None
            new_row = {col: row.get(col) for col in all_columns}
            rows.append(new_row)

    # Create DataFrame from the processed rows
    df = pd.DataFrame(rows)

    return df


@log
def get_dax_query_dependencies(
    dataset: str | UUID,
    dax_string: str | List[str],
    put_in_memory: bool = False,
    show_vertipaq_stats: bool = True,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Obtains the columns on which a DAX query depends, including model dependencies. Shows Vertipaq statistics (i.e. Total Size, Data Size, Dictionary Size, Hierarchy Size) for easy prioritizing.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    dax_string : str | List[str]
        The DAX query or list of DAX queries.
    put_in_memory : bool, default=False
        If True, ensures that the dependent columns are put into memory in order to give realistic Vertipaq stats (i.e. Total Size etc.).
    show_vertipaq_stats : bool, default=True
        If True, shows vertipaq stats (i.e. Total Size, Data Size, Dictionary Size, Hierarchy Size)
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the dependent columns of a given DAX query including model dependencies.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    fabric.refresh_tom_cache(workspace=workspace)

    if isinstance(dax_string, str):
        dax_string = [dax_string]

    final_df = pd.DataFrame(columns=["Object Type", "Table", "Object"])

    cd = get_model_calc_dependencies(dataset=dataset_id, workspace=workspace_id)

    for dax in dax_string:
        # Escape quotes in dax
        dax = dax.replace('"', '""')
        final_query = f"""
            EVALUATE
            VAR source_query = "{dax}"
            VAR all_dependencies = SELECTCOLUMNS(
                INFO.CALCDEPENDENCY("QUERY", source_query),
                    "Referenced Object Type",[REFERENCED_OBJECT_TYPE],
                    "Referenced Table", [REFERENCED_TABLE],
                    "Referenced Object", [REFERENCED_OBJECT]
                )             
            RETURN all_dependencies
            """
        dep = fabric.evaluate_dax(
            dataset=dataset_id, workspace=workspace_id, dax_string=final_query
        )

        # Clean up column names and values (remove outside square brackets, underscorees in object type)
        dep.columns = dep.columns.map(lambda x: x[1:-1])
        dep["Referenced Object Type"] = (
            dep["Referenced Object Type"].str.replace("_", " ").str.title()
        )

        # Dataframe df will contain the output of all dependencies of the objects used in the query
        df = dep.copy()

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
        final_df = pd.concat([df, final_df], ignore_index=True)

    final_df = final_df[
        (final_df["Object Type"].isin(["Column", "Calc Column"]))
        & (~final_df["Object"].str.startswith("RowNumber-"))
    ]
    final_df = final_df.drop_duplicates().reset_index(drop=True)
    final_df = final_df.rename(columns={"Table": "Table Name", "Object": "Column Name"})
    final_df.drop(columns=["Object Type"], inplace=True)

    if not show_vertipaq_stats:
        return final_df

    # Get vertipaq stats, filter to just the objects in the df dataframe
    final_df["Full Object"] = format_dax_object_name(
        final_df["Table Name"], final_df["Column Name"]
    )
    dfC = fabric.list_columns(dataset=dataset_id, workspace=workspace_id, extended=True)
    dfC["Full Object"] = format_dax_object_name(dfC["Table Name"], dfC["Column Name"])

    dfC_filtered = dfC[dfC["Full Object"].isin(final_df["Full Object"].values)][
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
        # Only put columns in memory if they are in a Direct Lake table (and are not already in memory)
        dfP = fabric.list_partitions(dataset=dataset, workspace=workspace)
        dl_tables = dfP[dfP["Mode"] == "DirectLake"]["Table Name"].unique().tolist()
        not_in_memory = dfC_filtered[
            (dfC_filtered["Table Name"].isin(dl_tables))
            & (dfC_filtered["Is Resident"] == False)
        ]

        if not not_in_memory.empty:
            _put_columns_into_memory(
                dataset=dataset,
                workspace=workspace,
                col_df=dfC_filtered,
                return_dataframe=False,
            )

            # Get column stats again
            dfC = fabric.list_columns(
                dataset=dataset_id, workspace=workspace_id, extended=True
            )
            dfC["Full Object"] = format_dax_object_name(
                dfC["Table Name"], dfC["Column Name"]
            )

            dfC_filtered = dfC[dfC["Full Object"].isin(final_df["Full Object"].values)][
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

    dfC_filtered.drop(["Full Object"], axis=1, inplace=True)

    return dfC_filtered


@log
def get_dax_query_memory_size(
    dataset: str | UUID, dax_string: str, workspace: Optional[str | UUID] = None
) -> int:
    """
    Obtains the total size, in bytes, used by all columns that a DAX query depends on.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    dax_string : str
        The DAX query.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    int
        The total size, in bytes, used by all columns that the DAX query depends on.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    df = get_dax_query_dependencies(
        dataset=dataset_id,
        workspace=workspace_id,
        dax_string=dax_string,
        put_in_memory=True,
    )

    return df["Total Size"].sum()


@log
def _dax_perf_test(
    dataset: str,
    dax_queries: dict,
    clear_cache_before_run: bool = False,
    refresh_type: Optional[str] = None,
    rest_time: int = 2,
    workspace: Optional[str] = None,
) -> Tuple[pd.DataFrame, dict]:
    """
    Runs a performance test on a set of DAX queries.

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
    clear_cache_before_run : bool, default=False
    refresh_type : str, default=None
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
    from sempy_labs._refresh_semantic_model import refresh_semantic_model
    from sempy_labs._clear_cache import clear_cache

    event_schema = {
        "QueryBegin": [
            "EventClass",
            "EventSubclass",
            "CurrentTime",
            "NTUserName",
            "TextData",
            "StartTime",
            "ApplicationName",
        ],
        "QueryEnd": [
            "EventClass",
            "EventSubclass",
            "CurrentTime",
            "NTUserName",
            "TextData",
            "StartTime",
            "EndTime",
            "Duration",
            "CpuTime",
            "Success",
            "ApplicationName",
        ],
        "VertiPaqSEQueryBegin": [
            "EventClass",
            "EventSubclass",
            "CurrentTime",
            "NTUserName",
            "TextData",
            "StartTime",
        ],
        "VertiPaqSEQueryEnd": [
            "EventClass",
            "EventSubclass",
            "CurrentTime",
            "NTUserName",
            "TextData",
            "StartTime",
            "EndTime",
            "Duration",
            "CpuTime",
            "Success",
        ],
        "VertiPaqSEQueryCacheMatch": [
            "EventClass",
            "EventSubclass",
            "CurrentTime",
            "NTUserName",
            "TextData",
        ],
    }

    # Add Execution Metrics
    event_schema["ExecutionMetrics"] = ["EventClass", "ApplicationName", "TextData"]
    # Add DAX Query Plan
    # event_schema["DAXQueryPlan"] = ["EventClass", "EventSubclass", "CurrentTime", "StartTime", "EndTime", "Duration", "CpuTime", "ApplicationName", "TextData"]

    query_results = {}

    # Establish trace connection
    with fabric.create_trace_connection(
        dataset=dataset, workspace=workspace
    ) as trace_connection:
        with trace_connection.create_trace(event_schema) as trace:
            trace.start()
            print(f"{icons.in_progress} Starting performance testing...")
            # Loop through DAX queries
            for name, dax in dax_queries.items():

                if clear_cache_before_run:
                    clear_cache(dataset=dataset, workspace=workspace)
                if refresh_type is not None:
                    refresh_semantic_model(
                        dataset=dataset, workspace=workspace, refresh_type=refresh_type
                    )

                # EVALUATE {1} is used to initate a warm cache
                fabric.evaluate_dax(
                    dataset=dataset, workspace=workspace, dax_string="""EVALUATE {1}"""
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

            # Step 1: Filter out unnecessary operations
            query_names = list(dax_queries.keys())
            df = df[
                ~df["Application Name"].isin(["PowerBI", "PowerBIEIM"])
                & (~df["Text Data"].str.startswith("EVALUATE {1}"))
            ]
            query_begin = df["Event Class"] == "QueryBegin"
            temp_column_name = "QueryName_INT"
            df = df.copy()
            df[temp_column_name] = query_begin.cumsum()
            df[temp_column_name] = (
                df[temp_column_name]
                .where(query_begin, None)  # Assign None to non-query begin rows
                .ffill()  # Forward fill None values
                .astype("Int64")  # Use pandas nullable integer type for numeric indices
            )

            df.loc[df[temp_column_name].notna(), "Query Name"] = (
                df[temp_column_name]
                .dropna()
                .astype(int)
                .map(lambda x: query_names[x - 1])
            )
            df = df[df[temp_column_name] != None]
            df = df.drop(columns=[temp_column_name])

            query_to_guid = {
                name: generate_guid() for name in df["Query Name"].unique()
            }
            df["Query ID"] = df["Query Name"].map(query_to_guid)

    df = df.reset_index(drop=True)

    return df, query_results


def _dax_perf_test_bulk(
    mapping: dict,
    clear_cache_before_run: bool = False,
    refresh_type: Optional[str] = None,
    rest_time: int = 2,
):
    """
    mapping is something like this:

    mapping = {
        "Workspace1": {
            "Dataset1": {
                "Query1": "EVALUATE ...",
                "Query2": "EVALUATE ...",
            },
            "Dataset2": {
                "Query3": "EVALUATE ...",
                "Query4": "EVALUATE ...",
            }
        },
        "Workspace2": {
            "Dataset3": {
                "Query5": "EVALUATE ...",
                "Query6": "EVALUATE ...",
            },
            "Dataset4": {
                "Query7": "EVALUATE ...",
                "Query8": "EVALUATE ...",
            }
        }
    }
    """

    for workspace, datasets in mapping.items():
        for dataset, queries in datasets.items():
            _dax_perf_test(
                dataset=dataset,
                dax_queries=queries,
                clear_cache_before_run=clear_cache_before_run,
                refresh_type=refresh_type,
                rest_time=rest_time,
                workspace=workspace,
            )
