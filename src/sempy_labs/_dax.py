import sempy.fabric as fabric
import pandas as pd
from sempy_labs._helper_functions import (
    format_dax_object_name,
    generate_guid,
    resolve_workspace_id,
    resolve_item_id,
    resolve_item_name_and_id,
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

    return fabric.evaluate_dax(
        dataset=dataset,
        dax_query=dax_query,
        effective_user_name=user_name,
        workspace=workspace,
    )


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
    dax_string : str | typing.List[str]
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

    workspace_id = resolve_workspace_id(workspace)
    dataset_id = resolve_item_id(
        item=dataset, type="SemanticModel", workspace=workspace_id
    )

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

    df = get_dax_query_dependencies(
        dataset=dataset,
        workspace=workspace,
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
    typing.Tuple[pandas.DataFrame, dict]
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


@log
def test(
    dataset: str | UUID,
    dax_string: str,
    workspace: Optional[str | UUID] = None,
    clear_cache: bool = True,
    visualize: bool = True,
) -> pd.DataFrame:
    """
    Runs a DAX query against a semantic model while capturing a server-side
    trace, and computes high-level performance statistics (Total Duration,
    Formula Engine Duration, Storage Engine Duration, and CPU time) using
    the same conventions as `DAX Studio <https://github.com/DaxStudio/DaxStudio>`_.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    dax_string : str
        The DAX query to execute.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    clear_cache : bool, default=True
        If True, clears the dataset cache before running the query so the
        run reflects a cold-cache state.
    visualize : bool, default=True
        If True, displays an interactive HTML widget showing the high-level
        timings (Duration, FE, SE, CPU) and a per-event details table.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe of the captured trace events, including the
        ``Event Class``, ``Event Subclass``, ``Duration`` and ``Cpu Time``
        for each event.
    """

    from sempy_labs._clear_cache import clear_cache as _clear_cache_fn
    from sempy_labs._helper_functions import resolve_workspace_name_and_id

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_item_name_and_id(item=dataset, type='SemanticModel', workspace=workspace_id)

    event_schema = {
        "QueryBegin": [
            "EventClass",
            "TextData",
            "ApplicationName",
            "StartTime",
        ],
        "QueryEnd": [
            "EventClass",
            "EventSubclass",
            "TextData",
            "StartTime",
            "EndTime",
            "Duration",
            "CpuTime",
            "Success",
            "ApplicationName",
        ],
        "VertiPaqSEQueryEnd": [
            "EventClass",
            "EventSubclass",
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
            "TextData",
            "StartTime",
        ],
    }

    if clear_cache:
        _clear_cache_fn(dataset=dataset_id, workspace=workspace_id)

    with fabric.create_trace_connection(
        dataset=datasetid, workspace=workspace_id
    ) as trace_connection:
        with trace_connection.create_trace(event_schema) as trace:
            trace.start()
            # Warm-up evaluation; filtered out of results below.
            fabric.evaluate_dax(
                dataset=datasetid,
                workspace=workspace_id,
                dax_string="EVALUATE {1}",
            )
            # Run the actual DAX query.
            fabric.evaluate_dax(
                dataset=dataset_id, workspace=workspace_id, dax_string=dax_string
            )
            # Allow the trace some time to flush events.
            time.sleep(2)
            df = trace.stop()

    # Drop events from other sessions / warm-up evaluation.
    if "Application Name" in df.columns:
        df = df[~df["Application Name"].isin(["PowerBI", "PowerBIEIM"])]
    if "Text Data" in df.columns:
        df = df[~df["Text Data"].astype(str).str.startswith("EVALUATE {1}")]
    df = df.reset_index(drop=True)

    # Compute aggregate stats using DAX Studio conventions:
    #   Total Duration = QueryEnd.Duration
    #   SE Duration    = sum of VertiPaqSEQueryEnd Duration, EXCLUDING
    #                    internal sub-queries (EventSubclass contains
    #                    "Internal") so we do not double-count time that
    #                    is already rolled up into the parent scan.
    #   FE Duration    = Total Duration - SE Duration
    #   CPU            = QueryEnd.CpuTime
    qe = df[df["Event Class"] == "QueryEnd"]
    total_duration = int(qe["Duration"].iloc[-1]) if not qe.empty else 0
    cpu_time = int(qe["Cpu Time"].iloc[-1]) if not qe.empty else 0

    se_events = df[df["Event Class"] == "VertiPaqSEQueryEnd"]
    if not se_events.empty:
        not_internal = ~se_events["Event Subclass"].astype(str).str.contains(
            "Internal", case=False, na=False
        )
        se_duration = int(se_events.loc[not_internal, "Duration"].sum())
    else:
        se_duration = 0
    fe_duration = max(total_duration - se_duration, 0)

    if visualize:
        _visualize_dax_test(
            df=df,
            total_duration=total_duration,
            fe_duration=fe_duration,
            se_duration=se_duration,
            cpu_time=cpu_time,
            dax_string=dax_string,
            dataset_name=str(dataset),
            workspace_name=workspace_name,
        )

    return df


def _visualize_dax_test(
    df: pd.DataFrame,
    total_duration: int,
    fe_duration: int,
    se_duration: int,
    cpu_time: int,
    dax_string: str,
    dataset_name: Optional[str] = None,
    workspace_name: Optional[str] = None,
    dark_mode: bool = False,
) -> None:
    """Render an Apple-inspired interactive UI for :func:`test` results."""

    import uuid
    from html import escape as _esc
    from IPython.display import display, HTML
    from sempy_labs._ui_components import (
        LIGHT_THEME_VARS as _UI_LIGHT_VARS,
        DARK_THEME_VARS as _UI_DARK_VARS,
        scoped_header_css as _ui_scoped_header_css,
        scoped_attribution_css as _ui_scoped_attribution_css,
        render_header_html as _ui_render_header_html,
        render_attribution_html as _ui_render_attribution_html,
        theme_toggle_script as _ui_theme_toggle_script,
    )

    uid = uuid.uuid4().hex[:8]
    root_selector = f".dtx-{uid}"
    theme_btn_id = f"dtx-theme-{uid}"

    # Compute share-of-total percentages for FE / SE bars.
    if total_duration > 0:
        fe_pct = round(fe_duration / total_duration * 100)
        se_pct = round(se_duration / total_duration * 100)
    else:
        fe_pct = 0
        se_pct = 0

    cards = [
        ("Duration", f"{total_duration:,}", "ms", None),
        ("FE Duration", f"{fe_duration:,}", "ms", fe_pct),
        ("SE Duration", f"{se_duration:,}", "ms", se_pct),
        ("CPU", f"{cpu_time:,}", "ms", None),
    ]

    # Build per-event rows for the details table. Only include events with
    # a meaningful Subclass/Duration (QueryEnd + VertiPaq events).
    detail_classes = {"QueryEnd", "VertiPaqSEQueryEnd", "VertiPaqSEQueryCacheMatch"}
    rows_df = df[df["Event Class"].isin(detail_classes)].copy() if not df.empty else df
    table_rows_html = []
    if not rows_df.empty:
        for _, row in rows_df.iterrows():
            ec = str(row.get("Event Class", "") or "")
            sc = str(row.get("Event Subclass", "") or "")
            dur = row.get("Duration", 0)
            cpu = row.get("Cpu Time", 0)
            try:
                dur_v = int(dur) if pd.notna(dur) else 0
            except (TypeError, ValueError):
                dur_v = 0
            try:
                cpu_v = int(cpu) if pd.notna(cpu) else 0
            except (TypeError, ValueError):
                cpu_v = 0
            subclass_display = sc if sc else ec
            table_rows_html.append(
                "<tr>"
                f"<td>{_esc(ec)}</td>"
                f"<td>{_esc(subclass_display)}</td>"
                f"<td class='dtx-num'>{dur_v:,}</td>"
                f"<td class='dtx-num'>{cpu_v:,}</td>"
                "</tr>"
            )
    if not table_rows_html:
        table_rows_html.append(
            "<tr><td colspan='4' class='dtx-empty'>No trace events captured.</td></tr>"
        )

    ui_header_css = _ui_scoped_header_css(root_selector)
    ui_attribution_css = _ui_scoped_attribution_css(root_selector)

    styles = f"""
    <style>
    {ui_header_css}
    {ui_attribution_css}
    .dtx-{uid} {{
        {_UI_LIGHT_VARS}
        font-family: -apple-system, BlinkMacSystemFont, "SF Pro Display",
            "SF Pro Text", "Helvetica Neue", Helvetica, Arial, sans-serif;
        color: var(--ui-text);
        -webkit-font-smoothing: antialiased;
        -moz-osx-font-smoothing: grayscale;
        max-width: 100%;
        margin: 0;
        padding: 0;
    }}
    .dtx-{uid}.dtx-dark {{
        {_UI_DARK_VARS}
    }}
    .dtx-{uid} *, .dtx-{uid} *::before, .dtx-{uid} *::after {{
        box-sizing: border-box;
    }}
    .dtx-{uid} .dtx-container {{
        background: var(--ui-bg);
        border: 1px solid var(--ui-border);
        border-radius: 12px;
        box-shadow: var(--ui-shadow-lg);
        overflow: hidden;
    }}
    .dtx-{uid} .dtx-header {{
        padding: 22px 24px 18px 24px;
        background: var(--ui-bg);
    }}
    .dtx-{uid} .dtx-cards {{
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 12px;
        padding: 0 24px 18px 24px;
    }}
    .dtx-{uid} .dtx-card {{
        background: var(--ui-bg-secondary);
        border: 1px solid var(--ui-border);
        border-radius: 8px;
        padding: 14px 16px;
        display: flex;
        flex-direction: column;
        gap: 4px;
    }}
    .dtx-{uid} .dtx-card-label {{
        font-size: 11px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.04em;
        color: var(--ui-text-tertiary);
    }}
    .dtx-{uid} .dtx-card-value {{
        font-size: 22px;
        font-weight: 700;
        letter-spacing: -0.02em;
        color: var(--ui-text);
        font-variant-numeric: tabular-nums;
        line-height: 1.1;
    }}
    .dtx-{uid} .dtx-card-unit {{
        font-size: 12px;
        font-weight: 500;
        color: var(--ui-text-tertiary);
        margin-left: 4px;
    }}
    .dtx-{uid} .dtx-card-sub {{
        font-size: 11px;
        color: var(--ui-text-secondary);
        font-variant-numeric: tabular-nums;
        margin-top: 2px;
    }}
    .dtx-{uid} .dtx-query {{
        margin: 0 24px 16px 24px;
        padding: 12px 14px;
        background: var(--ui-bg-tertiary);
        border: 1px solid var(--ui-border);
        border-radius: 8px;
        font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
        font-size: 12px;
        line-height: 1.5;
        color: var(--ui-text-secondary);
        white-space: pre-wrap;
        max-height: 140px;
        overflow: auto;
    }}
    .dtx-{uid} .dtx-section-title {{
        padding: 4px 24px 10px 24px;
        font-size: 11px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.04em;
        color: var(--ui-text-tertiary);
    }}
    .dtx-{uid} .dtx-table-wrap {{
        overflow-x: auto;
        overflow-y: auto;
        max-height: 480px;
        border-top: 1px solid var(--ui-border);
    }}
    .dtx-{uid} table {{
        width: 100%;
        border-collapse: separate;
        border-spacing: 0;
        font-size: 13px;
    }}
    .dtx-{uid} thead th {{
        position: sticky;
        top: 0;
        padding: 10px 16px;
        text-align: left;
        font-weight: 600;
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.04em;
        color: var(--ui-text-secondary);
        background: var(--ui-bg-secondary);
        border-bottom: 1px solid var(--ui-border-strong);
        white-space: nowrap;
    }}
    .dtx-{uid} tbody td {{
        padding: 9px 16px;
        border-bottom: 1px solid var(--ui-border);
        color: var(--ui-text);
        white-space: nowrap;
    }}
    .dtx-{uid} tbody tr:hover td {{
        background: var(--ui-surface-2);
    }}
    .dtx-{uid} td.dtx-num {{
        text-align: right;
        font-variant-numeric: tabular-nums;
    }}
    .dtx-{uid} td.dtx-empty {{
        text-align: center;
        color: var(--ui-text-tertiary);
        padding: 24px 16px;
    }}
    </style>
    """

    header_html = _ui_render_header_html(
        title="DAX Query Performance",
        dataset_name=dataset_name,
        workspace_name=workspace_name,
        theme_btn_id=theme_btn_id,
        dark_mode=dark_mode,
    )

    card_parts = []
    for label, value, unit, pct in cards:
        sub = f"<div class='dtx-card-sub'>{pct}% of total</div>" if pct is not None else ""
        card_parts.append(
            "<div class='dtx-card'>"
            f"<div class='dtx-card-label'>{_esc(label)}</div>"
            f"<div class='dtx-card-value'>{value}<span class='dtx-card-unit'>{unit}</span></div>"
            f"{sub}"
            "</div>"
        )
    cards_html = "<div class='dtx-cards'>" + "".join(card_parts) + "</div>"

    query_html = (
        f"<div class='dtx-query'>{_esc(dax_string.strip())}</div>"
        if dax_string and dax_string.strip()
        else ""
    )

    table_html = (
        "<div class='dtx-section-title'>Trace details</div>"
        "<div class='dtx-table-wrap'><table>"
        "<thead><tr>"
        "<th>Event Class</th>"
        "<th>Event Subclass</th>"
        "<th style='text-align:right'>Duration (ms)</th>"
        "<th style='text-align:right'>CPU (ms)</th>"
        "</tr></thead>"
        f"<tbody>{''.join(table_rows_html)}</tbody>"
        "</table></div>"
    )

    attribution_html = _ui_render_attribution_html()

    root_class = f"dtx-{uid}" + (" dtx-dark" if dark_mode else "")
    body_html = (
        f"<div class='{root_class}'>"
        "<div class='dtx-container'>"
        f"<div class='dtx-header'>{header_html}</div>"
        f"{cards_html}"
        f"{query_html}"
        f"{table_html}"
        f"{attribution_html}"
        "</div>"
        "</div>"
    )

    theme_script = _ui_theme_toggle_script(
        btn_id=theme_btn_id,
        root_selector=root_selector,
        dark_class="dtx-dark",
    )

    display(HTML(styles + body_html + theme_script))
