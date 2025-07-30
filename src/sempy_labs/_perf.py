import ipywidgets as widgets
from IPython.display import display
import threading
import time
import sempy_labs.report as rep
from sempy_labs._helper_functions import (
    save_as_delta_table,
    resolve_dataset_from_report,
    resolve_workspace_name_and_id,
    resolve_item_name_and_id,
)
from typing import Optional, List
from uuid import UUID
from sempy._utils._log import log
import sempy.fabric as fabric
import sempy_labs._icons as icons


@log
def trace_report(
    report: str | UUID,
    workspace: Optional[str | UUID] = None,
    table_name: Optional[str] = None,
    schema_name: Optional[str] = None,
    lakehouse: Optional[str | UUID] = None,
    lakehouse_workspace: Optional[str | UUID] = None,
    user_name: Optional[str | List[str]] = None,
    event_schema: Optional[dict] = None,
):
    """
    Captures queries from a report in a background thread. Wait for the notification that the trace has started before clicking the 'Render Report' button.

    After rendering the report and interacting as desired, click the 'Stop Trace' button to stop capturing queries. This will save the captured queries to a Delta table.

    Parameters
    ----------
    report : str | uuid.UUID
        The report name or ID.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID where the report is located. If None, uses the default workspace.
    table_name : str, default=None
        The name of the Delta table to save the trace data. If None, defaults to 'report_queries'.
    schema_name : str, default=None
        The schema name for the Delta table to be saved. Defaults to None which is used for tables with no schema.
    lakehouse : str | uuid.UUID, default=None
        The name or ID of the lakehouse where the Delta table will be saved.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    lakehouse_workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID in which the lakehouse exists.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    user_name : str | List[str], default=None
        The user name or list of user names to filter the queries by. If None, captures all queries.
    event_schema : dict, default=None
        The schema of the events to capture. If None, defaults to jus the QueryEnd event with common fields.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the captured trace data.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (report_name, report_id) = resolve_item_name_and_id(
        item=report, type="Report", workspace=workspace_id
    )

    if table_name is None:
        table_name = f"{workspace_name}_{report_name}_queries"
    table_name = table_name.replace(" ", "_")

    if isinstance(user_name, str):
        user_name = [user_name]

    # --- UI Elements ---
    start_output = widgets.Output()
    render_button = widgets.Button(
        description="Render Report", button_style="info", disabled=True
    )
    stop_button = widgets.Button(
        description="Stop Trace", button_style="danger", disabled=True
    )
    action_output = widgets.Output()

    display(start_output, render_button, stop_button, action_output)
    render_output = widgets.Output()
    display(render_output)

    # --- State control ---
    should_stop = threading.Event()
    trace_ready = threading.Event()

    # --- Button callbacks ---
    def on_render_click(b):
        with action_output:
            print("Rendering report...")
        with render_output:
            rep_display = rep.launch_report(report=report, workspace=workspace)
            display(rep_display)
            stop_button.disabled = False  # Allow stopping only after rendering

    def on_stop_click(b):
        with action_output:
            print("Stopping trace...")
        should_stop.set()

    render_button.on_click(on_render_click)
    stop_button.on_click(on_stop_click)

    (dataset_id, dataset_name, dataset_workspace_id, dataset_workspace_name) = (
        resolve_dataset_from_report(report=report, workspace=workspace)
    )

    if event_schema is None:
        event_schema = {
            "QueryEnd": [
                "EventClass",
                "EventSubclass",
                "CurrentTime",
                "NTUserName",
                "NTCanonicalUserName",
                "TextData",
                "StartTime",
                "EndTime",
                "Duration",
                "CpuTime",
                "Success",
                "ApplicationName",
                # "ApplicationContext",
            ]
        }

    # --- Background trace logic ---
    def trace_worker():
        with fabric.create_trace_connection(
            dataset=dataset_id, workspace=dataset_workspace_id
        ) as trace_connection:
            with trace_connection.create_trace(event_schema=event_schema) as trace:
                trace.start()
                with start_output:
                    print("✅ Trace started. Now click 'Render Report' and interact.")
                render_button.disabled = False  # Allow report rendering
                trace_ready.set()
                should_stop.wait()  # Wait for user to click stop

                df = trace.stop()
                time.sleep(3)

                if user_name:
                    df = df[df["NTCanonicalUserName"].isin(user_name)]

                with action_output:
                    print(f"✅ Trace stopped. Events collected: {len(df)}")
                globals()["df_trace"] = df
                if not df.empty:
                    save_as_delta_table(
                        dataframe=globals()["df_trace"],
                        delta_table_name=table_name,
                        write_mode="overwrite",
                        lakehouse=lakehouse,
                        workspace=lakehouse_workspace,
                        schema=schema_name,
                    )
                else:
                    print(
                        f"{icons.warning} No events captured. Check if the report was rendered and interacted with after starting the trace."
                    )

    # --- Start trace immediately in background ---
    threading.Thread(target=trace_worker, daemon=True).start()


@log
def trace_semantic_model(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    table_name: Optional[str] = None,
    schema_name: Optional[str] = None,
    lakehouse: Optional[str | UUID] = None,
    lakehouse_workspace: Optional[str | UUID] = None,
    user_name: Optional[str | List[str]] = None,
    event_schema: Optional[dict] = None,
):
    """
    Traces a semantic model by capturing queries in a background thread. Click the 'Start Trace' button to begin capturing queries. Click the 'Stop Trace' button to stop capturing queries and save the results to a Delta table.

    Parameters
    ----------
    dataset : str | uuid.UUID
        The report name or ID.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID where the report is located. If None, uses the default workspace.
    table_name : str, default=None
        The name of the Delta table to save the trace data. If None, defaults to 'report_queries'.
    schema_name : str, default=None
        The schema name for the Delta table to be saved. Defaults to None which is used for tables with no schema.
    lakehouse : str | uuid.UUID, default=None
        The name or ID of the lakehouse where the Delta table will be saved.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    lakehouse_workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID in which the lakehouse exists.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    user_name : str | List[str], default=None
        The user name or list of user names to filter the queries by. If None, captures all queries.
    event_schema : dict, default=None
        The schema of the events to capture. If None, defaults to jus the QueryEnd event with common fields.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the captured trace data.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_item_name_and_id(
        item=dataset, type="SemanticModel", workspace=workspace_id
    )

    if table_name is None:
        table_name = f"{workspace_name}_{dataset_name}_queries"
    table_name = table_name.replace(" ", "_")

    if isinstance(user_name, str):
        user_name = [user_name]

    # --- UI Elements ---
    start_output = widgets.Output()
    start_button = widgets.Button(
        description="Start Trace", button_style="info", disabled=False
    )
    stop_button = widgets.Button(
        description="Stop Trace", button_style="danger", disabled=True
    )
    action_output = widgets.Output()

    display(start_output, start_button, stop_button, action_output)

    # --- State control ---
    should_start = threading.Event()
    should_stop = threading.Event()
    trace_ready = threading.Event()

    # --- Button callbacks ---
    def on_start_click(b):
        with action_output:
            print("✅ Trace started. You may now run queries.")
        should_start.set()
        should_stop.clear()
        start_button.disabled = True
        stop_button.disabled = False

    def on_stop_click(b):
        with action_output:
            print("Stopping trace...")
        should_stop.set()
        stop_button.disabled = True

    stop_button.on_click(on_stop_click)
    start_button.on_click(on_start_click)

    if event_schema is None:
        event_schema = {
            "QueryEnd": [
                "EventClass",
                "EventSubclass",
                "CurrentTime",
                "NTUserName",
                "NTCanonicalUserName",
                "TextData",
                "StartTime",
                "EndTime",
                "Duration",
                "CpuTime",
                "Success",
                "ApplicationName",
                # "ApplicationContext",
            ]
        }

    # --- Background trace logic ---
    def trace_worker():
        with fabric.create_trace_connection(
            dataset=dataset, workspace=workspace
        ) as trace_connection:
            with trace_connection.create_trace(event_schema=event_schema) as trace:

                with start_output:
                    trace.start()

                trace_ready.set()
                should_stop.wait()  # Wait for user to click stop

                df = trace.stop()
                time.sleep(3)

                if user_name:
                    df = df[df["NTCanonicalUserName"].isin(user_name)]

                with action_output:
                    print(f"✅ Trace stopped. Events collected: {len(df)}")
                globals()["df_trace"] = df
                if not df.empty:
                    save_as_delta_table(
                        dataframe=globals()["df_trace"],
                        delta_table_name=table_name,
                        write_mode="overwrite",
                        lakehouse=lakehouse,
                        workspace=lakehouse_workspace,
                        schema=schema_name,
                    )
                else:
                    print(f"{icons.warning} No events captured.")

    # --- Start trace immediately in background ---
    threading.Thread(target=trace_worker, daemon=True).start()
