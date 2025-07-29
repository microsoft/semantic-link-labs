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
from typing import Optional
from uuid import UUID
from sempy._utils._log import log
import sempy.fabric as fabric


@log
def capture_report_queries(
    report: str | UUID,
    workspace: Optional[str | UUID] = None,
    table_name: Optional[str] = None,
    schema_name: Optional[str] = None,
    lakehouse: Optional[str | UUID] = None,
    lakehouse_workspace: Optional[str | UUID] = None,
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
    table_name = table_name.replace(' ', '_')

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

    (dataset_id, dataset_name, dataset_workspace_id, dataset_workspace_name) = resolve_dataset_from_report(report=report, workspace=workspace)

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

                with action_output:
                    print(f"✅ Trace stopped. Events collected: {len(df)}")
                globals()["df_trace"] = df
                save_as_delta_table(
                    dataframe=globals()["df_trace"],
                    delta_table_name=table_name,
                    write_mode="overwrite",
                    lakehouse=lakehouse,
                    workspace=lakehouse_workspace,
                    schema=schema_name,
                )

    # --- Start trace immediately in background ---
    threading.Thread(target=trace_worker, daemon=True).start()

    #return globals()["df_trace"]

