from uuid import UUID
import pandas as pd
import time
from typing import Optional
import sempy.fabric as fabric
from sempy_labs._helper_functions import (
    resolve_item_name_and_id,
    resolve_workspace_name_and_id,
)
from sempy_labs.report._items import list_reports_base
from sempy_labs.report._generate_embed_token import generate_embed_token
from sempy_labs.semantic_model._test_dax import (
    _TEST_EVENT_SCHEMA,
    _get_trace_logs,
)
from IPython.display import HTML, display
import sempy_labs._icons as icons
from sempy._utils._log import log


def embed_report_cycle_pages(
    embed_url: str, access_token: str, visible: bool = True
) -> None:
    """
    Embeds a Power BI report (in view mode) within the notebook and cycles
    through each of its pages, activating one page at a time.

    Activating a page causes the visuals on that page to render and issue their
    underlying DAX queries against the semantic model. This is the mechanism
    used to (eventually) capture the trace results of those queries.

    Each page is only advanced once the report signals (via the Power BI
    JavaScript client's ``rendered`` event) that the current page has fully
    finished rendering its visuals, rather than waiting a fixed amount of time.

    Parameters
    ----------
    embed_url : str
        The embed URL of the Power BI report.
    access_token : str
        The embed token used to authenticate the embedded report.
    visible : bool, default=True
        If True, the embedded report is displayed within the notebook while the
        pages are cycled through. If False, the report is embedded and cycled
        through in the background without being shown to the user.
    """

    container_style = (
        "height:800px;width:100%;"
        if visible
        else "height:800px;width:100%;display:none;"
    )

    html = f"""
    <div id="reportContainer" style="{container_style}"></div>

    <script src="https://cdn.jsdelivr.net/npm/powerbi-client@2.23.1/dist/powerbi.min.js"></script>
    <script>
        var models = window['powerbi-client'].models;

        // Embed configuration for the report (read-only / view mode).
        var embedConfig = {{
            type: 'report',
            tokenType: models.TokenType.Embed,
            accessToken: '{access_token}',
            embedUrl: '{embed_url}',
            permissions: models.Permissions.Read,
            viewMode: models.ViewMode.View
        }};

        // Get the container element where the report will be embedded.
        var reportContainer = document.getElementById('reportContainer');

        // Embed the report.
        var report = powerbi.embed(reportContainer, embedConfig);

        var pages = [];
        var index = 0;
        var started = false;

        // Activate the next page in the sequence. The report's 'rendered'
        // event (fired below) drives the progression: each page is only
        // advanced once the previous page has fully finished rendering.
        function activateNext() {{
            if (index >= pages.length) {{
                console.log("Finished cycling through all pages.");
                return;
            }}
            var page = pages[index];
            index++;
            page.setActive().then(function() {{
                console.log("Activated page: " + page.displayName);
            }}).catch(function(error) {{
                console.error("Error activating page:", error);
            }});
        }}

        // Once the report is loaded, retrieve its pages and activate the first
        // one. Subsequent pages are activated from the 'rendered' handler once
        // the current page has fully rendered.
        report.on('loaded', function() {{
            console.log("Report loaded. Cycling through pages...");
            report.getPages().then(function(reportPages) {{
                pages = reportPages;
                started = true;
                activateNext();
            }}).catch(function(error) {{
                console.error("Error retrieving pages:", error);
            }});
        }});

        // The 'rendered' event fires whenever the active page has finished
        // rendering its visuals (i.e. after its DAX queries have completed).
        // Use it to move on to the next page only once the current page is
        // fully rendered.
        report.on('rendered', function() {{
            if (!started) {{ return; }}
            console.log("Page fully rendered.");
            // Wait an additional 2 seconds after the page has rendered before
            // moving on to the next page.
            setTimeout(activateNext, 2000);
        }});

        // Error handling for embed.
        report.on('error', function(event) {{
            console.error("Error embedding the report:", event.detail);
        }});
    </script>
    """
    display(HTML(html))


@log
def capture_report_pages(
    report: str | UUID,
    workspace: Optional[str | UUID] = None,
    visible: bool = True,
    timeout: int = 120,
    quiet_period: int = 10,
) -> pd.DataFrame:
    """
    Opens a Power BI report (in view mode) within the notebook, cycles through
    each page of the report, and captures the trace logs of the DAX queries
    generated by the report's visuals.

    Before the report is embedded, a server-side trace is started against the
    semantic model behind the report (using the same trace events as
    :func:`sempy_labs.semantic_model.test`). The report is then embedded using
    the `Power BI JavaScript client
    <https://learn.microsoft.com/javascript/api/overview/powerbi/>`_ and each
    page is activated in turn. A page is only advanced once the report signals
    that the current page has fully finished rendering its visuals (rather than
    waiting a fixed amount of time), so that each page's visuals render and
    execute their DAX queries against the semantic model. The trace logs of
    those DAX queries are then returned.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    report : str | uuid.UUID
        Name or ID of the Power BI report.
    workspace : str | uuid.UUID, default=None
        The name or ID of the Fabric workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    visible : bool, default=True
        If True, the embedded report is displayed within the notebook while the
        pages are cycled through. If False, the report is embedded and cycled
        through in the background without being shown to the user.
    timeout : int, default=120
        The maximum number of seconds to keep the trace open while the report
        renders and cycles through its pages.
    quiet_period : int, default=10
        The trace is stopped early once no new trace events have been captured
        for this many seconds (after at least one event has been captured),
        indicating the report has finished rendering all of its pages.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe of the captured trace events generated by the report's
        visuals while cycling through each page.
    """

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    report_name, report_id = resolve_item_name_and_id(
        item=report, type="Report", workspace=workspace_id
    )
    """
    {"DatasetId":"974a1e22-6f9c-410b-be9c-45fc53c78cfc","Sources":[{"ReportId":"9be18c55-2e33-4d4e-a88b-c8846c5dfc24","VisualId":"ae7fd3c0d2e25807e685","HostProperties":{"ConsumptionMethod":"Power BI Web App","UserSession":"ca7b502c-a2d8-4a84-92fb-37d7032cf8e2"}}]}
    """

    # Retrieve the report's embed URL and underlying semantic model id.
    report_df = list_reports_base(report=report_id, workspace=workspace_id)
    if report_df.empty:
        raise ValueError(
            f"{icons.red_dot} The '{report_name}' report was not found within the '{workspace_name}' workspace."
        )

    embed_url = report_df["Embed Url"].iloc[0]
    dataset_id = report_df["Dataset Id"].iloc[0]
    dataset_workspace_id = report_df["Dataset Workspace Id"].iloc[0]

    if not embed_url or not dataset_id:
        raise ValueError(
            f"{icons.red_dot} The '{report_name}' report within the '{workspace_name}' workspace cannot be embedded (missing embed URL or semantic model)."
        )

    # Generate an embed token for the report.
    access_token = generate_embed_token(
        dataset_ids=[dataset_id],
        report_ids=[report_id],
    )

    # Start a server-side trace against the semantic model behind the report
    # (using the same trace events as sempy_labs.semantic_model.test), then
    # embed the report and cycle through its pages so the visuals execute their
    # DAX queries while the trace is running.
    df = pd.DataFrame()
    with fabric.create_trace_connection(
        dataset=dataset_id, workspace=dataset_workspace_id
    ) as trace_connection:
        with trace_connection.create_trace(_TEST_EVENT_SCHEMA) as trace:
            trace.start()

            embed_report_cycle_pages(
                embed_url=embed_url,
                access_token=access_token,
                visible=visible,
            )

            # Poll the trace while the report renders in the browser. Stop once
            # no new events have arrived for ``quiet_period`` seconds (the
            # report has finished cycling through its pages) or once the overall
            # ``timeout`` is reached.
            start_time = time.time()
            last_count = 0
            last_change_time = start_time
            while time.time() - start_time < timeout:
                time.sleep(1)
                logs = _get_trace_logs(trace)
                current_count = 0 if logs is None else len(logs)
                if current_count != last_count:
                    last_count = current_count
                    last_change_time = time.time()
                elif (
                    last_count > 0
                    and time.time() - last_change_time >= quiet_period
                ):
                    break

            try:
                stopped = trace.stop()
                if stopped is not None and not stopped.empty:
                    df = stopped
                else:
                    logs = _get_trace_logs(trace)
                    if logs is not None:
                        df = logs
            except Exception:
                logs = _get_trace_logs(trace)
                if logs is not None:
                    df = logs

    return df
