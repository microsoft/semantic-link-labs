from uuid import UUID
import pandas as pd
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_item_name_and_id,
    resolve_workspace_name_and_id,
    _create_dataframe,
)
from sempy_labs.report._items import list_reports_base
from sempy_labs.report._generate_embed_token import generate_embed_token
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
            activateNext();
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
) -> pd.DataFrame:
    """
    Opens a Power BI report (in view mode) within the notebook and cycles
    through each page of the report.

    The report is embedded using the `Power BI JavaScript client
    <https://learn.microsoft.com/javascript/api/overview/powerbi/>`_ and each
    page is activated in turn. A page is only advanced once the report signals
    that the current page has fully finished rendering its visuals (rather than
    waiting a fixed amount of time), so that each page's visuals render and
    execute their DAX queries against the semantic model before moving on. This
    is intended to be used as the basis for capturing the trace results of the
    DAX queries generated by each component on each page of the report.

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

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the report which was opened and cycled through.
    """

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    report_name, report_id = resolve_item_name_and_id(
        item=report, type="Report", workspace=workspace_id
    )

    columns = {
        "Workspace Name": "str",
        "Workspace Id": "str",
        "Report Name": "str",
        "Report Id": "str",
        "Embed Url": "str",
        "Dataset Id": "str",
    }
    df = _create_dataframe(columns=columns)

    # Retrieve the report's embed URL and underlying semantic model id.
    report_df = list_reports_base(report=report_id, workspace=workspace_id)
    if report_df.empty:
        raise ValueError(
            f"{icons.red_dot} The '{report_name}' report was not found within the '{workspace_name}' workspace."
        )

    embed_url = report_df["Embed Url"].iloc[0]
    dataset_id = report_df["Dataset Id"].iloc[0]

    if not embed_url or not dataset_id:
        raise ValueError(
            f"{icons.red_dot} The '{report_name}' report within the '{workspace_name}' workspace cannot be embedded (missing embed URL or semantic model)."
        )

    # Generate an embed token and embed the report, cycling through its pages.
    access_token = generate_embed_token(
        dataset_ids=[dataset_id],
        report_ids=[report_id],
    )

    embed_report_cycle_pages(
        embed_url=embed_url,
        access_token=access_token,
        visible=visible,
    )

    rows = [
        {
            "Workspace Name": workspace_name,
            "Workspace Id": workspace_id,
            "Report Name": report_name,
            "Report Id": report_id,
            "Embed Url": embed_url,
            "Dataset Id": dataset_id,
        }
    ]

    df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df
