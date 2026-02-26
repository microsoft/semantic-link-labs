from uuid import UUID
import pandas as pd
from typing import Optional, List
from tqdm.auto import tqdm
from sempy_labs._helper_functions import (
    resolve_item_name_and_id,
    resolve_workspace_name_and_id,
    _base_api,
    _create_dataframe,
)
from IPython.display import HTML, display
import sempy_labs._icons as icons
import time
from sempy_labs.report._generate_embed_token import generate_embed_token


def embed_report_save_in_edit_mode(embed_url, access_token: str) -> pd.DataFrame:
    html = f"""
    <div id="reportContainer" style="height:800px;width:100%;display:none;"></div>

    <script src="https://cdn.jsdelivr.net/npm/powerbi-client@2.23.1/dist/powerbi.min.js"></script>
    <script>
        var models = window['powerbi-client'].models;

        // Embed configuration for the report
        var embedConfig = {{
            type: 'report',
            tokenType: models.TokenType.Embed,
            accessToken: '{access_token}',
            embedUrl: '{embed_url}',
            permissions: models.Permissions.ReadWrite,
            viewMode: models.ViewMode.Edit
        }};

        // Get the container element where the report will be embedded (hidden from user)
        var reportContainer = document.getElementById('reportContainer');

        // Embed the report (will be invisible)
        var report = powerbi.embed(reportContainer, embedConfig);

        // Listen for the 'rendered' event to ensure the report is fully loaded
        report.on('rendered', function() {{
            console.log("Report rendered successfully in background.");

            // Trigger save once the report is rendered
            report.save().then(function() {{
                console.log("Report saved successfully in background!");
            }}).catch(function(error) {{
                console.error("Error saving the report:", error);
            }});
        }});

        // Error handling for embed
        report.on('error', function(event) {{
            console.error("Error embedding the report:", event.detail);
        }});
    </script>
    """
    display(HTML(html))


def upgrade_to_pbir(
    report: Optional[str | UUID | List[str | UUID]] = None,
    workspace: Optional[str | UUID | List[str | UUID]] = None,
):
    """
    Upgrades a Power BI report to the new `PBIR <https://powerbi.microsoft.com/blog/power-bi-enhanced-report-format-pbir-in-power-bi-desktop-developer-mode-preview>`_ format.

    Parameters
    ----------
    report : str | uuid.UUID | typing.List[str | uuid.UUID], default=None
        Name or ID of the Power BI report. Also accepts a list of report name/IDs.
        If set to None, upgrades all eligible reports in the specified workspace(s).
    workspace : str | uuid.UUID | typing.List[str | uuid.UUID], default=None
        The name or ID of the Fabric workspace(s).
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the format of all reports in the specified workspace(s) after conducting the upgrade.
    """

    if isinstance(workspace, (str, UUID)) or workspace is None:
        workspace = [workspace]

    workspaces = {}
    for w in workspace:
        workspace_name, workspace_id = resolve_workspace_name_and_id(w)
        workspaces[workspace_id] = workspace_name

    columns = {
        "Workspace Name": "str",
        "Workspace Id": "str",
        "Report Name": "str",
        "Report Id": "str",
        "Format": "str",
    }

    df = _create_dataframe(columns=columns)

    rows = []
    for workspace_id, workspace_name in workspaces.items():
        url = f"/v1.0/myorg/groups/{workspace_id}/reports"
        response = _base_api(request=url, client="fabric_sp")

        eligible_for_upgrade = {}
        # Collect reports eligible for upgrade within the workspace
        for rpt in response.json().get("value", []):
            rpt_id = rpt.get("id")
            rpt_name = rpt.get("name")
            rpt_format = rpt.get("format")
            embed_url = rpt.get("embedUrl")
            dataset_id = rpt.get("datasetId")
            if rpt_format == "PBIRLegacy" and dataset_id is not None and embed_url is not None:
                eligible_for_upgrade[rpt_id] = (embed_url, dataset_id, rpt_name)     

        # Determine which reports to process
        if report is None:
            report_ids_to_process = list(eligible_for_upgrade.keys())
        else:
            if isinstance(report, str):
                report = [report]

            report_ids_to_process = []

            for r in report:
                rpt_name, rpt_id = resolve_item_name_and_id(
                    item=r, type="Report", workspace=workspace_id
                )

                if rpt_id in eligible_for_upgrade:
                    report_ids_to_process.append(rpt_id)
                else:
                    print(
                        f"{icons.warning} The {rpt_name} report in the '{workspace_name}' workspace is not eligible for upgrade."
                    )

        # Run upgrade for selected reports
        total_reports = len(report_ids_to_process)
        for idx, rpt_id in enumerate(
            bar := tqdm(report_ids_to_process, desc="Upgrading reports", unit="report"),
            start=1,
        ):
            embed_url, dataset_id, rpt_name = eligible_for_upgrade[rpt_id]
            bar.set_description(
                f"Upgrading '{rpt_name}' within the '{workspace_name}' workspace ({idx}/{total_reports})"
            )

            access_token = generate_embed_token(
                dataset_ids=[dataset_id],
                report_ids=[rpt_id],
            )

            embed_report_save_in_edit_mode(embed_url, access_token)

            row = check_upgrade_status(
                report_id=rpt_id,
                report_name=rpt_name,
                workspace_id=workspace_id,
                workspace_name=workspace_name,
            )

            rows.append(row)

        # Double check if there are any reports that were not upgraded yet and check their status again (to account for any potential delay in the upgrade process)
        not_upgraded_yet = [
            row["Report Id"]
            for row in rows
            if row.get("Format") == "PBIRLegacy"
        ]
        if not_upgraded_yet:
            for r in rows:
                rpt_id = r.get('Report Id')
                rpt_name = r.get('Report Name')
                ws_id = r.get('Workspace Id')
                ws_name = r.get('Workspace Name')
                row = check_upgrade_status(
                    report_id=rpt_id,
                    report_name=rpt_name,
                    workspace_id=ws_id,
                    workspace_name=ws_name,
                    verbose=True,
                    time_limit=120) # check for up to 2 minutes
                if row.get('Format') == 'PBIR':
                    # Update the row in the list of rows
                    for idx, existing_row in enumerate(rows):
                        if existing_row.get('Report Id') == rpt_id:
                            rows[idx] = row
                            break

    if rows:
        df = pd.DataFrame(rows)

    return df


# Define the time limit (2 minute)
TIME_BETWEEN_REQUESTS = 2  # seconds


# Function to check the upgrade status
def check_upgrade_status(report_id, report_name, workspace_id, workspace_name, verbose=False, time_limit = 30):
    start_time = time.time()
    while time.time() - start_time < time_limit:
        response = _base_api(request=f"/v1.0/myorg/groups/{workspace_id}/reports/{report_id}", client="fabric_sp")
        format = response.json().get('format')
        if format == "PBIR":
            #print(
            #    f"{icons.green_dot} The '{report_name}' report within the '{workspace_name}' workspace has been upgraded to PBIR format."
            #)
            break

        # Wait for 2 seconds before the next request
        time.sleep(TIME_BETWEEN_REQUESTS)

    if format != "PBIR" and verbose:
        print(
            f"{icons.yellow_dot} The '{report_name}' report within the '{workspace_name}' workspace has not yet been upgraded to PBIR format."
        )

    return {
        "Workspace Name": workspace_name,
        "Workspace Id": workspace_id,
        "Report Name": report_name,
        "Report Id": report_id,
        "Format": format,
    }


