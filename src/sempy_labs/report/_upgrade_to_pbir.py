from uuid import UUID
from typing import Optional, List
from sempy_labs._helper_functions import (
    resolve_item_name_and_id,
    resolve_workspace_name_and_id,
    _base_api,
)
from IPython.display import HTML, display
import sempy_labs._icons as icons
import time


def generate_embed_token(dataset_ids: List[UUID], report_ids: List[UUID]):

    if not isinstance(dataset_ids, list):
        dataset_ids = [dataset_ids]
    if not isinstance(report_ids, list):
        report_ids = [report_ids]

    payload = {
        "datasets": [{"id": dataset_id} for dataset_id in dataset_ids],
        "reports": [{"id": report_id, "allowEdit": True} for report_id in report_ids],
    }

    response = _base_api(
        request="/v1.0/myorg/GenerateToken",
        method="post",
        client="fabric_sp",
        payload=payload,
    )
    return response.json().get("token")


def embed_report_edit_mode(embed_url, access_token: str, height=800):
    html = f"""
    <div id="reportContainer" style="height:{height}px;width:100%;display:none;"></div>

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

    if isinstance(workspace, (str, UUID)):
        workspace = [workspace]
    workspaces = {
        resolve_workspace_name_and_id(w)[1]: resolve_workspace_name_and_id(w)[0]
        for w in workspace
    }

    for workspace_id, workspace_name in workspaces.items():
        url = f"/v1.0/myorg/groups/{workspace_id}/reports"
        response = _base_api(request=url, client="fabric_sp")

        eligible_for_upgrade = {}
        updated_reports = []
        for rpt in response.json().get("value", []):
            rpt_id = rpt.get("id")
            rpt_format = rpt.get("format")
            embed_url = rpt.get("embedUrl")
            dataset_id = rpt.get("datasetId")
            if rpt_format == "PBIRLegacy":
                eligible_for_upgrade[rpt_id] = (embed_url, dataset_id)

        if report is None:
            for rpt_id, (embed_url, dataset_id) in eligible_for_upgrade.items():
                access_token = generate_embed_token(
                    dataset_ids=[dataset_id], report_ids=[rpt_id]
                )
                embed_report_edit_mode(embed_url, access_token)
                updated_reports.append(rpt_id)
        elif isinstance(report, list):
            for r in report:
                (rpt_name, rpt_id) = resolve_item_name_and_id(
                    item=r, type="Report", workspace=workspace_id
                )
                if rpt_id in eligible_for_upgrade:
                    embed_url, dataset_id = eligible_for_upgrade[rpt_id]
                    access_token = generate_embed_token(
                        dataset_ids=[dataset_id], report_ids=[rpt_id]
                    )
                    embed_report_edit_mode(embed_url, access_token)
                    updated_reports.append(rpt_id)
                else:
                    print(
                        f"{icons.warning} The {rpt_name} report in the '{workspace_name}' workspace is not eligible for upgrade."
                    )
        else:
            (rpt_name, rpt_id) = resolve_item_name_and_id(
                item=report, type="Report", workspace=workspace_id
            )
            if rpt_id in eligible_for_upgrade:
                embed_url, dataset_id = eligible_for_upgrade[rpt_id]
                access_token = generate_embed_token(
                    dataset_ids=[dataset_id], report_ids=[rpt_id]
                )
                embed_report_edit_mode(embed_url, access_token)
                updated_reports.append(rpt_id)
            else:
                print(
                    f"{icons.warning} The {rpt_name} report in the '{workspace_name}' workspace is not eligible for upgrade."
                )

        check_upgrade_status(url, updated_reports, workspace_name)


# Define the time limit (1 minute)
TIME_LIMIT = 60  # seconds
TIME_BETWEEN_REQUESTS = 2  # seconds


# Function to check the upgrade status
def check_upgrade_status(url, updated_reports, workspace_name):
    start_time = time.time()
    while time.time() - start_time < TIME_LIMIT:
        response = _base_api(request=url, client="fabric_sp")
        verified_reports = {}
        unverified_reports = {}

        for rpt in response.json().get("value", []):
            rpt_id = rpt.get("id")
            rpt_name = rpt.get("name")
            rpt_format = rpt.get("format")

            # Check if the report is updated and format is PBIR or not
            if rpt_id in updated_reports and rpt_format == "PBIR":
                verified_reports[rpt_id] = rpt_name
            elif rpt_id in updated_reports and rpt_format != "PBIR":
                unverified_reports[rpt_id] = rpt_name

        # If there are no unverified reports, break out of the loop
        if not unverified_reports:
            break

        # Wait for 2 seconds before the next request
        time.sleep(TIME_BETWEEN_REQUESTS)

        for rpt_id, rpt_name in updated_reports.items():
            print(
                f"{icons.green_dot} The '{rpt_name}' report within the '{workspace_name}' workspace has been upgraded to PBIR format."
            )
