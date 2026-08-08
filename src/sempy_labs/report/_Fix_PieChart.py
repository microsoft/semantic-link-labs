#Fix Pie Chart Visuals in Power BI Reports
#%pip install semantic-link-labs

from uuid import UUID
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_item_name_and_id,
)
from sempy._utils._log import log
import sempy_labs._icons as icons
from sempy_labs.report._reportwrapper import connect_report


@log
def fix_piecharts(
    report: str | UUID,
    target_visual_type: str = "clusteredBarChart",
    page_name: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
) -> None:
    """
    Replaces all pie chart visuals in a report with a specified visual type.

    This function scans through all visuals in a report and converts any pie charts
    to the target visual type (default: clustered bar chart).

    Parameters
    ----------
    report : str | uuid.UUID
        Name or ID of the report.
    target_visual_type : str, default="clusteredBarChart"
        The target visual type to replace pie charts with.
        Valid options: "barChart", "clusteredBarChart", "barChart", "columnChart", "clusteredColumnChart" etc.
    page_name : str, default=None
        The display name of the page to apply changes to.
        Defaults to None which applies changes to all pages.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    scan_only : bool, default=False
        If True, only scans and reports pie charts found without replacing them.

    Returns
    -------
    None
        This function does not return a value.
    """

    with connect_report(report=report, workspace=workspace, readonly=scan_only, show_diffs=False) as rw:
        # Guard: report must be in PBIR format
        if rw.format != "PBIR":
            print(
                f"{icons.red_dot} Report '{rw._report_name}' is in '{rw.format}' format, not PBIR. "
                f"Run 'Upgrade to PBIR' first."
            )
            return

        # Get all file paths in the report
        paths_df = rw.list_paths()
        pie_charts_found = 0
        pie_charts_replaced = 0

        page_id = rw.resolve_page_name(page_name) if page_name else None

        for file_path in paths_df["Path"]:
            # Only process visual.json files
            if not file_path.endswith("/visual.json"):
                continue

            if page_id and f"/{page_id}/" not in file_path:
                continue

            visual = rw.get(file_path=file_path)

            # Check if this is a pie chart
            if visual.get("visual", {}).get("visualType") == "pieChart":
                pie_charts_found += 1

                if scan_only:
                    print(f"{icons.yellow_dot} {file_path} — pie chart found (would be replaced with {target_visual_type})")
                    continue

                # Change the visual type to target type
                visual["visual"]["visualType"] = target_visual_type

                # Update the visual in the report
                rw.update(file_path=file_path, payload=visual)
                pie_charts_replaced += 1
                print(
                    f"{icons.green_dot} Replaced pie chart in {file_path} with {target_visual_type}"
                )

        if pie_charts_found == 0:
            print(
                f"{icons.info} No pie charts found in the '{rw._report_name}' report."
            )
        elif scan_only:
            print(
                f"\n{icons.yellow_dot} Scanned report — {pie_charts_found} pie chart(s) found that would be replaced with {target_visual_type}."
            )
        else:
            print(
                f"{icons.green_dot} Successfully replaced {pie_charts_replaced} pie chart(s) with {target_visual_type}."
            )


# Sample usage:
# fix_piecharts(report="ReportName")                                              # default: clusteredBarChart
# fix_piecharts(report="ReportName", target_visual_type="barChart")
# fix_piecharts(report="ReportName", page_name="PageName")                        # single page only
# fix_piecharts(report="ReportName", page_name="PageName", workspace="Your Workspace Name")