# Fix Pie Chart Visuals in Power BI Reports

from uuid import UUID
from typing import Optional
from sempy._utils._log import log
import sempy_labs._icons as icons
from sempy_labs.report._reportwrapper import connect_report


@log
def fix_pie_chart(
    report: str | UUID,
    target_visual_type: str = "clusteredBarChart",
    page_name: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
) -> None:
    """
    Replaces all pie/donut chart visuals in a report with a target visual type
    (default: clusteredBarChart).

    Pie/donut charts are widely discouraged in BI best practice (IBCS, Stephen Few):
    humans cannot accurately compare angles. This fixer scans the PBIR JSON and
    swaps the visual type on every ``pieChart`` / ``donutChart`` visual.

    Parameters
    ----------
    report : str | uuid.UUID
        Name or ID of the report.
    target_visual_type : str, default="clusteredBarChart"
        Visual type to replace pie/donut charts with.
    page_name : str, default=None
        Display name of the page to apply changes to. None = all pages.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
    scan_only : bool, default=False
        If True, only scans and reports findings without applying changes.
    """

    _PIE_TYPES = {"pieChart", "donutChart"}

    with connect_report(report=report, workspace=workspace, readonly=scan_only, show_diffs=False) as rw:
        if rw.format != "PBIR":
            print(
                f"{icons.red_dot} Report '{rw._report_name}' is in '{rw.format}' format, not PBIR. "
                f"Run 'Upgrade to PBIR' first."
            )
            return

        paths_df = rw.list_paths()
        found = 0
        replaced = 0
        page_id = rw.resolve_page_name(page_name) if page_name else None

        for file_path in paths_df["Path"]:
            if not file_path.endswith("/visual.json"):
                continue
            if page_id and f"/{page_id}/" not in file_path:
                continue

            visual = rw.get(file_path=file_path)
            vtype = visual.get("visual", {}).get("visualType")
            if vtype not in _PIE_TYPES:
                continue

            found += 1
            if scan_only:
                print(f"{icons.yellow_dot} {file_path} — {vtype} (would be replaced with {target_visual_type})")
                continue

            visual["visual"]["visualType"] = target_visual_type
            rw.update(file_path=file_path, payload=visual)
            replaced += 1
            print(f"{icons.green_dot} Replaced {vtype} → {target_visual_type} in {file_path}")

        if found == 0:
            print(f"{icons.info} No pie/donut charts found in the '{rw._report_name}' report.")
        elif scan_only:
            print(f"\n{icons.yellow_dot} Scanned report — {found} pie/donut chart(s) would be replaced with {target_visual_type}.")
        else:
            print(f"{icons.green_dot} Successfully replaced {replaced} pie/donut chart(s) with {target_visual_type}.")


# Backward-compatible alias
fix_piecharts = fix_pie_chart
