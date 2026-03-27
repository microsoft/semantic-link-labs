#Fix Bar Chart Visuals in Power BI Reports
#%pip install semantic-link-labs

from uuid import UUID
from typing import Optional
from sempy._utils._log import log
import sempy_labs._icons as icons
from sempy_labs.report._reportwrapper import connect_report


def _get_visual_property(visual: dict, object_name: str, property_name: str) -> str | None:
    obj_list = visual.get("visual", {}).get("objects", {}).get(object_name, [])
    if not obj_list:
        return None
    return obj_list[0].get("properties", {}).get(property_name, {}).get("expr", {}).get("Literal", {}).get("Value")


def _set_visual_property(visual: dict, object_name: str, property_name: str, value: str) -> None:
    objects = visual.setdefault("visual", {}).setdefault("objects", {})
    if object_name not in objects or not objects[object_name]:
        objects[object_name] = [{"properties": {}}]
    obj = objects[object_name][0]
    if "properties" not in obj:
        obj["properties"] = {}
    obj["properties"][property_name] = {"expr": {"Literal": {"Value": value}}}


_BAR_CHART_TYPES = {"barChart", "clusteredBarChart"}


@log
def fix_barcharts(
    report: str | UUID,
    page_name: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
) -> None:
    """
    Fixes bar chart visuals in a report by applying best practice formatting.

    Applies the following changes to all barChart and clusteredBarChart visuals:
    - Removes X axis title
    - Removes X axis values
    - Removes Y axis title
    - Adds data labels
    - Removes vertical gridlines

    Parameters
    ----------
    report : str | uuid.UUID
        Name or ID of the report.
    page_name : str, default=None
        The display name of the page to apply changes to.
        Defaults to None which applies changes to all pages.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    scan_only : bool, default=False
        If True, only scans and reports issues without applying fixes.

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

        paths_df = rw.list_paths()
        charts_found = 0
        charts_fixed = 0
        charts_need_fixing = 0

        _CHECK_LABELS = {
            ("valueAxis", "showAxisTitle"): "X axis title",
            ("valueAxis", "show"): "X axis values",
            ("categoryAxis", "showAxisTitle"): "Y axis title",
            ("labels", "show"): "Data labels",
            ("valueAxis", "gridlineShow"): "Vertical gridlines",
        }

        page_id = rw.resolve_page_name(page_name) if page_name else None

        for file_path in paths_df["Path"]:
            if not file_path.endswith("/visual.json"):
                continue

            if page_id and f"/{page_id}/" not in file_path:
                continue

            visual = rw.get(file_path=file_path)

            if visual.get("visual", {}).get("visualType") not in _BAR_CHART_TYPES:
                continue

            charts_found += 1

            # Check which properties need fixing before applying any changes
            checks = [
                ("valueAxis",   "showAxisTitle", "false"),
                ("valueAxis",   "show",          "false"),
                ("categoryAxis","showAxisTitle",  "false"),
                ("labels",      "show",           "true"),
                ("valueAxis",   "gridlineShow",   "false"),
            ]

            issues = [
                _CHECK_LABELS[(o, p)]
                for o, p, v in checks
                if _get_visual_property(visual, o, p) != v
            ]

            if not issues:
                if scan_only:
                    print(f"{icons.green_dot} {file_path} — all settings correct")
                continue  # already correct, skip

            charts_need_fixing += 1

            if scan_only:
                print(f"{icons.yellow_dot} {file_path} — needs fixing: {', '.join(issues)}")
                continue

            for object_name, property_name, value in checks:
                _set_visual_property(visual, object_name, property_name, value)

            rw.update(file_path=file_path, payload=visual)
            charts_fixed += 1
            print(f"{icons.green_dot} Fixed bar chart in {file_path}")

        if charts_found == 0:
            print(f"{icons.info} No bar charts found in the '{rw._report_name}' report.")
        elif scan_only:
            if charts_need_fixing == 0:
                print(f"\n{icons.green_dot} Scanned {charts_found} bar chart(s) — all have correct settings.")
            else:
                print(f"\n{icons.yellow_dot} Scanned {charts_found} bar chart(s) — {charts_need_fixing} need fixing.")
        elif charts_fixed == 0:
            print(f"{icons.info} Found {charts_found} bar chart(s) in the '{rw._report_name}' report — all already have correct settings.")
        else:
            print(f"{icons.green_dot} Successfully fixed {charts_fixed} of {charts_found} bar chart(s).")


# Sample usage:
# fix_barcharts(report="ReportName")
# fix_barcharts(report="ReportName", page_name="PageName")
# fix_barcharts(report="ReportName", page_name="PageName", workspace="Your Workspace Name")
