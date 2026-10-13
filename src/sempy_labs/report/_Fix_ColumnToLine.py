# Fix Column-to-Line Chart — switch column charts to line charts when the
# category axis uses a Date/DateTime column (too many data points for bars).

from uuid import UUID
from typing import Optional
from sempy._utils._log import log
import sempy_labs._icons as icons
from sempy_labs.report._reportwrapper import connect_report
from sempy_labs._helper_functions import resolve_dataset_from_report

_COLUMN_CHART_TYPES = {"columnChart", "clusteredColumnChart"}
_DATE_DATA_TYPES = {"DateTime", "DateTimeOffset"}


def _get_category_fields(visual: dict) -> list[tuple[str, str]]:
    """Return [(table, column), ...] from the Category axis projections."""
    projections = (
        visual.get("visual", {})
        .get("query", {})
        .get("queryState", {})
        .get("Category", {})
        .get("projections", [])
    )
    result = []
    for proj in projections:
        field = proj.get("field", {})
        col = field.get("Column", {})
        if not col:
            continue
        prop = col.get("Property")
        entity = col.get("Expression", {}).get("SourceRef", {}).get("Entity")
        if prop and entity:
            result.append((entity, prop))
    return result


@log
def fix_column_to_line(
    report: str | UUID,
    page_name: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
) -> None:
    """
    Changes column chart visuals to line charts when the category axis uses a Date/DateTime column.

    Column charts with many data points (e.g. daily dates) become unreadable.
    This fixer detects Date/DateTime columns on the category axis and switches the
    visual type from columnChart/clusteredColumnChart to lineChart.

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
    """

    with connect_report(report=report, workspace=workspace, readonly=scan_only, show_diffs=False) as rw:
        if rw.format != "PBIR":
            print(
                f"{icons.red_dot} Report '{rw._report_name}' is in '{rw.format}' format, not PBIR. "
                f"Run 'Upgrade to PBIR' first."
            )
            return

        # Build a set of (table, column) pairs that are Date/DateTime
        dataset_id, _, dataset_workspace_id, _ = resolve_dataset_from_report(
            report=rw._report_id, workspace=rw._workspace_id
        )
        import sempy.fabric as fabric

        df_cols = fabric.list_columns(dataset=dataset_id, workspace=dataset_workspace_id)
        date_columns = set()
        for _, row in df_cols.iterrows():
            if row.get("Data Type") in _DATE_DATA_TYPES:
                date_columns.add((row["Table Name"], row["Column Name"]))

        if not date_columns:
            print(f"{icons.info} No Date/DateTime columns found in the semantic model — nothing to check.")
            return

        paths_df = rw.list_paths()
        charts_found = 0
        charts_need_fixing = 0
        charts_fixed = 0

        page_id = rw.resolve_page_name(page_name) if page_name else None

        for file_path in paths_df["Path"]:
            if not file_path.endswith("/visual.json"):
                continue
            if page_id and f"/{page_id}/" not in file_path:
                continue

            visual = rw.get(file_path=file_path)
            if visual.get("visual", {}).get("visualType") not in _COLUMN_CHART_TYPES:
                continue

            charts_found += 1

            category_fields = _get_category_fields(visual)
            has_date_axis = any(f in date_columns for f in category_fields)

            if not has_date_axis:
                if scan_only:
                    cols = ", ".join(f"{t}.{c}" for t, c in category_fields) or "(no fields)"
                    print(f"{icons.green_dot} {file_path} — category axis [{cols}] is not Date — OK")
                continue

            charts_need_fixing += 1
            date_cols_str = ", ".join(f"{t}.{c}" for t, c in category_fields if (t, c) in date_columns)

            if scan_only:
                vtype = visual["visual"]["visualType"]
                print(f"{icons.yellow_dot} {file_path} — {vtype} with Date axis [{date_cols_str}] → should be lineChart")
                continue

            visual["visual"]["visualType"] = "lineChart"
            rw.update(file_path=file_path, payload=visual)
            charts_fixed += 1
            print(f"{icons.green_dot} Changed to lineChart: {file_path} (date axis: {date_cols_str})")

        if charts_found == 0:
            print(f"{icons.info} No column charts found in the '{rw._report_name}' report.")
        elif scan_only:
            if charts_need_fixing == 0:
                print(f"\n{icons.green_dot} Scanned {charts_found} column chart(s) — none have Date axes.")
            else:
                print(f"\n{icons.yellow_dot} Scanned {charts_found} column chart(s) — {charts_need_fixing} should be converted to line charts.")
        elif charts_fixed == 0:
            print(f"{icons.info} Found {charts_found} column chart(s) — none have Date axes.")
        else:
            print(f"{icons.green_dot} Converted {charts_fixed} of {charts_found} column chart(s) to line charts.")
