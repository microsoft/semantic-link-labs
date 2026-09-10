# Fix Line Chart Visuals in Power BI Reports (IBCS-friendly defaults)

from uuid import UUID
from typing import Optional, Iterable
from sempy._utils._log import log
import sempy_labs._icons as icons
from sempy_labs.report._reportwrapper import connect_report

_LINE_CHART_TYPES = {"lineChart", "lineClusteredColumnComboChart", "lineStackedColumnComboChart"}

# Rule key -> (object, property, value).
# Note: line charts KEEP the Y value axis values (no hide_value_axis_values rule).
_RULES: dict[str, tuple[str, str, object]] = {
    "hide_category_axis_title": ("categoryAxis", "showAxisTitle", False),
    "hide_value_axis_title": ("valueAxis", "showAxisTitle", False),
    "show_data_labels": ("labels", "show", True),
    "hide_gridlines": ("valueAxis", "gridlineShow", False),
}

ALL_RULES = set(_RULES.keys())


def _get_visual_property(visual: dict, obj: str, prop: str):
    objects = visual.get("visual", {}).get("objects", {})
    for entry in objects.get(obj, []):
        properties = entry.get("properties", {})
        if prop in properties:
            return properties[prop].get("expr", {}).get("Literal", {}).get("Value")
    return None


def _set_visual_property(visual: dict, obj: str, prop: str, value: object) -> bool:
    if isinstance(value, bool):
        literal = "true" if value else "false"
    elif isinstance(value, (int, float)):
        literal = str(value)
    else:
        literal = f"'{value}'"

    objects = visual.setdefault("visual", {}).setdefault("objects", {})
    entries = objects.setdefault(obj, [{"properties": {}}])
    if not entries:
        entries.append({"properties": {}})
    properties = entries[0].setdefault("properties", {})
    new_node = {"expr": {"Literal": {"Value": literal}}}
    if properties.get(prop) == new_node:
        return False
    properties[prop] = new_node
    return True


@log
def fix_line_chart(
    report: str | UUID,
    page_name: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
    rules: Optional[Iterable[str]] = None,
) -> None:
    # Normalize `rules` if a single string was passed
    if isinstance(rules, str):
        rules = [rules]
    """
    Apply IBCS-friendly formatting to line chart (`lineChart`,
    `lineClusteredColumnComboChart`, `lineStackedColumnComboChart`) visuals.

    Line charts keep their Y value axis (a continuous scale is required to
    read values without explicit data labels).

    Parameters
    ----------
    report : str | uuid.UUID
        Name or ID of the report.
    page_name : str, default=None
        Page to scope to. None = all pages.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
    scan_only : bool, default=False
        If True, only scan and report. No writes.
    rules : iterable of str, default=None
        None = all rules.

        Available rules:
          * ``hide_category_axis_title``
          * ``hide_value_axis_title``
          * ``show_data_labels``
          * ``hide_gridlines``
    """

    selected = ALL_RULES if rules is None else set(rules)
    fmt_rules = [(k, _RULES[k]) for k in selected if k in _RULES]

    with connect_report(report=report, workspace=workspace, readonly=scan_only, show_diffs=False) as rw:
        if rw.format != "PBIR":
            print(
                f"{icons.red_dot} Report '{rw._report_name}' is in '{rw.format}' format, not PBIR. "
                f"Run 'Upgrade to PBIR' first."
            )
            return

        paths_df = rw.list_paths()
        found = 0
        changed = 0
        page_id = rw.resolve_page_name(page_name) if page_name else None

        for file_path in paths_df["Path"]:
            if not file_path.endswith("/visual.json"):
                continue
            if page_id and f"/{page_id}/" not in file_path:
                continue

            visual = rw.get(file_path=file_path)
            vtype = visual.get("visual", {}).get("visualType")
            if vtype not in _LINE_CHART_TYPES:
                continue
            found += 1

            local_changes = []
            for key, (obj, prop, value) in fmt_rules:
                current = _get_visual_property(visual, obj, prop)
                if isinstance(value, bool):
                    cur_norm = None
                    if isinstance(current, str):
                        cur_norm = current.lower() == "true"
                    elif isinstance(current, bool):
                        cur_norm = current
                    if cur_norm == value:
                        continue
                else:
                    if current == value:
                        continue
                if scan_only:
                    local_changes.append(f"{key} ({obj}.{prop}={value})")
                else:
                    if _set_visual_property(visual, obj, prop, value):
                        local_changes.append(f"{key} ({obj}.{prop}={value})")

            if not local_changes:
                continue

            if scan_only:
                print(f"{icons.yellow_dot} {file_path} — would apply: {', '.join(local_changes)}")
            else:
                rw.update(file_path=file_path, payload=visual)
                changed += 1
                print(f"{icons.green_dot} {file_path} — {', '.join(local_changes)}")

        if found == 0:
            print(f"{icons.info} No line charts found in the '{rw._report_name}' report.")
        elif scan_only:
            print(f"\n{icons.yellow_dot} Scanned {found} line chart(s).")
        else:
            print(f"{icons.green_dot} Updated {changed} of {found} line chart(s).")


# Backward-compatible alias
fix_linecharts = fix_line_chart
