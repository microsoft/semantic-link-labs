# Fix Bar Chart Visuals in Power BI Reports (IBCS-friendly defaults)

from uuid import UUID
from typing import Optional, Iterable
from sempy._utils._log import log
import sempy_labs._icons as icons
from sempy_labs.report._reportwrapper import connect_report

_BAR_CHART_TYPES = {"barChart", "clusteredBarChart"}
_BAR_TO_COL = {"barChart": "columnChart", "clusteredBarChart": "clusteredColumnChart"}
_DATE_DATA_TYPES = {"DateTime", "DateTimeOffset"}

# Rule key -> (object, property, value)
# Each rule is independently toggleable. None = apply all formatting rules.
_RULES: dict[str, tuple[str, str, object]] = {
    "hide_category_axis_title": ("categoryAxis", "showAxisTitle", False),
    "hide_category_axis_values": ("categoryAxis", "show", False),
    "hide_value_axis_title": ("valueAxis", "showAxisTitle", False),
    "hide_value_axis_values": ("valueAxis", "show", False),
    "show_data_labels": ("labels", "show", True),
    "hide_gridlines": ("valueAxis", "gridlineShow", False),
}

# Special non-property rule: convert bar with time-based axis to a column chart.
_RULE_CONVERT_TIME_AXIS_TO_COLUMN = "convert_time_axis_to_column"

ALL_RULES = set(_RULES.keys())  # formatting rules only — opt-in conversion is excluded


def _get_visual_property(visual: dict, obj: str, prop: str):
    objects = visual.get("visual", {}).get("objects", {})
    for entry in objects.get(obj, []):
        properties = entry.get("properties", {})
        if prop in properties:
            return properties[prop].get("expr", {}).get("Literal", {}).get("Value")
    return None


def _set_visual_property(visual: dict, obj: str, prop: str, value: object) -> bool:
    """Set a literal property on the first object selector. Returns True if changed."""
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


def _get_category_fields(visual: dict) -> list[tuple[str, str]]:
    projections = (
        visual.get("visual", {}).get("query", {}).get("queryState", {})
        .get("Category", {}).get("projections", [])
    )
    out = []
    for proj in projections:
        col = proj.get("field", {}).get("Column", {})
        if not col:
            continue
        prop = col.get("Property")
        entity = col.get("Expression", {}).get("SourceRef", {}).get("Entity")
        if prop and entity:
            out.append((entity, prop))
    return out


@log
def fix_bar_chart(
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
    Apply IBCS-friendly formatting to bar chart (`barChart`, `clusteredBarChart`) visuals.

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
        Set of rule keys to apply. None = all formatting rules
        (excludes opt-in conversions like ``convert_time_axis_to_column``).

        Available rules:
          * ``hide_category_axis_title``
          * ``hide_category_axis_values``
          * ``hide_value_axis_title``
          * ``hide_value_axis_values``
          * ``show_data_labels``
          * ``hide_gridlines``
          * ``convert_time_axis_to_column`` (opt-in) — switch bar to column when
            the category axis is a Date/DateTime column.
    """

    selected = ALL_RULES if rules is None else set(rules)
    fmt_rules = [(k, _RULES[k]) for k in selected if k in _RULES]
    do_convert = _RULE_CONVERT_TIME_AXIS_TO_COLUMN in selected

    with connect_report(report=report, workspace=workspace, readonly=scan_only, show_diffs=False) as rw:
        if rw.format != "PBIR":
            print(
                f"{icons.red_dot} Report '{rw._report_name}' is in '{rw.format}' format, not PBIR. "
                f"Run 'Upgrade to PBIR' first."
            )
            return

        # Look up date columns once if needed
        date_columns: set[tuple[str, str]] = set()
        if do_convert:
            try:
                from sempy_labs._helper_functions import resolve_dataset_from_report
                import sempy.fabric as fabric
                dataset_id, _, dataset_workspace_id, _ = resolve_dataset_from_report(
                    report=rw._report_id, workspace=rw._workspace_id
                )
                df_cols = fabric.list_columns(dataset=dataset_id, workspace=dataset_workspace_id)
                for _, row in df_cols.iterrows():
                    if row.get("Data Type") in _DATE_DATA_TYPES:
                        date_columns.add((row["Table Name"], row["Column Name"]))
            except Exception as e:
                print(f"{icons.yellow_dot} Could not list date columns: {e}")

        paths_df = rw.list_paths()
        found = 0
        changed = 0
        converted = 0
        page_id = rw.resolve_page_name(page_name) if page_name else None

        for file_path in paths_df["Path"]:
            if not file_path.endswith("/visual.json"):
                continue
            if page_id and f"/{page_id}/" not in file_path:
                continue

            visual = rw.get(file_path=file_path)
            vtype = visual.get("visual", {}).get("visualType")
            if vtype not in _BAR_CHART_TYPES:
                continue
            found += 1

            local_changes = []

            # Optional conversion: bar with date axis → column
            if do_convert and date_columns:
                cat_fields = _get_category_fields(visual)
                if any(f in date_columns for f in cat_fields):
                    new_type = _BAR_TO_COL[vtype]
                    if scan_only:
                        local_changes.append(f"convert {vtype} → {new_type} (date axis)")
                    else:
                        visual["visual"]["visualType"] = new_type
                        local_changes.append(f"converted {vtype} → {new_type}")
                        converted += 1
                    # If converted, formatting checks below still useful — apply them.

            # Formatting rules
            for key, (obj, prop, value) in fmt_rules:
                current = _get_visual_property(visual, obj, prop)
                expected = value
                # Booleans are stored as lowercase strings or actual bools — normalize
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
                    local_changes.append(f"{key} ({obj}.{prop}={expected})")
                else:
                    if _set_visual_property(visual, obj, prop, value):
                        local_changes.append(f"{key} ({obj}.{prop}={expected})")

            if not local_changes:
                continue

            if scan_only:
                print(f"{icons.yellow_dot} {file_path} — would apply: {', '.join(local_changes)}")
            else:
                rw.update(file_path=file_path, payload=visual)
                changed += 1
                print(f"{icons.green_dot} {file_path} — {', '.join(local_changes)}")

        if found == 0:
            print(f"{icons.info} No bar charts found in the '{rw._report_name}' report.")
        elif scan_only:
            print(f"\n{icons.yellow_dot} Scanned {found} bar chart(s).")
        else:
            extra = f" ({converted} converted to column)" if converted else ""
            print(f"{icons.green_dot} Updated {changed} of {found} bar chart(s){extra}.")


# Backward-compatible alias
fix_barcharts = fix_bar_chart
