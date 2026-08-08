# Fix Column Chart Visuals in Power BI Reports (IBCS-friendly defaults)

from uuid import UUID
from typing import Optional, Iterable
from sempy._utils._log import log
import sempy_labs._icons as icons
from sempy_labs.report._reportwrapper import connect_report

_COLUMN_CHART_TYPES = {"columnChart", "clusteredColumnChart"}
_COL_TO_BAR = {"columnChart": "barChart", "clusteredColumnChart": "clusteredBarChart"}
_DATE_DATA_TYPES = {"DateTime", "DateTimeOffset"}

# Rule key -> (object, property, value)
_RULES: dict[str, tuple[str, str, object]] = {
    "hide_category_axis_title": ("categoryAxis", "showAxisTitle", False),
    "hide_value_axis_title": ("valueAxis", "showAxisTitle", False),
    "hide_value_axis_values": ("valueAxis", "show", False),
    "show_data_labels": ("labels", "show", True),
    "hide_gridlines": ("valueAxis", "gridlineShow", False),
}

# Opt-in conversion rules (mutually exclusive per visual)
_RULE_CONVERT_NON_TIME_TO_BAR = "convert_non_time_to_bar"
_RULE_CONVERT_DATE_AXIS_TO_LINE = "convert_date_axis_to_line"

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
def fix_column_chart(
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
    Apply IBCS-friendly formatting to column chart (`columnChart`,
    `clusteredColumnChart`) visuals.

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
        None = all formatting rules (excludes opt-in conversions).

        Available rules:
          * ``hide_category_axis_title``
          * ``hide_value_axis_title``
          * ``hide_value_axis_values``
          * ``show_data_labels``
          * ``hide_gridlines``
          * ``convert_non_time_to_bar`` (opt-in) — IBCS: structural categories
            should be horizontal. Convert column charts whose category axis is
            NOT a Date/DateTime column to bar charts.
          * ``convert_date_axis_to_line`` (opt-in) — convert column charts whose
            category axis IS a Date/DateTime column to line charts.
    """

    selected = ALL_RULES if rules is None else set(rules)
    fmt_rules = [(k, _RULES[k]) for k in selected if k in _RULES]
    do_to_bar = _RULE_CONVERT_NON_TIME_TO_BAR in selected
    do_to_line = _RULE_CONVERT_DATE_AXIS_TO_LINE in selected

    with connect_report(report=report, workspace=workspace, readonly=scan_only, show_diffs=False) as rw:
        if rw.format != "PBIR":
            print(
                f"{icons.red_dot} Report '{rw._report_name}' is in '{rw.format}' format, not PBIR. "
                f"Run 'Upgrade to PBIR' first."
            )
            return

        date_columns: set[tuple[str, str]] = set()
        if do_to_bar or do_to_line:
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
        to_bar = 0
        to_line = 0
        page_id = rw.resolve_page_name(page_name) if page_name else None

        for file_path in paths_df["Path"]:
            if not file_path.endswith("/visual.json"):
                continue
            if page_id and f"/{page_id}/" not in file_path:
                continue

            visual = rw.get(file_path=file_path)
            vtype = visual.get("visual", {}).get("visualType")
            if vtype not in _COLUMN_CHART_TYPES:
                continue
            found += 1

            local_changes = []
            cat_fields = _get_category_fields(visual) if (do_to_bar or do_to_line) else []
            has_date_axis = bool(cat_fields) and any(f in date_columns for f in cat_fields)

            # Conversion: date axis → line chart (precedence: line wins over bar)
            if do_to_line and has_date_axis:
                if scan_only:
                    local_changes.append(f"convert {vtype} → lineChart (date axis)")
                else:
                    visual["visual"]["visualType"] = "lineChart"
                    local_changes.append(f"converted {vtype} → lineChart")
                    to_line += 1
            # Conversion: non-time axis → bar
            elif do_to_bar and cat_fields and not has_date_axis:
                new_type = _COL_TO_BAR[vtype]
                if scan_only:
                    local_changes.append(f"convert {vtype} → {new_type} (non-time axis)")
                else:
                    visual["visual"]["visualType"] = new_type
                    local_changes.append(f"converted {vtype} → {new_type}")
                    to_bar += 1

            # Formatting rules
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
            print(f"{icons.info} No column charts found in the '{rw._report_name}' report.")
        elif scan_only:
            print(f"\n{icons.yellow_dot} Scanned {found} column chart(s).")
        else:
            extras = []
            if to_bar:
                extras.append(f"{to_bar} → bar")
            if to_line:
                extras.append(f"{to_line} → line")
            extra = f" ({', '.join(extras)})" if extras else ""
            print(f"{icons.green_dot} Updated {changed} of {found} column chart(s){extra}.")


# Backward-compatible alias
fix_columncharts = fix_column_chart
