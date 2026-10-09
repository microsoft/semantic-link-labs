# IBCS Variance Chart Fixer — adds PY measures, error bars, and IBCS formatting
# to bar/column charts with a single AC measure.
#
# Pipeline per visual:
#   1. Stacked → Clustered
#   2. Identify AC measure (skip if multiple)
#   3. Check/create PY, Δ PY, Max Green PY, Max Red AC measures
#   4. Add PY to visual if missing
#   5. Column→Bar for non-time axes by directly switching the visual type
#   6. Set error bars, overlap, labels, colors, axes, sorting
#   7. Warn if no Year slicer on page

import re
from uuid import UUID
from typing import Optional
from sempy._utils._log import log
import sempy_labs._icons as icons
from sempy_labs.report._reportwrapper import connect_report
from sempy_labs._helper_functions import resolve_dataset_from_report
from sempy_labs._refresh_semantic_model import refresh_semantic_model

# Visual types this fixer targets
_TARGET_TYPES = {
    "barChart", "clusteredBarChart",
    "columnChart", "clusteredColumnChart",
}

# Suffixes that identify PY-derived measures (not AC)
_PY_SUFFIXES = (" PY", " Δ PY", " Δ PY %", " Max Green PY", " Max Red AC")

# IBCS colors
_AC_COLOR = "#404040"
_PY_COLOR = "#A0A0A0"
_ERROR_RED = "#FF0000"
_ERROR_GREEN = "#92D050"

# Time-field detection
_DATE_DATA_TYPES = {"DateTime", "DateTimeOffset"}
_TIME_NAME_PATTERN = re.compile(
    r"(date|month|year|quarter|period|week|day|fiscal|calendar)",
    re.IGNORECASE,
)


def _get_y_measures(visual: dict) -> list[dict]:
    """Return Y-axis projection dicts."""
    return (
        visual.get("visual", {})
        .get("query", {})
        .get("queryState", {})
        .get("Y", {})
        .get("projections", [])
    )


def _get_category_fields(visual: dict) -> list[tuple[str, str]]:
    """Return [(table, column), ...] from category axis."""
    projections = (
        visual.get("visual", {})
        .get("query", {})
        .get("queryState", {})
        .get("Category", {})
        .get("projections", [])
    )
    result = []
    for proj in projections:
        col = proj.get("field", {}).get("Column", {})
        if not col:
            continue
        prop = col.get("Property")
        entity = col.get("Expression", {}).get("SourceRef", {}).get("Entity")
        if prop and entity:
            result.append((entity, prop))
    return result


def _measure_name_from_proj(proj: dict) -> tuple[str, str] | None:
    """Extract (table, measure_name) from a Y projection."""
    measure = proj.get("field", {}).get("Measure", {})
    if not measure:
        return None
    prop = measure.get("Property")
    entity = measure.get("Expression", {}).get("SourceRef", {}).get("Entity")
    if prop and entity:
        return (entity, prop)
    return None


def _is_ac_measure(name: str) -> bool:
    """True if the measure name does NOT end with a PY-derived suffix."""
    return not any(name.endswith(s) for s in _PY_SUFFIXES)


def _set_literal(value: str) -> dict:
    """Build a Literal expression node."""
    return {"expr": {"Literal": {"Value": value}}}


def _set_color(hex_color: str) -> dict:
    """Build a solid fill color node with a hex literal."""
    return {"solid": {"color": {"expr": {"Literal": {"Value": f"'{hex_color}'"}}}}}


def _set_measure_ref(table: str, measure: str) -> dict:
    """Build a Measure expression reference."""
    return {
        "expr": {
            "Measure": {
                "Expression": {"SourceRef": {"Entity": table}},
                "Property": measure,
            }
        }
    }


def _build_error_bar_config(
    ac_table: str, ac_measure: str, py_measure: str,
    max_red_measure: str, max_green_measure: str,
) -> list[dict]:
    """Build the full error bar objects array for IBCS variance display."""
    ac_metadata = f"{ac_table}.{ac_measure}"
    py_metadata = f"{ac_table}.{py_measure}"

    return [
        # Error range for AC (red — negative variance)
        {
            "properties": {
                "errorRange": {
                    "kind": "ErrorRange",
                    "explicit": {
                        "isRelative": _set_literal("false"),
                        "upperBound": _set_measure_ref(ac_table, max_red_measure),
                    },
                },
            },
            "selector": {
                "data": [{"dataViewWildcard": {"matchingOption": 0}}],
                "metadata": ac_metadata,
                "highlightMatching": 1,
            },
        },
        # Error bar style for AC (red)
        {
            "properties": {
                "enabled": _set_literal("true"),
                "barColor": _set_color(_ERROR_RED),
                "barWidth": _set_literal("10D"),
                "markerShow": _set_literal("false"),
                "tooltipShow": _set_literal("false"),
                "barBorderSize": _set_literal("0L"),
            },
            "selector": {"metadata": ac_metadata},
        },
        # Error range for PY (green — positive variance)
        {
            "properties": {
                "errorRange": {
                    "kind": "ErrorRange",
                    "explicit": {
                        "isRelative": _set_literal("false"),
                        "upperBound": _set_measure_ref(ac_table, max_green_measure),
                    },
                },
            },
            "selector": {
                "data": [{"dataViewWildcard": {"matchingOption": 0}}],
                "metadata": py_metadata,
                "highlightMatching": 1,
            },
        },
        # Error bar style for PY (green)
        {
            "properties": {
                "enabled": _set_literal("true"),
                "barColor": _set_color(_ERROR_GREEN),
                "barWidth": _set_literal("10D"),
                "markerShow": _set_literal("false"),
                "markerSize": _set_literal("5D"),
                "barBorderColor": _set_color(_ERROR_GREEN),
                "barBorderSize": _set_literal("0L"),
                "tooltipShow": _set_literal("false"),
            },
            "selector": {"metadata": py_metadata},
        },
    ]


def _build_label_config(ac_metadata: str, py_metadata: str) -> list[dict]:
    """Build labels config: AC with white background, PY hidden."""
    return [
        # Global labels
        {
            "properties": {
                "show": _set_literal("true"),
                "enableBackground": _set_literal("true"),
                "backgroundColor": _set_color("#FFFFFF"),
                "backgroundTransparency": _set_literal("50D"),
            },
        },
        # PY labels hidden
        {
            "properties": {
                "showSeries": _set_literal("false"),
            },
            "selector": {"metadata": py_metadata},
        },
    ]


def _build_datapoint_config(ac_metadata: str, py_metadata: str) -> list[dict]:
    """Build data point colors: AC dark grey, PY light grey."""
    return [
        {
            "properties": {"fill": _set_color(_AC_COLOR)},
            "selector": {"metadata": ac_metadata},
        },
        {
            "properties": {"fill": _set_color(_PY_COLOR)},
            "selector": {"metadata": py_metadata},
        },
    ]


def _build_layout_config() -> list[dict]:
    """Overlap enabled, 40% gap between series."""
    return [
        {
            "properties": {
                "clusteredGapOverlaps": _set_literal("true"),
                "clusteredGapSize": _set_literal("40D"),
            },
        },
    ]


def _build_py_projection(ac_table: str, py_measure: str) -> dict:
    """Build a Y projection dict for the PY measure."""
    return {
        "field": {
            "Measure": {
                "Expression": {"SourceRef": {"Entity": ac_table}},
                "Property": py_measure,
            },
        },
        "queryRef": f"{ac_table}.{py_measure}",
        "nativeQueryRef": py_measure,
    }


def _set_sort_descending(visual: dict, ac_table: str, ac_measure: str) -> None:
    """Set sort definition to descending by AC measure (for bar charts)."""
    query = visual.setdefault("visual", {}).setdefault("query", {})
    query["sortDefinition"] = {
        "sort": [
            {
                "field": {
                    "Measure": {
                        "Expression": {"SourceRef": {"Entity": ac_table}},
                        "Property": ac_measure,
                    },
                },
                "direction": "Descending",
            }
        ],
        "isDefaultSort": True,
    }


def _build_year_slicer(cal_table: str, year_col: str) -> dict:
    """Build a PBIR slicer visual.json for a Year dropdown on the calendar table."""
    return {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.0.0/schema.json",
        "name": "placeholder",
        "position": {
            "x": 0,
            "y": 0,
            "z": 20000,
            "height": 55,
            "width": 180,
            "tabOrder": 0,
        },
        "visual": {
            "visualType": "slicer",
            "drillFilterOtherVisuals": True,
            "query": {
                "queryState": {
                    "Values": {
                        "projections": [
                            {
                                "field": {
                                    "Column": {
                                        "Expression": {
                                            "SourceRef": {"Entity": cal_table}
                                        },
                                        "Property": year_col,
                                    }
                                },
                                "queryRef": f"{cal_table}.{year_col}",
                                "nativeQueryRef": year_col,
                                "active": True,
                            }
                        ]
                    }
                }
            },
            "objects": {
                "data": [
                    {
                        "properties": {
                            "mode": _set_literal("'Dropdown'"),
                        }
                    }
                ],
                "general": [
                    {
                        "properties": {
                            "orientation": _set_literal("0D"),
                        }
                    }
                ],
                "header": [
                    {
                        "properties": {
                            "show": _set_literal("false"),
                        }
                    }
                ],
            },
        },
    }


@log
def fix_ibcs_variance(
    report: str | UUID,
    page_name: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
) -> None:
    """
    Applies IBCS variance formatting to bar/column charts.

    For each bar/column chart with exactly one AC measure:
    - Creates PY, Δ PY, Max Green PY, Max Red AC measures if missing
    - Adds PY measure to the visual
    - Converts non-time column charts to clustered bar charts
    - Adds red/green error bars for variance display
    - Sets IBCS colors, labels, overlap, and axis formatting
    - Sorts bar charts descending by AC measure

    Parameters
    ----------
    report : str | uuid.UUID
        Name or ID of the report.
    page_name : str, default=None
        Page to apply changes to. None = all pages.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
    scan_only : bool, default=False
        If True, only scans and reports which charts could be enhanced.
    """

    # Resolve the semantic model behind this report
    dataset_id, dataset_name, dataset_workspace_id, _ = resolve_dataset_from_report(
        report=report, workspace=workspace
    )

    # --- Phase 1: Analyze the semantic model ---
    from sempy_labs.tom import connect_semantic_model
    import sempy.fabric as fabric

    # Get column types for time detection
    df_cols = fabric.list_columns(dataset=dataset_id, workspace=dataset_workspace_id)
    date_columns = set()
    for _, row in df_cols.iterrows():
        if row.get("Data Type") in _DATE_DATA_TYPES:
            date_columns.add((row["Table Name"], row["Column Name"]))

    # Read existing measures from the model
    existing_measures = {}  # {measure_name: (table_name, dax_expression)}
    cal_table = None
    cal_date_col = None

    with connect_semantic_model(
        dataset=dataset_id, readonly=True, workspace=dataset_workspace_id
    ) as tom:
        for t in tom.model.Tables:
            if str(getattr(t, "DataCategory", "")) == "Time":
                cal_table = t.Name
                for c in t.Columns:
                    if getattr(c, "IsKey", False):
                        cal_date_col = c.Name
                        break
                if cal_date_col is None:
                    for c in t.Columns:
                        if "date" in c.Name.lower():
                            cal_date_col = c.Name
                            break
            for m in t.Measures:
                existing_measures[m.Name] = (t.Name, str(m.Expression).strip())

    # --- Phase 2: Scan the report for candidate visuals ---
    with connect_report(report=report, workspace=workspace, readonly=True, show_diffs=False) as rw:
        if rw.format != "PBIR":
            print(
                f"{icons.red_dot} Report '{rw._report_name}' is in '{rw.format}' format, not PBIR. "
                f"Run 'Upgrade to PBIR' first."
            )
            return

        paths_df = rw.list_paths()
        page_id = rw.resolve_page_name(page_name) if page_name else None

        # Collect candidate visuals: (file_path, visual, ac_table, ac_measure, is_bar)
        candidates = []
        # Track pages for slicer check
        page_visuals = {}  # page_folder → list of visuals

        for file_path in paths_df["Path"]:
            if not file_path.endswith("/visual.json"):
                continue
            if page_id and f"/{page_id}/" not in file_path:
                continue

            # Extract page folder for slicer grouping
            parts = file_path.split("/")
            page_folder = None
            for i, p in enumerate(parts):
                if p == "pages" and i + 1 < len(parts):
                    page_folder = parts[i + 1]
                    break

            visual = rw.get(file_path=file_path)
            vtype = visual.get("visual", {}).get("visualType", "")

            # Collect all visuals per page for slicer analysis
            if page_folder:
                page_visuals.setdefault(page_folder, []).append((file_path, visual))

            if vtype not in _TARGET_TYPES:
                continue

            # Identify AC measures
            y_projs = _get_y_measures(visual)
            ac_measures = []
            for proj in y_projs:
                info = _measure_name_from_proj(proj)
                if info and _is_ac_measure(info[1]):
                    ac_measures.append(info)

            if len(ac_measures) != 1:
                if len(ac_measures) > 1 and scan_only:
                    names = ", ".join(f"[{m[1]}]" for m in ac_measures)
                    print(f"{icons.info} {file_path} — {len(ac_measures)} AC measures ({names}) — skipping (ambiguous)")
                continue

            ac_table, ac_measure = ac_measures[0]

            # Determine if this is/should be a bar chart (non-time axis)
            category_fields = _get_category_fields(visual)
            is_time = any(
                (t, c) in date_columns or _TIME_NAME_PATTERN.search(c)
                for t, c in category_fields
            )
            is_bar = vtype in ("barChart", "clusteredBarChart") or not is_time

            candidates.append((file_path, ac_table, ac_measure, is_bar, page_folder))

    if not candidates:
        if scan_only:
            print(f"{icons.info} No bar/column charts with a single AC measure found.")
        else:
            print(f"{icons.info} No charts to apply IBCS variance formatting to.")
        return

    # --- Phase 3: Determine which measures need to be created ---
    measures_to_create = []  # [(table, name, expression, format_string)]
    for _, ac_table, ac_measure, _, _ in candidates:
        py_name = f"{ac_measure} PY"
        delta_name = f"{ac_measure} Δ PY"
        max_green = f"{ac_measure} Max Green PY"
        max_red = f"{ac_measure} Max Red AC"

        # Get format string from AC measure
        ac_fmt = ""
        if ac_measure in existing_measures:
            pass  # we'll get format from TOM in phase 4 if needed

        if py_name not in existing_measures:
            if cal_table is None:
                # Need to create calendar first
                pass  # handled below
            else:
                measures_to_create.append((
                    ac_table, py_name,
                    f"CALCULATE([{ac_measure}], SAMEPERIODLASTYEAR('{cal_table}'[{cal_date_col}]))",
                ))

        if delta_name not in existing_measures:
            measures_to_create.append((
                ac_table, delta_name,
                f"[{ac_measure}] - [{ac_measure} PY]",
            ))

        if max_green not in existing_measures:
            measures_to_create.append((
                ac_table, max_green,
                f"IF([{ac_measure} Δ PY] > 0, MAX([{ac_measure}], [{ac_measure} PY]))",
            ))

        if max_red not in existing_measures:
            measures_to_create.append((
                ac_table, max_red,
                f"IF([{ac_measure} Δ PY] < 0, MAX([{ac_measure}], [{ac_measure} PY]))",
            ))

    # Deduplicate (same AC measure may appear on multiple visuals)
    seen = set()
    unique_measures = []
    for item in measures_to_create:
        if item[1] not in seen:
            seen.add(item[1])
            unique_measures.append(item)
    measures_to_create = unique_measures

    # --- Scan mode report ---
    if scan_only:
        print(f"\n{icons.info} IBCS Variance Scan — {len(candidates)} chart(s) could be enhanced:")
        for fp, ac_tbl, ac_m, is_bar, pg in candidates:
            chart_type = "bar" if is_bar else "column"
            py_exists = f"{ac_m} PY" in existing_measures
            err_exists = f"{ac_m} Max Green PY" in existing_measures and f"{ac_m} Max Red AC" in existing_measures
            py_status = "exists" if py_exists else "MISSING"
            err_status = "exist" if err_exists else "MISSING"
            cal_status = f"'{cal_table}'" if cal_table else "MISSING"
            print(
                f"  {icons.yellow_dot} {fp}\n"
                f"      AC: [{ac_m}] → {chart_type} | "
                f"PY: {py_status} | Error bars: {err_status} | Calendar: {cal_status}"
            )

        # Check for year slicers
        pages_checked = set()
        for _, _, _, _, pg in candidates:
            if pg and pg not in pages_checked:
                pages_checked.add(pg)
                has_slicer = False
                for _, vis in page_visuals.get(pg, []):
                    if vis.get("visual", {}).get("visualType") == "slicer":
                        vals = vis.get("visual", {}).get("query", {}).get("queryState", {}).get("Values", {}).get("projections", [])
                        for v in vals:
                            col = v.get("field", {}).get("Column", {})
                            prop = col.get("Property", "")
                            if _TIME_NAME_PATTERN.search(prop):
                                has_slicer = True
                                break
                    if has_slicer:
                        break
                if not has_slicer:
                    print(f"  {icons.yellow_dot} Page '{pg}' — no Year/FiscalYear slicer found")

        if measures_to_create:
            print(f"\n  {icons.yellow_dot} {len(measures_to_create)} measure(s) would be created in the semantic model.")
        if cal_table is None:
            print(f"  {icons.yellow_dot} No calendar table found — would be auto-created.")
        return

    # --- Phase 4: Create calendar if needed ---
    cal_created = False
    if cal_table is None:
        print(f"{icons.in_progress} No calendar table found — creating CalcCalendar...")
        try:
            from sempy_labs.semantic_model._Add_CalculatedTable_Calendar import add_calculated_calendar
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "No calendar table was found, and automatic calendar creation is not "
                "available in this installation. Please add a calendar/date table manually "
                "or upgrade sempy-labs to a version that includes add_calculated_calendar."
            ) from exc
        add_calculated_calendar(report=report, workspace=workspace, scan_only=False)
        cal_table = "CalcCalendar"
        cal_date_col = "Date"
        cal_created = True
        # Update PY measure expressions that were deferred
        for i, (tbl, name, expr) in enumerate(measures_to_create):
            if "SAMEPERIODLASTYEAR" in expr and "None" in expr:
                measures_to_create[i] = (
                    tbl, name,
                    f"CALCULATE([{name.replace(' PY', '')}], SAMEPERIODLASTYEAR('{cal_table}'[{cal_date_col}]))",
                )

    # Rebuild PY measures that were skipped due to missing calendar
    for _, ac_table, ac_measure, _, _ in candidates:
        py_name = f"{ac_measure} PY"
        if py_name not in existing_measures and py_name not in seen:
            measures_to_create.append((
                ac_table, py_name,
                f"CALCULATE([{ac_measure}], SAMEPERIODLASTYEAR('{cal_table}'[{cal_date_col}]))",
            ))
            seen.add(py_name)

    # --- Phase 5: Create measures in the semantic model ---
    if measures_to_create:
        print(f"{icons.in_progress} Creating {len(measures_to_create)} measure(s) in '{dataset_name}'...")
        with connect_semantic_model(
            dataset=dataset_id, readonly=False, workspace=dataset_workspace_id
        ) as tom:
            # Get format strings from AC measures
            ac_formats = {}
            for t in tom.model.Tables:
                for m in t.Measures:
                    if _is_ac_measure(m.Name):
                        ac_formats[m.Name] = str(m.FormatString) if m.FormatString else ""

            for tbl, name, expr in measures_to_create:
                # Determine format string from AC measure
                ac_base = name
                for suffix in _PY_SUFFIXES:
                    if name.endswith(suffix):
                        ac_base = name[: -len(suffix)]
                        break
                fmt = ac_formats.get(ac_base, "")
                # Percentage measures get % format
                if name.endswith(" Δ PY %"):
                    fmt = "0.0%;-0.0%;0.0%"

                tom.add_measure(
                    table_name=tbl,
                    measure_name=name,
                    expression=expr,
                    format_string=fmt,
                    display_folder="PY",
                )
                existing_measures[name] = (tbl, expr)
                print(f"  {icons.green_dot} Created [{name}]")

        print(f"{icons.green_dot} {len(measures_to_create)} measure(s) created.")

        # Recalculate the model so PY measures can resolve
        if cal_created:
            print(f"{icons.in_progress} Recalculating model...")
            refresh_semantic_model(
                dataset=dataset_id,
                refresh_type="calculate",
                workspace=dataset_workspace_id,
            )
            print(f"{icons.green_dot} Model recalculated.")

    # --- Phase 6: Apply IBCS formatting to visuals ---
    with connect_report(report=report, workspace=workspace, readonly=False, show_diffs=False) as rw:
        paths_df = rw.list_paths()
        charts_fixed = 0

        for file_path, ac_table, ac_measure, is_bar, page_folder in candidates:
            visual = rw.get(file_path=file_path)
            vtype = visual.get("visual", {}).get("visualType", "")
            py_measure = f"{ac_measure} PY"
            max_green = f"{ac_measure} Max Green PY"
            max_red = f"{ac_measure} Max Red AC"
            ac_metadata = f"{ac_table}.{ac_measure}"
            py_metadata = f"{ac_table}.{py_measure}"

            # Step 1: Stacked → Clustered + Column → Bar for non-time
            if vtype == "columnChart" and is_bar:
                visual["visual"]["visualType"] = "clusteredBarChart"
            elif vtype == "columnChart":
                visual["visual"]["visualType"] = "clusteredColumnChart"
            elif vtype == "barChart":
                visual["visual"]["visualType"] = "clusteredBarChart"
            # clusteredBarChart / clusteredColumnChart stay as-is

            # Step 2: Add PY to visual if missing
            y_projs = _get_y_measures(visual)
            has_py = any(
                _measure_name_from_proj(p) == (ac_table, py_measure)
                for p in y_projs
            )
            if not has_py:
                py_proj = _build_py_projection(ac_table, py_measure)
                y_projs.insert(0, py_proj)  # PY first (behind AC visually in overlap)

            # Step 3: Set objects
            objects = visual.setdefault("visual", {}).setdefault("objects", {})

            # Error bars
            objects["error"] = _build_error_bar_config(
                ac_table, ac_measure, py_measure, max_red, max_green
            )

            # Labels
            objects["labels"] = _build_label_config(ac_metadata, py_metadata)

            # Data point colors
            objects["dataPoint"] = _build_datapoint_config(ac_metadata, py_metadata)

            # Layout (overlap + gap)
            objects["layout"] = _build_layout_config()

            # Value axis: hidden, no gridlines, no title
            objects["valueAxis"] = [{
                "properties": {
                    "show": _set_literal("false"),
                    "gridlineShow": _set_literal("false"),
                    "showAxisTitle": _set_literal("false"),
                },
            }]

            # Category axis: visible, no title
            objects["categoryAxis"] = [{
                "properties": {
                    "show": _set_literal("true"),
                    "showAxisTitle": _set_literal("false"),
                },
            }]

            # Step 4: Sort descending by AC for bar charts
            final_type = visual["visual"]["visualType"]
            if final_type in ("barChart", "clusteredBarChart"):
                _set_sort_descending(visual, ac_table, ac_measure)

            rw.update(file_path=file_path, payload=visual)
            charts_fixed += 1
            print(f"{icons.green_dot} Applied IBCS variance to {file_path} — [{ac_measure}] ({final_type})")

        # Check for year slicers — add one if missing
        pages_warned = set()
        year_col = "Year"  # Default column name in CalcCalendar
        if cal_table:
            # Try to find a Year-like column on the calendar table
            for _, row in df_cols.iterrows():
                if row["Table Name"] == cal_table and _TIME_NAME_PATTERN.search(row["Column Name"]):
                    if "year" in row["Column Name"].lower():
                        year_col = row["Column Name"]
                        break

        # Build folder → displayName lookup from page.json files
        _folder_to_display = {}
        for fp2 in paths_df["Path"]:
            if fp2.endswith("/page.json"):
                try:
                    pj = rw.get(file_path=fp2)
                    parts = fp2.split("/")
                    for i, p in enumerate(parts):
                        if p == "pages" and i + 1 < len(parts):
                            _folder_to_display[parts[i + 1]] = pj.get("displayName", parts[i + 1])
                            break
                except Exception:
                    pass

        for _, _, _, _, pg in candidates:
            if pg and pg not in pages_warned:
                pages_warned.add(pg)
                has_slicer = False
                for fp2 in paths_df["Path"]:
                    if not fp2.endswith("/visual.json") or f"/{pg}/" not in fp2:
                        continue
                    vis2 = rw.get(file_path=fp2)
                    if vis2.get("visual", {}).get("visualType") == "slicer":
                        vals = vis2.get("visual", {}).get("query", {}).get("queryState", {}).get("Values", {}).get("projections", [])
                        for v in vals:
                            col = v.get("field", {}).get("Column", {})
                            prop = col.get("Property", "")
                            if _TIME_NAME_PATTERN.search(prop):
                                has_slicer = True
                                break
                    if has_slicer:
                        break
                if not has_slicer and cal_table:
                    page_display = _folder_to_display.get(pg, pg)
                    slicer_payload = _build_year_slicer(cal_table, year_col)
                    rw._add_visual(page=page_display, payload=slicer_payload, generate_id=True)
                    print(f"{icons.green_dot} Added Year slicer to page '{page_display}' — [{cal_table}][{year_col}]")

        if charts_fixed == 0:
            print(f"{icons.info} No charts to fix.")
        else:
            print(f"\n{icons.green_dot} Applied IBCS variance formatting to {charts_fixed} chart(s).")
