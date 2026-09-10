# Add Prior Year (PY) time intelligence measures for selected measures.
# For each source measure, creates: PY, Δ PY, Δ PY %, Max Green PY, Max Red AC.
# Ported from Tabular Editor macro: "ALL Y-1"

from typing import Optional, List
from uuid import UUID
from sempy._utils._log import log
import sempy_labs._icons as icons


@log
def add_py_measures(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    measures: Optional[List[str]] = None,
    calendar_table: Optional[str] = None,
    date_column: Optional[str] = None,
    target_table: Optional[str] = None,
    scan_only: bool = False,
):
    """
    Creates Prior Year (PY) time intelligence measures for each specified measure.

    For each source measure, generates 5 new measures:
    - ``{name} PY`` — SAMEPERIODLASTYEAR
    - ``{name} Δ PY`` — absolute variance
    - ``{name} Δ PY %`` — relative variance
    - ``{name} Max Green PY`` — positive variance highlight
    - ``{name} Max Red AC`` — negative variance highlight

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
    measures : list[str], default=None
        List of measure names to generate PY variants for.
        If None, generates for ALL measures in the model.
    calendar_table : str, default=None
        Name of the calendar/date table. If None, auto-detects
        by looking for a table with DataCategory="Time" or IsKey column.
    date_column : str, default=None
        Name of the date column. If None, auto-detects the key column.
    target_table : str, default=None
        Table to place new measures in. If None, measures are added
        to a "PY" display folder in the same table as the source measure.
    scan_only : bool, default=False
        If True, only reports what would be created without making changes.

    Returns
    -------
    int
        Number of measures created (or that would be created in scan mode).
    """
    from sempy_labs.tom import connect_semantic_model

    created = 0

    with connect_semantic_model(
        dataset=dataset, readonly=scan_only, workspace=workspace
    ) as tom:
        # Auto-detect calendar table
        cal_table = None
        if calendar_table:
            cal_table = tom.model.Tables.Find(calendar_table)
        else:
            for t in tom.model.Tables:
                if str(getattr(t, "DataCategory", "")) == "Time":
                    cal_table = t
                    break

        if cal_table is None:
            print(f"{icons.red_dot} No calendar table found. Specify calendar_table parameter.")
            return 0

        # Auto-detect date column
        dt_col = None
        if date_column:
            dt_col = date_column
        else:
            for c in cal_table.Columns:
                if getattr(c, "IsKey", False):
                    dt_col = c.Name
                    break
            if dt_col is None:
                for c in cal_table.Columns:
                    if "date" in c.Name.lower():
                        dt_col = c.Name
                        break
            if dt_col is None:
                print(f"{icons.red_dot} No date column found in '{cal_table.Name}'. Specify date_column parameter.")
                return 0

        cal_name = cal_table.Name
        print(f"{icons.info} Calendar: '{cal_name}'[{dt_col}]")

        # Resolve target table
        dest_table_obj = None
        if target_table:
            dest_table_obj = tom.model.Tables.Find(target_table)
            if dest_table_obj is None:
                print(f"{icons.red_dot} Target table '{target_table}' not found.")
                return 0
        else:
            # Auto-detect measure table by name
            for t in tom.model.Tables:
                if "measure" in t.Name.lower():
                    dest_table_obj = t
                    print(f"{icons.info} Auto-detected measure table: '{t.Name}'")
                    break

        # Collect source measures
        source_measures = []
        for table in tom.model.Tables:
            for m in table.Measures:
                if measures is None or m.Name in measures:
                    source_measures.append(m)

        if not source_measures:
            print(f"{icons.yellow_dot} No measures found to process.")
            return 0

        for m in source_measures:
            name = m.Name
            fmt = str(m.FormatString) if m.FormatString else ""
            folder = str(m.DisplayFolder) if m.DisplayFolder else ""
            py_folder = f"{folder}\\PY" if folder else "PY"
            dest = dest_table_obj or m.Table

            variants = [
                (f"{name} PY", f"CALCULATE([{name}], SAMEPERIODLASTYEAR('{cal_name}'[{dt_col}]))"),
                (f"{name} \u0394 PY", f"[{name}] - [{name} PY]"),
                (f"{name} \u0394 PY %", f"DIVIDE([{name}] - [{name} PY], [{name}])"),
                (f"{name} Max Green PY", f"IF([{name} \u0394 PY] > 0, MAX([{name}], [{name} PY]))"),
                (f"{name} Max Red AC", f"IF([{name} \u0394 PY] < 0, MAX([{name}], [{name} PY]))"),
            ]

            for v_name, v_expr in variants:
                existing = dest.Measures.Find(v_name)
                if existing is not None:
                    continue

                if scan_only:
                    print(f"{icons.yellow_dot} Would create: [{v_name}]")
                    created += 1
                    continue

                tom.add_measure(
                    table_name=dest.Name,
                    measure_name=v_name,
                    expression=v_expr,
                    format_string=fmt,
                    display_folder=py_folder,
                )
                created += 1

            if not scan_only:
                print(f"{icons.green_dot} Created PY variants for [{name}]")

        if not scan_only and created > 0:
            tom.model.SaveChanges()

    action = "Would create" if scan_only else "Created"
    print(f"{icons.info} {action} {created} PY measure(s) from {len(source_measures)} source measure(s).")
    return created
