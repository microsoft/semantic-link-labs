# Auto-create measures from columns based on SummarizeBy property.
# For each column where SummarizeBy != "None", creates a measure using
# the appropriate aggregation function and hides the source column.
# Ported from Tabular Editor macro: "Selected Measures based on Summarize By Property"

from typing import Optional
from uuid import UUID
from sempy._utils._log import log
import sempy_labs._icons as icons


@log
def add_measures_from_columns(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    target_table: Optional[str] = None,
    scan_only: bool = False,
):
    """
    Creates measures from columns based on their SummarizeBy property.

    For each column where SummarizeBy is not "None", a measure is created
    using the appropriate aggregation (SUM, COUNT, MIN, MAX, etc.).
    The source column is hidden after measure creation.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
    target_table : str, default=None
        Table to place new measures in. If None, measures are added to
        the same table as the source column.
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
        # Resolve target table if specified
        measures_table = None
        if target_table:
            measures_table = tom.model.Tables.Find(target_table)
            if measures_table is None:
                print(f"{icons.red_dot} Target table '{target_table}' not found.")
                return 0
        else:
            # Auto-detect measure table by name
            for t in tom.model.Tables:
                if "measure" in t.Name.lower():
                    measures_table = t
                    print(f"{icons.info} Auto-detected measure table: '{t.Name}'")
                    break

        for table in tom.model.Tables:
            for col in table.Columns:
                summarize_by = str(col.SummarizeBy) if hasattr(col, "SummarizeBy") else "None"
                if summarize_by == "None" or summarize_by == "Default":
                    continue

                agg_fn = summarize_by.upper()
                measure_name = col.Name
                dax_expr = f"{agg_fn}('{table.Name}'[{col.Name}])"
                dest_table = measures_table or table

                # Check if measure already exists
                existing = dest_table.Measures.Find(measure_name)
                if existing is not None:
                    continue

                if scan_only:
                    print(
                        f"{icons.yellow_dot} Would create: [{measure_name}] = {dax_expr} "
                        f"in '{dest_table.Name}'"
                    )
                    created += 1
                    continue

                tom.add_measure(
                    table_name=dest_table.Name,
                    measure_name=measure_name,
                    expression=dax_expr,
                    format_string="0.0",
                    description=(
                        f"Auto-created {agg_fn} measure from column "
                        f"'{table.Name}'[{col.Name}]"
                    ),
                    display_folder=table.Name,
                )
                col.IsHidden = True
                created += 1
                print(
                    f"{icons.green_dot} Created [{measure_name}] = {dax_expr} "
                    f"in '{dest_table.Name}'"
                )

        if not scan_only and created > 0:
            tom.model.SaveChanges()

    action = "Would create" if scan_only else "Created"
    print(f"{icons.info} {action} {created} measure(s) from columns.")
    return created
