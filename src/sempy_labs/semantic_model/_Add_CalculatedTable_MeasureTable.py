# Add Empty "Measure" Calculated Table to Semantic Model
# Based on: https://github.com/KornAlexander/PBI-Tools/.../2. Create Empty Measure Table (TE3).csx

from uuid import UUID
from typing import Optional
from sempy._utils._log import log
import sempy_labs._icons as icons
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_dataset_from_report,
)
from sempy_labs.tom import connect_semantic_model

# ---------------------------------------------------------------------------
# Table definition
# ---------------------------------------------------------------------------
_MT_TABLE_NAME = "Measure"

# A single-row, zero-column calculated table — the lightest possible placeholder.
_CALC_EXPRESSION = "{0}"


@log
def add_measure_table(
    report: str | UUID,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
) -> None:
    """
    Checks whether a "Measure" table (or any table whose name contains
    "measure") exists in the semantic model that backs the given report.
    If none is found, adds an empty calculated table named "Measure" that
    serves as a centralized container for measures.  The auto-generated
    column is hidden.

    Parameters
    ----------
    report : str | uuid.UUID
        Name or ID of the Power BI report whose semantic model will be checked.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    scan_only : bool, default=False
        If True, only reports whether a Measure table exists without
        making any changes.

    Returns
    -------
    None
    """

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)

    dataset_id, dataset_name, dataset_workspace_id, dataset_workspace_name = (
        resolve_dataset_from_report(report=report, workspace=workspace_id)
    )

    with connect_semantic_model(
        dataset=dataset_id,
        readonly=scan_only,
        workspace=dataset_workspace_id,
    ) as tom:

        # Fuzzy match: any table whose name contains "measure"
        measure_tables = [
            t.Name for t in tom.model.Tables if "measure" in t.Name.lower()
        ]

        if measure_tables:
            table_list = ", ".join(f"'{t}'" for t in measure_tables)
            if scan_only:
                print(
                    f"{icons.green_dot} A Measure table already exists: "
                    f"{table_list} — no action needed."
                )
            else:
                print(
                    f"{icons.info} A Measure table already exists: "
                    f"{table_list}. Skipping creation."
                )
            return

        # No measure table found
        if scan_only:
            print(
                f"{icons.yellow_dot} No table containing 'Measure' found in "
                f"'{dataset_name}'. A '{_MT_TABLE_NAME}' table would be added."
            )
            return

        # --- Fix mode: create the calculated table ---
        print(
            f"{icons.in_progress} Adding '{_MT_TABLE_NAME}' table to "
            f"'{dataset_name}' in '{dataset_workspace_name}'..."
        )

        # 1. Add the calculated table
        tom.add_calculated_table(
            name=_MT_TABLE_NAME,
            expression=_CALC_EXPRESSION,
        )

        # 2. Save so the auto-generated column materialises
        tom.model.SaveChanges()

        # 3. Hide the auto-generated column (typically named "Value")
        for col in tom.model.Tables[_MT_TABLE_NAME].Columns:
            col.IsHidden = True

        print(
            f"{icons.green_dot} '{_MT_TABLE_NAME}' table added successfully to "
            f"'{dataset_name}'. Move your measures into this table to keep "
            f"the model organised."
        )


# Sample usage:
# add_measure_table(report="My Report")
# add_measure_table(report="My Report", workspace="My Workspace")
# add_measure_table(report="My Report", scan_only=True)
