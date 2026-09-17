# Add "Last Refresh" Table to Semantic Model
# Based on: https://github.com/KornAlexander/PBI-Tools/.../Last Refresh.tmdl

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
# Table / column / measure definitions derived from the TMDL template.
# ---------------------------------------------------------------------------
_LR_TABLE_NAME = "Last Refresh"

_M_EXPRESSION = (
    "let\n"
    '    #"Today" = #table({"Last Refreshes"}, '
    "{{DateTime.From(DateTime.LocalNow())}})\n"
    "in\n"
    '    #"Today"'
)

_MEASURE_NAME = "Last Refresh Measure"
_MEASURE_EXPRESSION = "\"Last Refresh: \" & MAX('Last Refresh'[Last Refreshes])"
_MEASURE_DISPLAY_FOLDER = "Meta"


@log
def add_last_refresh_table(
    report: str | UUID,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
) -> None:
    """
    Checks whether a "Last Refresh" table (or any table whose name contains
    "refresh") exists in the semantic model that backs the given report.
    If none is found, adds a small M-partition table that records the timestamp
    of every data refresh together with a convenience measure.

    Parameters
    ----------
    report : str | uuid.UUID
        Name or ID of the Power BI report whose semantic model will be checked.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    scan_only : bool, default=False
        If True, only reports whether a "Last Refresh" table exists without
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

        # Fuzzy match: any table whose name contains "refresh"
        refresh_tables = [
            t.Name for t in tom.model.Tables if "refresh" in t.Name.lower()
        ]

        if refresh_tables:
            table_list = ", ".join(f"'{t}'" for t in refresh_tables)
            if scan_only:
                print(
                    f"{icons.green_dot} A refresh table already exists: {table_list} "
                    f"— no action needed."
                )
            else:
                print(
                    f"{icons.info} A refresh table already exists: {table_list}. "
                    f"Skipping creation."
                )
            return

        # No refresh table found
        if scan_only:
            print(
                f"{icons.yellow_dot} No table containing 'Refresh' found in "
                f"'{dataset_name}'. A '{_LR_TABLE_NAME}' table would be added."
            )
            return

        # --- Fix mode: create the table ---
        print(
            f"{icons.in_progress} Adding '{_LR_TABLE_NAME}' table to "
            f"'{dataset_name}' in '{dataset_workspace_name}'..."
        )

        # 1. Create the table (hidden — only the measure is user-facing)
        tom.add_table(name=_LR_TABLE_NAME, hidden=True)

        # 2. Add M-partition
        tom.add_m_partition(
            table_name=_LR_TABLE_NAME,
            partition_name=_LR_TABLE_NAME,
            expression=_M_EXPRESSION,
            mode="Import",
        )

        # 3. Add the data column
        tom.add_data_column(
            table_name=_LR_TABLE_NAME,
            column_name="Last Refreshes",
            source_column="Last Refreshes",
            data_type="String",
            hidden=False,
            summarize_by="None",
        )

        # 4. Add the measure — prefer the "Measure" table if it exists
        measure_tables = [
            t.Name for t in tom.model.Tables if "measure" in t.Name.lower()
        ]
        measure_target = measure_tables[0] if measure_tables else _LR_TABLE_NAME

        tom.add_measure(
            table_name=measure_target,
            measure_name=_MEASURE_NAME,
            expression=_MEASURE_EXPRESSION,
            display_folder=_MEASURE_DISPLAY_FOLDER,
        )

        target_info = (
            f" (measure placed in '{measure_target}')"
            if measure_target != _LR_TABLE_NAME
            else ""
        )
        print(
            f"{icons.green_dot} '{_LR_TABLE_NAME}' table added successfully to "
            f"'{dataset_name}' with 1 column and 1 measure{target_info}."
        )


# Sample usage:
# add_last_refresh_table(report="My Report")
# add_last_refresh_table(report="My Report", workspace="My Workspace")
# add_last_refresh_table(report="My Report", scan_only=True)
