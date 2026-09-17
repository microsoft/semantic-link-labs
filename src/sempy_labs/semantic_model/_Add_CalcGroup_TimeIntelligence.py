# Add "Time Intelligence" Calculation Group to Semantic Model
# Based on: https://github.com/KornAlexander/PBI-Tools/.../2. Time Intelligence.csx
#
# All division operations use DIVIDE() — no bare "/" operator.

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
# Calculation group definition
# ---------------------------------------------------------------------------
_TI_CG_NAME = "Time Intelligence"


def _build_calc_items(cal: str, date: str) -> list:
    """
    Build the list of (name, expression) tuples for all Time Intelligence
    calculation items.  ``cal`` is the calendar table name and ``date`` is the
    date column name.

    Always includes: AC, Y-1 … Y-3, YTD, YTD-1, YTD-2,
    absolute / relative / achievement variances for Y-1/Y-2 and YTD-1/YTD-2.
    """

    items = [
        # --- Base periods ---
        ("AC", "SELECTEDMEASURE()"),
        (
            "Y-1",
            f"CALCULATE(\n"
            f"    SELECTEDMEASURE(),\n"
            f"    SAMEPERIODLASTYEAR( '{cal}'[{date}] ),\n"
            f"    ALL( '{cal}' )\n"
            f")",
        ),
        (
            "Y-2",
            f"CALCULATE(\n"
            f"    SELECTEDMEASURE(),\n"
            f"    DATEADD( '{cal}'[{date}], -2, YEAR ),\n"
            f"    ALL( '{cal}' )\n"
            f")",
        ),
        (
            "Y-3",
            f"CALCULATE(\n"
            f"    SELECTEDMEASURE(),\n"
            f"    DATEADD( '{cal}'[{date}], -3, YEAR ),\n"
            f"    ALL( '{cal}' )\n"
            f")",
        ),
        # --- YTD ---
        (
            "YTD",
            f"CALCULATE(\n"
            f"    SELECTEDMEASURE(),\n"
            f"    DATESYTD( '{cal}'[{date}], \"12/31\" ),\n"
            f"    ALL( '{cal}' )\n"
            f")",
        ),
        (
            "YTD-1",
            f"CALCULATE(\n"
            f"    SELECTEDMEASURE(),\n"
            f"    DATEADD( DATESYTD( '{cal}'[{date}], \"12/31\" ), -1, YEAR ),\n"
            f"    ALL( '{cal}' )\n"
            f")",
        ),
        (
            "YTD-2",
            f"CALCULATE(\n"
            f"    SELECTEDMEASURE(),\n"
            f"    DATEADD( DATESYTD( '{cal}'[{date}], \"12/31\" ), -2, YEAR ),\n"
            f"    ALL( '{cal}' )\n"
            f")",
        ),
        # --- Absolute variances ---
        (
            "abs. AC vs Y-1",
            f"VAR AC =\n"
            f"    TOTALYTD( SELECTEDMEASURE(), DATESYTD( '{cal}'[{date}], \"12/31\" ), ALL( '{cal}' ) )\n"
            f"VAR Y1 =\n"
            f"    CALCULATE( SELECTEDMEASURE(), DATEADD( DATESYTD( '{cal}'[{date}], \"12/31\" ), -1, YEAR ), ALL( '{cal}' ) )\n"
            f"RETURN\n"
            f"    AC - Y1",
        ),
        (
            "abs. AC vs Y-2",
            f"VAR AC =\n"
            f"    TOTALYTD( SELECTEDMEASURE(), DATESYTD( '{cal}'[{date}], \"12/31\" ), ALL( '{cal}' ) )\n"
            f"VAR Y2 =\n"
            f"    CALCULATE( SELECTEDMEASURE(), DATEADD( DATESYTD( '{cal}'[{date}], \"12/31\" ), -2, YEAR ), ALL( '{cal}' ) )\n"
            f"RETURN\n"
            f"    AC - Y2",
        ),
        (
            "abs. AC vs YTD-1",
            f"VAR AC =\n"
            f"    TOTALYTD( SELECTEDMEASURE(), DATESYTD( '{cal}'[{date}], \"12/31\" ), ALL( '{cal}' ) )\n"
            f"VAR Y1 =\n"
            f"    CALCULATE( SELECTEDMEASURE(), DATEADD( DATESYTD( '{cal}'[{date}], \"12/31\" ), -1, YEAR ), ALL( '{cal}' ) )\n"
            f"RETURN\n"
            f"    AC - Y1",
        ),
        (
            "abs. AC vs YTD-2",
            f"VAR AC =\n"
            f"    TOTALYTD( SELECTEDMEASURE(), DATESYTD( '{cal}'[{date}], \"12/31\" ), ALL( '{cal}' ) )\n"
            f"VAR Y2 =\n"
            f"    CALCULATE( SELECTEDMEASURE(), DATEADD( DATESYTD( '{cal}'[{date}], \"12/31\" ), -2, YEAR ), ALL( '{cal}' ) )\n"
            f"RETURN\n"
            f"    AC - Y2",
        ),
        # --- Relative variances (%) ---
        (
            "AC vs Y-1",
            f"VAR AC =\n"
            f"    TOTALYTD( SELECTEDMEASURE(), DATESYTD( '{cal}'[{date}], \"12/31\" ), ALL( '{cal}' ) )\n"
            f"VAR Y1 =\n"
            f"    CALCULATE( SELECTEDMEASURE(), SAMEPERIODLASTYEAR( '{cal}'[{date}] ), ALL( '{cal}' ) )\n"
            f"RETURN\n"
            f"    DIVIDE( AC - Y1, Y1 )",
        ),
        (
            "AC vs Y-2",
            f"VAR AC =\n"
            f"    TOTALYTD( SELECTEDMEASURE(), DATESYTD( '{cal}'[{date}], \"12/31\" ), ALL( '{cal}' ) )\n"
            f"VAR Y2 =\n"
            f"    CALCULATE( SELECTEDMEASURE(), DATEADD( '{cal}'[{date}], -2, YEAR ), ALL( '{cal}' ) )\n"
            f"RETURN\n"
            f"    DIVIDE( AC - Y2, Y2 )",
        ),
        (
            "AC vs YTD-1",
            f"VAR AC =\n"
            f"    TOTALYTD( SELECTEDMEASURE(), DATESYTD( '{cal}'[{date}], \"12/31\" ), ALL( '{cal}' ) )\n"
            f"VAR Y1 =\n"
            f"    CALCULATE( SELECTEDMEASURE(), DATEADD( DATESYTD( '{cal}'[{date}], \"12/31\" ), -1, YEAR ), ALL( '{cal}' ) )\n"
            f"RETURN\n"
            f"    DIVIDE( AC - Y1, Y1 )",
        ),
        (
            "AC vs YTD-2",
            f"VAR AC =\n"
            f"    TOTALYTD( SELECTEDMEASURE(), DATESYTD( '{cal}'[{date}], \"12/31\" ), ALL( '{cal}' ) )\n"
            f"VAR Y2 =\n"
            f"    CALCULATE( SELECTEDMEASURE(), DATEADD( DATESYTD( '{cal}'[{date}], \"12/31\" ), -2, YEAR ), ALL( '{cal}' ) )\n"
            f"RETURN\n"
            f"    DIVIDE( AC - Y2, Y2 )",
        ),
        # --- Achievement variances ---
        (
            "achiev. AC vs Y-1",
            f"VAR AC =\n"
            f"    SELECTEDMEASURE()\n"
            f"VAR Y1 =\n"
            f"    CALCULATE( SELECTEDMEASURE(), SAMEPERIODLASTYEAR( '{cal}'[{date}] ), ALL( '{cal}' ) )\n"
            f"RETURN\n"
            f"    1 - DIVIDE( ( Y1 - AC ), Y1, 0 )",
        ),
        (
            "achiev. AC vs Y-2",
            f"VAR AC =\n"
            f"    SELECTEDMEASURE()\n"
            f"VAR Y2 =\n"
            f"    CALCULATE( SELECTEDMEASURE(), DATEADD( '{cal}'[{date}], -2, YEAR ), ALL( '{cal}' ) )\n"
            f"RETURN\n"
            f"    1 - DIVIDE( ( Y2 - AC ), Y2, 0 )",
        ),
        (
            "achiev. AC vs YTD-1",
            f"VAR AC =\n"
            f"    TOTALYTD( SELECTEDMEASURE(), DATESYTD( '{cal}'[{date}], \"12/31\" ), ALL( '{cal}' ) )\n"
            f"VAR Y1 =\n"
            f"    CALCULATE( SELECTEDMEASURE(), DATEADD( DATESYTD( '{cal}'[{date}], \"12/31\" ), -1, YEAR ), ALL( '{cal}' ) )\n"
            f"RETURN\n"
            f"    1 - DIVIDE( ( Y1 - AC ), Y1, 0 )",
        ),
        (
            "achiev. AC vs YTD-2",
            f"VAR AC =\n"
            f"    TOTALYTD( SELECTEDMEASURE(), DATESYTD( '{cal}'[{date}], \"12/31\" ), ALL( '{cal}' ) )\n"
            f"VAR Y2 =\n"
            f"    CALCULATE( SELECTEDMEASURE(), DATEADD( DATESYTD( '{cal}'[{date}], \"12/31\" ), -2, YEAR ), ALL( '{cal}' ) )\n"
            f"RETURN\n"
            f"    1 - DIVIDE( ( Y2 - AC ), Y2, 0 )",
        ),
    ]

    return items


@log
def add_calc_group_time_intelligence(
    report: str | UUID,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
) -> None:
    """
    Checks whether a "Time Intelligence" calculation group (or any table whose
    name contains "time intelligence") exists in the semantic model that backs
    the given report.  If none is found, locates the calendar table
    (DataCategory = "Time") and its key date column, then creates a calculation
    group with AC, Y-1/Y-2/Y-3, YTD/YTD-1/YTD-2, absolute/relative/achievement
    variances.

    Also sets DiscourageImplicitMeasures to True (required for calculation
    groups).

    All division operations use ``DIVIDE()`` — no bare ``/`` operators.

    Parameters
    ----------
    report : str | uuid.UUID
        Name or ID of the Power BI report whose semantic model will be checked.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    scan_only : bool, default=False
        If True, only reports whether a Time Intelligence calculation group exists
        without making any changes.

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

        # Fuzzy match: any table whose name contains "time intelligence"
        ti_tables = [
            t.Name for t in tom.model.Tables if "time intelligence" in t.Name.lower()
        ]

        if ti_tables:
            table_list = ", ".join(f"'{t}'" for t in ti_tables)
            if scan_only:
                print(
                    f"{icons.green_dot} A Time Intelligence calculation group "
                    f"already exists: {table_list} — no action needed."
                )
            else:
                print(
                    f"{icons.info} A Time Intelligence calculation group "
                    f"already exists: {table_list}. Skipping creation."
                )
            return

        # --- Locate the calendar table (DataCategory = "Time") ---
        calendar_table = None
        for t in tom.model.Tables:
            if t.DataCategory == "Time":
                calendar_table = t
                break

        if calendar_table is None:
            if scan_only:
                print(
                    f"{icons.yellow_dot} No table containing 'Time Intelligence' "
                    f"found in '{dataset_name}'. A '{_TI_CG_NAME}' "
                    f"calculation group would be added."
                )
                print(
                    f"{icons.yellow_dot} However, no calendar table "
                    f"(DataCategory = 'Time') was found. Please add one first "
                    f"(e.g. via the Calendar Table fixer)."
                )
            else:
                print(
                    f"{icons.red_dot} Cannot create Time Intelligence calculation "
                    f"group: no calendar table (DataCategory = 'Time') found in "
                    f"'{dataset_name}'. Add a calendar table first."
                )
            return

        cal_name = calendar_table.Name

        # Find the key (date) column
        date_column = None
        for c in calendar_table.Columns:
            if c.IsKey:
                date_column = c.Name
                break

        if date_column is None:
            # Fallback: look for a column named "Date"
            for c in calendar_table.Columns:
                if c.Name.lower() == "date":
                    date_column = c.Name
                    break

        if date_column is None:
            msg = (
                f"Cannot determine the date column in '{cal_name}'. "
                f"Please mark a date column as IsKey or name it 'Date'."
            )
            if scan_only:
                print(f"{icons.yellow_dot} {msg}")
            else:
                print(f"{icons.red_dot} {msg}")
            return

        # No TI table found — report or create
        if scan_only:
            print(
                f"{icons.yellow_dot} No table containing 'Time Intelligence' "
                f"found in '{dataset_name}'. A '{_TI_CG_NAME}' calculation "
                f"group would be added using '{cal_name}'[{date_column}]."
            )
            return

        # --- Fix mode: create the calculation group ---
        print(
            f"{icons.in_progress} Adding '{_TI_CG_NAME}' calculation group "
            f"to '{dataset_name}' using '{cal_name}'[{date_column}]..."
        )

        # 1. Determine a unique precedence (max existing + 10)
        existing_precedences = [
            t.CalculationGroup.Precedence
            for t in tom.model.Tables
            if t.CalculationGroup is not None
        ]
        precedence = max(existing_precedences, default=-10) + 10

        # 2. Create the calculation group (also sets DiscourageImplicitMeasures)
        tom.add_calculation_group(
            name=_TI_CG_NAME,
            precedence=precedence,
        )

        # Rename the default "Name" column to match the calc group name
        tom.model.Tables[_TI_CG_NAME].Columns["Name"].Name = _TI_CG_NAME

        # 3. Build and add all calculation items
        calc_items = _build_calc_items(cal=cal_name, date=date_column)

        for ordinal, (item_name, expression) in enumerate(calc_items):
            tom.add_calculation_item(
                table_name=_TI_CG_NAME,
                calculation_item_name=item_name,
                expression=expression,
                ordinal=ordinal,
            )

        print(
            f"{icons.green_dot} '{_TI_CG_NAME}' calculation group added "
            f"successfully to '{dataset_name}' with {len(calc_items)} items."
        )


# Sample usage:
# add_calc_group_time_intelligence(report="My Report")
# add_calc_group_time_intelligence(report="My Report", workspace="My Workspace")
# add_calc_group_time_intelligence(report="My Report", scan_only=True)
