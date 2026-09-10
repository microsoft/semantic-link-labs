# Add "Units" Calculation Group to Semantic Model
# Based on: https://github.com/KornAlexander/PBI-Tools/.../1. Units.csx

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
_UNITS_CG_NAME = "Units"

# Each item: (name, DAX expression)
# The expressions skip measures whose name contains "%" or "ratio" to avoid
# dividing percentage/ratio measures.
_UNITS_CALC_ITEMS = [
    (
        "Thousand",
        (
            "IF(\n"
            "    ISNUMBER( SELECTEDMEASURE() ),\n"
            "    IF(\n"
            "        NOT(\n"
            '            CONTAINSSTRING( SELECTEDMEASURENAME(), "%" )\n'
            '                || CONTAINSSTRING( SELECTEDMEASURENAME(), "ratio" )\n'
            "        ),\n"
            "        DIVIDE( SELECTEDMEASURE(), 1000 ),\n"
            "        SELECTEDMEASURE()\n"
            "    ),\n"
            "    SELECTEDMEASURE()\n"
            ")"
        ),
    ),
    (
        "Million",
        (
            "IF(\n"
            "    ISNUMBER( SELECTEDMEASURE() ),\n"
            "    IF(\n"
            "        NOT(\n"
            '            CONTAINSSTRING( SELECTEDMEASURENAME(), "%" )\n'
            '                || CONTAINSSTRING( SELECTEDMEASURENAME(), "ratio" )\n'
            "        ),\n"
            "        DIVIDE( SELECTEDMEASURE(), 1000000 ),\n"
            "        SELECTEDMEASURE()\n"
            "    ),\n"
            "    SELECTEDMEASURE()\n"
            ")"
        ),
    ),
]


@log
def add_calc_group_units(
    report: str | UUID,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
) -> None:
    """
    Checks whether a "Units" calculation group (or any table whose name
    contains "unit") exists in the semantic model that backs the given report.
    If none is found, creates a calculation group with "Thousand" and "Million"
    items that divide numeric measures accordingly (skipping percentage / ratio
    measures).

    Also sets DiscourageImplicitMeasures to True (required for calculation
    groups).

    Note: Calculation groups can have a performance impact when used inside
    reports.  Use them judiciously — especially with large models or complex
    measure chains.

    Parameters
    ----------
    report : str | uuid.UUID
        Name or ID of the Power BI report whose semantic model will be checked.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    scan_only : bool, default=False
        If True, only reports whether a Units calculation group exists without
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

        # Fuzzy match: any table whose name contains "unit"
        unit_tables = [t.Name for t in tom.model.Tables if "unit" in t.Name.lower()]

        if unit_tables:
            table_list = ", ".join(f"'{t}'" for t in unit_tables)
            if scan_only:
                print(
                    f"{icons.green_dot} A Units calculation group already exists: "
                    f"{table_list} — no action needed."
                )
            else:
                print(
                    f"{icons.info} A Units calculation group already exists: "
                    f"{table_list}. Skipping creation."
                )
            return

        # No units table found
        if scan_only:
            print(
                f"{icons.yellow_dot} No table containing 'Unit' found in "
                f"'{dataset_name}'. A '{_UNITS_CG_NAME}' calculation group "
                f"would be added."
            )
            print(
                f"{icons.info} Note: Calculation groups can have a performance "
                f"impact when used within reports."
            )
            return

        # --- Fix mode: create the calculation group ---
        print(
            f"{icons.in_progress} Adding '{_UNITS_CG_NAME}' calculation group "
            f"to '{dataset_name}' in '{dataset_workspace_name}'..."
        )
        print(
            f"{icons.info} Note: Calculation groups can have a performance "
            f"impact when used within reports."
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
            name=_UNITS_CG_NAME,
            precedence=precedence,
        )

        # Rename the default "Name" column to match the calc group name
        tom.model.Tables[_UNITS_CG_NAME].Columns["Name"].Name = _UNITS_CG_NAME

        # 3. Add calculation items
        for ordinal, (item_name, expression) in enumerate(_UNITS_CALC_ITEMS):
            tom.add_calculation_item(
                table_name=_UNITS_CG_NAME,
                calculation_item_name=item_name,
                expression=expression,
                ordinal=ordinal,
            )

        print(
            f"{icons.green_dot} '{_UNITS_CG_NAME}' calculation group added "
            f"successfully to '{dataset_name}' with {len(_UNITS_CALC_ITEMS)} items "
            f"(Thousand, Million)."
        )


# Sample usage:
# add_calc_group_units(report="My Report")
# add_calc_group_units(report="My Report", workspace="My Workspace")
# add_calc_group_units(report="My Report", scan_only=True)
