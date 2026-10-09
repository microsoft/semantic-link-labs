# Add Calculated Calendar Table to Semantic Model
# %pip install semantic-link-labs

from uuid import UUID
from typing import Optional
from sempy._utils._log import log
import sempy_labs._icons as icons
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_dataset_from_report,
)
from sempy_labs.tom import connect_semantic_model
from sempy_labs._refresh_semantic_model import refresh_semantic_model

# ---------------------------------------------------------------------------
# The full DAX expression for the calculated calendar table.
# Source: https://community.fabric.microsoft.com/t5/TMDL-Gallery/Calc-Table-Calendar/td-p/4798025
# ---------------------------------------------------------------------------
_CALENDAR_DAX = """
VAR Today = TODAY()
VAR MonthStartFiscalYear = 10
RETURN
    ADDCOLUMNS(
        CALENDARAUTO(),
        "Year", YEAR([Date]),
        "Quarter", "Q " & QUARTER([Date]),
        "Month", MONTH([Date]),
        "Month (MMM)", FORMAT([Date], "MMM"),
        "Day", DAY([Date]),
        "Fiscal Year", YEAR([Date]) + IF(MONTH([Date]) >= MonthStartFiscalYear, 1, 0),
        "End of Month", EOMONTH([Date], 0),
        "Week of Year", WEEKNUM([Date]),
        "Weekday", WEEKDAY([Date]),
        "Is Current or Past Month", IF([Date] <= EOMONTH(TODAY(), 0), "Yes", "No"),
        "Is Before This Month", FORMAT([Date],"YYYYMM") < FORMAT(Today,"YYYYMM"),
        "Is Current Fiscal Year",
            VAR CurrentFiscalYear = YEAR(Today) + IF(MONTH(Today) >= MonthStartFiscalYear, 1, 0)
            RETURN YEAR([Date]) + IF(MONTH([Date]) >= MonthStartFiscalYear, 1, 0) = CurrentFiscalYear,
        "Is Previous Fiscal Year",
            VAR CurrentFiscalYear = YEAR(Today) + IF(MONTH(Today) >= MonthStartFiscalYear, 1, 0)
            RETURN YEAR([Date]) + IF(MONTH([Date]) >= MonthStartFiscalYear, 1, 0) = CurrentFiscalYear - 1,
        "Is Current Calendar Year", YEAR([Date]) = YEAR(Today),
        "Is Previous Calendar Year", YEAR([Date]) = YEAR(Today) - 1,
        "Is Current Month",
            YEAR([Date]) = YEAR(Today) && MONTH([Date]) = MONTH(Today),
        "Is Previous Month",
            VAR PrevMonthYear = IF(MONTH(Today) = 1, YEAR(Today) - 1, YEAR(Today))
            VAR PrevMonth = IF(MONTH(Today) = 1, 12, MONTH(Today) - 1)
            RETURN YEAR([Date]) = PrevMonthYear && MONTH([Date]) = PrevMonth,
        "Month Key", YEAR([Date]) * 100 + MONTH([Date]),
        "Relative Month", (YEAR([Date]) - YEAR(Today)) * 12 + (MONTH([Date]) - MONTH(Today))
    )
""".strip()

# Table and column names matching the TMDL definition
_CAL_TABLE_NAME = "CalcCalendar"

# Boolean format string matching the original TMDL
_BOOL_FMT = '"""TRUE"";""TRUE"";""FALSE"""'

# Column definitions:
#   (column_name, source_column, data_type, format_string, is_key, hidden, summarize_by, display_folder)
_COLUMNS = [
    ("Date",                        "CalcCalendar.[Date]",                        "DateTime", "Short Date",    True,  False, "None",    "1. Favorites"),
    ("Month",                       "CalcCalendar.[Month]",                       "Int64",    "0",             False, False, "Sum",     "2. Calendar Date\\2. Number Columns"),
    ("Fiscal Year",                 "CalcCalendar.[Fiscal Year]",                 "Int64",    "0",             False, False, "Sum",     "3. Fiscal Date\\2. Numbers;1. Favorites"),
    ("Year",                        "CalcCalendar.[Year]",                        "Int64",    "0",             False, False, "Sum",     "2. Calendar Date\\2. Number Columns;1. Favorites"),
    ("Month (MMM)",                 "CalcCalendar.[Month (MMM)]",                 "String",   None,            False, False, "None",    "2. Calendar Date\\3. Text Columns;1. Favorites"),
    ("Day",                         "CalcCalendar.[Day]",                         "Int64",    "0",             False, False, "Sum",     "2. Calendar Date\\2. Number Columns"),
    ("Is Before This Month",        "CalcCalendar.[Is Before This Month]",        "Boolean",  _BOOL_FMT,       False, False, "None",    "4. Flags"),
    ("Is Current Fiscal Year",      "CalcCalendar.[Is Current Fiscal Year]",      "Boolean",  _BOOL_FMT,       False, False, "None",    "4. Flags"),
    ("Is Previous Fiscal Year",     "CalcCalendar.[Is Previous Fiscal Year]",     "Boolean",  _BOOL_FMT,       False, False, "None",    "4. Flags"),
    ("Is Current Calendar Year",    "CalcCalendar.[Is Current Calendar Year]",    "Boolean",  _BOOL_FMT,       False, False, "None",    "4. Flags"),
    ("Is Previous Calendar Year",   "CalcCalendar.[Is Previous Calendar Year]",   "Boolean",  _BOOL_FMT,       False, False, "None",    "4. Flags"),
    ("Is Current Month",            "CalcCalendar.[Is Current Month]",            "Boolean",  _BOOL_FMT,       False, False, "None",    "4. Flags"),
    ("Is Previous Month",           "CalcCalendar.[Is Previous Month]",           "Boolean",  _BOOL_FMT,       False, False, "None",    "4. Flags"),
    ("Year Month Key",              "CalcCalendar.[Month Key]",                   "Int64",    "0",             False, False, "Count",   "2. Calendar Date\\2. Number Columns"),
    ("Relative Month",              "CalcCalendar.[Relative Month]",              "Int64",    "0",             False, False, "Sum",     "4. Flags"),
    ("Quarter",                     "CalcCalendar.[Quarter]",                     "String",   None,            False, False, "None",    "2. Calendar Date\\3. Text Columns"),
    ("End of Month",                "CalcCalendar.[End of Month]",                "DateTime", "General Date",  False, False, "None",    "2. Calendar Date\\2. Number Columns"),
    ("Week of Year",                "CalcCalendar.[Week of Year]",                "Int64",    "0",             False, False, "Sum",     "2. Calendar Date\\2. Number Columns"),
    ("Weekday",                     "CalcCalendar.[Weekday]",                     "Int64",    "0",             False, False, "Sum",     "2. Calendar Date\\2. Number Columns"),
    ("Is Current or Past Months",   "CalcCalendar.[Is Current or Past Month]",    "String",   None,            False, False, "None",    "4. Flags"),
]

# Sort-by-column mappings: (column_name, sort_by_column_name)
_SORT_BY = [
    ("Month (MMM)", "Month"),
]

# Hierarchy definitions: (hierarchy_name, [columns], [levels], display_folder)
_HIERARCHIES = [
    ("Date Hierarchy",        ["Year", "Quarter", "Month", "Day"],                None, "2. Calendar Date\\1. Hierarchy"),
    ("Fiscal Date Hierarchy", ["Fiscal Year", "Quarter", "Month", "Day"],         None, "3. Fiscal Date\\1. Hierarchy"),
    ("Calendar Hierarchy",    ["Year", "Month (MMM)", "Week of Year", "Weekday"], None, "1. Favorites"),
]


@log
def add_calculated_calendar(
    report: str | UUID,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
    relationships: Optional[list] = None,
) -> None:
    """
    Checks whether a calendar table (DataCategory = 'Time') exists in the
    semantic model that backs the given report. If none is found, adds the
    CalcCalendar calculated table with a full set of date-intelligence columns
    and hierarchies. Optionally creates relationships to Date/DateTime columns.

    Parameters
    ----------
    report : str | uuid.UUID
        Name or ID of the Power BI report whose semantic model will be checked.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    scan_only : bool, default=False
        If True, only reports whether a Time-category table exists without
        making any changes.
    relationships : list, default=None
        List of (from_table, from_column, calendar_column) tuples specifying
        relationships to create from fact tables to the calendar table.
        If None, auto-detects all Date/DateTime columns not already in a
        relationship and creates Many→One relationships to CalcCalendar[Date].

    Returns
    -------
    None
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    # Resolve the semantic model behind this report
    dataset_id, dataset_name, dataset_workspace_id, dataset_workspace_name = (
        resolve_dataset_from_report(report=report, workspace=workspace_id)
    )

    with connect_semantic_model(
        dataset=dataset_id,
        readonly=scan_only,
        workspace=dataset_workspace_id,
    ) as tom:

        # Check for any table with DataCategory == "Time"
        time_tables = [
            t.Name for t in tom.model.Tables if t.DataCategory == "Time"
        ]

        if time_tables:
            table_list = ", ".join(f"'{t}'" for t in time_tables)
            if scan_only:
                print(
                    f"{icons.green_dot} Calendar table already exists: {table_list} "
                    f"(DataCategory = Time) — no action needed."
                )
            else:
                print(
                    f"{icons.info} Calendar table already exists: {table_list} "
                    f"(DataCategory = Time). Skipping creation."
                )
            return

        # No Time-category table found
        if scan_only:
            print(
                f"{icons.yellow_dot} No table with DataCategory 'Time' found in "
                f"'{dataset_name}'. A CalcCalendar table would be added."
            )
            return

        # --- Fix mode: create the calculated table ---
        print(
            f"{icons.in_progress} Adding CalcCalendar table to "
            f"'{dataset_name}' in '{dataset_workspace_name}'..."
        )

        # 1. Add the calculated table
        tom.add_calculated_table(
            name=_CAL_TABLE_NAME,
            expression=_CALENDAR_DAX,
            data_category="Time",
        )

        # 2. Add all columns (with display folders)
        for col_name, source_col, data_type, fmt, is_key, hidden, summarize, folder in _COLUMNS:
            tom.add_calculated_table_column(
                table_name=_CAL_TABLE_NAME,
                column_name=col_name,
                source_column=source_col,
                data_type=data_type,
                format_string=fmt,
                hidden=hidden,
                key=is_key,
                summarize_by=summarize,
                display_folder=folder,
            )

        # 3. Mark as date table (sets DataCategory=Time + IsKey on Date column)
        tom.mark_as_date_table(
            table_name=_CAL_TABLE_NAME,
            column_name="Date",
        )

        # 4. Intermediate save — server must assign column IDs before
        #    hierarchies can reference them.
        tom.model.SaveChanges()

        # 4b. Re-apply display folders AFTER SaveChanges — the server
        #     regenerates CalculatedTableColumn objects during save and
        #     may drop display folders set before the round-trip.
        cal_table = tom.model.Tables.Find(_CAL_TABLE_NAME)
        if cal_table is not None:
            for col_name, _sc, _dt, _fmt, _ik, _hid, _sum, folder in _COLUMNS:
                col_obj = cal_table.Columns.Find(col_name)
                if col_obj is not None and folder:
                    col_obj.DisplayFolder = folder

        # 5. Set sort-by-column relationships
        for col_name, sort_col in _SORT_BY:
            tom.set_sort_by_column(
                table_name=_CAL_TABLE_NAME,
                column_name=col_name,
                sort_by_column=sort_col,
            )

        # 6. Add hierarchies (with display folders)
        for hier_name, columns, levels, folder in _HIERARCHIES:
            tom.add_hierarchy(
                table_name=_CAL_TABLE_NAME,
                hierarchy_name=hier_name,
                columns=columns,
                levels=levels,
            )
            # Set display folder on the hierarchy object (use .Find() for pythonnet safety)
            if cal_table is not None:
                hier_obj = cal_table.Hierarchies.Find(hier_name)
                if hier_obj is not None:
                    hier_obj.DisplayFolder = folder

        # 7. Create relationships to Date/DateTime columns
        #    If relationships=None, auto-detect all Date/DateTime columns not already in a relationship.
        if relationships is None:
            relationships = []
            existing_rels = set()
            for r in tom.model.Relationships:
                existing_rels.add((str(r.FromColumn.Table.Name), str(r.FromColumn.Name)))
            for t in tom.model.Tables:
                if t.Name == _CAL_TABLE_NAME:
                    continue
                for c in t.Columns:
                    dt = str(c.DataType)
                    if dt in ("DateTime", "DateTimeOffset"):
                        if (t.Name, c.Name) not in existing_rels:
                            relationships.append((t.Name, c.Name, "Date"))

        rels_created = 0
        if relationships:
            for from_table, from_column, cal_column in relationships:
                try:
                    tom.add_relationship(
                        from_table=from_table,
                        from_column=from_column,
                        to_table=_CAL_TABLE_NAME,
                        to_column=cal_column,
                        from_cardinality="Many",
                        to_cardinality="One",
                    )
                    rels_created += 1
                    print(
                        f"{icons.green_dot} Created relationship: "
                        f"'{from_table}'[{from_column}] → '{_CAL_TABLE_NAME}'[{cal_column}]"
                    )
                except Exception as e:
                    print(
                        f"{icons.yellow_dot} Could not create relationship "
                        f"'{from_table}'[{from_column}] → '{_CAL_TABLE_NAME}'[{cal_column}]: {e}"
                    )

        rel_msg = f" and {rels_created} relationship(s)" if rels_created else ""
        print(
            f"{icons.green_dot} CalcCalendar table added successfully to "
            f"'{dataset_name}' with {len(_COLUMNS)} columns, "
            f"{len(_HIERARCHIES)} hierarchies{rel_msg}."
        )

    # Refresh to populate the calculated table data
    print(f"{icons.in_progress} Refreshing CalcCalendar table...")
    refresh_semantic_model(
        dataset=dataset_id,
        tables=[_CAL_TABLE_NAME],
        refresh_type="calculate",
        workspace=dataset_workspace_id,
    )
    print(f"{icons.green_dot} CalcCalendar table refreshed.")

# Sample usage:
# add_calculated_calendar(report="My Report")
# add_calculated_calendar(report="My Report", workspace="My Workspace")
# add_calculated_calendar(report="My Report", scan_only=True)