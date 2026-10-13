# Fix "Month (as a string) must be sorted" — standalone BPA fixer.
# Sets SortByColumn on month name columns to a corresponding month number column.

from typing import Optional
from uuid import UUID
import re


def fix_sort_month_column(
    dataset: str,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
):
    """
    Sets SortByColumn on month name (string) columns to point to a month number column.

    Auto-detects by naming convention: looks for a column with 'month' AND 'num'/'no'/'number'/'sort'/'order'/'key'
    in the same table. Falls back to a column named 'MonthNo', 'Month Number', 'MonthKey', etc.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
    scan_only : bool, default=False
        If True, only reports what would be fixed without making changes.
    """
    from sempy_labs.tom import connect_semantic_model

    fixed = 0
    with connect_semantic_model(dataset=dataset, readonly=scan_only, workspace=workspace) as tom:
        import Microsoft.AnalysisServices.Tabular as TOM

        for table in tom.model.Tables:
            # Find month name column (string type, name contains 'month')
            month_name_cols = []
            month_num_cols = []
            for col in table.Columns:
                name_lower = col.Name.lower()
                if "month" in name_lower:
                    if col.DataType == TOM.DataType.String:
                        month_name_cols.append(col)
                    elif col.DataType in (TOM.DataType.Int64, TOM.DataType.Double, TOM.DataType.Decimal):
                        month_num_cols.append(col)
                # Also check for columns like "MonthNo", "MonthKey", "MonthSort"
                if col.DataType in (TOM.DataType.Int64, TOM.DataType.Double, TOM.DataType.Decimal):
                    if any(k in name_lower for k in ("monthno", "monthnum", "monthkey", "monthsort", "month_no", "month_num", "month_key", "month_sort", "month number", "month order")):
                        if col not in month_num_cols:
                            month_num_cols.append(col)

            if not month_name_cols or not month_num_cols:
                continue

            sort_col = month_num_cols[0]  # pick first numeric month column
            for mc in month_name_cols:
                # Skip if already sorted
                if mc.SortByColumn is not None:
                    continue
                if scan_only:
                    print(f"  Would fix: '{table.Name}'[{mc.Name}] SortByColumn → [{sort_col.Name}]")
                else:
                    mc.SortByColumn = sort_col
                    print(f"  Fixed: '{table.Name}'[{mc.Name}] SortByColumn → [{sort_col.Name}]")
                fixed += 1
        if not scan_only and fixed > 0:
            tom.model.SaveChanges()

    action = "Would fix" if scan_only else "Fixed"
    print(f"  {action} {fixed} month column(s).")
    return fixed
