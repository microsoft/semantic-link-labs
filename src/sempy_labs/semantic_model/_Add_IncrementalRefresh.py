# Setup Incremental Refresh — standalone fixer.
# Auto-detects the first DateTime column and configures incremental refresh.

from typing import Optional
from uuid import UUID
from datetime import datetime, timedelta
import re
from sempy._utils._log import log


@log
def add_incremental_refresh(
    dataset: str | UUID,
    table_name: str,
    workspace: Optional[str | UUID] = None,
    column_name: Optional[str] = None,
    rolling_window_years: int = 3,
    incremental_days: int = 30,
    only_refresh_complete_days: bool = False,
    scan_only: bool = False,
):
    """
    Sets up an incremental refresh policy on a table.

    Auto-detects the first DateTime column if column_name is not specified.

    Parameters
    ----------
    dataset : str | UUID
        Name or ID of the semantic model.
    table_name : str
        Name of the table to configure.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
    column_name : str, default=None
        The DateTime column to use. If None, auto-detects the first DateTime column.
    rolling_window_years : int, default=3
        Number of years to keep in the rolling window.
    incremental_days : int, default=30
        Number of days to refresh incrementally.
    only_refresh_complete_days : bool, default=False
        If True, only refresh complete days (offset -1).
    scan_only : bool, default=False
        If True, only reports what would be configured without making changes.
    """
    if rolling_window_years < 1:
        raise ValueError(f"rolling_window_years must be >= 1, got {rolling_window_years}")
    if incremental_days < 1:
        raise ValueError(f"incremental_days must be >= 1, got {incremental_days}")
    from sempy_labs.tom import connect_semantic_model

    with connect_semantic_model(dataset=dataset, readonly=scan_only, workspace=workspace) as tom:
        import Microsoft.AnalysisServices.Tabular as TOM

        t = tom.model.Tables.Find(table_name)
        if t is None:
            print(f"  Table '{table_name}' not found. Skipping.")
            return

        # Auto-detect date column
        date_col = column_name
        if date_col is None:
            for col in t.Columns:
                if col.DataType == TOM.DataType.DateTime:
                    date_col = col.Name
                    break
            if date_col is None:
                print(f"  No DateTime column found in '{table_name}'. Cannot set up incremental refresh.")
                return

        # Verify column type
        c = t.Columns.Find(date_col)
        if c is None:
            print(f"  Column '{date_col}' not found in '{table_name}'. Skipping.")
            return
        if c.DataType != TOM.DataType.DateTime:
            print(f"  Column '{date_col}' is not DateTime ({c.DataType}). Skipping.")
            return

        # Check if already has refresh policy
        try:
            if t.RefreshPolicy is not None:
                print(f"  Table '{table_name}' already has an incremental refresh policy. Skipping.")
                return
        except Exception:
            pass

        # Dates
        end_date = datetime.now().strftime("%m/%d/%Y")
        start_date = (datetime.now() - timedelta(days=rolling_window_years * 365)).strftime("%m/%d/%Y")

        if scan_only:
            print(f"  Would set up incremental refresh on '{table_name}':")
            print(f"    Column: [{date_col}]")
            print(f"    Rolling window: {rolling_window_years} year(s)")
            print(f"    Incremental refresh: {incremental_days} day(s)")
            print(f"    Only complete days: {only_refresh_complete_days}")
            print(f"    Range: {start_date} - {end_date}")
            return

        import System

        # Inline incremental refresh setup (bypasses tom.add_incremental_refresh_policy
        # which has a bug using p.Expression instead of p.Source.Expression)
        first_partition_expr = None
        for idx, p in enumerate(t.Partitions):
            if p.SourceType != TOM.PartitionSourceType.M:
                print(f"  Partition '{p.Name}' is not M-partition ({p.SourceType}). Skipping table.")
                return
            if idx == 0:
                text = p.Source.Expression.rstrip()
                # Skip if already has the IR filter step
                if '#"Filtered Rows IR"' in text:
                    print(f"  M expression already contains IR filter step. Skipping expression edit.")
                    first_partition_expr = text
                else:
                    # Find the last "in\n  <identifier>" pattern
                    pattern = r"in\s+(\S.*?)$"
                    match = re.search(pattern, text, re.DOTALL)
                    if not match:
                        print(f"  Could not parse M-partition expression for '{table_name}'. Skipping.")
                        return
                    obj = match.group(1).strip()
                    text_before = text[:match.start()].rstrip()
                    if not text_before.endswith(","):
                        text_before += ","
                    new_step = (
                        f'\n    #"Filtered Rows IR" = Table.SelectRows({obj}, '
                        f'each [{date_col}] >= RangeStart and [{date_col}] <= RangeEnd)'
                    )
                    p.Source.Expression = f'{text_before}{new_step}\nin\n    #"Filtered Rows IR"'
                    first_partition_expr = p.Source.Expression

        # Add RangeStart / RangeEnd expressions (skip if they already exist)
        date_fmt = "%m/%d/%Y"
        ds = datetime.strptime(start_date, date_fmt)
        de = datetime.strptime(end_date, date_fmt)
        existing_exprs = {str(e.Name) for e in tom.model.Expressions}
        if "RangeStart" not in existing_exprs:
            tom.add_expression(
                name="RangeStart",
                expression=f'datetime({ds.year}, {ds.month}, {ds.day}, 0, 0, 0) meta [IsParameterQuery=true, Type="DateTime", IsParameterQueryRequired=true]',
            )
        if "RangeEnd" not in existing_exprs:
            tom.add_expression(
                name="RangeEnd",
                expression=f'datetime({de.year}, {de.month}, {de.day}, 0, 0, 0) meta [IsParameterQuery=true, Type="DateTime", IsParameterQueryRequired=true]',
            )

        # Set refresh policy
        rp = TOM.BasicRefreshPolicy()
        rp.IncrementalPeriods = incremental_days
        rp.IncrementalGranularity = System.Enum.Parse(TOM.RefreshGranularityType, "Day")
        rp.RollingWindowPeriods = rolling_window_years
        rp.RollingWindowGranularity = System.Enum.Parse(TOM.RefreshGranularityType, "Year")
        rp.SourceExpression = first_partition_expr or ""
        if only_refresh_complete_days:
            rp.IncrementalPeriodsOffset = -1
        t.RefreshPolicy = rp

        print(f"  \u2713 Incremental refresh configured on '{table_name}'[{date_col}]")
        print(f"    Rolling window: {rolling_window_years} year(s), Refresh: {incremental_days} day(s)")
