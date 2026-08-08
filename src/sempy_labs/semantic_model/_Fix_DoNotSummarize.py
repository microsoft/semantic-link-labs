# Fix "Do not summarize numeric columns" — standalone BPA fixer.
# Sets SummarizeBy to None on all numeric data columns.

from typing import Optional
from uuid import UUID


def fix_do_not_summarize(
    dataset: str,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
):
    """
    Sets SummarizeBy to None on numeric data columns.

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
            for col in table.Columns:
                if col.Type != TOM.ColumnType.Data:
                    continue
                if col.DataType not in (TOM.DataType.Int64, TOM.DataType.Double, TOM.DataType.Decimal):
                    continue
                if str(col.SummarizeBy) == "None":
                    continue
                if scan_only:
                    print(f"  Would fix: '{table.Name}'[{col.Name}] SummarizeBy={col.SummarizeBy} → None")
                else:
                    col.SummarizeBy = TOM.AggregateFunction.Default  # None
                    try:
                        col.SummarizeBy = getattr(TOM.AggregateFunction, "None_", TOM.AggregateFunction.Default)
                    except Exception:
                        pass
                    print(f"  Fixed: '{table.Name}'[{col.Name}] → SummarizeBy=None")
                fixed += 1
        if not scan_only and fixed > 0:
            tom.model.SaveChanges()

    action = "Would fix" if scan_only else "Fixed"
    print(f"  {action} {fixed} column(s).")
    return fixed
