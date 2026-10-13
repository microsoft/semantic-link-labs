# Fix "Format flag columns as Yes/No value strings" — standalone BPA fixer.

from typing import Optional
from uuid import UUID


def fix_flag_column_format(
    dataset: str,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
):
    """
    Sets a Yes/No format on integer flag columns (names starting with 'Is' or ending with 'Flag').

    Uses the DAX format string '"Yes";"Yes";"No"' pattern.

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

    _FLAG_FMT = '"Yes";"Yes";"No"'

    fixed = 0
    with connect_semantic_model(dataset=dataset, readonly=scan_only, workspace=workspace) as tom:
        import Microsoft.AnalysisServices.Tabular as TOM

        for table in tom.model.Tables:
            for col in table.Columns:
                if col.IsHidden or table.IsHidden:
                    continue
                if col.DataType != TOM.DataType.Int64:
                    continue
                name = col.Name
                if not (name.lower().startswith("is") or name.lower().endswith("flag") or name.lower().endswith(" flag")):
                    continue
                current_fmt = str(col.FormatString) if col.FormatString else ""
                if current_fmt.strip():
                    continue
                if scan_only:
                    print(f"  Would fix: '{table.Name}'[{col.Name}] → Yes/No format")
                else:
                    col.FormatString = _FLAG_FMT
                    print(f"  Fixed: '{table.Name}'[{col.Name}] → Yes/No format")
                fixed += 1
        if not scan_only and fixed > 0:
            tom.model.SaveChanges()

    action = "Would fix" if scan_only else "Fixed"
    print(f"  {action} {fixed} flag column(s).")
    return fixed
