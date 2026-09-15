# Fix "Whole numbers should be formatted with thousands separators" — standalone BPA fixer.

from typing import Optional
from uuid import UUID


def fix_whole_number_format(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
) -> int:
    """
    Sets format string to #,0 on integer-typed measures (DataType=Int64) that
    have neither a static FormatString nor a dynamic FormatStringDefinition.

    Parameters
    ----------
    dataset : str | UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
    scan_only : bool, default=False
        If True, only reports what would be fixed without making changes.

    Returns
    -------
    int
        Number of items fixed.
    """
    from sempy_labs.tom import connect_semantic_model

    fixed = 0
    with connect_semantic_model(dataset=dataset, readonly=scan_only, workspace=workspace) as tom:
        import Microsoft.AnalysisServices.Tabular as TOM

        for table in tom.model.Tables:
            for m in table.Measures:
                # Only target measures that actually return an integer
                if m.DataType != TOM.DataType.Int64:
                    continue
                # Skip measures with a dynamic format string
                if m.FormatStringDefinition is not None:
                    continue
                # Skip measures that already have a static format string
                fmt = str(m.FormatString) if m.FormatString else ""
                if fmt.strip():
                    continue
                if scan_only:
                    print(f"  Would fix: [{m.Name}] → #,0")
                else:
                    m.FormatString = "#,0"
                    print(f"  Fixed: [{m.Name}] → #,0")
                fixed += 1
        # Context manager (TOMWrapper.close) persists changes; no SaveChanges() needed.

    action = "Would fix" if scan_only else "Fixed"
    print(f"  {action} {fixed} measure(s).")
    return fixed
