# Fix "Whole numbers should be formatted with thousands separators" — standalone BPA fixer.

from typing import Optional
from uuid import UUID


def fix_whole_number_format(
    dataset: str,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
):
    """
    Sets format string to #,0 on integer-typed measures without a format string.

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
            for m in table.Measures:
                fmt = str(m.FormatString) if m.FormatString else ""
                if fmt.strip():
                    continue  # already has a format
                # Check if the measure likely returns an integer
                # (heuristic: name doesn't suggest % or ratio)
                name_lower = m.Name.lower()
                if any(k in name_lower for k in ("%", "percent", "pct", "ratio", "rate")):
                    continue
                if scan_only:
                    print(f"  Would fix: [{m.Name}] → #,0")
                else:
                    m.FormatString = "#,0"
                    print(f"  Fixed: [{m.Name}] → #,0")
                fixed += 1
        if not scan_only and fixed > 0:
            tom.model.SaveChanges()

    action = "Would fix" if scan_only else "Fixed"
    print(f"  {action} {fixed} measure(s).")
    return fixed
