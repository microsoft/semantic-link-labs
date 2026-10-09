# Fix month column format — standalone BPA fixer.
# Sets format string for columns named 'Month' to MMMM yyyy.

from typing import Optional
from uuid import UUID


def fix_month_column_format(
    dataset: str,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
):
    """
    Sets the format string of 'Month' columns to MMMM yyyy.

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
        for table in tom.model.Tables:
            for col in table.Columns:
                name_lower = col.Name.lower()
                if name_lower == "month" and (not col.FormatString or str(col.FormatString).strip() == ""):
                    if scan_only:
                        print(f"  Would fix: '{table.Name}'[{col.Name}] → MMMM yyyy")
                    else:
                        col.FormatString = "MMMM yyyy"
                        print(f"  Fixed: '{table.Name}'[{col.Name}] → MMMM yyyy")
                    fixed += 1
        if not scan_only and fixed > 0:
            tom.model.SaveChanges()

    action = "Would fix" if scan_only else "Fixed"
    print(f"  {action} {fixed} month column(s).")
    return fixed
