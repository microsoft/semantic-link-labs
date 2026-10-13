# Fix date column format — standalone BPA fixer.
# Sets format string for columns named 'Date' to mm/dd/yyyy.

from typing import Optional
from uuid import UUID


def fix_date_column_format(
    dataset: str,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
):
    """
    Sets the format string of 'Date' columns to mm/dd/yyyy.

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
                if name_lower == "date" and (not col.FormatString or str(col.FormatString).strip() == ""):
                    if scan_only:
                        print(f"  Would fix: '{table.Name}'[{col.Name}] → mm/dd/yyyy")
                    else:
                        col.FormatString = "mm/dd/yyyy"
                        print(f"  Fixed: '{table.Name}'[{col.Name}] → mm/dd/yyyy")
                    fixed += 1
        if not scan_only and fixed > 0:
            tom.model.SaveChanges()

    action = "Would fix" if scan_only else "Fixed"
    print(f"  {action} {fixed} date column(s).")
    return fixed
