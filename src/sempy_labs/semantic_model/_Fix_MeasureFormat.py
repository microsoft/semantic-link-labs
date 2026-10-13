# Fix measure format — standalone BPA fixer.
# Sets format string for measures without a format to #,0.

from typing import Optional
from uuid import UUID


def fix_measure_format(
    dataset: str,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
):
    """
    Sets the format string of measures that have no format string to #,0.

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
            for m in table.Measures:
                fmt = str(m.FormatString) if m.FormatString else ""
                if not fmt.strip():
                    if scan_only:
                        print(f"  Would fix: [{m.Name}] → #,0")
                    else:
                        m.FormatString = "#,0"
                        print(f"  Fixed: [{m.Name}] → #,0")
                    fixed += 1
        if not scan_only and fixed > 0:
            tom.model.SaveChanges()

    action = "Would fix" if scan_only else "Fixed"
    print(f"  {action} {fixed} measure format(s).")
    return fixed
