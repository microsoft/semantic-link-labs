# Fix IsAvailableInMDX — standalone BPA fixer.
# Sets IsAvailableInMDX to False on non-attribute columns.

from typing import Optional
from uuid import UUID


def fix_isavailable_in_mdx(
    dataset: str,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
):
    """
    Sets IsAvailableInMDX to False on columns where it is True.

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
                if getattr(col, "IsAvailableInMDX", False):
                    if scan_only:
                        print(f"  Would fix: '{table.Name}'[{col.Name}] IsAvailableInMDX → False")
                    else:
                        col.IsAvailableInMDX = False
                        print(f"  Fixed: '{table.Name}'[{col.Name}] IsAvailableInMDX → False")
                    fixed += 1
        if not scan_only and fixed > 0:
            tom.model.SaveChanges()

    action = "Would fix" if scan_only else "Fixed"
    print(f"  {action} {fixed} column(s).")
    return fixed
