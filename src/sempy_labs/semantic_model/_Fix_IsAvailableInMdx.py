# Fix IsAvailableInMDX — standalone BPA fixer.
# Sets IsAvailableInMDX to False on non-attribute columns.

from typing import Optional
from uuid import UUID
from sempy._utils._log import log


@log
def fix_isavailable_in_mdx(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
) -> int:
    """
    Sets IsAvailableInMDX to False on columns where it is True.

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
        for table in tom.model.Tables:
            for col in table.Columns:
                if getattr(col, "IsAvailableInMDX", False):
                    if scan_only:
                        print(f"  Would fix: '{table.Name}'[{col.Name}] IsAvailableInMDX → False")
                    else:
                        col.IsAvailableInMDX = False
                        print(f"  Fixed: '{table.Name}'[{col.Name}] IsAvailableInMDX → False")
                    fixed += 1

    action = "Would fix" if scan_only else "Fixed"
    print(f"  {action} {fixed} column(s).")
    return fixed
