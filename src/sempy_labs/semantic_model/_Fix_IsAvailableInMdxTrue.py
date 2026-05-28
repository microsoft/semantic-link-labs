# Fix "Set IsAvailableInMdx to true on necessary columns" — standalone BPA fixer.

from typing import Optional
from uuid import UUID


def fix_isavailable_in_mdx_true(
    dataset: str,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
):
    """
    Sets IsAvailableInMDX to True on key/attribute columns that incorrectly have it False.

    Targets columns that are: (a) used as keys in relationships (To side), or
    (b) used in hierarchies, or (c) marked as IsKey.

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
        # Collect columns that should have IsAvailableInMDX = True
        needed = set()
        for rel in tom.model.Relationships:
            needed.add((str(rel.ToTable.Name), str(rel.ToColumn.Name)))
        for table in tom.model.Tables:
            for h in table.Hierarchies:
                for lvl in h.Levels:
                    needed.add((str(table.Name), str(lvl.Column.Name)))
            for col in table.Columns:
                if col.IsKey:
                    needed.add((str(table.Name), str(col.Name)))

        for table in tom.model.Tables:
            for col in table.Columns:
                if (table.Name, col.Name) in needed and not col.IsAvailableInMDX:
                    if scan_only:
                        print(f"  Would fix: '{table.Name}'[{col.Name}] IsAvailableInMDX → True")
                    else:
                        col.IsAvailableInMDX = True
                        print(f"  Fixed: '{table.Name}'[{col.Name}] IsAvailableInMDX → True")
                    fixed += 1
        if not scan_only and fixed > 0:
            tom.model.SaveChanges()

    action = "Would fix" if scan_only else "Fixed"
    print(f"  {action} {fixed} column(s).")
    return fixed
