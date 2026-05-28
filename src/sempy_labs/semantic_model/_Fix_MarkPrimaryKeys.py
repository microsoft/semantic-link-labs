# Fix "Mark primary keys" — standalone BPA fixer.
# Sets IsKey=True on the To-side column of relationships.

from typing import Optional
from uuid import UUID


def fix_mark_primary_keys(
    dataset: str,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
):
    """
    Sets IsKey=True on columns that are the primary key side (To) of relationships.

    Only applies when the column is not already a key and the table has no existing key.

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
        for rel in tom.model.Relationships:
            to_col = rel.ToColumn
            to_table = rel.ToTable
            if to_col.IsKey:
                continue
            # Check if table already has a key column
            has_key = any(c.IsKey for c in to_table.Columns)
            if has_key:
                continue
            if scan_only:
                print(f"  Would mark: '{to_table.Name}'[{to_col.Name}] as primary key")
            else:
                to_col.IsKey = True
                print(f"  Marked: '{to_table.Name}'[{to_col.Name}] as primary key")
            fixed += 1
        if not scan_only and fixed > 0:
            tom.model.SaveChanges()

    action = "Would mark" if scan_only else "Marked"
    print(f"  {action} {fixed} primary key column(s).")
    return fixed
