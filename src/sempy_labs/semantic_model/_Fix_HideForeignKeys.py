# Fix hidden foreign keys — standalone BPA fixer.
# Hides columns that are used as foreign keys in relationships.

from typing import Optional
from uuid import UUID


def fix_hide_foreign_keys(
    dataset: str,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
):
    """
    Hides columns that participate as foreign keys (From side) in relationships.

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
        # Collect foreign key columns (From side of relationships)
        fk_cols = set()
        for rel in tom.model.Relationships:
            fk_cols.add((str(rel.FromTable.Name), str(rel.FromColumn.Name)))

        for table in tom.model.Tables:
            for col in table.Columns:
                if (table.Name, col.Name) in fk_cols and not col.IsHidden:
                    if scan_only:
                        print(f"  Would hide: '{table.Name}'[{col.Name}]")
                    else:
                        col.IsHidden = True
                        print(f"  Hidden: '{table.Name}'[{col.Name}]")
                    fixed += 1
        if not scan_only and fixed > 0:
            tom.model.SaveChanges()

    action = "Would hide" if scan_only else "Hidden"
    print(f"  {action} {fixed} foreign key column(s).")
    return fixed
