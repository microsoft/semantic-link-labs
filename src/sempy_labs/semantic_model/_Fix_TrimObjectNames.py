# Fix "Objects should not start or end with a space" — standalone BPA fixer.
# Trims leading/trailing whitespace from object names.

from typing import Optional
from uuid import UUID


def fix_trim_object_names(
    dataset: str,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
):
    """
    Trims leading/trailing whitespace from table, column, measure, hierarchy, and partition names.

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
            if table.Name != table.Name.strip():
                if scan_only:
                    print(f"  Would trim table: '{table.Name}'")
                else:
                    table.Name = table.Name.strip()
                    print(f"  Trimmed table: '{table.Name}'")
                fixed += 1
            for col in table.Columns:
                if col.Name != col.Name.strip():
                    if scan_only:
                        print(f"  Would trim column: '{table.Name}'[{col.Name}]")
                    else:
                        col.Name = col.Name.strip()
                        print(f"  Trimmed column: '{table.Name}'[{col.Name}]")
                    fixed += 1
            for m in table.Measures:
                if m.Name != m.Name.strip():
                    if scan_only:
                        print(f"  Would trim measure: [{m.Name}]")
                    else:
                        m.Name = m.Name.strip()
                        print(f"  Trimmed measure: [{m.Name}]")
                    fixed += 1
            for h in table.Hierarchies:
                if h.Name != h.Name.strip():
                    if scan_only:
                        print(f"  Would trim hierarchy: {h.Name}")
                    else:
                        h.Name = h.Name.strip()
                        print(f"  Trimmed hierarchy: {h.Name}")
                    fixed += 1
            try:
                for p in table.Partitions:
                    if p.Name != p.Name.strip():
                        if scan_only:
                            print(f"  Would trim partition: {p.Name}")
                        else:
                            p.Name = p.Name.strip()
                            print(f"  Trimmed partition: {p.Name}")
                        fixed += 1
            except Exception:
                pass
        if not scan_only and fixed > 0:
            tom.model.SaveChanges()

    action = "Would trim" if scan_only else "Trimmed"
    print(f"  {action} {fixed} object name(s).")
    return fixed
