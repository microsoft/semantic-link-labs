# Fix "First letter of objects must be capitalized" — standalone BPA fixer.

from typing import Optional
from uuid import UUID


def fix_capitalize_object_names(
    dataset: str,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
):
    """
    Capitalizes the first letter of table, column, measure, hierarchy, and partition names.

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

    def _cap(name):
        return name[0].upper() + name[1:] if name and name[0] != name[0].upper() else None

    fixed = 0
    with connect_semantic_model(dataset=dataset, readonly=scan_only, workspace=workspace) as tom:
        for table in tom.model.Tables:
            new_name = _cap(table.Name)
            if new_name:
                if scan_only:
                    print(f"  Would capitalize table: '{table.Name}' → '{new_name}'")
                else:
                    table.Name = new_name
                    print(f"  Capitalized table: '{new_name}'")
                fixed += 1
            for col in table.Columns:
                new_name = _cap(col.Name)
                if new_name:
                    if scan_only:
                        print(f"  Would capitalize column: '{table.Name}'[{col.Name}]")
                    else:
                        col.Name = new_name
                        print(f"  Capitalized column: '{table.Name}'[{new_name}]")
                    fixed += 1
            for m in table.Measures:
                new_name = _cap(m.Name)
                if new_name:
                    if scan_only:
                        print(f"  Would capitalize measure: [{m.Name}]")
                    else:
                        m.Name = new_name
                        print(f"  Capitalized measure: [{new_name}]")
                    fixed += 1
            for h in table.Hierarchies:
                new_name = _cap(h.Name)
                if new_name:
                    if scan_only:
                        print(f"  Would capitalize hierarchy: {h.Name}")
                    else:
                        h.Name = new_name
                        print(f"  Capitalized hierarchy: {new_name}")
                    fixed += 1
        if not scan_only and fixed > 0:
            tom.model.SaveChanges()

    action = "Would capitalize" if scan_only else "Capitalized"
    print(f"  {action} {fixed} object name(s).")
    return fixed
