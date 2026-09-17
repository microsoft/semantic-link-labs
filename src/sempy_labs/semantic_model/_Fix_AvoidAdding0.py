# Fix "Avoid adding 0 to a measure" — standalone BPA fixer.
# Strips "0+" or "0 +" prefix from measure expressions.

from typing import Optional
from uuid import UUID
import re


def fix_avoid_adding_zero(
    dataset: str,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
):
    """
    Removes '0+' or '0 +' prefix from measure expressions.

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
                expr = str(m.Expression) if m.Expression else ""
                stripped = expr.replace(" ", "")
                if stripped.startswith("0+"):
                    new_expr = re.sub(r'^\s*0\s*\+\s*', '', expr)
                    if new_expr != expr:
                        if scan_only:
                            print(f"  Would fix: [{m.Name}] — remove '0+' prefix")
                        else:
                            m.Expression = new_expr
                            print(f"  Fixed: [{m.Name}] — removed '0+' prefix")
                        fixed += 1
        if not scan_only and fixed > 0:
            tom.model.SaveChanges()

    action = "Would fix" if scan_only else "Fixed"
    print(f"  {action} {fixed} measure(s).")
    return fixed
