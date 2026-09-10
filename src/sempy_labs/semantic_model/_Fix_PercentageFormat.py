# Fix "Percentages should be formatted with thousands separators" — standalone BPA fixer.

from typing import Optional
from uuid import UUID
import re


def fix_percentage_format(
    dataset: str,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
):
    """
    Sets format string on measures whose name contains %, Percent, or Pct.

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

    _PCT_FMT = "#,0.0%;-#,0.0%;#,0.0%"

    fixed = 0
    with connect_semantic_model(dataset=dataset, readonly=scan_only, workspace=workspace) as tom:
        for table in tom.model.Tables:
            for m in table.Measures:
                name_lower = m.Name.lower()
                if not any(k in name_lower for k in ("%", "percent", "pct")):
                    continue
                current_fmt = str(m.FormatString) if m.FormatString else ""
                if "%" in current_fmt:
                    continue  # already has a percentage format
                if scan_only:
                    print(f"  Would fix: [{m.Name}] → {_PCT_FMT}")
                else:
                    m.FormatString = _PCT_FMT
                    print(f"  Fixed: [{m.Name}] → {_PCT_FMT}")
                fixed += 1
        if not scan_only and fixed > 0:
            tom.model.SaveChanges()

    action = "Would fix" if scan_only else "Fixed"
    print(f"  {action} {fixed} percentage measure(s).")
    return fixed
