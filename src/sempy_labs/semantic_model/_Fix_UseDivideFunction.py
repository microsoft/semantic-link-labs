# Fix "Use the DIVIDE function for division" — standalone BPA fixer.
# Replaces simple A / B patterns in measure expressions with DIVIDE(A, B).

from typing import Optional
from uuid import UUID
import re
from sempy._utils._log import log


@log
def fix_use_divide_function(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
) -> int:
    """
    Replaces simple division operators (/) in measure expressions with DIVIDE().

    Only fixes patterns like `] / ` or `) / ` (single-level divisions).
    Nested or complex cases are skipped.

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

    # Pattern: captures `<expr>] / <expr>` or `<expr>) / <expr>`
    # We match: (something ending in ] or )) followed by whitespace / whitespace (something)
    _div_pattern = re.compile(
        r'(\[[^\]]+\]|\([^()]*\))\s*/\s*(\[[^\]]+\]|\([^()]*\))',
        re.IGNORECASE,
    )

    fixed = 0
    with connect_semantic_model(dataset=dataset, readonly=scan_only, workspace=workspace) as tom:
        for table in tom.model.Tables:
            for m in table.Measures:
                expr = str(m.Expression) if m.Expression else ""
                if "/" not in expr:
                    continue
                # Skip if already using DIVIDE predominantly
                if expr.upper().count("DIVIDE") > expr.count("/"):
                    continue
                # Simple replacement: ] / [ or ] / ( or ) / [
                new_expr = re.sub(r"\[([^\]]+)\]\s*/\s*\[([^\]]+)\]", r"DIVIDE([\1], [\2])", expr,
                    count=0,
                )
                if new_expr != expr:
                    if scan_only:
                        print(f"  Would fix: [{m.Name}]")
                    else:
                        m.Expression = new_expr
                        print(f"  Fixed: [{m.Name}]")
                    fixed += 1

    action = "Would fix" if scan_only else "Fixed"
    print(f"  {action} {fixed} measure(s) with division operator.")
    return fixed
