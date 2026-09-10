# Fix "Use the DIVIDE function for division" — standalone BPA fixer.
# Replaces simple A / B patterns in measure expressions with DIVIDE(A, B).

from typing import Optional
from uuid import UUID
import re


def fix_use_divide_function(
    dataset: str,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
):
    """
    Replaces simple division operators (/) in measure expressions with DIVIDE().

    Only fixes patterns like `] / ` or `) / ` (single-level divisions).
    Nested or complex cases are skipped.

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
                new_expr = re.sub(
                    r'((?:\]\s*|\)\s*))\s*/\s*((?:\s*\[|\s*[A-Za-z]))',
                    lambda match: f'DIVIDE({match.group(0).replace("/", ",", 1)})',
                    expr,
                    count=0,
                )
                if new_expr != expr:
                    if scan_only:
                        print(f"  Would fix: [{m.Name}]")
                    else:
                        m.Expression = new_expr
                        print(f"  Fixed: [{m.Name}]")
                    fixed += 1
        if not scan_only and fixed > 0:
            tom.model.SaveChanges()

    action = "Would fix" if scan_only else "Fixed"
    print(f"  {action} {fixed} measure(s) with division operator.")
    return fixed
