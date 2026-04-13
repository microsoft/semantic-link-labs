# Fix Report BPA — Standalone function to scan Report BPA and auto-fix all fixable violations.
# Runs run_report_bpa(), maps violations to standalone report fixer files, executes them.

from uuid import UUID
from typing import Optional
from sempy._utils._log import log
import sempy_labs._icons as icons


# Mapping: Report BPA rule name (lowercased) → (module_path, function_name)
_RULE_TO_FIXER = {
    "remove custom visuals which are not used in the report": (
        "sempy_labs.report._Fix_RemoveUnusedCustomVisuals", "fix_remove_unused_custom_visuals"),
    "avoid setting 'show items with no data' on columns": (
        "sempy_labs.report._Fix_DisableShowItemsNoData", "fix_disable_show_items_no_data"),
    "move report-level measures into the semantic model.": (
        "sempy_labs.report._Fix_MigrateReportLevelMeasures", "fix_migrate_report_level_measures"),
    "avoid tall report pages with vertical scrolling": (
        "sempy_labs.report._Fix_PageSize", "fix_page_size"),
}


@log
def fix_report_bpa(
    report: str | UUID,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
) -> None:
    """
    Scans Report BPA rules and auto-fixes all fixable violations using
    standalone fixer scripts.

    Parameters
    ----------
    report : str | uuid.UUID
        Name or ID of the report.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
    scan_only : bool, default=False
        If True, only reports which fixers would run without applying changes.

    Returns
    -------
    None
    """
    import importlib

    from sempy_labs.report import run_report_bpa
    import IPython.display as _ipd
    _orig_display = _ipd.display
    _ipd.display = lambda *a, **kw: None
    try:
        df = run_report_bpa(report=report, workspace=workspace, return_dataframe=True)
    finally:
        _ipd.display = _orig_display

    if df is None or len(df) == 0:
        print(f"{icons.green_dot} No Report BPA violations found for '{report}'.")
        return

    # Determine which fixers are needed
    violated_rules = set(df["Rule Name"].str.lower().str.strip().unique())
    fixers_to_run = {}
    for rule in violated_rules:
        if rule in _RULE_TO_FIXER:
            mod_path, fn_name = _RULE_TO_FIXER[rule]
            fixers_to_run[rule] = (mod_path, fn_name)

    unfixable = violated_rules - set(fixers_to_run.keys())

    if not fixers_to_run:
        print(f"{icons.info} {len(df)} violation(s) found but none have auto-fixers.")
        for rule in sorted(violated_rules):
            count = len(df[df["Rule Name"].str.lower().str.strip() == rule])
            print(f"  \u2022 {rule} ({count} violation(s)) \u2014 no auto-fixer")
        return

    print(f"{icons.info} {len(df)} violation(s) across {len(violated_rules)} rule(s). "
          f"{len(fixers_to_run)} fixable, {len(unfixable)} informational.")

    if unfixable:
        for rule in sorted(unfixable):
            count = len(df[df["Rule Name"].str.lower().str.strip() == rule])
            print(f"  \u26a0\ufe0f {rule} ({count}) \u2014 no auto-fixer")

    ok = 0
    errors = 0
    for rule, (mod_path, fn_name) in sorted(fixers_to_run.items()):
        count = len(df[df["Rule Name"].str.lower().str.strip() == rule])
        if scan_only:
            print(f"  [SCAN] Would run {fn_name}() for '{rule}' ({count} violation(s))")
            ok += 1
            continue
        try:
            mod = importlib.import_module(mod_path)
            fn = getattr(mod, fn_name)
            print(f"  \u25b6 Running {fn_name}() for '{rule}' ({count} violation(s))\u2026")
            fn(report=report, workspace=workspace, scan_only=False)
            ok += 1
        except Exception as e:
            print(f"  \u274c Error running {fn_name}(): {e}")
            errors += 1

    if scan_only:
        print(f"\n{icons.info} Scan complete: {ok} fixer(s) would run.")
    else:
        summary = f"\n{icons.green_dot} Applied {ok} fixer(s)."
        if errors:
            summary = f"\n{icons.warning} {ok} OK, {errors} error(s)."
        print(summary)
