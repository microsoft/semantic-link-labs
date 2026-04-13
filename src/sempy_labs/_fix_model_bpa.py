# Fix Model BPA — Standalone function to scan Model BPA and auto-fix all fixable violations.
# Runs run_model_bpa(), maps violations to standalone SM fixer files, executes them.

from uuid import UUID
from typing import Optional
from sempy._utils._log import log
import sempy_labs._icons as icons


# Mapping: BPA rule name (lowercased) → (module_path, function_name)
_RULE_TO_FIXER = {
    "do not use floating point data types": ("sempy_labs.semantic_model._Fix_FloatingPointDataType", "fix_floating_point_datatype"),
    "do not use floating point data type": ("sempy_labs.semantic_model._Fix_FloatingPointDataType", "fix_floating_point_datatype"),
    "set isavailableinmdx to false on non-attribute columns": ("sempy_labs.semantic_model._Fix_IsAvailableInMdx", "fix_isavailable_in_mdx"),
    "set isavailableinmdx to true on necessary columns": ("sempy_labs.semantic_model._Fix_IsAvailableInMdxTrue", "fix_isavailable_in_mdx_true"),
    "provide format string for 'date' columns": ("sempy_labs.semantic_model._Fix_DateColumnFormat", "fix_date_column_format"),
    "provide format string for 'month' columns": ("sempy_labs.semantic_model._Fix_MonthColumnFormat", "fix_month_column_format"),
    "provide format string for measures": ("sempy_labs.semantic_model._Fix_MeasureFormat", "fix_measure_format"),
    "hide foreign keys": ("sempy_labs.semantic_model._Fix_HideForeignKeys", "fix_hide_foreign_keys"),
    "objects should not start or end with a space": ("sempy_labs.semantic_model._Fix_TrimObjectNames", "fix_trim_object_names"),
    "first letter of objects must be capitalized": ("sempy_labs.semantic_model._Fix_CapitalizeObjectNames", "fix_capitalize_object_names"),
    "do not summarize numeric columns": ("sempy_labs.semantic_model._Fix_DoNotSummarize", "fix_do_not_summarize"),
    "mark primary keys": ("sempy_labs.semantic_model._Fix_MarkPrimaryKeys", "fix_mark_primary_keys"),
    "percentages should be formatted with thousands separators and 1 decimal": ("sempy_labs.semantic_model._Fix_PercentageFormat", "fix_percentage_format"),
    "format flag columns as yes/no value strings": ("sempy_labs.semantic_model._Fix_FlagColumnFormat", "fix_flag_column_format"),
    "visible objects with no description": ("sempy_labs.semantic_model._Fix_MeasureDescriptions", "fix_measure_descriptions"),
    "avoid adding 0 to a measure": ("sempy_labs.semantic_model._Fix_AvoidAdding0", "fix_avoid_adding_zero"),
    "use the divide function for division": ("sempy_labs.semantic_model._Fix_UseDivideFunction", "fix_use_divide_function"),
    "whole numbers should be formatted with thousands separators": ("sempy_labs.semantic_model._Fix_WholeNumberFormat", "fix_whole_number_format"),
    "month (as a string) must be sorted": ("sempy_labs.semantic_model._Fix_SortMonthColumn", "fix_sort_month_column"),
    "add data category for columns": ("sempy_labs.semantic_model._Fix_DataCategory", "fix_data_category"),
}


@log
def fix_model_bpa(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
) -> None:
    """
    Scans Model BPA rules and auto-fixes all fixable violations using
    standalone fixer scripts. Each fixer runs in bulk (entire model),
    not per-violation.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
    scan_only : bool, default=False
        If True, only reports which fixers would run without applying changes.

    Returns
    -------
    None
    """
    import importlib

    from sempy_labs import run_model_bpa
    import IPython.display as _ipd
    _orig_display = _ipd.display
    _ipd.display = lambda *a, **kw: None
    try:
        df = run_model_bpa(dataset=dataset, workspace=workspace, return_dataframe=True)
    finally:
        _ipd.display = _orig_display

    if df is None or len(df) == 0:
        print(f"{icons.green_dot} No Model BPA violations found for '{dataset}'.")
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
            print(f"  \u2022 {rule} ({count} violation(s)) — no auto-fixer")
        return

    print(f"{icons.info} {len(df)} violation(s) across {len(violated_rules)} rule(s). "
          f"{len(fixers_to_run)} fixable, {len(unfixable)} informational.")

    if unfixable:
        for rule in sorted(unfixable):
            count = len(df[df["Rule Name"].str.lower().str.strip() == rule])
            print(f"  \u26a0\ufe0f {rule} ({count}) — no auto-fixer")

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
            fn(dataset=dataset, workspace=workspace, scan_only=False)
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
