import re
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import pandas as pd

from sempy_labs._helper_functions import (
    format_dax_object_name,
    create_relationship_name,
)

# Rule names which support an automatic, programmable fix.
FIXABLE_RULES = {
    "Column references should be fully qualified",
    "Measure references should be unqualified",
    "Set IsAvailableInMdx to false on non-attribute columns",
    "Do not summarize numeric columns",
    "Do not use floating point data types",
    "Hide foreign keys",
    "Mark primary keys",
    "First letter of objects must be capitalized",
    "Partition name should match table name for single partition tables",
    "Remove auto-date table",
    "Remove unnecessary columns",
    "Remove unnecessary measures",
}

# Presentation order for severities (most severe first).
SEVERITY_ORDER = {"Error": 0, "Warning": 1, "Info": 2}


def _rule_id(rule_name: str) -> str:
    """
    Converts a rule name into a stable identifier (a slug) so that rule state such
    as enabled/disabled can be persisted independently of display formatting.
    """

    slug = re.sub(r"[^a-z0-9]+", "_", str(rule_name).strip().lower())
    return slug.strip("_")


def _scope_objects(tom, scope: str) -> Tuple[Iterable, Callable[[Any], str]]:
    """
    Returns the model objects in a rule scope together with a function producing
    each object's display name.
    """

    scope_map: Dict[str, Tuple[Any, Callable[[Any], str]]] = {
        "Relationship": (
            tom.model.Relationships,
            lambda obj: create_relationship_name(
                obj.FromTable.Name,
                obj.FromColumn.Name,
                obj.ToTable.Name,
                obj.ToColumn.Name,
            ),
        ),
        "Column": (
            tom.all_columns(),
            lambda obj: format_dax_object_name(obj.Parent.Name, obj.Name),
        ),
        "Calculated Column": (
            tom.all_calculated_columns(),
            lambda obj: format_dax_object_name(obj.Parent.Name, obj.Name),
        ),
        "Measure": (tom.all_measures(), lambda obj: obj.Name),
        "Hierarchy": (
            tom.all_hierarchies(),
            lambda obj: format_dax_object_name(obj.Parent.Name, obj.Name),
        ),
        "Table": (tom.model.Tables, lambda obj: obj.Name),
        "Calculated Table": (tom.all_calculated_tables(), lambda obj: obj.Name),
        "Role": (tom.model.Roles, lambda obj: obj.Name),
        "Model": ([tom.model], lambda obj: obj.Model.Name),
        "Calculation Item": (
            tom.all_calculation_items(),
            lambda obj: format_dax_object_name(obj.Parent.Table.Name, obj.Name),
        ),
        "Row Level Security": (
            tom.all_rls(),
            lambda obj: format_dax_object_name(obj.Parent.Name, obj.Name),
        ),
        "Partition": (
            tom.all_partitions(),
            lambda obj: format_dax_object_name(obj.Parent.Name, obj.Name),
        ),
        "Function": (tom.all_functions(), lambda obj: obj.Name),
    }

    return scope_map.get(scope, ([], lambda obj: str(obj)))


def rules_payload(rules: pd.DataFrame) -> List[dict]:
    """
    Converts a BPA rules dataframe into a JSON-serializable descriptor list for the
    interactive user interface.

    Parameters
    ----------
    rules : pandas.DataFrame
        A rules dataframe in the shape produced by
        :func:`sempy_labs.model_bpa_rules`.

    Returns
    -------
    List[dict]
        One descriptor per rule containing its id, name, category, severity, scopes,
        description, url and whether an automatic fix is available.
    """

    payload = []
    for _, r in rules.iterrows():
        scopes = r["Scope"]
        if isinstance(scopes, str):
            scopes = [scopes]
        rule_name = str(r["Rule Name"])
        url = r.get("URL")
        payload.append(
            {
                "id": _rule_id(rule_name),
                "name": rule_name,
                "category": str(r["Category"]),
                "severity": str(r["Severity"]),
                "scopes": list(scopes),
                "description": (
                    "" if pd.isna(r["Description"]) else str(r["Description"])
                ),
                "url": None if url is None or pd.isna(url) else str(url),
                "fixable": rule_name in FIXABLE_RULES,
            }
        )

    payload.sort(key=lambda x: (x["category"].lower(), x["name"].lower()))
    return payload


def scan_model(
    tom,
    rules: pd.DataFrame,
    disabled_rule_ids: Optional[Iterable[str]] = None,
) -> List[dict]:
    """
    Evaluates the best practice rules against a connected semantic model.

    Parameters
    ----------
    tom : TOMWrapper
        A connected semantic model, as produced by
        :func:`sempy_labs.tom.connect_semantic_model`.
    rules : pandas.DataFrame
        A rules dataframe in the shape produced by
        :func:`sempy_labs.model_bpa_rules`.
    disabled_rule_ids : Iterable[str], default=None
        Rule ids (see :func:`rules_payload`) which should be skipped.

    Returns
    -------
    List[dict]
        The violations, each containing the category, rule name, severity, object
        type, object name, description and url.
    """

    disabled = set(disabled_rule_ids or [])
    violations: List[dict] = []

    if tom.model.Tables.Count == 0:
        return violations

    for _, r in rules.iterrows():
        rule_name = str(r["Rule Name"])
        if _rule_id(rule_name) in disabled:
            continue

        expression = r["Expression"]
        scopes = r["Scope"]
        if isinstance(scopes, str):
            scopes = [scopes]
        url = r.get("URL")
        description = "" if pd.isna(r["Description"]) else str(r["Description"])

        for scope in scopes:
            objects, name_of = _scope_objects(tom, scope)
            for obj in objects:
                try:
                    hit = bool(expression(obj, tom))
                except Exception:
                    # A malformed object or rule must never abort the scan.
                    continue
                if not hit:
                    continue
                violations.append(
                    {
                        "category": str(r["Category"]),
                        "ruleName": rule_name,
                        "ruleId": _rule_id(rule_name),
                        "severity": str(r["Severity"]),
                        "objectType": scope,
                        "objectName": name_of(obj),
                        "description": description,
                        "url": None if url is None or pd.isna(url) else str(url),
                        "fixable": rule_name in FIXABLE_RULES,
                    }
                )

    violations.sort(
        key=lambda v: (
            v["category"].lower(),
            SEVERITY_ORDER.get(v["severity"], 9),
            v["ruleName"].lower(),
            v["objectType"].lower(),
            v["objectName"].lower(),
        )
    )

    return violations


def _violating_objects(tom, rules: pd.DataFrame, rule_name: str):
    """
    Re-evaluates a single rule and yields ``(scope, object, display_name)`` for every
    object it flags, so that a fix targets exactly the objects reported by the scan.
    """

    matched = rules[rules["Rule Name"] == rule_name]
    if matched.empty:
        return

    row = matched.iloc[0]
    expression = row["Expression"]
    scopes = row["Scope"]
    if isinstance(scopes, str):
        scopes = [scopes]

    for scope in scopes:
        objects, name_of = _scope_objects(tom, scope)
        for obj in list(objects):
            try:
                hit = bool(expression(obj, tom))
            except Exception:
                continue
            if hit:
                yield scope, obj, name_of(obj)


def _dax_objects(tom):
    """
    Yields ``(object_type, display_name, get_expression, set_expression)`` for every
    DAX-bearing object in the model.
    """

    for m in tom.all_measures():
        yield (
            "Measure",
            m.Name,
            (lambda measure=m: measure.Expression or ""),
            (lambda value, measure=m: setattr(measure, "Expression", value)),
        )

    for c in tom.all_calculated_columns():
        yield (
            "Calculated Column",
            format_dax_object_name(c.Parent.Name, c.Name),
            (lambda column=c: column.Expression or ""),
            (lambda value, column=c: setattr(column, "Expression", value)),
        )

    for ci in tom.all_calculation_items():
        yield (
            "Calculation Item",
            format_dax_object_name(ci.Parent.Table.Name, ci.Name),
            (lambda item=ci: item.Expression or ""),
            (lambda value, item=ci: setattr(item, "Expression", value)),
        )

    for t in tom.all_calculated_tables():
        partitions = list(t.Partitions)
        if not partitions:
            continue
        source = partitions[0].Source
        if not hasattr(source, "Expression"):
            continue
        yield (
            "Calculated Table",
            t.Name,
            (lambda src=source: src.Expression or ""),
            (lambda value, src=source: setattr(src, "Expression", value)),
        )


def _dax_transform_fixes(tom, transform: Callable[[str], str]) -> List[dict]:
    """Applies an expression transform to every DAX object and returns the changes."""

    fixes = []
    for object_type, name, get_expression, set_expression in _dax_objects(tom):
        before = get_expression()
        after = transform(before)
        if before == after:
            continue
        fixes.append(
            {
                "objectType": object_type,
                "objectName": name,
                "before": before,
                "after": after,
                "_apply": (lambda setter=set_expression, value=after: setter(value)),
            }
        )

    return fixes


def _qualify_column_references(tom) -> List[dict]:
    """Fully qualifies unambiguous column references, e.g. ``[Col]`` -> ``'Table'[Col]``."""

    measure_names = {m.Name for m in tom.all_measures()}
    by_name: Dict[str, str] = {}
    ambiguous = set()
    for c in tom.all_columns():
        if c.Name in measure_names:
            continue
        existing = by_name.get(c.Name)
        if existing is None:
            by_name[c.Name] = c.Parent.Name
        elif existing != c.Parent.Name:
            ambiguous.add(c.Name)

    for name in ambiguous:
        by_name.pop(name, None)

    def transform(expression: str) -> str:
        if not expression:
            return expression
        result = expression
        for column_name, table_name in by_name.items():
            # [Name] not already preceded by a table reference (quote, word char or ]).
            pattern = r"(?<![\w'\]])\[" + re.escape(column_name) + r"\]"
            replacement = f"'{table_name}'[{column_name}]"
            result = re.sub(pattern, lambda _m, r=replacement: r, result)
        return result

    return _dax_transform_fixes(tom, transform)


def _unqualify_measure_references(tom) -> List[dict]:
    """Removes the table qualifier from measure references, e.g. ``'Table'[M]`` -> ``[M]``."""

    column_names = {c.Name for c in tom.all_columns()}
    measure_names = [m.Name for m in tom.all_measures() if m.Name not in column_names]

    def transform(expression: str) -> str:
        if not expression:
            return expression
        result = expression
        for measure_name in measure_names:
            pattern = r"(?:'[^']+'|\w+)\[" + re.escape(measure_name) + r"\]"
            replacement = f"[{measure_name}]"
            result = re.sub(pattern, lambda _m, r=replacement: r, result)
        return result

    return _dax_transform_fixes(tom, transform)


def _column_property_fixes(
    tom,
    rules: pd.DataFrame,
    rule_name: str,
    describe: Callable[[Any], Tuple[str, str]],
    apply: Callable[[Any], None],
) -> List[dict]:
    """Builds a property-assignment fix for every column flagged by a rule."""

    fixes = []
    for _scope, column, display_name in _violating_objects(tom, rules, rule_name):
        before, after = describe(column)
        fixes.append(
            {
                "objectType": "Column",
                "objectName": display_name,
                "before": before,
                "after": after,
                "_apply": (lambda target=column: apply(target)),
            }
        )

    return fixes


def _capitalize_fixes(tom, rules: pd.DataFrame, rule_name: str) -> List[dict]:
    """Capitalizes the first letter of every object flagged by a rule."""

    fixes = []
    for scope, obj, display_name in _violating_objects(tom, rules, rule_name):
        before = obj.Name
        if not before:
            continue
        after = before[0].upper() + before[1:]
        if before == after:
            continue
        fixes.append(
            {
                "objectType": scope,
                "objectName": display_name,
                "before": before,
                "after": after,
                "_apply": (
                    lambda target=obj, value=after: setattr(target, "Name", value)
                ),
            }
        )

    return fixes


def _partition_name_fixes(tom, rules: pd.DataFrame, rule_name: str) -> List[dict]:
    """Renames single-table partitions so their name matches the table name."""

    fixes = []
    for _scope, partition, display_name in _violating_objects(tom, rules, rule_name):
        before = partition.Name
        after = partition.Parent.Name
        if before == after:
            continue
        fixes.append(
            {
                "objectType": "Partition",
                "objectName": display_name,
                "before": before,
                "after": after,
                "_apply": (
                    lambda target=partition, value=after: setattr(target, "Name", value)
                ),
            }
        )

    return fixes


def _delete_object(obj) -> None:
    """Removes a supported model object from its parent collection."""

    import Microsoft.AnalysisServices.Tabular as TOM

    if obj.ObjectType == TOM.ObjectType.Measure:
        obj.Parent.Measures.Remove(obj)
    elif obj.ObjectType == TOM.ObjectType.Column:
        obj.Parent.Columns.Remove(obj)
    elif obj.ObjectType == TOM.ObjectType.Table:
        obj.Model.Tables.Remove(obj)


def _delete_fixes(tom, rules: pd.DataFrame, rule_name: str) -> List[dict]:
    """Builds a delete fix for every object flagged by a rule."""

    fixes = []
    for scope, obj, display_name in _violating_objects(tom, rules, rule_name):
        fixes.append(
            {
                "objectType": scope,
                "objectName": display_name,
                "before": obj.Name,
                "after": "(deleted)",
                "_apply": (lambda target=obj: _delete_object(target)),
            }
        )

    return fixes


def _set_summarize_by_none(column) -> None:
    import Microsoft.AnalysisServices.Tabular as TOM
    import System

    column.SummarizeBy = System.Enum.Parse(TOM.AggregateFunction, "None")


def _set_data_type_decimal(column) -> None:
    import Microsoft.AnalysisServices.Tabular as TOM

    column.DataType = TOM.DataType.Decimal


def collect_fixes(tom, rules: pd.DataFrame, rule_name: str) -> List[dict]:
    """
    Computes the changes an automatic fix would make for a rule.

    Parameters
    ----------
    tom : TOMWrapper
        A connected semantic model.
    rules : pandas.DataFrame
        A rules dataframe in the shape produced by
        :func:`sempy_labs.model_bpa_rules`.
    rule_name : str
        The name of the rule to fix.

    Returns
    -------
    List[dict]
        One entry per change with the object type, object name, the ``before`` and
        ``after`` values, and a private ``_apply`` callable which performs the change.
    """

    if rule_name not in FIXABLE_RULES:
        return []

    if rule_name == "Column references should be fully qualified":
        return _qualify_column_references(tom)
    if rule_name == "Measure references should be unqualified":
        return _unqualify_measure_references(tom)
    if rule_name == "Set IsAvailableInMdx to false on non-attribute columns":
        return _column_property_fixes(
            tom,
            rules,
            rule_name,
            lambda c: (
                f"IsAvailableInMdx = {c.IsAvailableInMDX}",
                "IsAvailableInMdx = False",
            ),
            lambda c: setattr(c, "IsAvailableInMDX", False),
        )
    if rule_name == "Do not summarize numeric columns":
        return _column_property_fixes(
            tom,
            rules,
            rule_name,
            lambda c: (f"SummarizeBy = {c.SummarizeBy}", "SummarizeBy = None"),
            _set_summarize_by_none,
        )
    if rule_name == "Do not use floating point data types":
        return _column_property_fixes(
            tom,
            rules,
            rule_name,
            lambda c: (f"Data type = {c.DataType}", "Data type = Decimal"),
            _set_data_type_decimal,
        )
    if rule_name == "Hide foreign keys":
        return _column_property_fixes(
            tom,
            rules,
            rule_name,
            lambda c: (f"IsHidden = {c.IsHidden}", "IsHidden = True"),
            lambda c: setattr(c, "IsHidden", True),
        )
    if rule_name == "Mark primary keys":
        return _column_property_fixes(
            tom,
            rules,
            rule_name,
            lambda c: (f"IsKey = {c.IsKey}", "IsKey = True"),
            lambda c: setattr(c, "IsKey", True),
        )
    if rule_name == "First letter of objects must be capitalized":
        return _capitalize_fixes(tom, rules, rule_name)
    if (
        rule_name
        == "Partition name should match table name for single partition tables"
    ):
        return _partition_name_fixes(tom, rules, rule_name)

    return _delete_fixes(tom, rules, rule_name)


def preview_fixes(tom, rules: pd.DataFrame, rule_name: str) -> List[dict]:
    """
    Returns the JSON-serializable preview of an automatic fix (without the private
    ``_apply`` callable).
    """

    return [
        {k: v for k, v in fix.items() if not k.startswith("_")}
        for fix in collect_fixes(tom, rules, rule_name)
    ]


def apply_fixes(
    tom,
    rules: pd.DataFrame,
    rule_name: str,
    object_names: Optional[Iterable[str]] = None,
) -> int:
    """
    Applies the automatic fix for a rule to the connected model.

    Parameters
    ----------
    tom : TOMWrapper
        A connected semantic model opened in read/write mode.
    rules : pandas.DataFrame
        A rules dataframe in the shape produced by
        :func:`sempy_labs.model_bpa_rules`.
    rule_name : str
        The name of the rule to fix.
    object_names : Iterable[str], default=None
        Restricts the fix to these object names. Defaults to None which fixes every
        flagged object.

    Returns
    -------
    int
        The number of objects changed.
    """

    selected = set(object_names) if object_names else None
    applied = 0
    for fix in collect_fixes(tom, rules, rule_name):
        if selected is not None and fix["objectName"] not in selected:
            continue
        fix["_apply"]()
        applied += 1

    return applied
