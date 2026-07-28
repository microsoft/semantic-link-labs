import ast
import inspect
import os
import re
import textwrap
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
    "Ensure Direct Lake source tables are V-Ordered",
    "Remove auto-date table",
    "Remove unnecessary columns",
    "Remove unnecessary measures",
}

# Presentation order for severities (most severe first).
SEVERITY_ORDER = {"Error": 0, "Warning": 1, "Info": 2}

# Presentation order for rule categories.
CATEGORY_ORDER = {
    "Performance": 0,
    "Error Prevention": 1,
    "DAX Expressions": 2,
    "Maintenance": 3,
    "Formatting": 4,
    "Naming Conventions": 5,
}

# Rule logic is compiled Python, so the source of each rule's predicate is read
# straight out of the rules module. This keeps the exported ``Expression`` in sync
# with the code that actually runs, and never needs manual maintenance.
_RULES_MODULE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "_model_bpa_rules.py",
)
_SEVERITIES = ("Error", "Warning", "Info")
_builtin_expression_cache: Optional[Dict[str, str]] = None


def _normalize_lambda_source(source: str) -> str:
    """Re-indents a multi-line lambda so it reads well outside its original tuple."""

    lines = source.splitlines()
    if len(lines) == 1:
        return lines[0].strip()

    continuation = [ln for ln in lines[1:] if ln.strip()]
    indent = min((len(ln) - len(ln.lstrip()) for ln in continuation), default=0)
    out = [lines[0].strip()]
    for line in lines[1:]:
        out.append("    " + line[indent:].rstrip() if line.strip() else "")

    return "\n".join(out).rstrip()


def _builtin_rule_expressions() -> Dict[str, str]:
    """Maps each built-in rule name to the source code of its predicate."""

    global _builtin_expression_cache
    if _builtin_expression_cache is not None:
        return _builtin_expression_cache

    expressions: Dict[str, str] = {}
    try:
        with open(_RULES_MODULE, "r", encoding="utf-8") as f:
            source = f.read()
        for node in ast.walk(ast.parse(source)):
            if not isinstance(node, ast.Tuple) or len(node.elts) < 5:
                continue
            severity, name, predicate = node.elts[2], node.elts[3], node.elts[4]
            if (
                isinstance(severity, ast.Constant)
                and severity.value in _SEVERITIES
                and isinstance(name, ast.Constant)
                and isinstance(name.value, str)
                and isinstance(predicate, ast.Lambda)
            ):
                segment = ast.get_source_segment(source, predicate)
                if segment:
                    expressions[name.value] = _normalize_lambda_source(segment)
    except Exception:
        # Losing the expressions must never break a scan or an export.
        expressions = {}

    _builtin_expression_cache = expressions
    return expressions


def rule_expression(rule_name: str, predicate=None) -> str:
    """
    Returns the source code of a rule's predicate.

    Parameters
    ----------
    rule_name : str
        The name of the rule.
    predicate : Callable, default=None
        The rule's compiled predicate, used as a fallback for rules which are not
        defined in :mod:`sempy_labs._model_bpa_rules` (for example a custom rule
        supplied as a dataframe).

    Returns
    -------
    str
        The predicate source, or an empty string when it cannot be recovered.
    """

    source = _builtin_rule_expressions().get(rule_name)
    if source:
        return source
    if predicate is None:
        return ""

    try:
        text = textwrap.dedent(inspect.getsource(predicate))
    except Exception:
        return ""

    index = text.find("lambda")
    if index == -1:
        return ""

    return _normalize_lambda_source(text[index:].rstrip().rstrip(","))


# The Python performed by each automatic fix, keyed by rule name (only for
# FIXABLE_RULES). These mirror the implementations further down this module.
RULE_FIX_EXPRESSIONS = {
    "Column references should be fully qualified": (
        "obj.Expression = re.sub(\n"
        '    r"(?<![\\w\'\\]])\\[Column\\]", "\'Table\'[Column]", obj.Expression\n'
        ")"
    ),
    "Measure references should be unqualified": (
        "obj.Expression = re.sub(\n"
        '    r"(?:\'[^\']+\'|\\w+)\\[Measure\\]", "[Measure]", obj.Expression\n'
        ")"
    ),
    "Set IsAvailableInMdx to false on non-attribute columns": (
        "obj.IsAvailableInMDX = False"
    ),
    "Do not summarize numeric columns": (
        'obj.SummarizeBy = System.Enum.Parse(TOM.AggregateFunction, "None")'
    ),
    "Do not use floating point data types": "obj.DataType = TOM.DataType.Decimal",
    "Hide foreign keys": "obj.IsHidden = True",
    "Mark primary keys": "obj.IsKey = True",
    "First letter of objects must be capitalized": (
        "obj.Name = obj.Name[0].upper() + obj.Name[1:]"
    ),
    "Partition name should match table name for single partition tables": (
        "obj.Name = obj.Parent.Name"
    ),
    "Ensure Direct Lake source tables are V-Ordered": (
        "run_table_maintenance(\n"
        "    table_name=obj.Source.EntityName,\n"
        "    schema=obj.Source.SchemaName,\n"
        "    optimize=False,\n"
        "    v_order=True,\n"
        "    lakehouse=<Direct Lake lakehouse>,\n"
        "    workspace=<Direct Lake workspace>,\n"
        ")"
    ),
    "Remove auto-date table": "obj.Model.Tables.Remove(obj)",
    "Remove unnecessary columns": "obj.Parent.Columns.Remove(obj)",
    "Remove unnecessary measures": "obj.Parent.Measures.Remove(obj)",
}


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


# The scopes a rule may be evaluated against (the keys of the dispatch table above).
RULE_SCOPES = [
    "Model",
    "Table",
    "Calculated Table",
    "Column",
    "Calculated Column",
    "Measure",
    "Hierarchy",
    "Relationship",
    "Role",
    "Row Level Security",
    "Partition",
    "Calculation Item",
    "Function",
]

# Severity codes used by the Best Practice Rules JSON format.
SEVERITY_TO_CODE = {"Error": 3, "Warning": 2, "Info": 1}
CODE_TO_SEVERITY = {3: "Error", 2: "Warning", 1: "Info"}

_RULES_COLUMNS = [
    "Category",
    "Scope",
    "Severity",
    "Rule Name",
    "Expression",
    "Description",
    "URL",
]


def _strip_category_prefix(name: str) -> str:
    """Removes a leading ``[Category] `` prefix, as used by BPARules.json names."""

    return re.sub(r"^\s*\[[^\]]+\]\s*", "", str(name))


def _entry_value(entry: dict, *names):
    """Reads the first present key from a rule entry, ignoring key casing."""

    lowered = {str(k).lower(): v for k, v in entry.items()}
    for name in names:
        if name.lower() in lowered:
            return lowered[name.lower()]
    return None


def rules_to_json(
    rules: pd.DataFrame, disabled_rule_ids: Optional[Iterable[str]] = None
) -> List[dict]:
    """
    Exports a rules dataframe to the `Best Practice Rules
    <https://github.com/microsoft/Analysis-Services/tree/master/BestPracticeRules>`_
    JSON format.

    Each entry also carries an ``Expression`` holding the source code of the rule's
    predicate and, where an automatic fix exists, a ``FixExpression`` holding the
    code the fix runs. Both are informational: the rule logic is compiled in Python
    and is never read back from the file.

    Parameters
    ----------
    rules : pandas.DataFrame
        A rules dataframe in the shape produced by
        :func:`sempy_labs.model_bpa_rules`.
    disabled_rule_ids : Iterable[str], default=None
        Rule ids which should be exported with ``Enabled`` set to False.

    Returns
    -------
    List[dict]
        One dictionary per rule.
    """

    disabled = set(disabled_rule_ids or [])
    entries = []
    for _, r in rules.iterrows():
        scopes = r["Scope"]
        if isinstance(scopes, str):
            scopes = [scopes]
        rule_name = str(r["Rule Name"])
        url = r.get("URL")
        entry = {
            "ID": _rule_id(rule_name).upper(),
            "Name": rule_name,
            "Category": str(r["Category"]),
            "Description": ("" if pd.isna(r["Description"]) else str(r["Description"])),
            "Severity": SEVERITY_TO_CODE.get(str(r["Severity"]), 2),
            "Scope": ", ".join(scopes),
            "Expression": rule_expression(rule_name, r["Expression"]),
            "Url": None if url is None or pd.isna(url) else str(url),
            "Enabled": _rule_id(rule_name) not in disabled,
        }
        fix_expression = RULE_FIX_EXPRESSIONS.get(rule_name)
        if fix_expression:
            entry["FixExpression"] = fix_expression
        entries.append(entry)

    return entries


def _match_default_rule(entry: dict, by_id: Dict[str, Any]):
    """Resolves a JSON rule entry to the built-in rule which supplies its logic."""

    candidates = []
    identifier = _entry_value(entry, "ID", "Id")
    if identifier:
        candidates.append(_rule_id(str(identifier)))
    name = _entry_value(entry, "Name", "Rule Name", "RuleName")
    if name:
        candidates.append(_rule_id(str(name)))
        candidates.append(_rule_id(_strip_category_prefix(str(name))))

    for candidate in candidates:
        if candidate in by_id:
            return candidate, by_id[candidate]

    return None, None


def validate_rules_json(
    entries: Any, default_rules: pd.DataFrame
) -> Tuple[List[str], List[str]]:
    """
    Checks a Best Practice Rules JSON ruleset before it is imported.

    The rule logic is compiled in Python, so an entry is only usable when it can be
    matched to a built-in rule and its overridable properties are well-formed. This
    reports both the problems which make the ruleset unusable and the ones which
    cause an entry to be skipped or to fall back to its built-in definition.

    Parameters
    ----------
    entries : Any
        The parsed contents of the ruleset file. A list of rule entries, or a dict
        containing such a list under a ``rules`` key.
    default_rules : pandas.DataFrame
        The built-in rules supplying each rule's logic, in the shape produced by
        :func:`sempy_labs.model_bpa_rules`.

    Returns
    -------
    Tuple[List[str], List[str]]
        The errors (the ruleset cannot be imported) and the warnings (the entry is
        skipped, or the built-in value is used instead).
    """

    if isinstance(entries, dict):
        entries = _entry_value(entries, "rules")
    if not isinstance(entries, list):
        return (
            [
                "The file must contain a list of rules, or an object with a 'rules' "
                "list."
            ],
            [],
        )
    if not entries:
        return (["The file does not contain any rules."], [])

    by_id = {_rule_id(str(r["Rule Name"])): r for _, r in default_rules.iterrows()}
    valid_scopes = {s.lower() for s in RULE_SCOPES}

    errors: List[str] = []
    warnings: List[str] = []
    seen: Dict[str, str] = {}

    for index, entry in enumerate(entries, start=1):
        label = f"Rule {index}"
        if not isinstance(entry, dict):
            warnings.append(f"{label}: is not a rule object; it was skipped.")
            continue

        name = _entry_value(entry, "Name", "Rule Name", "RuleName")
        identifier = _entry_value(entry, "ID", "Id")
        if name:
            label = f"'{_strip_category_prefix(str(name))}'"
        elif identifier:
            label = f"'{identifier}'"
        else:
            warnings.append(f"{label}: has no 'ID' or 'Name'; it was skipped.")
            continue

        rule_id, base = _match_default_rule(entry, by_id)
        if base is None:
            warnings.append(
                f"{label}: does not match a built-in rule (rules are matched by "
                "their 'ID' or 'Name'); it was skipped."
            )
            continue
        if rule_id in seen:
            warnings.append(
                f"{label}: is a duplicate of '{seen[rule_id]}'; only the first "
                "entry is used."
            )
            continue
        seen[rule_id] = str(base["Rule Name"])

        severity = _entry_value(entry, "Severity")
        if severity is not None:
            valid_severity = (
                not isinstance(severity, bool)
                and isinstance(severity, (int, float))
                and int(severity) in CODE_TO_SEVERITY
            ) or (
                isinstance(severity, str) and severity.capitalize() in SEVERITY_TO_CODE
            )
            if not valid_severity:
                warnings.append(
                    f"{label}: 'Severity' must be 1, 2 or 3 (or Info, Warning, "
                    f"Error) but is {severity!r}; the built-in severity is used."
                )

        raw_scope = _entry_value(entry, "Scope", "Scopes")
        if raw_scope is not None:
            tokens = (
                [s.strip() for s in raw_scope.split(",") if s.strip()]
                if isinstance(raw_scope, str)
                else raw_scope
            )
            if (
                not isinstance(tokens, list)
                or not tokens
                or any(
                    not isinstance(s, str) or s.strip().lower() not in valid_scopes
                    for s in tokens
                )
            ):
                warnings.append(
                    f"{label}: 'Scope' contains a value which is not one of "
                    f"{', '.join(RULE_SCOPES)}; the built-in scope is used."
                )

        enabled = _entry_value(entry, "Enabled")
        if enabled is not None and not isinstance(enabled, bool):
            warnings.append(
                f"{label}: 'Enabled' must be true or false but is {enabled!r}; "
                "the rule is treated as enabled."
            )

        description = _entry_value(entry, "Description")
        if description is not None and not isinstance(description, str):
            warnings.append(
                f"{label}: 'Description' must be text; the value is converted to text."
            )

    if not seen:
        errors.append(
            "None of the rules in the file matched a built-in rule. Rules are "
            "matched by their 'ID' or 'Name'."
        )

    return errors, warnings


def parse_rules_json(
    entries: Iterable[dict], default_rules: pd.DataFrame
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Builds a rules dataframe from Best Practice Rules JSON entries.

    Each entry is matched to a built-in rule by ``ID`` (or by ``Name``, with an
    optional ``[Category]`` prefix removed) because the rule logic is compiled in
    Python and cannot be read from the file. ``Category``, ``Severity``,
    ``Description``, ``Url`` and ``Scope`` may be overridden; ``Expression`` and
    ``FixExpression`` are informational and are always taken from the current
    built-in definition. Entries which do not match a built-in rule are ignored.

    Parameters
    ----------
    entries : Iterable[dict]
        The rule entries.
    default_rules : pandas.DataFrame
        The built-in rules supplying each rule's logic, in the shape produced by
        :func:`sempy_labs.model_bpa_rules`.

    Returns
    -------
    Tuple[pandas.DataFrame, List[str]]
        The rules dataframe and the ids of the rules marked as disabled.
    """

    by_id = {}
    for _, r in default_rules.iterrows():
        by_id[_rule_id(str(r["Rule Name"]))] = r

    valid_scopes = {s.lower(): s for s in RULE_SCOPES}
    rows = []
    disabled: List[str] = []
    seen = set()

    for entry in entries:
        if not isinstance(entry, dict):
            continue
        rule_id, base = _match_default_rule(entry, by_id)
        if base is None or rule_id in seen:
            continue
        seen.add(rule_id)

        if _entry_value(entry, "Enabled") is False:
            disabled.append(rule_id)

        category = _entry_value(entry, "Category") or base["Category"]

        severity = _entry_value(entry, "Severity")
        if isinstance(severity, bool) or severity is None:
            severity = base["Severity"]
        elif isinstance(severity, (int, float)):
            severity = CODE_TO_SEVERITY.get(int(severity), str(base["Severity"]))
        elif str(severity).capitalize() in SEVERITY_TO_CODE:
            severity = str(severity).capitalize()
        else:
            severity = base["Severity"]

        # A scope is only honored when every token is one this library can evaluate.
        scope = base["Scope"]
        raw_scope = _entry_value(entry, "Scope", "Scopes")
        if isinstance(raw_scope, str):
            raw_scope = [s.strip() for s in raw_scope.split(",") if s.strip()]
        if isinstance(raw_scope, list) and raw_scope:
            mapped = [valid_scopes.get(str(s).strip().lower()) for s in raw_scope]
            if all(mapped):
                scope = mapped

        description = _entry_value(entry, "Description")
        if description is None:
            description = base["Description"]

        url = _entry_value(entry, "Url", "URL", "Link")
        if url is None:
            url = base.get("URL")

        rows.append(
            (
                str(category),
                scope,
                str(severity),
                str(base["Rule Name"]),
                base["Expression"],
                "" if description is None else str(description),
                url,
            )
        )

    return pd.DataFrame(rows, columns=_RULES_COLUMNS), disabled


def normalize_rules(rules, default_rules: pd.DataFrame) -> pd.DataFrame:
    """
    Coerces a user-supplied ruleset into a rules dataframe.

    Parameters
    ----------
    rules : pandas.DataFrame | List[dict] | dict | None
        A rules dataframe, a list of Best Practice Rules JSON entries, or a dict
        containing such a list under a ``rules`` key. None returns the defaults.
    default_rules : pandas.DataFrame
        The built-in rules supplying each rule's logic.

    Returns
    -------
    pandas.DataFrame
        The effective rules dataframe.
    """

    if rules is None:
        return default_rules
    if isinstance(rules, pd.DataFrame):
        return rules
    if isinstance(rules, dict):
        rules = _entry_value(rules, "rules") or []
    if not isinstance(rules, list):
        raise ValueError(
            "The 'rules' parameter must be a pandas dataframe, a list of rule "
            "dictionaries, or a dict containing a 'rules' list."
        )

    parsed, _ = parse_rules_json(rules, default_rules)
    if parsed.empty:
        raise ValueError(
            "None of the supplied rules matched a built-in rule. Rules are matched "
            "by their 'ID' or 'Name'."
        )

    return parsed


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
        description, url, expression, fix expression and whether an automatic fix is
        available.
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
                "expression": rule_expression(rule_name, r["Expression"]),
                "fixExpression": RULE_FIX_EXPRESSIONS.get(rule_name, ""),
            }
        )

    payload.sort(
        key=lambda x: (
            CATEGORY_ORDER.get(x["category"], 99),
            SEVERITY_ORDER.get(x["severity"], 9),
            x["name"].lower(),
        )
    )
    return payload


def scan_model(
    tom,
    rules: pd.DataFrame,
    disabled_rule_ids: Optional[Iterable[str]] = None,
    should_cancel: Optional[Callable[[], bool]] = None,
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
    should_cancel : Callable[[], bool], default=None
        Called before each rule is evaluated. Returning True stops the scan early
        and returns the violations found so far.

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
        if should_cancel is not None and should_cancel():
            break
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
            SEVERITY_ORDER.get(v["severity"], 9),
            CATEGORY_ORDER.get(v["category"], 99),
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


def _direct_lake_lakehouse(tom) -> Optional[dict]:
    """Returns the lakehouse source descriptor of a Direct Lake model, if it has one."""

    try:
        sources = tom.get_direct_lake_sources()
    except Exception:
        return None

    return next((s for s in sources if s.get("itemType") == "Lakehouse"), None)


def _apply_v_order(table_name: str, schema: Optional[str], source: dict) -> None:
    """Re-writes a lakehouse delta table with V-Order enabled."""

    from sempy_labs.lakehouse._lakehouse import run_table_maintenance

    run_table_maintenance(
        table_name=table_name,
        optimize=True,
        v_order=True,
        schema=schema,
        lakehouse=source.get("itemId"),
        workspace=source.get("workspaceId"),
    )


def _v_order_fixes(tom, rules: pd.DataFrame, rule_name: str) -> List[dict]:
    """
    Builds a V-Order table maintenance fix for the lakehouse table behind every
    flagged Direct Lake partition.
    """

    source = _direct_lake_lakehouse(tom)
    if source is None:
        return []

    fixes = []
    for _scope, partition, display_name in _violating_objects(tom, rules, rule_name):
        table_name = (
            getattr(partition.Source, "EntityName", None) or partition.Parent.Name
        )
        schema_name = getattr(partition.Source, "SchemaName", None) or None
        fixes.append(
            {
                "objectType": "Partition",
                "objectName": display_name,
                "before": f"{table_name} is not V-Ordered",
                "after": f"{table_name} is V-Ordered",
                "_apply": (
                    lambda t=table_name, s=schema_name: _apply_v_order(t, s, source)
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
    if rule_name == "Ensure Direct Lake source tables are V-Ordered":
        return _v_order_fixes(tom, rules, rule_name)

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
