"""DAX performance optimization rule engine.

This module evaluates a set of declarative optimization rules (defined in
``_dax_optimization_rules.json``) against the artifacts produced by the
interactive DAX test widget:

* the DAX query text,
* the semantic model metadata (from TOM),
* the query dependencies (referenced tables/columns),
* the trace details (Formula/Storage Engine timings and events),
* the DAX query plan (logical/physical), and
* (optionally) Vertipaq Analyzer statistics -- only used when the column
  cardinalities for ``Data`` columns are not all ``1`` (i.e. there is
  something meaningful to analyze).

The rules are intentionally data-driven so the catalog can be reviewed and
extended without changing the evaluation code. See the ``dax-optimization``
skill for the methodology behind each rule.
"""

from __future__ import annotations

import json
import os
import re
from typing import Any, Optional

import pandas as pd

_RULES_FILE = os.path.join(os.path.dirname(__file__), "_dax_optimization_rules.json")

# Iterator (row-by-row) DAX functions. Used to flag nested/expensive iteration.
_ITERATOR_FUNCTIONS = [
    "SUMX",
    "AVERAGEX",
    "MINX",
    "MAXX",
    "COUNTX",
    "COUNTAX",
    "PRODUCTX",
    "CONCATENATEX",
    "RANKX",
    "MEDIANX",
    "GEOMEANX",
    "FILTER",
    "ADDCOLUMNS",
    "GENERATE",
    "GENERATEALL",
    "TOPN",
]


def _load_rules() -> dict:
    """Load the optimization rules catalog from the packaged JSON file.

    Returns an empty (but well-formed) catalog if the file cannot be read so
    the caller can degrade gracefully rather than raising in the UI."""

    try:
        with open(_RULES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict) or "rules" not in data:
            return {"rules": [], "severity_order": []}
        return data
    except Exception:
        return {"rules": [], "severity_order": []}


# --------------------------------------------------------------------------- #
# Condition evaluation
# --------------------------------------------------------------------------- #
def _compare(actual: Any, op: str, expected: Any) -> bool:
    """Evaluate a single comparison ``actual <op> expected`` defensively.

    Unknown operators and type errors evaluate to ``False`` so a malformed
    rule can never crash the analysis."""

    try:
        if op in (">", ">=", "<", "<="):
            a = float(actual)
            b = float(expected)
            if op == ">":
                return a > b
            if op == ">=":
                return a >= b
            if op == "<":
                return a < b
            return a <= b
        if op == "==":
            return actual == expected
        if op == "!=":
            return actual != expected
        if op == "contains":
            return str(expected).lower() in str(actual).lower()
        if op == "not_contains":
            return str(expected).lower() not in str(actual).lower()
        if op == "regex":
            return re.search(str(expected), str(actual), re.IGNORECASE) is not None
        if op == "in":
            return actual in (expected or [])
        if op == "not_in":
            return actual not in (expected or [])
    except Exception:
        return False
    return False


def _eval_condition(cond: Any, ctx: dict) -> bool:
    """Recursively evaluate a condition tree against a context dict.

    Supports composites ``all`` / ``any`` / ``not`` and leaf comparisons that
    reference either a ``metric`` (scalar rules) or a ``field`` (per-item
    ``for_each`` rules)."""

    if not isinstance(cond, dict):
        return False
    if "all" in cond:
        return all(_eval_condition(c, ctx) for c in cond["all"])
    if "any" in cond:
        return any(_eval_condition(c, ctx) for c in cond["any"])
    if "not" in cond:
        return not _eval_condition(cond["not"], ctx)
    key = cond.get("metric", cond.get("field"))
    if key is None:
        return False
    return _compare(ctx.get(key), cond.get("op", "=="), cond.get("value"))


def _render(template: str, ctx: dict) -> str:
    """Render ``{placeholder}`` tokens in ``template`` from ``ctx``.

    Missing placeholders are left as-is rather than raising, keeping the
    message readable even if a rule references a value that wasn't computed."""

    def _sub(m: "re.Match") -> str:
        name = m.group(1)
        if name in ctx and ctx[name] is not None:
            return str(ctx[name])
        return m.group(0)

    try:
        return re.sub(r"\{([a-zA-Z0-9_]+)\}", _sub, template or "")
    except Exception:
        return template or ""


# --------------------------------------------------------------------------- #
# Query text heuristics
# --------------------------------------------------------------------------- #
def _strip_strings_and_comments(dax: str) -> str:
    """Return the DAX text with string literals and comments removed so that
    keyword/operator heuristics don't match inside strings or comments."""

    if not dax:
        return ""
    # Remove block comments, line comments, then double-quoted strings.
    no_block = re.sub(r"/\*.*?\*/", " ", dax, flags=re.DOTALL)
    no_line = re.sub(r"//[^\n]*", " ", no_block)
    no_str = re.sub(r'"(?:[^"]|"")*"', '""', no_line)
    return no_str


def _count_iterators(dax_clean: str) -> int:
    total = 0
    for fn in _ITERATOR_FUNCTIONS:
        total += len(re.findall(rf"\b{fn}\s*\(", dax_clean, re.IGNORECASE))
    return total


def _has_nested_iterator(dax_clean: str) -> bool:
    """Detect whether any iterator function is invoked inside the argument
    list of another iterator function (parenthesis-depth aware)."""

    upper = dax_clean.upper()
    iter_pattern = re.compile(
        r"\b(" + "|".join(_ITERATOR_FUNCTIONS) + r")\s*\(", re.IGNORECASE
    )
    # Stack of paren depths at which an iterator call was opened.
    open_iter_depths: list = []
    depth = 0
    i = 0
    n = len(upper)
    while i < n:
        ch = upper[i]
        m = iter_pattern.match(upper, i)
        if m:
            # An iterator opens here. If we're already inside another
            # iterator's parentheses, this is a nested iterator.
            if open_iter_depths:
                return True
            # Advance to the '(' and record the depth it opens at.
            paren_pos = m.end() - 1
            depth += 1
            open_iter_depths.append(depth)
            i = paren_pos + 1
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            if open_iter_depths and depth == open_iter_depths[-1]:
                open_iter_depths.pop()
            depth = max(depth - 1, 0)
        i += 1
    return False


def _count_filter_full_table(dax_clean: str) -> int:
    """Count ``FILTER(<table>, ...)`` occurrences where the first argument is a
    whole-table reference (a bare or quoted table name) rather than a column,
    a function call, or a table expression."""

    count = 0
    for m in re.finditer(r"\bFILTER\s*\(\s*", dax_clean, re.IGNORECASE):
        pos = m.end()
        rest = dax_clean[pos:]
        # Quoted table name: 'Table Name' immediately followed by a comma.
        q = re.match(r"'(?:[^']|'')+'\s*,", rest)
        if q:
            count += 1
            continue
        # Bare table identifier directly followed by a comma (not '[' column,
        # not '(' function call).
        b = re.match(r"[A-Za-z_][A-Za-z0-9_ ]*?\s*,", rest)
        if b:
            token = b.group(0)
            if "[" not in token and "(" not in token:
                count += 1
    return count


# --------------------------------------------------------------------------- #
# Metric / collection construction
# --------------------------------------------------------------------------- #
def _vertipaq_columns_df(vertipaq: Optional[dict]) -> Optional[pd.DataFrame]:
    """Return the Vertipaq 'Columns' dataframe from the analyzer result dict,
    tolerating either the 'Columns' or 'Column' key."""

    if not isinstance(vertipaq, dict):
        return None
    for key in ("Columns", "Column"):
        df = vertipaq.get(key)
        if isinstance(df, pd.DataFrame) and not df.empty:
            return df
    return None


def _is_floating_point(data_type: str) -> bool:
    dt = str(data_type or "").lower()
    return "double" in dt or "float" in dt


def build_context(
    *,
    dax_query: str = "",
    trace_rows: Optional[list] = None,
    total_duration_ms: int = 0,
    fe_duration_ms: int = 0,
    se_duration_ms: int = 0,
    cpu_time_ms: int = 0,
    query_plan_rows: Optional[list] = None,
    dependency_columns: Optional[list] = None,
    model_tree: Optional[list] = None,
    vertipaq: Optional[dict] = None,
    cold_cache: bool = True,
) -> dict:
    """Build the ``(metrics, collections)`` evaluation context from the raw
    widget artifacts.

    Returns a dict with ``"metrics"`` (flat dict for scalar rules) and
    ``"collections"`` (named lists of item dicts for ``for_each`` rules), plus
    a small ``"inputs"`` summary describing which artifacts were available.
    """

    trace_rows = trace_rows or []
    query_plan_rows = query_plan_rows or []
    dependency_columns = dependency_columns or []
    model_tree = model_tree or []

    dax_clean = _strip_strings_and_comments(dax_query or "")

    # ---- Query text metrics ----
    iterator_count = _count_iterators(dax_clean)
    metrics: dict = {
        "has_query": bool((dax_query or "").strip()),
        "query_length": len(dax_query or ""),
        "iterator_count": iterator_count,
        "nested_iterator": _has_nested_iterator(dax_clean),
        "uses_iferror": bool(
            re.search(r"\b(IFERROR|ISERROR)\s*\(", dax_clean, re.IGNORECASE)
        ),
        "uses_divide_function": bool(
            re.search(r"\bDIVIDE\s*\(", dax_clean, re.IGNORECASE)
        ),
        "uses_division_operator": bool(re.search(r"(?<![/])/(?![/])", dax_clean)),
        "filter_full_table_count": _count_filter_full_table(dax_clean),
        "cold_cache": bool(cold_cache),
    }

    # ---- Trace metrics ----
    has_trace = bool(trace_rows)
    se_rows = [
        r for r in trace_rows if str(r.get("event_class", "")) == "VertiPaqSEQueryEnd"
    ]
    se_non_internal = [
        r for r in se_rows if "internal" not in str(r.get("event_subclass", "")).lower()
    ]
    se_internal = [r for r in se_rows if r not in se_non_internal]
    cache_matches = [
        r
        for r in trace_rows
        if str(r.get("event_class", "")) == "VertiPaqSEQueryCacheMatch"
    ]
    se_cpu_ms = 0
    for r in se_non_internal:
        try:
            se_cpu_ms += int(r.get("cpu", 0) or 0)
        except (TypeError, ValueError):
            pass

    total = int(total_duration_ms or 0)
    se = int(se_duration_ms or 0)
    fe = int(fe_duration_ms or 0)
    metrics.update(
        {
            "has_trace": has_trace,
            "total_duration_ms": total,
            "se_duration_ms": se,
            "fe_duration_ms": fe,
            "cpu_time_ms": int(cpu_time_ms or 0),
            "se_pct": (se / total) if total > 0 else 0.0,
            "fe_pct": (fe / total) if total > 0 else 0.0,
            "se_query_count": len(se_non_internal),
            "se_internal_count": len(se_internal),
            "se_cache_match_count": len(cache_matches),
            "se_cpu_ms": se_cpu_ms,
            "se_parallelism": (se_cpu_ms / se) if se > 0 else 0.0,
        }
    )

    # ---- Query plan metrics ----
    physical_text = "\n".join(
        str(r.get("text", "") or "")
        for r in query_plan_rows
        if str(r.get("plan_type", "")).lower() == "physical"
    )
    if not physical_text:
        # Fall back to all plan text if plan_type wasn't classified.
        physical_text = "\n".join(str(r.get("text", "") or "") for r in query_plan_rows)
    metrics.update(
        {
            "has_query_plan": bool(query_plan_rows),
            "callback_dataid_count": len(
                re.findall(r"CallbackDataID", physical_text, re.IGNORECASE)
            ),
            "encode_callback_count": len(
                re.findall(r"EncodeCallback", physical_text, re.IGNORECASE)
            ),
            "spool_count": len(re.findall(r"\bSpool\b", physical_text, re.IGNORECASE)),
        }
    )

    # ---- Dependency metrics ----
    ref_pairs = set()
    for c in dependency_columns:
        t = str(c.get("table", "") or "")
        col = str(c.get("column", "") or "")
        if t or col:
            ref_pairs.add((t, col))
    metrics["referenced_column_count"] = len(ref_pairs)
    metrics["referenced_table_count"] = len({t for (t, _) in ref_pairs})
    metrics["has_dependencies"] = bool(ref_pairs)

    # ---- Vertipaq metrics & collections ----
    # Vertipaq stats are only used when the Data columns are not all
    # cardinality 1 (i.e. there is something meaningful to analyze).
    collections: dict = {"slow_se_queries": [], "high_cardinality_columns": []}
    vertipaq_available = False
    vertipaq_skipped_trivial = False
    max_data_card = 0
    high_card_count = 0
    cols_df = _vertipaq_columns_df(vertipaq)
    if cols_df is not None:
        df = cols_df
        type_col = "Type" if "Type" in df.columns else None
        card_col = "Cardinality" if "Cardinality" in df.columns else None
        if card_col:
            data_df = df
            if type_col:
                data_df = df[df[type_col].astype(str).str.lower() == "data"]
            cardinalities = pd.to_numeric(data_df[card_col], errors="coerce").fillna(0)
            max_data_card = int(cardinalities.max()) if len(cardinalities) else 0
            all_ones = len(cardinalities) > 0 and bool((cardinalities <= 1).all())
            if len(cardinalities) == 0 or all_ones:
                # Nothing meaningful to analyze -> skip vertipaq rules.
                vertipaq_skipped_trivial = True
            else:
                vertipaq_available = True
                # Focus on columns referenced by the query when dependency
                # info is available; otherwise consider all data columns.
                items = []
                for _, row in data_df.iterrows():
                    t = str(row.get("Table Name", "") or "")
                    col = str(row.get("Column Name", "") or "")
                    if ref_pairs and (t, col) not in ref_pairs:
                        continue
                    try:
                        card = int(
                            pd.to_numeric(row.get(card_col), errors="coerce") or 0
                        )
                    except (TypeError, ValueError):
                        card = 0
                    dtype = str(row.get("Data Type", "") or "")
                    items.append(
                        {
                            "table": t,
                            "column": col,
                            "cardinality": card,
                            "cardinality_display": f"{card:,}",
                            "data_type": dtype,
                            "is_floating_point": _is_floating_point(dtype),
                            "data_size": int(
                                pd.to_numeric(row.get("Data Size"), errors="coerce")
                                or 0
                            ),
                            "encoding": str(row.get("Encoding", "") or ""),
                        }
                    )
                    if card >= 1000000:
                        high_card_count += 1
                items.sort(key=lambda x: x["cardinality"], reverse=True)
                collections["high_cardinality_columns"] = items
    metrics.update(
        {
            "vertipaq_available": vertipaq_available,
            "vertipaq_skipped_trivial": vertipaq_skipped_trivial,
            "max_data_column_cardinality": max_data_card,
            "high_cardinality_data_column_count": high_card_count,
        }
    )

    # ---- Slow SE query collection ----
    slow = []
    for r in se_non_internal:
        try:
            dur = int(r.get("duration", 0) or 0)
        except (TypeError, ValueError):
            dur = 0
        try:
            cpu = int(r.get("cpu", 0) or 0)
        except (TypeError, ValueError):
            cpu = 0
        slow.append(
            {
                "subclass": str(r.get("event_subclass", "") or ""),
                "duration": dur,
                "cpu": cpu,
            }
        )
    slow.sort(key=lambda x: x["duration"], reverse=True)
    collections["slow_se_queries"] = slow[:10]

    # ---- Display-friendly derived values for message templates ----
    metrics["se_pct_display"] = f"{metrics['se_pct'] * 100:.0f}%"
    metrics["fe_pct_display"] = f"{metrics['fe_pct'] * 100:.0f}%"
    metrics["se_parallelism_display"] = f"{metrics['se_parallelism']:.1f}x"

    inputs = {
        "query": metrics["has_query"],
        "trace": has_trace,
        "query_plan": metrics["has_query_plan"],
        "dependencies": metrics["has_dependencies"],
        "vertipaq": vertipaq_available,
        "model": bool(model_tree),
    }

    return {"metrics": metrics, "collections": collections, "inputs": inputs}


# --------------------------------------------------------------------------- #
# Rule evaluation
# --------------------------------------------------------------------------- #
def _requirements_met(rule: dict, inputs: dict) -> bool:
    """Return True only if every artifact listed in the rule's ``requires`` is
    available, so rules don't fire (or report misleading 'no issue') on
    missing data. The two diagnostics rules deliberately have permissive
    requirements and handle availability in their conditions."""

    for req in rule.get("requires", []) or []:
        if not inputs.get(req, False):
            return False
    return True


def evaluate_rules(context: dict, rules_catalog: Optional[dict] = None) -> list:
    """Evaluate all rules against a context produced by :func:`build_context`.

    Returns a list of finding dicts ``{id, title, category, severity, message,
    recommendation, references, evidence}`` ordered by severity.
    """

    catalog = rules_catalog or _load_rules()
    rules = catalog.get("rules", [])
    severity_order = catalog.get("severity_order", ["high", "medium", "low", "info"])
    metrics = context.get("metrics", {})
    collections = context.get("collections", {})
    inputs = context.get("inputs", {})

    findings: list = []
    for rule in rules:
        if not _requirements_met(rule, inputs):
            continue
        kind = rule.get("kind", "scalar")
        if kind == "for_each":
            coll = collections.get(rule.get("collection", ""), [])
            where = rule.get("where")
            max_findings = int(rule.get("max_findings", 5))
            matched = 0
            for item in coll:
                if where is not None and not _eval_condition(where, item):
                    continue
                ctx = dict(metrics)
                ctx.update(item)
                findings.append(
                    {
                        "id": rule.get("id", ""),
                        "title": rule.get("title", ""),
                        "category": rule.get("category", ""),
                        "severity": rule.get("severity", "info"),
                        "message": _render(rule.get("message", ""), ctx),
                        "recommendation": rule.get("recommendation", ""),
                        "references": rule.get("references", []),
                        "evidence": item,
                    }
                )
                matched += 1
                if matched >= max_findings:
                    break
        else:
            cond = rule.get("condition")
            if cond is not None and not _eval_condition(cond, metrics):
                continue
            if cond is None:
                continue
            findings.append(
                {
                    "id": rule.get("id", ""),
                    "title": rule.get("title", ""),
                    "category": rule.get("category", ""),
                    "severity": rule.get("severity", "info"),
                    "message": _render(rule.get("message", ""), metrics),
                    "recommendation": rule.get("recommendation", ""),
                    "references": rule.get("references", []),
                    "evidence": None,
                }
            )

    def _sev_key(f: dict) -> int:
        try:
            return severity_order.index(f.get("severity", "info"))
        except ValueError:
            return len(severity_order)

    findings.sort(key=_sev_key)
    return findings


def analyze_dax_performance(
    *,
    dax_query: str = "",
    trace_rows: Optional[list] = None,
    total_duration_ms: int = 0,
    fe_duration_ms: int = 0,
    se_duration_ms: int = 0,
    cpu_time_ms: int = 0,
    query_plan_rows: Optional[list] = None,
    dependency_columns: Optional[list] = None,
    model_tree: Optional[list] = None,
    vertipaq: Optional[dict] = None,
    cold_cache: bool = True,
) -> dict:
    """Run the full DAX performance analysis.

    Builds the evaluation context from the supplied artifacts, evaluates the
    rule catalog, and returns a result dict with ``findings``, a ``summary``
    (severity counts + engine balance), the computed ``metrics`` and the
    ``inputs`` availability map. This is the single entry point used by the
    interactive DAX test widget.
    """

    context = build_context(
        dax_query=dax_query,
        trace_rows=trace_rows,
        total_duration_ms=total_duration_ms,
        fe_duration_ms=fe_duration_ms,
        se_duration_ms=se_duration_ms,
        cpu_time_ms=cpu_time_ms,
        query_plan_rows=query_plan_rows,
        dependency_columns=dependency_columns,
        model_tree=model_tree,
        vertipaq=vertipaq,
        cold_cache=cold_cache,
    )
    findings = evaluate_rules(context)
    metrics = context["metrics"]

    severity_counts: dict = {}
    for f in findings:
        sev = f.get("severity", "info")
        severity_counts[sev] = severity_counts.get(sev, 0) + 1

    summary = {
        "total_findings": len(findings),
        "severity_counts": severity_counts,
        "total_duration_ms": metrics.get("total_duration_ms", 0),
        "fe_duration_ms": metrics.get("fe_duration_ms", 0),
        "se_duration_ms": metrics.get("se_duration_ms", 0),
        "fe_pct": metrics.get("fe_pct", 0.0),
        "se_pct": metrics.get("se_pct", 0.0),
    }

    return {
        "findings": findings,
        "summary": summary,
        "metrics": metrics,
        "inputs": context["inputs"],
    }
