import sempy.fabric as fabric
import pandas as pd
import json
import csv
import re
import os
import inspect
import warnings
import datetime
from IPython.display import display, HTML
from sempy_labs._model_dependencies import get_model_calc_dependencies
from sempy_labs._helper_functions import (
    format_dax_object_name,
    create_relationship_name,
    save_as_delta_table,
    resolve_workspace_capacity,
    resolve_dataset_name_and_id,
    get_language_codes,
    _get_column_aggregate,
    resolve_workspace_name_and_id,
    _create_spark_session,
)
from sempy_labs.lakehouse import get_lakehouse_tables, lakehouse_attached
from sempy_labs.tom import connect_semantic_model
from typing import Optional, List
from sempy._utils._log import log
import sempy_labs._icons as icons
from pyspark.sql.functions import col, flatten
from pyspark.sql.types import StructType, StructField, StringType
from uuid import UUID
from pathlib import Path
import sempy_labs.semantic_model._bpa_rules as bpa_rules


severity_map = {
    "Warning": icons.warning,
    "Error": icons.error,
    "Info": icons.info,
}

_plural_map = {
        "Column": "Columns",
        "Calculated Column": "Calculated Columns",
        "Measure": "Measures",
        "Table": "Tables",
        "Calculated Table": "Calculated Tables",
        "Relationship": "Relationships",
        "Partition": "Partitions",
        "Hierarchy": "Hierarchies",
        "Role": "Roles",
        "Model": "Models",
        "Calculation Item": "Calculation Items",
        "Row Level Security": "Row Level Security",
        "Function": "Functions",
    }

# Module-level state shared between bpa() landing page and _execute_bpa() callback
_bpa_state = {}


def _highlight_python(source):
    """Return HTML with syntax-highlighted Python source code."""
    import re
    from html import escape

    if not source:
        return ""

    _KW = {
        "and",
        "as",
        "assert",
        "async",
        "await",
        "break",
        "class",
        "continue",
        "def",
        "del",
        "elif",
        "else",
        "except",
        "finally",
        "for",
        "from",
        "global",
        "if",
        "import",
        "in",
        "is",
        "lambda",
        "nonlocal",
        "not",
        "or",
        "pass",
        "raise",
        "return",
        "try",
        "while",
        "with",
        "yield",
    }
    _CONST = {"True", "False", "None"}
    _BUILT = {
        "any",
        "all",
        "sum",
        "max",
        "min",
        "len",
        "int",
        "str",
        "float",
        "bool",
        "list",
        "dict",
        "set",
        "tuple",
        "range",
        "print",
        "isinstance",
        "hasattr",
        "getattr",
        "setattr",
        "type",
        "enumerate",
        "zip",
        "map",
        "filter",
        "sorted",
        "reversed",
        "abs",
        "round",
        "re",
    }

    # Tokenise source into ('str', ...), ('cmt', ...) and ('code', ...)
    tokens = []
    i = 0
    n = len(source)
    while i < n:
        ch = source[i]
        if source[i : i + 3] in ('"""', "'''"):
            q3 = source[i : i + 3]
            end = source.find(q3, i + 3)
            end = end + 3 if end != -1 else n
            tokens.append(("str", source[i:end]))
            i = end
        elif ch in ('"', "'"):
            j = i + 1
            while j < n and source[j] != ch:
                if source[j] == "\\":
                    j += 1
                j += 1
            j = min(j + 1, n)
            tokens.append(("str", source[i:j]))
            i = j
        elif ch == "#":
            end = source.find("\n", i)
            end = end if end != -1 else n
            tokens.append(("cmt", source[i:end]))
            i = end
        else:
            nxt = n
            for marker in ('"', "'", "#"):
                pos = source.find(marker, i)
                if pos != -1 and pos < nxt:
                    nxt = pos
            tokens.append(("code", source[i:nxt]))
            i = nxt

    _CODE_PAT = re.compile(
        r"(TOM(?:\.\w+)+)"
        r"|([a-zA-Z_]\w*)"
        r"|(\d+\.?\d*(?:e[+-]?\d+)?)"
        r"|(==|!=|>=|<=|>(?!=)|<(?!=))"
    )

    def _code_repl(m):
        if m.group(1):
            return '<span class="hl-t">' + escape(m.group(1)) + "</span>"
        if m.group(2):
            w = m.group(2)
            ew = escape(w)
            if w in _KW:
                return '<span class="hl-k">' + ew + "</span>"
            if w in _CONST:
                return '<span class="hl-cn">' + ew + "</span>"
            if w in _BUILT:
                return '<span class="hl-bi">' + ew + "</span>"
            return ew
        if m.group(3):
            return '<span class="hl-n">' + escape(m.group(3)) + "</span>"
        if m.group(4):
            return '<span class="hl-o">' + escape(m.group(4)) + "</span>"
        return escape(m.group(0))

    parts = []
    for ttype, text in tokens:
        escaped = escape(text)
        if ttype == "str":
            parts.append('<span class="hl-s">' + escaped + "</span>")
        elif ttype == "cmt":
            parts.append('<span class="hl-c">' + escaped + "</span>")
        else:
            last = 0
            buf = []
            for m in _CODE_PAT.finditer(text):
                if m.start() > last:
                    buf.append(escape(text[last : m.start()]))
                buf.append(_code_repl(m))
                last = m.end()
            if last < len(text):
                buf.append(escape(text[last:]))
            parts.append("".join(buf))

    return "".join(parts)


def _extract_lambda_source(expr):
    """
    Extract the full lambda body from its source file.

    inspect.getsource() fails for multi-line lambdas that use implicit
    line continuation (e.g. ``== 0`` at EOL followed by ``and ...``).
    This function reads the actual source file and parses forward from
    the lambda's starting line, tracking bracket depth to find where the
    lambda body truly ends.
    """
    try:
        source_file = inspect.getfile(expr)
        start_line = expr.__code__.co_firstlineno
    except (TypeError, OSError):
        return None

    try:
        with open(source_file, "r", encoding="utf-8") as f:
            all_lines = f.readlines()
    except (IOError, OSError):
        return None

    # Join from the lambda line onward
    text = "".join(all_lines[start_line - 1 :])

    # Locate "lambda obj, tom:" (or similar) and skip past the colon
    lam_idx = text.find("lambda ")
    if lam_idx < 0:
        return None
    colon_idx = text.find(":", lam_idx + 7)
    if colon_idx < 0:
        return None

    # Parse the lambda body, tracking paren/bracket depth.
    # The body ends at a comma or closing paren at depth 0.
    body_start = colon_idx + 1
    depth = 0
    i = body_start
    n = len(text)

    while i < n:
        ch = text[i]
        if ch in ('"', "'"):
            q3 = text[i : i + 3]
            if q3 in ('"""', "'''"):
                end = text.find(q3, i + 3)
                i = end + 3 if end != -1 else n
            else:
                j = i + 1
                while j < n:
                    if text[j] == "\\":
                        j += 2
                    elif text[j] == ch:
                        j += 1
                        break
                    else:
                        j += 1
                i = j
        elif ch == "#":
            end = text.find("\n", i)
            i = end if end != -1 else n
        elif ch in ("(", "[", "{"):
            depth += 1
            i += 1
        elif ch in (")", "]", "}"):
            if depth == 0:
                break  # closing paren of containing tuple
            depth -= 1
            i += 1
        elif ch == "," and depth == 0:
            break  # comma separating from next tuple element
        else:
            i += 1

    return text[body_start:i]


def _format_rule_source(src):
    """Normalise extracted lambda body with proper indentation."""
    import ast as _ast

    src = src.strip()
    if not src:
        return src

    # Dedent manually first (for multi-line sources)
    lines = src.split("\n")
    while lines and not lines[0].strip():
        lines.pop(0)
    while lines and not lines[-1].strip():
        lines.pop()
    if lines:
        min_indent = min(
            (len(l) - len(l.lstrip()) for l in lines if l.strip()),
            default=0,
        )
        if min_indent > 0:
            lines = [l[min_indent:] if len(l) >= min_indent else l for l in lines]
        src = "\n".join(lines).strip()

    # Round-trip through AST to normalise spacing
    try:
        tree = _ast.parse(src, mode="eval")
        src = _ast.unparse(tree.body)
    except (SyntaxError, ValueError):
        pass  # keep the dedented version

    if len(src) <= 80:
        return src

    # Split at top-level 'and'/'or' (outside parens/strings)
    parts = []
    depth = 0
    seg_start = 0
    seg_kw = ""
    i = 0
    n = len(src)

    while i < n:
        ch = src[i]
        if ch in ("(", "[", "{"):
            depth += 1
            i += 1
        elif ch in (")", "]", "}"):
            depth -= 1
            i += 1
        elif ch in ('"', "'"):
            q3 = src[i : i + 3]
            if q3 in ('"""', "'''"):
                end = src.find(q3, i + 3)
                i = end + 3 if end != -1 else n
            else:
                j = i + 1
                while j < n:
                    if src[j] == "\\":
                        j += 2
                    elif src[j] == ch:
                        j += 1
                        break
                    else:
                        j += 1
                i = j
        elif depth == 0:
            matched = False
            for kw in (" and ", " or "):
                if src[i : i + len(kw)] == kw:
                    parts.append((seg_kw, src[seg_start:i].strip()))
                    seg_kw = kw.strip()
                    i += len(kw)
                    seg_start = i
                    matched = True
                    break
            if not matched:
                i += 1
        else:
            i += 1

    parts.append((seg_kw, src[seg_start:].strip()))

    if len(parts) <= 1:
        return src

    result = []
    for kw, expr in parts:
        if kw:
            result.append(kw + " " + expr)
        else:
            result.append(expr)

    return "\n".join(result)


@log
def bpa(
    dataset: str | UUID,
    rules: Optional[List[dict]] = None,
    workspace: Optional[str | UUID] = None,
    export: bool = False,
    return_dataframe: bool = False,
    extended: bool = False,
    language: Optional[str] = None,
    check_dependencies: bool = True,
):
    """
    Displays an HTML visualization of the results of the Best Practice Analyzer scan for a semantic model.

    The Best Practice Analyzer rules are based on the rules defined `here <https://github.com/microsoft/Analysis-Services/tree/master/BestPracticeRules>`_. The framework for the Best Practice Analyzer and rules are based on the foundation set by `Tabular Editor <https://github.com/TabularEditor/TabularEditor>`_.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    rules : pandas.DataFrame, default=None
        A pandas dataframe containing rules to be evaluated. Defaults to the rules in the model_bpa_rules function.
        Custom rules may be provided in a pandas.DataFrame format.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    export : bool, default=False
        If True, exports the resulting dataframe to a delta table in the lakehouse attached to the notebook.
    return_dataframe : bool, default=False
        If True, returns a pandas dataframe instead of the visualization.
    extended : bool, default=False
        If True, runs the set_vertipaq_annotations function to collect Vertipaq Analyzer statistics to be used in the analysis of the semantic model.
    language : str, default=None
        Specifying a language name or code (i.e. 'it-IT' for Italian) will auto-translate the Category, Rule Name and Description into the specified language.
        Defaults to None which resolves to English.
    check_dependencies : bool, default=True
        If True, leverages the model dependencies from get_model_calc_dependencies to evaluate the rules. Set this parameter to False if running the rules against a semantic model in a shared capacity.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe in HTML format showing semantic model objects which violated the best practice analyzer rules.
    """


    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(
        dataset, workspace=workspace_id
    )

    if check_dependencies:
        dep = get_model_calc_dependencies(
            dataset=dataset_id, workspace=workspace_id
        )
    else:
        dep = pd.DataFrame(
            columns=[
                "Table Name",
                "Object Name",
                "Object Type",
                "Expression",
                "Referenced Table",
                "Referenced Object",
                "Referenced Object Type",
                "Full Object Name",
                "Referenced Full Object Name",
                "Parent Node",
            ]
        )

    rules = bpa_rules.rules

    # Open TOM connection once upfront
    with connect_semantic_model(
        dataset=dataset_id, workspace=workspace_id, readonly=True
    ) as tom:

        if extended:
            tom.set_vertipaq_annotations()

        # Store state for the _execute_bpa callback (triggered by UI Run BPA button)
        _bpa_state.update(
            {
                "dataset_id": str(dataset_id),
                "dataset_name": dataset_name,
                "workspace_id": str(workspace_id),
                "workspace_name": workspace_name,
                "dep": dep,
                "rules": [dict(r) for r in rules],
                "extended": extended,
                "export": export,
                "return_dataframe": return_dataframe,
                "tom": tom,
            }
        )

    # Register globals in the notebook namespace so the Run BPA button
    # can call _execute_bpa() directly without any module imports.
    try:
        _ip = get_ipython()  # noqa: F821
        _ip.user_ns["_bpa_state"] = _bpa_state
        _ip.user_ns["_execute_bpa"] = _execute_bpa
    except Exception:
        pass

    if return_dataframe:
        violations_df = capture_violations(tom, dep, rules)
        return violations_df  # TODO

    # Show the landing page; violations are captured when the user clicks Run BPA
    if not export:
        _show_landing(rules, dataset_name, str(workspace_id))


def _execute_bpa():
    """
    Callback triggered by the Run BPA button in the landing page.

    References the tom, dep, and rules that were already captured
    in bpa() and stored in _bpa_state.
    """

    state = _bpa_state
    tom = state["tom"]
    dep = state["dep"]
    rules = state["rules"]
    workspace_name = state["workspace_name"]
    workspace_id = state["workspace_id"]
    dataset_name = state["dataset_name"]

    if tom.model.Tables.Count == 0:
        print(
            f"{icons.warning} The '{dataset_name}' semantic model within the "
            f"'{workspace_name}' workspace has no tables and therefore there "
            f"are no valid BPA results."
        )
        return

    violations_df = capture_violations(tom, dep, rules)
    visualize_bpa(rules, violations_df, dataset_name, workspace_id)


def _show_landing(rules, dataset_name, workspace_id):
    """
    Render the BPA landing page with Run BPA, Edit Rules, and Import Rules.

    When the user clicks Run BPA, JavaScript communicates with the Python
    kernel to call ``_execute_bpa()`` which captures violations and renders
    the results view.
    """

    # Build rules payload for the editor
    all_rules_for_editor = []
    for rule in rules:
        sev = rule.get("Severity", "")
        scope = rule.get("Scope", "")
        if isinstance(scope, list):
            scope_str = ", ".join(scope)
        else:
            scope_str = scope
        all_rules_for_editor.append(
            {
                "Category": rule.get("Category", ""),
                "Scope": scope_str,
                "Severity": sev,
                "Name": rule.get("Name", ""),
                "Expression": rule.get("Expression", ""),
                "Description": rule.get("Description", "") or "",
                "URL": rule.get("URL", "") or "",
            }
        )

    all_rules_json = json.dumps(all_rules_for_editor)
    dataset_name_json = json.dumps(dataset_name)
    workspace_id_json = json.dumps(workspace_id)

    html_output = f"""
    <style>
        .bpa-root {{
            font-family: -apple-system, BlinkMacSystemFont, "SF Pro Display", "SF Pro Text", "Helvetica Neue", Arial, sans-serif;
            color: #1d1d1f;
            max-width: 100%;
            -webkit-font-smoothing: antialiased;
            -moz-osx-font-smoothing: grayscale;
        }}

        /* ── Landing page ── */
        .bpa-landing {{
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            padding: 60px 20px;
            text-align: center;
        }}
        .bpa-landing-icon {{
            margin-bottom: 16px;
            opacity: 0.85;
        }}
        .bpa-landing-title {{
            font-size: 22px;
            font-weight: 700;
            color: #1d1d1f;
            margin: 0 0 8px 0;
        }}
        .bpa-landing-desc {{
            font-size: 14px;
            color: #6e6e73;
            margin: 0 0 24px 0;
            max-width: 420px;
        }}
        .bpa-run-btn {{
            display: inline-flex;
            align-items: center;
            gap: 8px;
            padding: 12px 28px;
            background: #0071e3;
            color: #fff;
            border: none;
            border-radius: 10px;
            font-size: 15px;
            font-weight: 600;
            cursor: pointer;
            transition: background 0.2s, transform 0.1s;
            font-family: inherit;
        }}
        .bpa-run-btn:hover {{ background: #0077ed; transform: scale(1.02); }}
        .bpa-run-btn:active {{ transform: scale(0.98); }}
        .bpa-run-btn:disabled {{
            background: #86868b;
            cursor: not-allowed;
            transform: none;
        }}
        .bpa-editor-btn {{
            padding: 8px 18px;
            border: 1px solid #d2d2d7;
            border-radius: 8px;
            background: #fff;
            font-size: 13px;
            font-weight: 500;
            color: #0071e3;
            cursor: pointer;
            transition: all 0.2s;
            display: inline-flex;
            align-items: center;
            gap: 6px;
        }}
        .bpa-editor-btn:hover {{ background: #f0f5ff; border-color: #0071e3; }}
        .bpa-import-btn {{
            display: inline-flex;
            align-items: center;
            gap: 6px;
            padding: 8px 20px;
            background: #fff;
            color: #1d1d1f;
            border: 1px solid #d2d2d7;
            border-radius: 8px;
            font-size: 13px;
            font-weight: 500;
            cursor: pointer;
            transition: all 0.15s;
            font-family: inherit;
            margin-top: 8px;
        }}
        .bpa-import-btn:hover {{ background: #f5f5f7; }}
        .bpa-import-btn svg {{ width: 14px; height: 14px; }}
        .bpa-import-status {{
            font-size: 12px;
            margin-top: 10px;
            padding: 6px 14px;
            border-radius: 8px;
            display: none;
        }}
        .bpa-import-status.success {{
            display: inline-block;
            background: #f0fff4;
            color: #34c759;
            border: 1px solid #34c759;
        }}
        .bpa-import-status.error {{
            display: inline-block;
            background: #fff5f5;
            color: #ff3b30;
            border: 1px solid #ff3b30;
        }}

        /* ── Spinner ── */
        @keyframes bpa-spin {{
            to {{ transform: rotate(360deg); }}
        }}
        .bpa-spinner {{
            width: 28px;
            height: 28px;
            border: 3px solid #e8e8ed;
            border-top-color: #0071e3;
            border-radius: 50%;
            animation: bpa-spin 0.8s linear infinite;
            margin: 0 auto;
        }}

        /* ── Rule Editor ── */
        .bpa-editor-overlay {{
            display: none;
            position: fixed;
            top: 0; left: 0; right: 0; bottom: 0;
            background: rgba(0,0,0,0.4);
            z-index: 9999;
            justify-content: center;
            align-items: flex-start;
            padding: 40px 20px;
            overflow-y: auto;
        }}
        .bpa-editor-overlay.open {{ display: flex; }}
        .bpa-editor-panel {{
            background: #fff;
            border-radius: 16px;
            box-shadow: 0 8px 40px rgba(0,0,0,0.18);
            width: 100%;
            max-width: 1100px;
            height: 92vh;
            max-height: 92vh;
            display: flex;
            flex-direction: column;
            overflow: hidden;
        }}
        .bpa-editor-topbar {{
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 16px 24px;
            border-bottom: 1px solid #e8e8ed;
            flex-shrink: 0;
        }}
        .bpa-editor-topbar h2 {{
            margin: 0;
            font-size: 18px;
            font-weight: 600;
            color: #1d1d1f;
        }}
        .bpa-editor-topbar-actions {{
            display: flex;
            gap: 8px;
            align-items: center;
        }}
        .bpa-editor-close {{
            background: none;
            border: none;
            font-size: 22px;
            color: #86868b;
            cursor: pointer;
            padding: 4px 8px;
            border-radius: 6px;
            line-height: 1;
        }}
        .bpa-editor-close:hover {{ background: #f0f0f5; color: #1d1d1f; }}
        .bpa-editor-header {{
            flex-shrink: 0;
            padding: 0 24px;
            background: #fff;
        }}
        .bpa-editor-body {{
            overflow-y: auto;
            padding: 0 24px 16px 24px;
            flex: 1;
        }}
        .bpa-editor-table {{
            width: 100%;
            table-layout: fixed;
            border-collapse: separate;
            border-spacing: 0;
            font-size: 12px;
        }}
        .bpa-editor-table col.col-name {{ width: 20%; }}
        .bpa-editor-table col.col-category {{ width: 14%; }}
        .bpa-editor-table col.col-scope {{ width: 12%; }}
        .bpa-editor-table col.col-severity {{ width: 10%; }}
        .bpa-editor-table col.col-expression {{ width: 30%; }}
        .bpa-editor-table col.col-actions {{ width: 14%; }}
        .bpa-editor-table th {{
            text-align: left;
            padding: 8px 10px;
            font-weight: 600;
            color: #86868b;
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 0.3px;
            border-bottom: 2px solid #e8e8ed;
            white-space: nowrap;
            background: #fff;
        }}
        .bpa-editor-table td {{
            text-align: left;
            padding: 8px 10px;
            border-bottom: 1px solid #f0f0f5;
            vertical-align: top;
            color: #1d1d1f;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }}
        .bpa-editor-table tr:hover td {{ background: #fafafa; }}
        .bpa-editor-table .bpa-ed-actions {{
            white-space: nowrap;
            display: flex;
            gap: 4px;
        }}
        .bpa-ed-btn {{
            padding: 4px 10px;
            border: 1px solid #d2d2d7;
            border-radius: 6px;
            background: #fff;
            font-size: 11px;
            cursor: pointer;
            transition: all 0.15s;
            color: #1d1d1f;
        }}
        .bpa-ed-btn:hover {{ border-color: #0071e3; color: #0071e3; }}
        .bpa-ed-btn.danger {{ color: #ff3b30; }}
        .bpa-ed-btn.danger:hover {{ border-color: #ff3b30; background: #fff5f5; }}
        .bpa-ed-btn.primary {{
            background: #0071e3;
            color: #fff;
            border-color: #0071e3;
        }}
        .bpa-ed-btn.primary:hover {{ background: #0077ed; }}
        .bpa-ed-btn.add {{
            background: #fff;
            color: #34c759;
            border-color: #34c759;
            font-weight: 600;
        }}
        .bpa-ed-btn.add:hover {{ background: #f0fff4; }}
        .bpa-save-status {{
            font-size: 12px;
            padding: 6px 14px;
            border-radius: 8px;
            display: none;
            align-items: center;
            gap: 6px;
        }}
        .bpa-save-status.success {{
            display: inline-flex;
            background: #f0fff4;
            color: #34c759;
            border: 1px solid #34c759;
        }}
        .bpa-save-status.error {{
            display: inline-flex;
            background: #fff5f5;
            color: #ff3b30;
            border: 1px solid #ff3b30;
        }}

        /* ── Rule Form (modal inside editor) ── */
        .bpa-form-overlay {{
            display: none;
            position: fixed;
            top: 0; left: 0; right: 0; bottom: 0;
            background: rgba(0,0,0,0.35);
            z-index: 10001;
            justify-content: center;
            align-items: center;
            padding: 20px;
        }}
        .bpa-form-overlay.open {{ display: flex; }}
        .bpa-form-panel {{
            background: #fff;
            border-radius: 14px;
            box-shadow: 0 8px 40px rgba(0,0,0,0.2);
            width: 100%;
            max-width: 640px;
            max-height: 92vh;
            overflow-y: auto;
            padding: 24px;
        }}
        .bpa-form-panel h3 {{
            margin: 0 0 18px 0;
            font-size: 16px;
            font-weight: 600;
            color: #1d1d1f;
        }}
        .bpa-form-group {{
            margin-bottom: 14px;
        }}
        .bpa-form-group label {{
            display: block;
            font-size: 12px;
            font-weight: 600;
            color: #6e6e73;
            margin-bottom: 4px;
            text-transform: uppercase;
            letter-spacing: 0.3px;
        }}
        .bpa-form-group label .bpa-req {{
            color: #ff3b30;
            margin-left: 2px;
        }}
        .bpa-form-group input,
        .bpa-form-group textarea,
        .bpa-form-group select {{
            width: 100%;
            padding: 8px 12px;
            border: 1px solid #d2d2d7;
            border-radius: 8px;
            font-size: 13px;
            font-family: inherit;
            color: #1d1d1f;
            box-sizing: border-box;
            transition: border-color 0.2s;
        }}
        .bpa-form-group input:focus,
        .bpa-form-group textarea:focus,
        .bpa-form-group select:focus {{
            outline: none;
            border-color: #0071e3;
            box-shadow: 0 0 0 3px rgba(0,113,227,0.15);
        }}
        .bpa-form-group textarea {{
            min-height: 70px;
            resize: vertical;
            font-family: "SF Mono", SFMono-Regular, Menlo, Consolas, monospace;
            font-size: 12px;
        }}
        .bpa-form-group .bpa-form-error {{
            color: #ff3b30;
            font-size: 11px;
            margin-top: 3px;
            display: none;
        }}
        .bpa-form-actions {{
            display: flex;
            gap: 8px;
            justify-content: flex-end;
            margin-top: 20px;
        }}
    </style>

    <div class="bpa-root" id="bpaRoot">
        <div class="bpa-landing" id="bpaLanding">
            <div class="bpa-landing-icon">
                <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="#0071e3" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/><polyline points="9 12 11 14 15 10"/></svg>
            </div>
            <h2 class="bpa-landing-title">Best Practice Analyzer</h2>
            <p class="bpa-landing-desc">Click below to analyze your semantic model against best practice rules, or edit the rules first.</p>
            <button class="bpa-run-btn" id="runBpaBtn">
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="5 3 19 12 5 21 5 3"/></svg>
                Run BPA
            </button>
            <button class="bpa-editor-btn" id="openEditorBtnLanding" style="margin-top: 12px;">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>
                Edit Rules
            </button>
            <input type="file" id="importRulesFile" accept=".json" style="display:none;"/>
            <button class="bpa-import-btn" id="importRulesBtn">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg>
                Import Rules
            </button>
            <span class="bpa-import-status" id="importStatus"></span>
            <div id="ruleCountDisplay" style="margin-top: 28px; display: inline-flex; align-items: center; gap: 8px; padding: 8px 20px; background: #f0f0f5; border-radius: 20px;">
                <span style="font-size: 22px; font-weight: 700; color: #0071e3;">{len(rules)}</span>
                <span style="font-size: 14px; font-weight: 500; color: #6e6e73;">{'rule' if len(rules) == 1 else 'rules'} loaded</span>
            </div>
        </div>
    </div>

    <!-- Rule Editor Overlay -->
    <div class="bpa-editor-overlay" id="editorOverlay">
        <div class="bpa-editor-panel">
            <div class="bpa-editor-topbar">
                <h2>Rule Editor</h2>
                <div class="bpa-editor-topbar-actions">
                    <span class="bpa-save-status" id="editorSaveStatus"></span>
                    <button class="bpa-ed-btn add" id="addRuleBtn">+ Add Rule</button>
                    <button class="bpa-ed-btn primary" id="saveRulesBtn">
                        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
                        Save Rules
                    </button>
                    <button class="bpa-editor-close" id="closeEditorBtn">&times;</button>
                </div>
            </div>
            <div class="bpa-editor-header">
                <table class="bpa-editor-table">
                    <colgroup>
                        <col class="col-name"/>
                        <col class="col-category"/>
                        <col class="col-scope"/>
                        <col class="col-severity"/>
                        <col class="col-expression"/>
                        <col class="col-actions"/>
                    </colgroup>
                    <thead>
                        <tr>
                            <th>Name</th>
                            <th>Category</th>
                            <th>Scope</th>
                            <th>Severity</th>
                            <th>Expression</th>
                            <th>Actions</th>
                        </tr>
                    </thead>
                </table>
            </div>
            <div class="bpa-editor-body">
                <table class="bpa-editor-table">
                    <colgroup>
                        <col class="col-name"/>
                        <col class="col-category"/>
                        <col class="col-scope"/>
                        <col class="col-severity"/>
                        <col class="col-expression"/>
                        <col class="col-actions"/>
                    </colgroup>
                    <tbody id="editorTableBody"></tbody>
                </table>
            </div>
        </div>
    </div>

    <!-- Rule Form Overlay (for Add/Edit) -->
    <div class="bpa-form-overlay" id="formOverlay">
        <div class="bpa-form-panel">
            <h3 id="formTitle">Edit Rule</h3>
            <input type="hidden" id="formIdx" value="-1"/>
            <div class="bpa-form-group">
                <label>Name <span class="bpa-req">*</span></label>
                <input type="text" id="formName" placeholder="Rule name"/>
                <div class="bpa-form-error" id="formNameErr">Name is required.</div>
            </div>
            <div class="bpa-form-group">
                <label>Category <span class="bpa-req">*</span></label>
                <input type="text" id="formCategory" placeholder="e.g. Performance"/>
                <div class="bpa-form-error" id="formCategoryErr">Category is required.</div>
            </div>
            <div class="bpa-form-group">
                <label>Scope <span class="bpa-req">*</span> (comma-separated for multiple)</label>
                <input type="text" id="formScope" placeholder="e.g. Column, Measure"/>
                <div class="bpa-form-error" id="formScopeErr">Scope is required.</div>
            </div>
            <div class="bpa-form-group">
                <label>Severity <span class="bpa-req">*</span></label>
                <select id="formSeverity">
                    <option value="">-- Select --</option>
                    <option value="Warning">Warning</option>
                    <option value="Error">Error</option>
                    <option value="Info">Info</option>
                </select>
                <div class="bpa-form-error" id="formSeverityErr">Severity is required.</div>
            </div>
            <div class="bpa-form-group">
                <label>Expression <span class="bpa-req">*</span></label>
                <textarea id="formExpression" placeholder="Python expression..."></textarea>
                <div class="bpa-form-error" id="formExpressionErr">Expression is required.</div>
            </div>
            <div class="bpa-form-group">
                <label>Description</label>
                <textarea id="formDescription" placeholder="Optional description..." style="font-family:inherit;"></textarea>
            </div>
            <div class="bpa-form-group">
                <label>URL</label>
                <input type="text" id="formURL" placeholder="Optional reference URL"/>
            </div>
            <div class="bpa-form-actions">
                <button class="bpa-ed-btn" id="formCancelBtn">Cancel</button>
                <button class="bpa-ed-btn primary" id="formSaveBtn">Save</button>
            </div>
        </div>
    </div>

    <script>
    (function() {{
        var ALL_RULES = {all_rules_json};
        var DATASET_NAME = {dataset_name_json};
        var WORKSPACE_ID = {workspace_id_json};
        var rulesModified = false;

        function updateRuleCount() {{
            var el = document.getElementById('ruleCountDisplay');
            if (el) {{
                var n = ALL_RULES.length;
                el.innerHTML = '<span style="font-size:22px;font-weight:700;color:#0071e3;">' + n + '</span>' +
                    '<span style="font-size:14px;font-weight:500;color:#6e6e73;">' + (n === 1 ? 'rule' : 'rules') + ' loaded</span>';
            }}
        }}

        /* ── Run BPA via kernel ── */
        document.getElementById('runBpaBtn').addEventListener('click', function() {{
            var btn = this;
            btn.disabled = true;
            btn.innerHTML = '<span class="bpa-spinner" style="width:16px;height:16px;border-width:2px;display:inline-block;vertical-align:middle;"></span> Running\u2026';

            // Build kernel command referencing the global _execute_bpa / _bpa_state
            var cmd = '';
            if (rulesModified) {{
                // Push edited/imported rules into the already-registered global state
                var rulesForPython = ALL_RULES.map(function(r) {{
                    var scopeVal = r.Scope;
                    if (typeof scopeVal === 'string' && scopeVal.indexOf(',') !== -1) {{
                        scopeVal = scopeVal.split(',').map(function(s) {{ return s.trim(); }});
                    }}
                    return {{
                        Category: r.Category,
                        Scope: scopeVal,
                        Severity: r.Severity,
                        Name: r.Name,
                        Expression: r.Expression,
                        Description: r.Description || null,
                        URL: r.URL || null
                    }};
                }});
                var rulesB64 = btoa(unescape(encodeURIComponent(JSON.stringify(rulesForPython))));
                cmd += 'import json as _j, base64 as _b\\n'
                    + '_bpa_state["rules"] = _j.loads(_b.b64decode("' + rulesB64 + '").decode("utf-8"))\\n';
            }}
            cmd += '_execute_bpa()';

            var kernel = null;
            try {{
                if (typeof Jupyter !== 'undefined' && Jupyter.notebook && Jupyter.notebook.kernel) {{
                    kernel = Jupyter.notebook.kernel;
                }} else if (typeof IPython !== 'undefined' && IPython.notebook && IPython.notebook.kernel) {{
                    kernel = IPython.notebook.kernel;
                }}
            }} catch(e) {{}}

            if (kernel) {{
                kernel.execute(cmd);
            }} else {{
                var statusEl = document.getElementById('importStatus');
                statusEl.textContent = 'Could not access notebook kernel. Please run _execute_bpa() in a cell.';
                statusEl.className = 'bpa-import-status error';
                statusEl.style.display = '';
                btn.disabled = false;
                btn.innerHTML = '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="5 3 19 12 5 21 5 3"/></svg> Run BPA';
            }}
        }});

        /* ── Import Rules Logic ── */
        var REQUIRED_KEYS = ['Category', 'Scope', 'Severity', 'Name', 'Expression'];
        var importFileInput = document.getElementById('importRulesFile');
        var importStatus = document.getElementById('importStatus');

        document.getElementById('importRulesBtn').addEventListener('click', function() {{
            importFileInput.value = '';
            importFileInput.click();
        }});

        importFileInput.addEventListener('change', function(e) {{
            var file = e.target.files[0];
            if (!file) return;
            if (!file.name.toLowerCase().endsWith('.json')) {{
                importStatus.textContent = 'Invalid file type. Please select a .json file.';
                importStatus.className = 'bpa-import-status error';
                return;
            }}
            var reader = new FileReader();
            reader.onload = function(ev) {{
                try {{
                    var parsed = JSON.parse(ev.target.result);
                }} catch (err) {{
                    importStatus.textContent = 'Invalid JSON: ' + err.message;
                    importStatus.className = 'bpa-import-status error';
                    return;
                }}
                if (!Array.isArray(parsed) || parsed.length === 0) {{
                    importStatus.textContent = 'Rules file must be a non-empty JSON array.';
                    importStatus.className = 'bpa-import-status error';
                    return;
                }}
                for (var i = 0; i < parsed.length; i++) {{
                    var rule = parsed[i];
                    if (typeof rule !== 'object' || rule === null || Array.isArray(rule)) {{
                        importStatus.textContent = 'Rule #' + (i + 1) + ' is not a valid object.';
                        importStatus.className = 'bpa-import-status error';
                        return;
                    }}
                    for (var k = 0; k < REQUIRED_KEYS.length; k++) {{
                        if (!(REQUIRED_KEYS[k] in rule) || !rule[REQUIRED_KEYS[k]]) {{
                            importStatus.textContent = 'Rule #' + (i + 1) + ' is missing required key: ' + REQUIRED_KEYS[k];
                            importStatus.className = 'bpa-import-status error';
                            return;
                        }}
                    }}
                }}
                var imported = parsed.map(function(r) {{
                    var scope = r.Scope;
                    if (Array.isArray(scope)) scope = scope.join(', ');
                    return {{
                        Category: r.Category || '',
                        Scope: scope || '',
                        Severity: r.Severity || '',
                        Name: r.Name || '',
                        Expression: r.Expression || '',
                        Description: r.Description || '',
                        URL: r.URL || ''
                    }};
                }});
                ALL_RULES = imported;
                rulesModified = true;
                renderEditorTable();
                updateRuleCount();
                importStatus.textContent = 'Imported ' + imported.length + ' rule' + (imported.length !== 1 ? 's' : '') + ' from ' + file.name;
                importStatus.className = 'bpa-import-status success';
            }};
            reader.readAsText(file);
        }});

        /* ── Rule Editor Logic ── */
        var editorOverlay = document.getElementById('editorOverlay');
        var formOverlay = document.getElementById('formOverlay');
        var editorTableBody = document.getElementById('editorTableBody');
        var editorSaveStatus = document.getElementById('editorSaveStatus');

        function openEditor() {{
            renderEditorTable();
            editorOverlay.classList.add('open');
        }}
        document.getElementById('openEditorBtnLanding').addEventListener('click', openEditor);
        document.getElementById('closeEditorBtn').addEventListener('click', function() {{
            editorOverlay.classList.remove('open');
            editorSaveStatus.className = 'bpa-save-status';
            editorSaveStatus.style.display = 'none';
        }});
        editorOverlay.addEventListener('click', function(e) {{
            if (e.target === editorOverlay) {{
                editorOverlay.classList.remove('open');
                editorSaveStatus.className = 'bpa-save-status';
                editorSaveStatus.style.display = 'none';
            }}
        }});

        function escHtml(str) {{
            if (!str) return '';
            var d = document.createElement('div');
            d.textContent = str;
            return d.innerHTML;
        }}

        function renderEditorTable() {{
            editorTableBody.innerHTML = '';
            ALL_RULES.forEach(function(rule, idx) {{
                var tr = document.createElement('tr');
                tr.innerHTML =
                    '<td title="' + escHtml(rule.Name) + '">' + escHtml(rule.Name) + '</td>' +
                    '<td>' + escHtml(rule.Category) + '</td>' +
                    '<td>' + escHtml(rule.Scope) + '</td>' +
                    '<td>' + escHtml(rule.Severity) + '</td>' +
                    '<td title="' + escHtml(rule.Expression) + '">' + escHtml(rule.Expression).substring(0, 50) + (rule.Expression.length > 50 ? '...' : '') + '</td>' +
                    '<td class="bpa-ed-actions"></td>';
                var actTd = tr.querySelector('.bpa-ed-actions');
                var editBtn = document.createElement('button');
                editBtn.className = 'bpa-ed-btn';
                editBtn.textContent = 'Edit';
                editBtn.addEventListener('click', (function(i) {{ return function() {{ openForm(i); }}; }})(idx));
                var delBtn = document.createElement('button');
                delBtn.className = 'bpa-ed-btn danger';
                delBtn.textContent = 'Delete';
                delBtn.addEventListener('click', (function(i) {{ return function() {{ deleteRule(i); }}; }})(idx));
                actTd.appendChild(editBtn);
                actTd.appendChild(delBtn);
                editorTableBody.appendChild(tr);
            }});
        }}

        function downloadRulesJson() {{
            var rulesForSave = ALL_RULES.map(function(r) {{
                var scopeVal = r.Scope;
                if (scopeVal.indexOf(',') !== -1) {{
                    scopeVal = scopeVal.split(',').map(function(s) {{ return s.trim(); }});
                }}
                return {{
                    Category: r.Category,
                    Scope: scopeVal,
                    Severity: r.Severity,
                    Name: r.Name,
                    Expression: r.Expression,
                    Description: r.Description || null,
                    URL: r.URL || null
                }};
            }});
            var jsonStr = JSON.stringify(rulesForSave, null, 4);
            var blob = new Blob([jsonStr], {{ type: 'application/json' }});
            var dlUrl = URL.createObjectURL(blob);
            var a = document.createElement('a');
            a.href = dlUrl;
            a.download = 'rules.json';
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(dlUrl);
            return 'rules.json';
        }}

        function deleteRule(idx) {{
            var rows = document.getElementById('editorTableBody').querySelectorAll('tr');
            if (!rows[idx]) return;
            var actTd = rows[idx].querySelector('td:last-child');
            if (actTd.querySelector('.bpa-confirm-delete')) return;

            var wrapper = document.createElement('span');
            wrapper.className = 'bpa-confirm-delete';
            wrapper.style.cssText = 'display:inline-flex;gap:4px;align-items:center;';
            var label = document.createElement('span');
            label.textContent = 'Delete?';
            label.style.cssText = 'color:#ff3b30;font-size:11px;font-weight:600;';
            var yesBtn = document.createElement('button');
            yesBtn.className = 'bpa-ed-btn danger';
            yesBtn.textContent = 'Yes';
            yesBtn.style.cssText = 'padding:2px 8px;font-size:11px;';
            var noBtn = document.createElement('button');
            noBtn.className = 'bpa-ed-btn';
            noBtn.textContent = 'No';
            noBtn.style.cssText = 'padding:2px 8px;font-size:11px;';
            wrapper.appendChild(label);
            wrapper.appendChild(yesBtn);
            wrapper.appendChild(noBtn);
            actTd.innerHTML = '';
            actTd.appendChild(wrapper);

            yesBtn.addEventListener('click', function() {{
                ALL_RULES.splice(idx, 1);
                rulesModified = true;
                renderEditorTable();
                updateRuleCount();
            }});
            noBtn.addEventListener('click', function() {{
                renderEditorTable();
            }});
        }}

        // Form logic
        document.getElementById('addRuleBtn').addEventListener('click', function() {{
            openForm(-1);
        }});
        document.getElementById('formCancelBtn').addEventListener('click', function() {{
            formOverlay.classList.remove('open');
        }});
        formOverlay.addEventListener('click', function(e) {{
            if (e.target === formOverlay) formOverlay.classList.remove('open');
        }});

        function openForm(idx) {{
            var title = document.getElementById('formTitle');
            document.getElementById('formIdx').value = idx;
            ['formNameErr','formCategoryErr','formScopeErr','formSeverityErr','formExpressionErr'].forEach(function(id) {{
                document.getElementById(id).style.display = 'none';
            }});
            if (idx >= 0) {{
                title.textContent = 'Edit Rule';
                var r = ALL_RULES[idx];
                document.getElementById('formName').value = r.Name || '';
                document.getElementById('formCategory').value = r.Category || '';
                document.getElementById('formScope').value = r.Scope || '';
                document.getElementById('formSeverity').value = r.Severity || '';
                document.getElementById('formExpression').value = r.Expression || '';
                document.getElementById('formDescription').value = r.Description || '';
                document.getElementById('formURL').value = r.URL || '';
            }} else {{
                title.textContent = 'Add New Rule';
                document.getElementById('formName').value = '';
                document.getElementById('formCategory').value = '';
                document.getElementById('formScope').value = '';
                document.getElementById('formSeverity').value = '';
                document.getElementById('formExpression').value = '';
                document.getElementById('formDescription').value = '';
                document.getElementById('formURL').value = '';
            }}
            formOverlay.classList.add('open');
        }}

        document.getElementById('formSaveBtn').addEventListener('click', function() {{
            var idx = parseInt(document.getElementById('formIdx').value, 10);
            var name = document.getElementById('formName').value.trim();
            var category = document.getElementById('formCategory').value.trim();
            var scope = document.getElementById('formScope').value.trim();
            var severity = document.getElementById('formSeverity').value.trim();
            var expression = document.getElementById('formExpression').value.trim();
            var description = document.getElementById('formDescription').value.trim();
            var url = document.getElementById('formURL').value.trim();

            var valid = true;
            if (!name) {{ document.getElementById('formNameErr').style.display = 'block'; valid = false; }}
            else {{ document.getElementById('formNameErr').style.display = 'none'; }}
            if (!category) {{ document.getElementById('formCategoryErr').style.display = 'block'; valid = false; }}
            else {{ document.getElementById('formCategoryErr').style.display = 'none'; }}
            if (!scope) {{ document.getElementById('formScopeErr').style.display = 'block'; valid = false; }}
            else {{ document.getElementById('formScopeErr').style.display = 'none'; }}
            if (!severity) {{ document.getElementById('formSeverityErr').style.display = 'block'; valid = false; }}
            else {{ document.getElementById('formSeverityErr').style.display = 'none'; }}
            if (!expression) {{ document.getElementById('formExpressionErr').style.display = 'block'; valid = false; }}
            else {{ document.getElementById('formExpressionErr').style.display = 'none'; }}
            if (!valid) return;

            var ruleObj = {{
                Name: name,
                Category: category,
                Scope: scope,
                Severity: severity,
                Expression: expression,
                Description: description || '',
                URL: url || ''
            }};

            if (idx >= 0) {{
                ALL_RULES[idx] = ruleObj;
            }} else {{
                ALL_RULES.push(ruleObj);
            }}
            rulesModified = true;
            renderEditorTable();
            updateRuleCount();
            formOverlay.classList.remove('open');
        }});

        // Save Rules (download)
        document.getElementById('saveRulesBtn').addEventListener('click', function() {{
            var fileName = downloadRulesJson();
            editorSaveStatus.textContent = 'Downloaded ' + fileName;
            editorSaveStatus.className = 'bpa-save-status success';
            editorSaveStatus.style.display = '';
        }});
    }})();
    </script>
    """

    return display(HTML(html_output))


def get_rule_logic_map(rules):
    """Build a mapping from rule name to syntax-highlighted Python source."""
    rule_logic_map = {}
    for rule in rules:
        rname = rule.get("Name")
        expr = rule.get("Expression")
        src = None
        if expr and isinstance(expr, str):
            try:
                src = _format_rule_source(expr)
            except Exception:
                src = expr
        rule_logic_map[rname] = _highlight_python(src.strip()) if src else ""
    return rule_logic_map


def capture_violations(tom, dep, rules):

    violations = pd.DataFrame(columns=["Object Name", "Scope", "Rule Name"])
    rules_df = pd.DataFrame(rules, columns=["Category", "Scope", "Severity", "Name", "Expression", "Description", "URL"])

    scope_to_dataframe = {
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
        "Model": (tom.model, lambda obj: obj.Model.Name),
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
        "Function": (
            tom.all_functions(),
            lambda obj: obj.Name,
        ),
    }

    import sempy
    sempy.fabric._client._utils._init_analysis_services()
    import Microsoft.AnalysisServices.Tabular as TOM

    rows = []
    for rule in rules:
        ruleName = rule.get("Name")
        expr_text = rule.get("Expression")
        scopes = rule.get("Scope")
        expr = eval(f"lambda obj, tom, dep, TOM, re: ({expr_text})")

        if isinstance(scopes, str):
            scopes = [scopes]

        for scope in scopes:
            func = scope_to_dataframe[scope][0]
            nm = scope_to_dataframe[scope][1]

            if scope == "Model":
                x = []
                if expr(func, tom, dep, TOM, re):
                    x = ["Model"]
            elif scope == "Measure":
                x = [
                    nm(obj)
                    for obj in tom.all_measures()
                    if expr(obj, tom, dep, TOM, re)
                ]
            elif scope == "Function":
                x = [
                    nm(obj)
                    for obj in tom.all_functions()
                    if expr(obj, tom, dep, TOM, re)
                ]
            elif scope == "Column":
                x = [
                    nm(obj)
                    for obj in tom.all_columns()
                    if expr(obj, tom, dep, TOM, re)
                ]
            elif scope == "Partition":
                x = [
                    nm(obj)
                    for obj in tom.all_partitions()
                    if expr(obj, tom, dep, TOM, re)
                ]
            elif scope == "Hierarchy":
                x = [
                    nm(obj)
                    for obj in tom.all_hierarchies()
                    if expr(obj, tom, dep, TOM, re)
                ]
            elif scope == "Table":
                x = [
                    nm(obj)
                    for obj in tom.model.Tables
                    if expr(obj, tom, dep, TOM, re)
                ]
            elif scope == "Calculated Table":
                x = [
                    nm(obj)
                    for obj in tom.all_calculated_tables()
                    if expr(obj, tom, dep, TOM, re)
                ]
            elif scope == "Relationship":
                x = [
                    nm(obj)
                    for obj in tom.model.Relationships
                    if expr(obj, tom, dep, TOM, re)
                ]
            elif scope == "Role":
                x = [
                    nm(obj)
                    for obj in tom.model.Roles
                    if expr(obj, tom, dep, TOM, re)
                ]
            elif scope == "Row Level Security":
                x = [
                    nm(obj) for obj in tom.all_rls() if expr(obj, tom, dep, TOM, re)
                ]
            elif scope == "Calculation Item":
                x = [
                    nm(obj)
                    for obj in tom.all_calculation_items()
                    if expr(obj, tom, dep, TOM, re)
                ]
            elif scope == "Calculated Column":
                x = [
                    nm(obj)
                    for obj in tom.all_calculated_columns()
                    if expr(obj, tom, dep, TOM, re)
                ]

            if len(x) > 0:
                rows.append({
                    "Object Name": x,
                    "Scope": scope,
                    "Rule Name": ruleName,
                })

    if rows:
        violations = pd.DataFrame(rows, columns=["Object Name", "Scope", "Rule Name"])

    prepDF = pd.merge(
        violations,
        rules_df[["Name", "Category", "Severity", "Description", "URL"]],
        left_on="Rule Name",
        right_on="Name",
        how="left",
    )

    prepDF.rename(columns={"Scope": "Object Type"}, inplace=True)

    prepDF["Object Type"] = (
        prepDF["Object Type"].map(_plural_map).fillna(prepDF["Object Type"])
    )
    finalDF = prepDF[
        [
            "Category",
            "Rule Name",
            "Severity",
            "Object Type",
            "Object Name",
            "Description",
            "URL",
        ]
    ]

    finalDF = (
        finalDF[
            [
                "Category",
                "Rule Name",
                "Object Type",
                "Object Name",
                "Severity",
                "Description",
                "URL",
            ]
        ]
        .sort_values(["Category", "Rule Name", "Object Type", "Object Name"])
        .set_index(["Category", "Rule Name"])
    )

    violations_df = finalDF.reset_index()

    return violations_df


def visualize_bpa(rules, violations_df, dataset_name="", workspace_id=""):

    # Map severity text to icons for display (without mutating original rules)
    sev_icon_map = {}
    for rule in rules:
        sev = rule.get("Severity", "")
        if sev in severity_map:
            sev_icon_map[sev] = severity_map[sev]

    rule_logic_map = get_rule_logic_map(rules)

    # Replace severity text with icons in violations_df for display
    violations_display = violations_df.copy()
    violations_display["Severity"] = violations_display["Severity"].replace(sev_icon_map)

    bpa_dict = {
        cat: violations_display[violations_display["Category"] == cat].drop("Category", axis=1)
        for cat in violations_display["Category"].drop_duplicates().values
    }

    # Collect unique filter values
    all_rule_names = sorted(violations_display["Rule Name"].dropna().unique().tolist())
    all_object_types = sorted(violations_display["Object Type"].dropna().unique().tolist())
    all_severities = sorted(violations_display["Severity"].dropna().unique().tolist())

    # Build data payload for JS
    categories_data = {}
    for cat, df in bpa_dict.items():
        if df.shape[0] == 0:
            continue
        rows = []
        for _, row in df.iterrows():
            rows.append(
                {
                    "rule": str(row["Rule Name"]),
                    "objectType": str(row["Object Type"]),
                    "objectName": str(row["Object Name"]),
                    "severity": str(row["Severity"]),
                    "description": (
                        str(row["Description"])
                        if pd.notnull(row["Description"])
                        else ""
                    ),
                    "url": str(row["URL"]) if pd.notnull(row["URL"]) else "",
                }
            )
        categories_data[cat] = rows

    # Build the full rules payload for the editor (already text severity)
    all_rules_for_editor = []
    for rule in rules:
        sev = rule.get("Severity", "")
        scope = rule.get("Scope", "")
        if isinstance(scope, list):
            scope_str = ", ".join(scope)
        else:
            scope_str = scope
        all_rules_for_editor.append({
            "Category": rule.get("Category", ""),
            "Scope": scope_str,
            "Severity": sev,
            "Name": rule.get("Name", ""),
            "Expression": rule.get("Expression", ""),
            "Description": rule.get("Description", "") or "",
            "URL": rule.get("URL", "") or "",
        })

    data_json = json.dumps(categories_data)
    rule_names_json = json.dumps(all_rule_names)
    object_types_json = json.dumps(all_object_types)
    severities_json = json.dumps(all_severities)
    severity_label_json = json.dumps(icons.severity_mapping)
    rule_logic_json = json.dumps(rule_logic_map)
    all_rules_json = json.dumps(all_rules_for_editor)
    dataset_name_json = json.dumps(dataset_name)
    workspace_id_json = json.dumps(workspace_id)

    html_output = f"""
    <style>
        .bpa-root {{
            font-family: -apple-system, BlinkMacSystemFont, "SF Pro Display", "SF Pro Text", "Helvetica Neue", Arial, sans-serif;
            color: #1d1d1f;
            max-width: 100%;
            -webkit-font-smoothing: antialiased;
            -moz-osx-font-smoothing: grayscale;
        }}

        /* ── Tab bar ── */
        .bpa-tabs {{
            display: flex;
            gap: 6px;
            padding: 4px;
            background: #f5f5f7;
            border-radius: 12px;
            margin-bottom: 16px;
            flex-wrap: wrap;
        }}
        .bpa-tab-btn {{
            display: flex;
            align-items: center;
            gap: 10px;
            padding: 10px 18px;
            border: none;
            border-radius: 10px;
            background: transparent;
            cursor: pointer;
            font-size: 13px;
            font-weight: 500;
            color: #6e6e73;
            transition: all 0.25s ease;
            text-align: left;
            line-height: 1.4;
            white-space: nowrap;
        }}
        .bpa-tab-btn:hover {{ background: rgba(0,0,0,0.04); }}
        .bpa-tab-btn.active {{
            background: #fff;
            color: #1d1d1f;
            box-shadow: 0 1px 4px rgba(0,0,0,0.08);
            font-weight: 600;
        }}
        .bpa-tab-btn .tab-count {{
            font-size: 11px;
            font-weight: 400;
            color: #86868b;
            display: block;
            margin-top: 2px;
        }}
        .bpa-tab-text {{
            display: flex;
            flex-direction: column;
        }}
        .bpa-cat-icon {{
            width: 20px;
            height: 20px;
            flex-shrink: 0;
            opacity: 0.55;
        }}
        .bpa-tab-btn.active .bpa-cat-icon {{
            opacity: 1;
            color: #0071e3;
        }}

        /* ── Filter bar ── */
        .bpa-filters {{
            display: flex;
            gap: 10px;
            margin-bottom: 14px;
            flex-wrap: wrap;
            align-items: center;
        }}
        .bpa-filter-label {{
            font-size: 12px;
            font-weight: 600;
            color: #86868b;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-right: 2px;
        }}
        .bpa-filter-select {{
            padding: 7px 30px 7px 12px;
            border: 1px solid #d2d2d7;
            border-radius: 8px;
            background: #fff;
            font-size: 13px;
            color: #1d1d1f;
            font-family: inherit;
            appearance: none;
            -webkit-appearance: none;
            background-image: url("data:image/svg+xml,%3Csvg width='10' height='6' viewBox='0 0 10 6' fill='none' xmlns='http://www.w3.org/2000/svg'%3E%3Cpath d='M1 1L5 5L9 1' stroke='%2386868b' stroke-width='1.5' stroke-linecap='round' stroke-linejoin='round'/%3E%3C/svg%3E");
            background-repeat: no-repeat;
            background-position: right 10px center;
            cursor: pointer;
            transition: border-color 0.2s;
            min-width: 140px;
        }}
        .bpa-filter-select:hover {{ border-color: #0071e3; }}
        .bpa-filter-select:focus {{ outline: none; border-color: #0071e3; box-shadow: 0 0 0 3px rgba(0,113,227,0.15); }}

        /* ── Custom severity dropdown ── */
        .bpa-sev-dropdown {{
            position: relative;
            display: inline-block;
            min-width: 140px;
        }}
        .bpa-sev-toggle {{
            padding: 7px 30px 7px 12px;
            border: 1px solid #d2d2d7;
            border-radius: 8px;
            background: #fff;
            font-size: 13px;
            color: #1d1d1f;
            font-family: inherit;
            cursor: pointer;
            transition: border-color 0.2s;
            min-width: 140px;
            box-sizing: border-box;
            user-select: none;
            white-space: nowrap;
            background-image: url("data:image/svg+xml,%3Csvg width='10' height='6' viewBox='0 0 10 6' fill='none' xmlns='http://www.w3.org/2000/svg'%3E%3Cpath d='M1 1L5 5L9 1' stroke='%2386868b' stroke-width='1.5' stroke-linecap='round' stroke-linejoin='round'/%3E%3C/svg%3E");
            background-repeat: no-repeat;
            background-position: right 10px center;
        }}
        .bpa-sev-toggle:hover {{ border-color: #0071e3; }}
        .bpa-sev-dropdown.open .bpa-sev-toggle {{ border-color: #0071e3; box-shadow: 0 0 0 3px rgba(0,113,227,0.15); }}
        .bpa-sev-menu {{
            display: none;
            position: absolute;
            top: 100%;
            left: 0;
            right: 0;
            margin-top: 2px;
            background: #fff;
            border: 1px solid #d2d2d7;
            border-radius: 8px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.1);
            z-index: 100;
            padding: 4px 0;
            min-width: 100%;
        }}
        .bpa-sev-dropdown.open .bpa-sev-menu {{ display: block; }}
        .bpa-sev-item {{
            padding: 6px 12px;
            cursor: pointer;
            font-size: 13px;
            color: #1d1d1f;
            white-space: nowrap;
            display: flex;
            align-items: center;
        }}
        .bpa-sev-item:hover {{ background: #f0f0f5; }}
        .bpa-sev-item.selected {{ background: #e8f0fe; }}
        .bpa-sev-icon {{
            display: inline-block;
            width: 1.2em;
            text-align: center;
            flex-shrink: 0;
        }}
        .bpa-sev-label {{
            margin-left: 6px;
        }}

        /* ── Tab content ── */
        .bpa-tab-content {{ display: none; }}
        .bpa-tab-content.active {{ display: block; }}

        /* ── Collapsible rule group ── */
        .bpa-rule-group {{
            background: #fff;
            border: 1px solid #e8e8ed;
            border-radius: 12px;
            margin-bottom: 10px;
            overflow: hidden;
            transition: box-shadow 0.2s ease;
        }}
        .bpa-rule-group:hover {{ box-shadow: 0 2px 12px rgba(0,0,0,0.06); }}
        .bpa-rule-header {{
            display: flex;
            align-items: center;
            padding: 14px 18px;
            cursor: pointer;
            user-select: none;
            gap: 12px;
            transition: background 0.15s;
        }}
        .bpa-rule-header:hover {{ background: #fafafa; }}
        .bpa-chevron {{
            width: 18px;
            height: 18px;
            flex-shrink: 0;
            transition: transform 0.25s ease;
            color: #86868b;
        }}
        .bpa-rule-group.open .bpa-chevron {{ transform: rotate(90deg); }}
        .bpa-rule-title {{
            font-size: 14px;
            font-weight: 600;
            color: #1d1d1f;
            flex: 1;
        }}
        .bpa-rule-title a {{
            color: #0071e3;
            text-decoration: none;
        }}
        .bpa-rule-title a:hover {{ text-decoration: underline; }}
        .bpa-rule-badge {{
            font-size: 11px;
            font-weight: 600;
            padding: 2px 10px;
            border-radius: 20px;
            background: #f5f5f7;
            color: #6e6e73;
            white-space: nowrap;
            min-width: 62px;
            text-align: center;
            box-sizing: border-box;
        }}
        .bpa-rule-severity {{
            font-size: 14px;
            flex-shrink: 0;
            width: 1.4em;
            text-align: center;
        }}
        .bpa-rule-desc {{
            padding: 0 18px 8px 48px;
            font-size: 12px;
            color: #86868b;
            line-height: 1.5;
            display: none;
        }}
        .bpa-rule-group.open .bpa-rule-desc {{ display: block; }}
        .bpa-rule-logic-toggle {{
            padding: 4px 18px 10px 48px;
            font-size: 12px;
            color: #0071e3;
            cursor: pointer;
            user-select: none;
            display: none;
            align-items: center;
            gap: 6px;
        }}
        .bpa-rule-group.open .bpa-rule-logic-toggle {{ display: inline-flex; }}
        .bpa-rule-logic-toggle:hover {{ color: #0077ed; text-decoration: underline; }}
        .bpa-rule-logic-icon {{
            width: 14px;
            height: 14px;
            flex-shrink: 0;
        }}
        .bpa-rule-logic {{
            display: none;
            margin: 0 18px 14px 48px;
            border-radius: 10px;
            overflow: hidden;
            border: 1px solid #383838;
        }}
        .bpa-rule-logic.visible {{ display: block; }}
        .bpa-rule-logic-hdr {{
            background: #2d2d2d;
            color: #9d9da0;
            padding: 8px 14px;
            font-size: 11px;
            font-weight: 600;
            font-family: -apple-system, BlinkMacSystemFont, "Helvetica Neue", Arial, sans-serif;
            display: flex;
            align-items: center;
            gap: 6px;
            letter-spacing: 0.3px;
            text-transform: uppercase;
            border-bottom: 1px solid #383838;
        }}
        .bpa-rule-logic-hdr svg {{
            width: 13px;
            height: 13px;
            opacity: 0.6;
        }}
        .bpa-rule-logic-code {{
            background: #1e1e1e;
            color: #d4d4d4;
            padding: 14px 16px;
            font-family: "SF Mono", SFMono-Regular, Menlo, Consolas, monospace;
            font-size: 11.5px;
            line-height: 1.7;
            overflow-x: auto;
            white-space: pre;
            margin: 0;
        }}
        .hl-k  {{ color: #c586c0; }}
        .hl-cn {{ color: #569cd6; }}
        .hl-s  {{ color: #ce9178; }}
        .hl-c  {{ color: #6a9955; font-style: italic; }}
        .hl-n  {{ color: #b5cea8; }}
        .hl-t  {{ color: #4ec9b0; }}
        .hl-bi {{ color: #dcdcaa; }}
        .hl-o  {{ color: #d4d4d4; }}
        .bpa-rule-body {{
            display: none;
            border-top: 1px solid #f0f0f5;
        }}
        .bpa-rule-group.open .bpa-rule-body {{ display: block; }}

        /* ── Object type sub-group ── */
        .bpa-otype-group {{ border-top: 1px solid #f0f0f5; }}
        .bpa-otype-header {{
            display: flex;
            align-items: center;
            padding: 10px 18px 10px 36px;
            cursor: pointer;
            user-select: none;
            gap: 10px;
            font-size: 13px;
            font-weight: 500;
            color: #6e6e73;
            transition: background 0.15s;
        }}
        .bpa-otype-header:hover {{ background: #fafafa; }}
        .bpa-otype-chevron {{
            width: 14px;
            height: 14px;
            flex-shrink: 0;
            transition: transform 0.25s ease;
            color: #aeaeb2;
        }}
        .bpa-otype-group.open .bpa-otype-chevron {{ transform: rotate(90deg); }}
        .bpa-otype-badge {{
            font-size: 11px;
            padding: 1px 8px;
            border-radius: 20px;
            background: #f0f0f5;
            color: #86868b;
        }}
        .bpa-obj-list {{
            display: none;
            padding: 0 18px 8px 60px;
        }}
        .bpa-otype-group.open .bpa-obj-list {{ display: block; }}
        .bpa-obj-item {{
            padding: 6px 0;
            font-size: 13px;
            color: #424245;
            border-bottom: 1px solid #f5f5f7;
            font-family: "SF Mono", SFMono-Regular, Menlo, Consolas, monospace;
        }}
        .bpa-obj-item:last-child {{ border-bottom: none; }}

        /* ── Empty state ── */
        .bpa-empty {{
            text-align: center;
            padding: 40px 20px;
            color: #86868b;
            font-size: 14px;
        }}

        /* ── Rule Editor ── */
        .bpa-editor-btn {{
            padding: 8px 18px;
            border: 1px solid #d2d2d7;
            border-radius: 8px;
            background: #fff;
            font-size: 13px;
            font-weight: 500;
            color: #0071e3;
            cursor: pointer;
            transition: all 0.2s;
            display: inline-flex;
            align-items: center;
            gap: 6px;
        }}
        .bpa-editor-btn:hover {{ background: #f0f5ff; border-color: #0071e3; }}
        .bpa-editor-overlay {{
            display: none;
            position: fixed;
            top: 0; left: 0; right: 0; bottom: 0;
            background: rgba(0,0,0,0.4);
            z-index: 9999;
            justify-content: center;
            align-items: flex-start;
            padding: 40px 20px;
            overflow-y: auto;
        }}
        .bpa-editor-overlay.open {{ display: flex; }}
        .bpa-editor-panel {{
            background: #fff;
            border-radius: 16px;
            box-shadow: 0 8px 40px rgba(0,0,0,0.18);
            width: 100%;
            max-width: 1100px;
            height: 92vh;
            max-height: 92vh;
            display: flex;
            flex-direction: column;
            overflow: hidden;
        }}
        .bpa-editor-topbar {{
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 16px 24px;
            border-bottom: 1px solid #e8e8ed;
            flex-shrink: 0;
        }}
        .bpa-editor-topbar h2 {{
            margin: 0;
            font-size: 18px;
            font-weight: 600;
            color: #1d1d1f;
        }}
        .bpa-editor-topbar-actions {{
            display: flex;
            gap: 8px;
            align-items: center;
        }}
        .bpa-editor-close {{
            background: none;
            border: none;
            font-size: 22px;
            color: #86868b;
            cursor: pointer;
            padding: 4px 8px;
            border-radius: 6px;
            line-height: 1;
        }}
        .bpa-editor-close:hover {{ background: #f0f0f5; color: #1d1d1f; }}
        .bpa-editor-header {{
            flex-shrink: 0;
            padding: 0 24px;
            background: #fff;
        }}
        .bpa-editor-body {{
            overflow-y: auto;
            padding: 0 24px 16px 24px;
            flex: 1;
        }}
        .bpa-editor-table {{
            width: 100%;
            table-layout: fixed;
            border-collapse: separate;
            border-spacing: 0;
            font-size: 12px;
        }}
        .bpa-editor-table col.col-name {{ width: 20%; }}
        .bpa-editor-table col.col-category {{ width: 14%; }}
        .bpa-editor-table col.col-scope {{ width: 12%; }}
        .bpa-editor-table col.col-severity {{ width: 10%; }}
        .bpa-editor-table col.col-expression {{ width: 30%; }}
        .bpa-editor-table col.col-actions {{ width: 14%; }}
        .bpa-editor-table th {{
            text-align: left;
            padding: 8px 10px;
            font-weight: 600;
            color: #86868b;
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 0.3px;
            border-bottom: 2px solid #e8e8ed;
            white-space: nowrap;
            background: #fff;
        }}
        .bpa-editor-table td {{
            text-align: left;
            padding: 8px 10px;
            border-bottom: 1px solid #f0f0f5;
            vertical-align: top;
            color: #1d1d1f;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }}
        .bpa-editor-table tr:hover td {{ background: #fafafa; }}
        .bpa-editor-table .bpa-ed-actions {{
            white-space: nowrap;
            display: flex;
            gap: 4px;
        }}
        .bpa-ed-btn {{
            padding: 4px 10px;
            border: 1px solid #d2d2d7;
            border-radius: 6px;
            background: #fff;
            font-size: 11px;
            cursor: pointer;
            transition: all 0.15s;
            color: #1d1d1f;
        }}
        .bpa-ed-btn:hover {{ border-color: #0071e3; color: #0071e3; }}
        .bpa-ed-btn.danger {{ color: #ff3b30; }}
        .bpa-ed-btn.danger:hover {{ border-color: #ff3b30; background: #fff5f5; }}
        .bpa-ed-btn.primary {{
            background: #0071e3;
            color: #fff;
            border-color: #0071e3;
        }}
        .bpa-ed-btn.primary:hover {{ background: #0077ed; }}
        .bpa-ed-btn.add {{
            background: #fff;
            color: #34c759;
            border-color: #34c759;
            font-weight: 600;
        }}
        .bpa-ed-btn.add:hover {{ background: #f0fff4; }}
        .bpa-save-dropdown {{
            position: relative;
            display: inline-flex;
        }}
        .bpa-save-dropdown-toggle {{
            display: inline-flex;
            align-items: center;
            gap: 5px;
            padding: 4px 10px;
            background: #0071e3;
            color: #fff;
            border: 1px solid #0071e3;
            border-radius: 6px;
            font-size: 11px;
            font-weight: 500;
            cursor: pointer;
            transition: background 0.15s;
        }}
        .bpa-save-dropdown-toggle:hover {{ background: #0077ed; }}
        .bpa-save-dropdown-toggle svg {{
            width: 10px;
            height: 10px;
            fill: currentColor;
        }}
        .bpa-save-dropdown-menu {{
            display: none;
            position: absolute;
            top: 100%;
            right: 0;
            margin-top: 4px;
            background: #fff;
            border: 1px solid #d2d2d7;
            border-radius: 8px;
            box-shadow: 0 4px 16px rgba(0,0,0,0.12);
            z-index: 100;
            min-width: 180px;
            overflow: hidden;
        }}
        .bpa-save-dropdown.open .bpa-save-dropdown-menu {{ display: block; }}
        .bpa-save-dropdown-item {{
            display: flex;
            align-items: center;
            gap: 8px;
            padding: 8px 14px;
            font-size: 12px;
            color: #1d1d1f;
            cursor: pointer;
            border: none;
            background: none;
            width: 100%;
            text-align: left;
            transition: background 0.1s;
        }}
        .bpa-save-dropdown-item:hover {{ background: #f5f5f7; }}
        .bpa-save-dropdown-item svg {{
            width: 14px;
            height: 14px;
            flex-shrink: 0;
        }}
        /* ── Rule Form (modal inside editor) ── */
        .bpa-form-overlay {{
            display: none;
            position: fixed;
            top: 0; left: 0; right: 0; bottom: 0;
            background: rgba(0,0,0,0.35);
            z-index: 10001;
            justify-content: center;
            align-items: center;
            padding: 20px;
        }}
        .bpa-form-overlay.open {{ display: flex; }}
        .bpa-form-panel {{
            background: #fff;
            border-radius: 14px;
            box-shadow: 0 8px 40px rgba(0,0,0,0.2);
            width: 100%;
            max-width: 640px;
            max-height: 92vh;
            overflow-y: auto;
            padding: 24px;
        }}
        .bpa-form-panel h3 {{
            margin: 0 0 18px 0;
            font-size: 16px;
            font-weight: 600;
            color: #1d1d1f;
        }}
        .bpa-form-group {{
            margin-bottom: 14px;
        }}
        .bpa-form-group label {{
            display: block;
            font-size: 12px;
            font-weight: 600;
            color: #6e6e73;
            margin-bottom: 4px;
            text-transform: uppercase;
            letter-spacing: 0.3px;
        }}
        .bpa-form-group label .bpa-req {{
            color: #ff3b30;
            margin-left: 2px;
        }}
        .bpa-form-group input,
        .bpa-form-group textarea,
        .bpa-form-group select {{
            width: 100%;
            padding: 8px 12px;
            border: 1px solid #d2d2d7;
            border-radius: 8px;
            font-size: 13px;
            font-family: inherit;
            color: #1d1d1f;
            box-sizing: border-box;
            transition: border-color 0.2s;
        }}
        .bpa-form-group input:focus,
        .bpa-form-group textarea:focus,
        .bpa-form-group select:focus {{
            outline: none;
            border-color: #0071e3;
            box-shadow: 0 0 0 3px rgba(0,113,227,0.15);
        }}
        .bpa-form-group textarea {{
            min-height: 70px;
            resize: vertical;
            font-family: "SF Mono", SFMono-Regular, Menlo, Consolas, monospace;
            font-size: 12px;
        }}
        .bpa-form-group .bpa-form-error {{
            color: #ff3b30;
            font-size: 11px;
            margin-top: 3px;
            display: none;
        }}
        .bpa-form-actions {{
            display: flex;
            gap: 8px;
            justify-content: flex-end;
            margin-top: 20px;
        }}
        .bpa-save-status {{
            font-size: 12px;
            padding: 6px 14px;
            border-radius: 8px;
            display: none;
            align-items: center;
            gap: 6px;
        }}
        .bpa-save-status.success {{
            display: inline-flex;
            background: #f0fff4;
            color: #34c759;
            border: 1px solid #34c759;
        }}
        .bpa-save-status.error {{
            display: inline-flex;
            background: #fff5f5;
            color: #ff3b30;
            border: 1px solid #ff3b30;
        }}

        /* ── Re-run button ── */
        .bpa-rerun-btn {{
            display: inline-flex;
            align-items: center;
            gap: 5px;
            padding: 6px 14px;
            background: #f5f5f7;
            color: #1d1d1f;
            border: 1px solid #d2d2d7;
            border-radius: 8px;
            font-size: 12px;
            font-weight: 500;
            cursor: pointer;
            transition: all 0.15s;
            font-family: inherit;
        }}
        .bpa-rerun-btn:hover {{ background: #e8e8ed; }}
        .bpa-rerun-btn svg {{ width: 13px; height: 13px; }}
        .bpa-rerun-status {{
            font-size: 11px;
            color: #34c759;
            margin-left: 8px;
            font-weight: 500;
        }}

    </style>

    <div class="bpa-root" id="bpaRoot">
        <div class="bpa-results" id="bpaResults">
            <div class="bpa-tabs" id="bpaTabs"></div>
            <div class="bpa-filters" id="bpaFilters">
                <span class="bpa-filter-label">Filters</span>
                <select class="bpa-filter-select" id="filterRule"><option value="">All Rules</option></select>
                <select class="bpa-filter-select" id="filterObjType"><option value="">All Object Types</option></select>
                <div class="bpa-sev-dropdown" id="sevDropdown">
                    <div class="bpa-sev-toggle" id="sevToggle">All Severities</div>
                    <div class="bpa-sev-menu" id="sevMenu"></div>
                </div>
                <button class="bpa-editor-btn" id="openEditorBtn">
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>
                    Edit Rules
                </button>
                <button class="bpa-rerun-btn" id="rerunBpaBtn">
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="23 4 23 10 17 10"/><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/></svg>
                    Re-run BPA
                </button>
                <span class="bpa-rerun-status" id="rerunStatus"></span>
            </div>
            <div id="bpaContent"></div>
        </div>
    </div>

    <!-- Rule Editor Overlay -->
    <div class="bpa-editor-overlay" id="editorOverlay">
        <div class="bpa-editor-panel">
            <div class="bpa-editor-topbar">
                <h2>Rule Editor</h2>
                <div class="bpa-editor-topbar-actions">
                    <span class="bpa-save-status" id="editorSaveStatus"></span>
                    <button class="bpa-ed-btn add" id="addRuleBtn">+ Add Rule</button>
                    <button class="bpa-ed-btn primary" id="saveRulesBtn">
                        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
                        Save Rules
                    </button>
                    <button class="bpa-editor-close" id="closeEditorBtn">&times;</button>
                </div>
            </div>
            <div class="bpa-editor-header">
                <table class="bpa-editor-table">
                    <colgroup>
                        <col class="col-name"/>
                        <col class="col-category"/>
                        <col class="col-scope"/>
                        <col class="col-severity"/>
                        <col class="col-expression"/>
                        <col class="col-actions"/>
                    </colgroup>
                    <thead>
                        <tr>
                            <th>Name</th>
                            <th>Category</th>
                            <th>Scope</th>
                            <th>Severity</th>
                            <th>Expression</th>
                            <th>Actions</th>
                        </tr>
                    </thead>
                </table>
            </div>
            <div class="bpa-editor-body">
                <table class="bpa-editor-table">
                    <colgroup>
                        <col class="col-name"/>
                        <col class="col-category"/>
                        <col class="col-scope"/>
                        <col class="col-severity"/>
                        <col class="col-expression"/>
                        <col class="col-actions"/>
                    </colgroup>
                    <tbody id="editorTableBody"></tbody>
                </table>
            </div>
        </div>
    </div>

    <!-- Rule Form Overlay (for Add/Edit) -->
    <div class="bpa-form-overlay" id="formOverlay">
        <div class="bpa-form-panel">
            <h3 id="formTitle">Edit Rule</h3>
            <input type="hidden" id="formIdx" value="-1"/>
            <div class="bpa-form-group">
                <label>Name <span class="bpa-req">*</span></label>
                <input type="text" id="formName" placeholder="Rule name"/>
                <div class="bpa-form-error" id="formNameErr">Name is required.</div>
            </div>
            <div class="bpa-form-group">
                <label>Category <span class="bpa-req">*</span></label>
                <input type="text" id="formCategory" placeholder="e.g. Performance"/>
                <div class="bpa-form-error" id="formCategoryErr">Category is required.</div>
            </div>
            <div class="bpa-form-group">
                <label>Scope <span class="bpa-req">*</span> (comma-separated for multiple)</label>
                <input type="text" id="formScope" placeholder="e.g. Column, Measure"/>
                <div class="bpa-form-error" id="formScopeErr">Scope is required.</div>
            </div>
            <div class="bpa-form-group">
                <label>Severity <span class="bpa-req">*</span></label>
                <select id="formSeverity">
                    <option value="">-- Select --</option>
                    <option value="Warning">Warning</option>
                    <option value="Error">Error</option>
                    <option value="Info">Info</option>
                </select>
                <div class="bpa-form-error" id="formSeverityErr">Severity is required.</div>
            </div>
            <div class="bpa-form-group">
                <label>Expression <span class="bpa-req">*</span></label>
                <textarea id="formExpression" placeholder="Python expression..."></textarea>
                <div class="bpa-form-error" id="formExpressionErr">Expression is required.</div>
            </div>
            <div class="bpa-form-group">
                <label>Description</label>
                <textarea id="formDescription" placeholder="Optional description..." style="font-family:inherit;"></textarea>
            </div>
            <div class="bpa-form-group">
                <label>URL</label>
                <input type="text" id="formURL" placeholder="Optional reference URL"/>
            </div>
            <div class="bpa-form-actions">
                <button class="bpa-ed-btn" id="formCancelBtn">Cancel</button>
                <button class="bpa-ed-btn primary" id="formSaveBtn">Save</button>
            </div>
        </div>
    </div>

    <script>
    (function() {{
        var DATA = {data_json};
        var RULE_NAMES = {rule_names_json};
        var OBJ_TYPES = {object_types_json};
        var SEVERITIES = {severities_json};
        var SEV_LABELS = {severity_label_json};
        var RULE_LOGIC = {rule_logic_json};
        var ALL_RULES = {all_rules_json};
        var DATASET_NAME = {dataset_name_json};
        var WORKSPACE_ID = {workspace_id_json};

        var rulesModified = false;
        var activeTab = null;

        // Populate filter dropdowns
        var filterRule = document.getElementById('filterRule');
        var filterObjType = document.getElementById('filterObjType');
        var sevDropdown = document.getElementById('sevDropdown');
        var sevToggle = document.getElementById('sevToggle');
        var sevMenu = document.getElementById('sevMenu');
        var filterSeverityValue = '';

        RULE_NAMES.forEach(function(r) {{
            var o = document.createElement('option'); o.value = r; o.textContent = r;
            filterRule.appendChild(o);
        }});
        var OBJ_TYPE_ICONS = {{
            'Columns': '▯',
            'Calculated Columns': '▯',
            'Measures': '∑',
            'Tables': '▦',
            'Calculated Tables': '▦',
            'Relationships': '⟷',
            'Hierarchies': '≡',
            'Partitions': '▤',
            'Roles': '🔒',
            'Row Level Security': '🛡',
            'Calculation Items': '⚙',
            'Models': '⧈',
            'Functions': '𝑓'
        }};

        OBJ_TYPES.forEach(function(t) {{
            var o = document.createElement('option'); o.value = t;
            o.textContent = (OBJ_TYPE_ICONS[t] || '') + '  ' + t;
            filterObjType.appendChild(o);
        }});
        // Build custom severity dropdown items
        var sevAllItem = document.createElement('div');
        sevAllItem.className = 'bpa-sev-item selected';
        sevAllItem.setAttribute('data-value', '');
        sevAllItem.textContent = 'All Severities';
        sevMenu.appendChild(sevAllItem);

        SEVERITIES.forEach(function(s) {{
            var item = document.createElement('div');
            item.className = 'bpa-sev-item';
            item.setAttribute('data-value', s);
            var iconSpan = document.createElement('span');
            iconSpan.className = 'bpa-sev-icon';
            iconSpan.textContent = s;
            var labelSpan = document.createElement('span');
            labelSpan.className = 'bpa-sev-label';
            labelSpan.textContent = SEV_LABELS[s] || s;
            item.appendChild(iconSpan);
            item.appendChild(labelSpan);
            sevMenu.appendChild(item);
        }});

        // Severity dropdown interaction
        sevToggle.addEventListener('click', function(e) {{
            e.stopPropagation();
            sevDropdown.classList.toggle('open');
        }});
        document.addEventListener('click', function() {{
            sevDropdown.classList.remove('open');
        }});
        sevMenu.addEventListener('click', function(e) {{
            var item = e.target.closest('.bpa-sev-item');
            if (!item) return;
            var val = item.getAttribute('data-value');
            filterSeverityValue = val;
            // Update selected state
            sevMenu.querySelectorAll('.bpa-sev-item').forEach(function(el) {{ el.classList.remove('selected'); }});
            item.classList.add('selected');
            // Update toggle display
            if (val === '') {{
                sevToggle.textContent = 'All Severities';
            }} else {{
                sevToggle.textContent = '';
                var tIcon = document.createElement('span');
                tIcon.className = 'bpa-sev-icon';
                tIcon.textContent = val;
                var tLabel = document.createElement('span');
                tLabel.className = 'bpa-sev-label';
                tLabel.textContent = SEV_LABELS[val] || val;
                sevToggle.appendChild(tIcon);
                sevToggle.appendChild(tLabel);
            }}
            sevDropdown.classList.remove('open');
            render();
        }});

        filterRule.addEventListener('change', render);
        filterObjType.addEventListener('change', render);

        // Category icon map (SVG inline icons)
        var CAT_ICONS = {{
            'Performance': '<svg class="bpa-cat-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>',
            'Formatting': '<svg class="bpa-cat-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>',
            'Maintenance': '<svg class="bpa-cat-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>',
            'DAX Expressions': '<svg class="bpa-cat-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/><line x1="14" y1="4" x2="10" y2="20"/></svg>',
            'Error Prevention': '<svg class="bpa-cat-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>',
            'Naming Conventions': '<svg class="bpa-cat-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M4 7V4h16v3"/><line x1="9" y1="20" x2="15" y2="20"/><line x1="12" y1="4" x2="12" y2="20"/></svg>'
        }};
        var DEFAULT_ICON = '<svg class="bpa-cat-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg>';

        // Build tabs
        var tabsEl = document.getElementById('bpaTabs');
        var cats = Object.keys(DATA);
        cats.forEach(function(cat, idx) {{
            var btn = document.createElement('button');
            btn.className = 'bpa-tab-btn' + (idx === 0 ? ' active' : '');
            var icon = CAT_ICONS[cat] || DEFAULT_ICON;
            btn.innerHTML = icon + '<span class="bpa-tab-text"><strong>' + cat + '</strong><span class="tab-count">' + DATA[cat].length + ' violations</span></span>';
            btn.addEventListener('click', function() {{
                tabsEl.querySelectorAll('.bpa-tab-btn').forEach(function(b) {{ b.classList.remove('active'); }});
                btn.classList.add('active');
                activeTab = cat;
                render();
            }});
            tabsEl.appendChild(btn);
        }});
        activeTab = cats[0] || null;

        var chevronSvg = '<svg class="bpa-chevron" viewBox="0 0 20 20" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="7 4 13 10 7 16"/></svg>';
        var chevronSmall = '<svg class="bpa-otype-chevron" viewBox="0 0 20 20" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="7 4 13 10 7 16"/></svg>';

        function render() {{
            var container = document.getElementById('bpaContent');
            container.innerHTML = '';
            if (!activeTab || !DATA[activeTab]) return;

            var fRule = filterRule.value;
            var fType = filterObjType.value;
            var fSev = filterSeverityValue;

            var rows = DATA[activeTab].filter(function(r) {{
                if (fRule && r.rule !== fRule) return false;
                if (fType && r.objectType !== fType) return false;
                if (fSev && r.severity !== fSev) return false;
                return true;
            }});

            if (rows.length === 0) {{
                container.innerHTML = '<div class="bpa-empty">No violations match the current filters.</div>';
                return;
            }}

            // Group by rule
            var ruleMap = {{}};
            rows.forEach(function(r) {{
                if (!ruleMap[r.rule]) ruleMap[r.rule] = [];
                ruleMap[r.rule].push(r);
            }});

            Object.keys(ruleMap).sort().forEach(function(rule) {{
                var items = ruleMap[rule];
                var first = items[0];

                var group = document.createElement('div');
                group.className = 'bpa-rule-group';

                // Header
                var header = document.createElement('div');
                header.className = 'bpa-rule-header';
                var titleHtml = first.url ? '<a href="' + first.url + '" target="_blank">' + rule + '</a>' : rule;
                header.innerHTML = chevronSvg +
                    '<span class="bpa-rule-title">' + titleHtml + '</span>' +
                    '<span class="bpa-rule-badge">' + items.length + ' object' + (items.length !== 1 ? 's' : '') + '</span>' +
                    '<span class="bpa-rule-severity">' + first.severity + '</span>';
                header.addEventListener('click', function() {{
                    var wasOpen = group.classList.contains('open');
                    group.classList.toggle('open');
                    if (wasOpen) {{
                        var lw = group.querySelector('.bpa-rule-logic.visible');
                        if (lw) {{
                            lw.classList.remove('visible');
                            var lt = group.querySelector('.bpa-rule-logic-toggle');
                            if (lt) {{
                                var ci = '<svg class="bpa-rule-logic-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/></svg>';
                                lt.innerHTML = ci + ' View Rule Logic';
                            }}
                        }}
                    }}
                }});
                group.appendChild(header);

                // Description
                if (first.description) {{
                    var desc = document.createElement('div');
                    desc.className = 'bpa-rule-desc';
                    desc.textContent = first.description;
                    group.appendChild(desc);
                }}

                // Rule Logic
                if (RULE_LOGIC[rule]) {{
                    var logicToggle = document.createElement('div');
                    logicToggle.className = 'bpa-rule-logic-toggle';
                    var codeIcon = '<svg class="bpa-rule-logic-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/></svg>';
                    logicToggle.innerHTML = codeIcon + ' View Rule Logic';
                    var logicWrapper = document.createElement('div');
                    logicWrapper.className = 'bpa-rule-logic';
                    var logicHdr = document.createElement('div');
                    logicHdr.className = 'bpa-rule-logic-hdr';
                    logicHdr.innerHTML = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/></svg> Rule Logic';
                    logicWrapper.appendChild(logicHdr);
                    var logicCode = document.createElement('pre');
                    logicCode.className = 'bpa-rule-logic-code';
                    logicCode.innerHTML = RULE_LOGIC[rule];
                    logicWrapper.appendChild(logicCode);
                    logicToggle.addEventListener('click', function(toggle, wrapper, icon) {{
                        return function(e) {{
                            e.stopPropagation();
                            var isVisible = wrapper.classList.toggle('visible');
                            toggle.innerHTML = icon + (isVisible ? ' Hide Rule Logic' : ' View Rule Logic');
                        }};
                    }}(logicToggle, logicWrapper, codeIcon));
                    group.appendChild(logicToggle);
                    group.appendChild(logicWrapper);
                }}

                // Body - grouped by object type
                var body = document.createElement('div');
                body.className = 'bpa-rule-body';

                var typeMap = {{}};
                items.forEach(function(it) {{
                    if (!typeMap[it.objectType]) typeMap[it.objectType] = [];
                    typeMap[it.objectType].push(it);
                }});

                Object.keys(typeMap).sort().forEach(function(otype) {{
                    var objs = typeMap[otype];
                    var otGroup = document.createElement('div');
                    otGroup.className = 'bpa-otype-group';

                    var otHeader = document.createElement('div');
                    otHeader.className = 'bpa-otype-header';
                    otHeader.innerHTML = chevronSmall +
                        '<span>' + otype + '</span>' +
                        '<span class="bpa-otype-badge">' + objs.length + '</span>';
                    otHeader.addEventListener('click', function(e) {{
                        e.stopPropagation();
                        otGroup.classList.toggle('open');
                    }});
                    otGroup.appendChild(otHeader);

                    var list = document.createElement('div');
                    list.className = 'bpa-obj-list';
                    objs.forEach(function(o) {{
                        var item = document.createElement('div');
                        item.className = 'bpa-obj-item';
                        item.textContent = o.objectName;
                        list.appendChild(item);
                    }});
                    otGroup.appendChild(list);
                    body.appendChild(otGroup);
                }});

                group.appendChild(body);
                container.appendChild(group);
            }});
        }}

        render();

        /* ── Run / Re-run BPA Logic ── */
        var bpaLanding = document.getElementById('bpaLanding');
        var bpaResults = document.getElementById('bpaResults');

        document.getElementById('runBpaBtn').addEventListener('click', function() {{
            if (!rulesModified) {{
                // Rules unchanged – show pre-computed results
                bpaLanding.style.display = 'none';
                bpaResults.style.display = '';
            }} else {{
                // Rules were imported or edited – copy run command to clipboard
                var code = buildRerunCode();
                var statusEl = document.getElementById('importStatus');
                function showCopied() {{
                    statusEl.textContent = 'Copied run command \u2014 paste in a new cell to run BPA with the updated rules';
                    statusEl.className = 'bpa-import-status success';
                    statusEl.style.display = '';
                }}
                function showFallback() {{
                    // Fallback: select from a textarea
                    var ta = document.createElement('textarea');
                    ta.value = code;
                    ta.style.cssText = 'position:fixed;top:50%;left:50%;transform:translate(-50%,-50%);width:500px;height:200px;z-index:99999;font-size:12px;padding:10px;';
                    document.body.appendChild(ta);
                    ta.focus();
                    ta.select();
                    statusEl.textContent = 'Copy the code from the text box, paste in a new cell, and run it.';
                    statusEl.className = 'bpa-import-status success';
                    statusEl.style.display = '';
                    ta.addEventListener('blur', function() {{ document.body.removeChild(ta); }});
                }}
                try {{
                    if (navigator.clipboard && navigator.clipboard.writeText) {{
                        navigator.clipboard.writeText(code).then(showCopied, showFallback);
                    }} else {{
                        showFallback();
                    }}
                }} catch(e) {{
                    showFallback();
                }}
            }}
        }});

        /* ── Import Rules Logic ── */
        var REQUIRED_KEYS = ['Category', 'Scope', 'Severity', 'Name', 'Expression'];
        var VALID_KEYS = ['Category', 'Scope', 'Severity', 'Name', 'Expression', 'Description', 'URL'];
        var importFileInput = document.getElementById('importRulesFile');
        var importStatus = document.getElementById('importStatus');

        document.getElementById('importRulesBtn').addEventListener('click', function() {{
            importFileInput.value = '';
            importFileInput.click();
        }});

        importFileInput.addEventListener('change', function(e) {{
            var file = e.target.files[0];
            if (!file) return;

            // Check extension
            if (!file.name.toLowerCase().endsWith('.json')) {{
                importStatus.textContent = 'Invalid file type. Please select a .json file.';
                importStatus.className = 'bpa-import-status error';
                return;
            }}

            var reader = new FileReader();
            reader.onload = function(ev) {{
                try {{
                    var parsed = JSON.parse(ev.target.result);
                }} catch (err) {{
                    importStatus.textContent = 'Invalid JSON: ' + err.message;
                    importStatus.className = 'bpa-import-status error';
                    return;
                }}

                if (!Array.isArray(parsed) || parsed.length === 0) {{
                    importStatus.textContent = 'Rules file must be a non-empty JSON array.';
                    importStatus.className = 'bpa-import-status error';
                    return;
                }}

                // Validate each rule has all required keys
                for (var i = 0; i < parsed.length; i++) {{
                    var rule = parsed[i];
                    if (typeof rule !== 'object' || rule === null || Array.isArray(rule)) {{
                        importStatus.textContent = 'Rule #' + (i + 1) + ' is not a valid object.';
                        importStatus.className = 'bpa-import-status error';
                        return;
                    }}
                    for (var k = 0; k < REQUIRED_KEYS.length; k++) {{
                        if (!(REQUIRED_KEYS[k] in rule) || !rule[REQUIRED_KEYS[k]]) {{
                            importStatus.textContent = 'Rule #' + (i + 1) + ' is missing required key: ' + REQUIRED_KEYS[k];
                            importStatus.className = 'bpa-import-status error';
                            return;
                        }}
                    }}
                }}

                // Convert to editor format
                var imported = parsed.map(function(r) {{
                    var scope = r.Scope;
                    if (Array.isArray(scope)) scope = scope.join(', ');
                    return {{
                        Category: r.Category || '',
                        Scope: scope || '',
                        Severity: r.Severity || '',
                        Name: r.Name || '',
                        Expression: r.Expression || '',
                        Description: r.Description || '',
                        URL: r.URL || ''
                    }};
                }});

                ALL_RULES = imported;
                rulesModified = true;
                importStatus.textContent = 'Imported ' + imported.length + ' rule' + (imported.length !== 1 ? 's' : '') + ' from ' + file.name;
                importStatus.className = 'bpa-import-status success';
            }};
            reader.readAsText(file);
        }});

        function buildRerunCode() {{
            var rulesArr = ALL_RULES.map(function(r) {{
                var scopeVal = r.Scope;
                if (scopeVal.indexOf(',') !== -1) {{
                    scopeVal = scopeVal.split(',').map(function(s) {{ return s.trim(); }});
                }}
                return {{
                    Category: r.Category,
                    Scope: scopeVal,
                    Severity: r.Severity,
                    Name: r.Name,
                    Expression: r.Expression,
                    Description: r.Description || null,
                    URL: r.URL || null
                }};
            }});
            var b64 = btoa(unescape(encodeURIComponent(JSON.stringify(rulesArr))));
            var code = 'import json, base64\\n'
                + '_rules = json.loads(base64.b64decode(\"' + b64 + '\").decode(\"utf-8\"))\\n'
                + 'from sempy_labs.semantic_model._bpa import run_model_bpa\\n'
                + 'run_model_bpa(\"' + DATASET_NAME.replace(/\"/g, '\\\\\"') + '\", workspace=\"' + WORKSPACE_ID + '\", rules=_rules)';
            return code;
        }}

        document.getElementById('rerunBpaBtn').addEventListener('click', function() {{
            var code = buildRerunCode();
            var statusEl = document.getElementById('rerunStatus');
            if (navigator.clipboard && navigator.clipboard.writeText) {{
                navigator.clipboard.writeText(code).then(function() {{
                    statusEl.textContent = 'Copied — paste in a new cell to re-run';
                    setTimeout(function() {{ statusEl.textContent = ''; }}, 4000);
                }});
            }}
        }});

        /* ── Rule Editor Logic ── */
        var editorOverlay = document.getElementById('editorOverlay');
        var formOverlay = document.getElementById('formOverlay');
        var editorTableBody = document.getElementById('editorTableBody');
        var editorSaveStatus = document.getElementById('editorSaveStatus');

        function openEditor() {{
            renderEditorTable();
            editorOverlay.classList.add('open');
        }}
        document.getElementById('openEditorBtn').addEventListener('click', openEditor);
        document.getElementById('openEditorBtnLanding').addEventListener('click', openEditor);
        document.getElementById('closeEditorBtn').addEventListener('click', function() {{
            editorOverlay.classList.remove('open');
            editorSaveStatus.className = 'bpa-save-status';
            editorSaveStatus.style.display = 'none';
        }});
        editorOverlay.addEventListener('click', function(e) {{
            if (e.target === editorOverlay) {{
                editorOverlay.classList.remove('open');
                editorSaveStatus.className = 'bpa-save-status';
                editorSaveStatus.style.display = 'none';
            }}
        }});

        function renderEditorTable() {{
            editorTableBody.innerHTML = '';
            ALL_RULES.forEach(function(rule, idx) {{
                var tr = document.createElement('tr');
                tr.innerHTML =
                    '<td title="' + escHtml(rule.Name) + '">' + escHtml(rule.Name) + '</td>' +
                    '<td>' + escHtml(rule.Category) + '</td>' +
                    '<td>' + escHtml(rule.Scope) + '</td>' +
                    '<td>' + escHtml(rule.Severity) + '</td>' +
                    '<td title="' + escHtml(rule.Expression) + '">' + escHtml(rule.Expression).substring(0, 50) + (rule.Expression.length > 50 ? '...' : '') + '</td>' +
                    '<td class="bpa-ed-actions"></td>';
                var actTd = tr.querySelector('.bpa-ed-actions');
                var editBtn = document.createElement('button');
                editBtn.className = 'bpa-ed-btn';
                editBtn.textContent = 'Edit';
                editBtn.addEventListener('click', (function(i) {{ return function() {{ openForm(i); }}; }})(idx));
                var delBtn = document.createElement('button');
                delBtn.className = 'bpa-ed-btn danger';
                delBtn.textContent = 'Delete';
                delBtn.addEventListener('click', (function(i) {{ return function() {{ deleteRule(i); }}; }})(idx));
                actTd.appendChild(editBtn);
                actTd.appendChild(delBtn);
                editorTableBody.appendChild(tr);
            }});
        }}

        function escHtml(str) {{
            if (!str) return '';
            var d = document.createElement('div');
            d.textContent = str;
            return d.innerHTML;
        }}

        function downloadRulesJson() {{
            var rulesForSave = ALL_RULES.map(function(r) {{
                var scopeVal = r.Scope;
                if (scopeVal.indexOf(',') !== -1) {{
                    scopeVal = scopeVal.split(',').map(function(s) {{ return s.trim(); }});
                }}
                return {{
                    Category: r.Category,
                    Scope: scopeVal,
                    Severity: r.Severity,
                    Name: r.Name,
                    Expression: r.Expression,
                    Description: r.Description || null,
                    URL: r.URL || null
                }};
            }});
            var jsonStr = JSON.stringify(rulesForSave, null, 4);
            var blob = new Blob([jsonStr], {{ type: 'application/json' }});
            var dlUrl = URL.createObjectURL(blob);
            var a = document.createElement('a');
            a.href = dlUrl;
            var fileName = RULES_FILE_PATH.split('/').pop() || 'rules.json';
            a.download = fileName;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(dlUrl);
            return fileName;
        }}

        function deleteRule(idx) {{
            var rule = ALL_RULES[idx];
            // Show inline confirmation on the delete button
            var rows = document.getElementById('editorTableBody').querySelectorAll('tr');
            if (!rows[idx]) return;
            var actTd = rows[idx].querySelector('td:last-child');
            if (actTd.querySelector('.bpa-confirm-delete')) return; // already showing

            var wrapper = document.createElement('span');
            wrapper.className = 'bpa-confirm-delete';
            wrapper.style.cssText = 'display:inline-flex;gap:4px;align-items:center;';
            var label = document.createElement('span');
            label.textContent = 'Delete?';
            label.style.cssText = 'color:#ff3b30;font-size:11px;font-weight:600;';
            var yesBtn = document.createElement('button');
            yesBtn.className = 'bpa-ed-btn danger';
            yesBtn.textContent = 'Yes';
            yesBtn.style.cssText = 'padding:2px 8px;font-size:11px;';
            var noBtn = document.createElement('button');
            noBtn.className = 'bpa-ed-btn';
            noBtn.textContent = 'No';
            noBtn.style.cssText = 'padding:2px 8px;font-size:11px;';
            wrapper.appendChild(label);
            wrapper.appendChild(yesBtn);
            wrapper.appendChild(noBtn);
            actTd.innerHTML = '';
            actTd.appendChild(wrapper);

            yesBtn.addEventListener('click', function() {{
                ALL_RULES.splice(idx, 1);
                rulesModified = true;
                renderEditorTable();
            }});
            noBtn.addEventListener('click', function() {{
                renderEditorTable();
            }});
        }}

        // Form logic
        document.getElementById('addRuleBtn').addEventListener('click', function() {{
            openForm(-1);
        }});
        document.getElementById('formCancelBtn').addEventListener('click', function() {{
            formOverlay.classList.remove('open');
        }});
        formOverlay.addEventListener('click', function(e) {{
            if (e.target === formOverlay) formOverlay.classList.remove('open');
        }});

        function openForm(idx) {{
            var title = document.getElementById('formTitle');
            document.getElementById('formIdx').value = idx;
            // Clear errors
            ['formNameErr','formCategoryErr','formScopeErr','formSeverityErr','formExpressionErr'].forEach(function(id) {{
                document.getElementById(id).style.display = 'none';
            }});
            if (idx >= 0) {{
                title.textContent = 'Edit Rule';
                var r = ALL_RULES[idx];
                document.getElementById('formName').value = r.Name || '';
                document.getElementById('formCategory').value = r.Category || '';
                document.getElementById('formScope').value = r.Scope || '';
                document.getElementById('formSeverity').value = r.Severity || '';
                document.getElementById('formExpression').value = r.Expression || '';
                document.getElementById('formDescription').value = r.Description || '';
                document.getElementById('formURL').value = r.URL || '';
            }} else {{
                title.textContent = 'Add New Rule';
                document.getElementById('formName').value = '';
                document.getElementById('formCategory').value = '';
                document.getElementById('formScope').value = '';
                document.getElementById('formSeverity').value = '';
                document.getElementById('formExpression').value = '';
                document.getElementById('formDescription').value = '';
                document.getElementById('formURL').value = '';
            }}
            formOverlay.classList.add('open');
        }}

        document.getElementById('formSaveBtn').addEventListener('click', function() {{
            var idx = parseInt(document.getElementById('formIdx').value, 10);
            var name = document.getElementById('formName').value.trim();
            var category = document.getElementById('formCategory').value.trim();
            var scope = document.getElementById('formScope').value.trim();
            var severity = document.getElementById('formSeverity').value.trim();
            var expression = document.getElementById('formExpression').value.trim();
            var description = document.getElementById('formDescription').value.trim();
            var url = document.getElementById('formURL').value.trim();

            // Validate required fields
            var valid = true;
            if (!name) {{ document.getElementById('formNameErr').style.display = 'block'; valid = false; }}
            else {{ document.getElementById('formNameErr').style.display = 'none'; }}
            if (!category) {{ document.getElementById('formCategoryErr').style.display = 'block'; valid = false; }}
            else {{ document.getElementById('formCategoryErr').style.display = 'none'; }}
            if (!scope) {{ document.getElementById('formScopeErr').style.display = 'block'; valid = false; }}
            else {{ document.getElementById('formScopeErr').style.display = 'none'; }}
            if (!severity) {{ document.getElementById('formSeverityErr').style.display = 'block'; valid = false; }}
            else {{ document.getElementById('formSeverityErr').style.display = 'none'; }}
            if (!expression) {{ document.getElementById('formExpressionErr').style.display = 'block'; valid = false; }}
            else {{ document.getElementById('formExpressionErr').style.display = 'none'; }}
            if (!valid) return;

            var ruleObj = {{
                Name: name,
                Category: category,
                Scope: scope,
                Severity: severity,
                Expression: expression,
                Description: description || '',
                URL: url || ''
            }};

            if (idx >= 0) {{
                ALL_RULES[idx] = ruleObj;
            }} else {{
                ALL_RULES.push(ruleObj);
            }}
            rulesModified = true;
            renderEditorTable();
            formOverlay.classList.remove('open');
        }});

        // Save Rules (download)
        document.getElementById('saveRulesBtn').addEventListener('click', function() {{
            var fileName = downloadRulesJson();
            editorSaveStatus.textContent = 'Downloaded ' + fileName;
            editorSaveStatus.className = 'bpa-save-status success';
            editorSaveStatus.style.display = '';
        }});
    }})();
    </script>
    """

    # Display the modern BPA visualization
    return display(HTML(html_output))
