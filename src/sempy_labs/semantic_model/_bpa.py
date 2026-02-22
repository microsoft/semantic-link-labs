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
from typing import Optional
from sempy._utils._log import log
import sempy_labs._icons as icons
from pyspark.sql.functions import col, flatten
from pyspark.sql.types import StructType, StructField, StringType
from uuid import UUID
from pathlib import Path
import sempy_labs.semantic_model._bpa_rules as bpa


def save_as_file(
    file_path: str, content, file_type: Optional[str] = None, overwrite: bool = False
):
    """
    Saves the content to a file in the specified format.

    Parameters
    ----------
    file_path : str
        The file path where the content will be saved.
        Example: "./builtin/myfolder/myfile.json" (will save to the Notebook resources)
    content : any
        The content to be saved.
    file_type : str, default=None
        The file type to save the content as. If None, it will be inferred from the file path.
    overwrite : bool, default=False
        If True, will overwrite the file if it already exists. If False, will raise an
    """

    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)

    # Check if folder exists
    if not file_path.parent.exists():
        if overwrite:
            file_path.parent.mkdir(parents=True, exist_ok=True)
        else:
            # print(f"Folder '{file_path.parent}' does not exist. Cannot save the file.")
            return

    # Check if file exists
    if file_path.exists() and not overwrite:
        # print(f"File '{file_path}' already exists. Skipping write because overwrite=False.")
        return

    if file_type is None:
        file_type = file_path.suffix.lower()

    if file_type == ".json":
        with open(file_path, "w") as f:
            json.dump(content, f, indent=4)
    elif file_type == ".txt":
        with open(file_path, "w") as f:
            f.write(str(content))
    elif file_type in [".csv", ".tsv"]:
        delimiter = "\t" if file_type == ".tsv" else ","
        with open(file_path, "w", newline="") as f:

            writer = csv.writer(f, delimiter=delimiter)
            writer.writerows(content)
    else:
        with open(file_path, "wb") as f:
            f.write(content)


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
def run_model_bpa(
    dataset: str | UUID,
    rules: Optional[pd.DataFrame] = None,
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

    import polib

    warnings.filterwarnings(
        "ignore",
        message="This pattern is interpreted as a regular expression, and has match groups.",
    )
    warnings.filterwarnings(
        "ignore", category=UserWarning, message=".*Arrow optimization.*"
    )

    language_list = list(icons.language_map.keys())
    if language is not None:
        language = get_language_codes(languages=language)[0]

    # Map languages to the closest language (first 2 letters matching)
    def map_language(language, language_list):

        mapped = False

        if language in language_list:
            mapped is True
            return language

        language_prefix = language[:2]
        for lang_code in language_list:
            if lang_code.startswith(language_prefix):
                mapped is True
                return lang_code
        if not mapped:
            return language

        if language is not None:
            language = map_language(language, language_list)

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(
        dataset, workspace=workspace_id
    )

    if language is not None and language not in language_list:
        print(
            f"{icons.yellow_dot} The '{language}' language code is not in our predefined language list. Please file an issue and let us know which language code you are using: https://github.com/microsoft/semantic-link-labs/issues/new?assignees=&labels=&projects=&template=bug_report.md&title=."
        )

    with connect_semantic_model(
        dataset=dataset_id, workspace=workspace_id, readonly=True
    ) as tom:

        if extended:
            tom.set_vertipaq_annotations()

        # Do not run BPA for models with no tables
        if tom.model.Tables.Count == 0:
            print(
                f"{icons.warning} The '{dataset_name}' semantic model within the '{workspace_name}' workspace has no tables and therefore there are no valid BPA results."
            )
            return

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

        def translate_using_po(rule_file):
            current_dir = os.path.dirname(os.path.abspath(__file__))
            translation_file = (
                f"{current_dir}/_bpa_translation/_model/_translations_{language}.po"
            )
            for c in ["Category", "Description", "Rule Name"]:
                po = polib.pofile(translation_file)
                for entry in po:
                    if entry.tcomment == c.lower().replace(" ", "_"):
                        rule_file.loc[rule_file["Rule Name"] == entry.msgid, c] = (
                            entry.msgstr
                        )

        translated = False

        default_rules = bpa.rules
        # Translations
        if language is not None and rules is None and language in language_list:
            rules = default_rules
            rules_df = pd.DataFrame(rules, columns=["Category", "Scope", "Severity", "Name", "Expression", "Description", "URL"])
            translate_using_po(rules_df)
            translated = True
        if rules is None:
            rules = default_rules
        elif isinstance(rules, str):
            with open(rules, "r") as f:
                data = json.load(f)
            rules = json.loads(data)

        rules_df = pd.DataFrame(rules, columns=["Category", "Scope", "Severity", "Name", "Expression", "Description", "URL"])

        # Save Rules locally
        file_path = "./builtin/SLL/modelbpa/rules.json"
        save_as_file(file_path=file_path, content=rules, overwrite=False)

        if language is not None and not translated:

            def translate_using_spark(rule_file):

                from synapse.ml.services import Translate

                rules_temp = rule_df.copy()
                rules_temp = rules_temp.drop(["Expression", "URL", "Severity"], axis=1)

                schema = StructType(
                    [
                        StructField("Category", StringType(), True),
                        StructField("Scope", StringType(), True),
                        StructField("Rule Name", StringType(), True),
                        StructField("Description", StringType(), True),
                    ]
                )

                spark = _create_spark_session()
                dfRules = spark.createDataFrame(rules_temp, schema)

                columns = ["Category", "Rule Name", "Description"]
                for clm in columns:
                    translate = (
                        Translate()
                        .setTextCol(clm)
                        .setToLanguage(language)
                        .setOutputCol("translation")
                        .setConcurrency(5)
                    )

                    if clm == "Rule Name":
                        transDF = (
                            translate.transform(dfRules)
                            .withColumn(
                                "translation",
                                flatten(col("translation.translations")),
                            )
                            .withColumn("translation", col("translation.text"))
                            .select(clm, "translation")
                        )
                    else:
                        transDF = (
                            translate.transform(dfRules)
                            .withColumn(
                                "translation",
                                flatten(col("translation.translations")),
                            )
                            .withColumn("translation", col("translation.text"))
                            .select("Rule Name", clm, "translation")
                        )

                    df_panda = transDF.toPandas()
                    rule_file = pd.merge(
                        rule_file,
                        df_panda[["Rule Name", "translation"]],
                        on="Rule Name",
                        how="left",
                    )

                    rule_file = rule_file.rename(
                        columns={"translation": f"{clm}Translated"}
                    )
                    rule_file[f"{clm}Translated"] = rule_file[f"{clm}Translated"].apply(
                        lambda x: x[0] if x is not None else None
                    )

                for clm in columns:
                    rule_file = rule_file.drop([clm], axis=1)
                    rule_file = rule_file.rename(columns={f"{clm}Translated": clm})

                return rule_file

            rules = translate_using_spark(rules_df)

        for rule in rules:
            if rule["Severity"] == "Warning":
                rule["Severity"] = icons.warning
            elif rule["Severity"] == "Error":
                rule["Severity"] = icons.error
            elif rule["Severity"] == "Info":
                rule["Severity"] = icons.info

        # Rebuild rules_df after severity icons have been applied
        rules_df = pd.DataFrame(rules, columns=["Category", "Scope", "Severity", "Name", "Expression", "Description", "URL"])

        # Extract rule logic source code for display
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

        pd.set_option("display.max_colwidth", 1000)

        violations = pd.DataFrame(columns=["Object Name", "Scope", "Rule Name"])

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
                    new_data = {
                        "Object Name": x,
                        "Scope": scope,
                        "Rule Name": ruleName,
                    }
                    violations = pd.concat(
                        [violations, pd.DataFrame(new_data)], ignore_index=True
                    )

        prepDF = pd.merge(
            violations,
            rules_df[["Name", "Category", "Severity", "Description", "URL"]],
            left_on="Rule Name",
            right_on="Name",
            how="left",
        )
    
        prepDF.rename(columns={"Scope": "Object Type"}, inplace=True)
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

    if export:
        if not lakehouse_attached():
            raise ValueError(
                f"{icons.red_dot} In order to save the Best Practice Analyzer results, a lakehouse must be attached to the notebook. Please attach a lakehouse to this notebook."
            )

        dfExport = finalDF.copy()
        delta_table_name = "modelbparesults"

        lakeT = get_lakehouse_tables()
        lakeT_filt = lakeT[lakeT["Table Name"] == delta_table_name]

        dfExport["Severity"] = dfExport["Severity"].replace(icons.severity_mapping)

        if len(lakeT_filt) == 0:
            runId = 1
        else:
            max_run_id = _get_column_aggregate(table_name=delta_table_name)
            runId = max_run_id + 1

        now = datetime.datetime.now()
        dfD = fabric.list_datasets(workspace=workspace_id, mode="rest")
        dfD_filt = dfD[dfD["Dataset Id"] == dataset_id]
        configured_by = dfD_filt["Configured By"].iloc[0]
        capacity_id, capacity_name = resolve_workspace_capacity(workspace=workspace_id)
        dfExport["Capacity Name"] = capacity_name
        dfExport["Capacity Id"] = capacity_id
        dfExport["Workspace Name"] = workspace_name
        dfExport["Workspace Id"] = workspace_id
        dfExport["Dataset Name"] = dataset_name
        dfExport["Dataset Id"] = dataset_id
        dfExport["Configured By"] = configured_by
        dfExport["Timestamp"] = now
        dfExport["RunId"] = runId
        dfExport["RunId"] = dfExport["RunId"].astype("int")

        dfExport = dfExport[list(icons.bpa_schema.keys())]
        dfExport["RunId"] = dfExport["RunId"].astype("int")
        schema = {
            key.replace(" ", "_"): value for key, value in icons.bpa_schema.items()
        }
        save_as_delta_table(
            dataframe=dfExport,
            delta_table_name=delta_table_name,
            write_mode="append",
            schema=schema,
            merge_schema=True,
        )

    if return_dataframe:
        return finalDF

    pd.set_option("display.max_colwidth", 100)

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

    bpa2 = finalDF.reset_index()
    bpa_dict = {
        cat: bpa2[bpa2["Category"] == cat].drop("Category", axis=1)
        for cat in bpa2["Category"].drop_duplicates().values
    }

    # Collect unique filter values
    all_rule_names = sorted(bpa2["Rule Name"].dropna().unique().tolist())
    all_object_types = sorted(bpa2["Object Type"].dropna().unique().tolist())
    all_severities = sorted(bpa2["Severity"].dropna().unique().tolist())

    severity_label_map = {
        icons.warning: "Warning",
        icons.error: "Error",
        icons.info: "Info",
    }

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

    data_json = json.dumps(categories_data)
    rule_names_json = json.dumps(all_rule_names)
    object_types_json = json.dumps(all_object_types)
    severities_json = json.dumps(all_severities)
    severity_label_json = json.dumps(severity_label_map)
    rule_logic_json = json.dumps(rule_logic_map)

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
    </style>

    <div class="bpa-root" id="bpaRoot">
        <div class="bpa-tabs" id="bpaTabs"></div>
        <div class="bpa-filters" id="bpaFilters">
            <span class="bpa-filter-label">Filters</span>
            <select class="bpa-filter-select" id="filterRule"><option value="">All Rules</option></select>
            <select class="bpa-filter-select" id="filterObjType"><option value="">All Object Types</option></select>
            <div class="bpa-sev-dropdown" id="sevDropdown">
                <div class="bpa-sev-toggle" id="sevToggle">All Severities</div>
                <div class="bpa-sev-menu" id="sevMenu"></div>
            </div>
        </div>
        <div id="bpaContent"></div>
    </div>

    <script>
    (function() {{
        var DATA = {data_json};
        var RULE_NAMES = {rule_names_json};
        var OBJ_TYPES = {object_types_json};
        var SEVERITIES = {severities_json};
        var SEV_LABELS = {severity_label_json};
        var RULE_LOGIC = {rule_logic_json};

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
    }})();
    </script>
    """

    # Display the modern BPA visualization
    if not export:
        return display(HTML(html_output))
