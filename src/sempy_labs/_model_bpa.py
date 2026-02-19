import sempy.fabric as fabric
import pandas as pd
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
from sempy_labs._model_bpa_rules import model_bpa_rules
from typing import Optional
from sempy._utils._log import log
import sempy_labs._icons as icons
from pyspark.sql.functions import col, flatten
from pyspark.sql.types import StructType, StructField, StringType
import os
from uuid import UUID


@log
def run_model_bpa(
    dataset: str | UUID,
    rules: Optional[pd.DataFrame] = None,
    workspace: Optional[str] = None,
    export: bool = False,
    return_dataframe: bool = False,
    extended: bool = False,
    language: Optional[str] = None,
    check_dependencies: bool = True,
    **kwargs,
):
    """
    Displays an HTML visualization of the results of the Best Practice Analyzer scan for a semantic model.

    The Best Practice Analyzer rules are based on the rules defined `here <https://github.com/microsoft/Analysis-Services/tree/master/BestPracticeRules>`_. The framework for the Best Practice Analyzer and rules are based on the foundation set by `Tabular Editor <https://github.com/TabularEditor/TabularEditor>`_.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    rules : pandas.DataFrame, default=None
        A pandas dataframe containing rules to be evaluated.
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

    if "extend" in kwargs:
        print(
            "The 'extend' parameter has been deprecated. Please remove this parameter from the function going forward."
        )
        del kwargs["extend"]

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

        # Translations
        if language is not None and rules is None and language in language_list:
            rules = model_bpa_rules(dependencies=dep)
            translate_using_po(rules)
            translated = True
        if rules is None:
            rules = model_bpa_rules(dependencies=dep)
        if language is not None and not translated:

            def translate_using_spark(rule_file):

                from synapse.ml.services import Translate

                rules_temp = rule_file.copy()
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

            rules = translate_using_spark(rules)

        rules.loc[rules["Severity"] == "Warning", "Severity"] = icons.warning
        rules.loc[rules["Severity"] == "Error", "Severity"] = icons.error
        rules.loc[rules["Severity"] == "Info", "Severity"] = icons.info

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

        for i, r in rules.iterrows():
            ruleName = r["Rule Name"]
            expr = r["Expression"]
            scopes = r["Scope"]

            if isinstance(scopes, str):
                scopes = [scopes]

            for scope in scopes:
                func = scope_to_dataframe[scope][0]
                nm = scope_to_dataframe[scope][1]

                if scope == "Model":
                    x = []
                    if expr(func, tom):
                        x = ["Model"]
                elif scope == "Measure":
                    x = [nm(obj) for obj in tom.all_measures() if expr(obj, tom)]
                elif scope == "Function":
                    x = [nm(obj) for obj in tom.all_functions() if expr(obj, tom)]
                elif scope == "Column":
                    x = [nm(obj) for obj in tom.all_columns() if expr(obj, tom)]
                elif scope == "Partition":
                    x = [nm(obj) for obj in tom.all_partitions() if expr(obj, tom)]
                elif scope == "Hierarchy":
                    x = [nm(obj) for obj in tom.all_hierarchies() if expr(obj, tom)]
                elif scope == "Table":
                    x = [nm(obj) for obj in tom.model.Tables if expr(obj, tom)]
                elif scope == "Calculated Table":
                    x = [
                        nm(obj) for obj in tom.all_calculated_tables() if expr(obj, tom)
                    ]
                elif scope == "Relationship":
                    x = [nm(obj) for obj in tom.model.Relationships if expr(obj, tom)]
                elif scope == "Role":
                    x = [nm(obj) for obj in tom.model.Roles if expr(obj, tom)]
                elif scope == "Row Level Security":
                    x = [nm(obj) for obj in tom.all_rls() if expr(obj, tom)]
                elif scope == "Calculation Item":
                    x = [
                        nm(obj) for obj in tom.all_calculation_items() if expr(obj, tom)
                    ]
                elif scope == "Calculated Column":
                    x = [
                        nm(obj)
                        for obj in tom.all_calculated_columns()
                        if expr(obj, tom)
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
            rules[["Rule Name", "Category", "Severity", "Description", "URL"]],
            left_on="Rule Name",
            right_on="Rule Name",
            how="left",
        )
        prepDF.rename(columns={"Scope": "Object Type"}, inplace=True)
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

    import json as _json

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
                    "description": str(row["Description"]) if pd.notnull(row["Description"]) else "",
                    "url": str(row["URL"]) if pd.notnull(row["URL"]) else "",
                }
            )
        categories_data[cat] = rows

    data_json = _json.dumps(categories_data)
    rule_names_json = _json.dumps(all_rule_names)
    object_types_json = _json.dumps(all_object_types)
    severities_json = _json.dumps(all_severities)
    severity_label_json = _json.dumps(severity_label_map)

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
        }}
        .bpa-rule-severity {{
            font-size: 14px;
            flex-shrink: 0;
        }}
        .bpa-rule-desc {{
            padding: 0 18px 8px 48px;
            font-size: 12px;
            color: #86868b;
            line-height: 1.5;
            display: none;
        }}
        .bpa-rule-group.open .bpa-rule-desc {{ display: block; }}
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
            <select class="bpa-filter-select" id="filterSeverity"><option value="">All Severities</option></select>
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

        var activeTab = null;

        // Populate filter dropdowns
        var filterRule = document.getElementById('filterRule');
        var filterObjType = document.getElementById('filterObjType');
        var filterSeverity = document.getElementById('filterSeverity');

        RULE_NAMES.forEach(function(r) {{
            var o = document.createElement('option'); o.value = r; o.textContent = r;
            filterRule.appendChild(o);
        }});
        var OBJ_TYPE_ICONS = {{
            'Column': '▯',
            'Calculated Column': '▯',
            'Measure': '∑',
            'Table': '▦',
            'Calculated Table': '▦',
            'Relationship': '⟷',
            'Hierarchy': '≡',
            'Partition': '▤',
            'Role': '🔒',
            'Row Level Security': '🛡',
            'Calculation Item': '⚙',
            'Model': '⧈',
            'Function': '𝑓'
        }};

        OBJ_TYPES.forEach(function(t) {{
            var o = document.createElement('option'); o.value = t;
            o.textContent = (OBJ_TYPE_ICONS[t] || '') + '  ' + t;
            filterObjType.appendChild(o);
        }});
        SEVERITIES.forEach(function(s) {{
            var o = document.createElement('option');
            o.value = s;
            o.textContent = s + '  ' + (SEV_LABELS[s] || s);
            filterSeverity.appendChild(o);
        }});

        filterRule.addEventListener('change', render);
        filterObjType.addEventListener('change', render);
        filterSeverity.addEventListener('change', render);

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
            var fSev = filterSeverity.value;

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
                header.addEventListener('click', function() {{ group.classList.toggle('open'); }});
                group.appendChild(header);

                // Description
                if (first.description) {{
                    var desc = document.createElement('div');
                    desc.className = 'bpa-rule-desc';
                    desc.textContent = first.description;
                    group.appendChild(desc);
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
