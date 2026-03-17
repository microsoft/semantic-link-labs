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

    styles = """
    <style>
        .tab { overflow: hidden; border: 1px solid #ccc; background-color: #f1f1f1; }
        .tab button { background-color: inherit; float: left; border: none; outline: none; cursor: pointer; padding: 14px 16px; transition: 0.3s; }
        .tab button:hover { background-color: #ddd; }
        .tab button.active { background-color: #ccc; }
        .tabcontent { display: none; padding: 6px 12px; border: 1px solid #ccc; border-top: none; }
        .tabcontent.active { display: block; }
        .tooltip { position: relative; display: inline-block; }
        .tooltip .tooltiptext { visibility: hidden; width: 300px; background-color: #555; color: #fff; text-align: center; border-radius: 6px; padding: 5px; position: absolute; z-index: 1; bottom: 125%; left: 50%; margin-left: -110px; opacity: 0; transition: opacity 0.3s; }
        .tooltip:hover .tooltiptext { visibility: visible; opacity: 1; }
    </style>
    """

    # JavaScript for tab functionality
    script = """
    <script>
    function openTab(evt, tabName) {
        var i, tabcontent, tablinks;
        tabcontent = document.getElementsByClassName("tabcontent");
        for (i = 0; i < tabcontent.length; i++) {
            tabcontent[i].style.display = "none";
        }
        tablinks = document.getElementsByClassName("tablinks");
        for (i = 0; i < tablinks.length; i++) {
            tablinks[i].className = tablinks[i].className.replace(" active", "");
        }
        document.getElementById(tabName).style.display = "block";
        evt.currentTarget.className += " active";
    }
    </script>
    """

    # JavaScript for dynamic tooltip positioning
    dynamic_script = """
    <script>
    function adjustTooltipPosition(event) {
        var tooltip = event.target.querySelector('.tooltiptext');
        var rect = tooltip.getBoundingClientRect();
        var topSpace = rect.top;
        var bottomSpace = window.innerHeight - rect.bottom;

        if (topSpace < bottomSpace) {
            tooltip.style.bottom = '125%';
        } else {
            tooltip.style.bottom = 'auto';
            tooltip.style.top = '125%';
        }
    }
    </script>
    """

    # HTML for tabs
    tab_html = '<div class="tab">'
    content_html = ""
    for i, (title, df) in enumerate(bpa_dict.items()):
        if df.shape[0] == 0:
            continue

        tab_id = f"tab{i}"
        active_class = ""
        if i == 0:
            active_class = "active"

        summary = " + ".join(
            [f"{idx} ({v})" for idx, v in df["Severity"].value_counts().items()]
        )
        tab_html += f'<button class="tablinks {active_class}" onclick="openTab(event, \'{tab_id}\')"><b>{title}</b><br/>{summary}</button>'
        content_html += f'<div id="{tab_id}" class="tabcontent {active_class}">'

        # Adding tooltip for Rule Name using Description column
        content_html += '<table border="1">'
        content_html += "<tr><th>Rule Name</th><th>Object Type</th><th>Object Name</th><th>Severity</th></tr>"
        for _, row in df.iterrows():
            content_html += "<tr>"
            if pd.notnull(row["URL"]):
                content_html += f'<td class="tooltip" onmouseover="adjustTooltipPosition(event)"><a href="{row["URL"]}">{row["Rule Name"]}</a><span class="tooltiptext">{row["Description"]}</span></td>'
            elif pd.notnull(row["Description"]):
                content_html += f'<td class="tooltip" onmouseover="adjustTooltipPosition(event)">{row["Rule Name"]}<span class="tooltiptext">{row["Description"]}</span></td>'
            else:
                content_html += f'<td>{row["Rule Name"]}</td>'
            content_html += f'<td>{row["Object Type"]}</td>'
            content_html += f'<td>{row["Object Name"]}</td>'
            content_html += f'<td style="text-align: center;">{row["Severity"]}</td>'
            # content_html += f'<td>{row["Severity"]}</td>'
            content_html += "</tr>"
        content_html += "</table>"

        content_html += "</div>"
    tab_html += "</div>"

    # Display the tabs, tab contents, and run the script
    if not export:
        return display(HTML(styles + tab_html + content_html + script))
