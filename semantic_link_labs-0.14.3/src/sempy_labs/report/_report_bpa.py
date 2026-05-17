from typing import Optional
import pandas as pd
import datetime
from sempy._utils._log import log
from sempy_labs.report._reportwrapper import connect_report
from sempy_labs.report._report_bpa_rules import report_bpa_rules
from sempy_labs._helper_functions import (
    format_dax_object_name,
    save_as_delta_table,
    resolve_item_name_and_id,
    resolve_workspace_capacity,
    _get_column_aggregate,
    resolve_workspace_name_and_id,
)
from sempy_labs.lakehouse import get_lakehouse_tables, lakehouse_attached
import sempy_labs._icons as icons
from IPython.display import display, HTML
from uuid import UUID


@log
def run_report_bpa(
    report: str,
    rules: Optional[pd.DataFrame] = None,
    workspace: Optional[str | UUID] = None,
    # language: Optional[str] = None,
    export: bool = False,
    return_dataframe: bool = False,
):
    """
    Displays an HTML visualization of the results of the Best Practice Analyzer scan for a report.
    Note: As with all functions which rely on the ReportWrapper, this function requires the report to be in the 'PBIR' format.

    Parameters
    ----------
    report : str
        Name of the report.
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

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe in HTML format showing report objects which violated the best practice analyzer rules.
    """

    with connect_report(
        report=report, workspace=workspace, readonly=True, show_diffs=False
    ) as rpt:

        dfCV = rpt.list_custom_visuals()
        dfP = rpt.list_pages()
        dfRF = rpt.list_report_filters()
        dfRF["Filter Object"] = format_dax_object_name(
            dfRF["Table Name"], dfRF["Object Name"]
        )
        dfPF = rpt.list_page_filters()

        dfPF["Filter Object"] = (
            dfPF["Page Display Name"]
            + " : "
            + format_dax_object_name(dfPF["Table Name"], dfPF["Object Name"])
        )
        dfVF = rpt.list_visual_filters()
        dfVF["Filter Object"] = (
            format_dax_object_name(dfVF["Page Display Name"], dfVF["Visual Name"])
            + " : "
            + format_dax_object_name(dfVF["Table Name"], dfVF["Object Name"])
        )
        dfRLM = rpt.list_report_level_measures()
        dfV = rpt.list_visuals()
        dfV["Visual Full Name"] = format_dax_object_name(
            dfV["Page Display Name"], dfV["Visual Name"]
        )
        dfSMO = rpt.list_semantic_model_objects(extended=True)

        # Translations
        if rules is None:
            rules = report_bpa_rules()

        scope_to_dataframe = {
            "Custom Visual": (dfCV, ["Custom Visual Display Name"]),
            "Page": (dfP, ["Page Display Name"]),
            "Visual": (dfV, ["Visual Full Name"]),
            "Report Filter": (dfRF, ["Filter Object"]),
            "Page Filter": (dfPF, ["Filter Object"]),
            "Visual Filter": (dfVF, ["Filter Object"]),
            "Report Level Measure": (dfRLM, ["Measure Name"]),
            "Semantic Model": (dfSMO, ["Report Source Object"]),
        }

        def execute_rule(row):
            scopes = row["Scope"]

            # support both str and list as scope type
            if isinstance(scopes, str):
                scopes = [scopes]

            # collect output dataframes
            df_outputs = []

            for scope in scopes:
                # common fields for each scope
                (df, violation_cols_or_func) = scope_to_dataframe[scope]

                # execute rule and subset df
                df_violations = df[row["Expression"](df)]

                # subset the right output columns (e.g. Table Name & Column Name)
                if isinstance(violation_cols_or_func, list):
                    violation_func = lambda violations: violations[
                        violation_cols_or_func
                    ]
                else:
                    violation_func = violation_cols_or_func

                # build output data frame
                df_output = violation_func(df_violations).copy()

                df_output.columns = ["Object Name"]
                df_output["Rule Name"] = row["Rule Name"]
                df_output["Category"] = row["Category"]

                # Handle 'Object Type' based on scope
                if scope == "Semantic Model":
                    # Ensure 'Report Source Object' is unique in dfSMO
                    dfSMO_unique = dfSMO.drop_duplicates(subset="Report Source Object")
                    # Map 'Object Name' to the 'Report Source' column in dfSMO
                    df_output["Object Type"] = df_output["Object Name"].map(
                        dfSMO_unique.set_index("Report Source Object")["Report Source"]
                    )
                else:
                    df_output["Object Type"] = scope

                df_output["Severity"] = row["Severity"]
                df_output["Description"] = row["Description"]
                if scope in ["Page", "Page Filter"]:
                    if not df_output.empty:
                        for idx, r in df_output.iterrows():
                            df_output.loc[idx, "URL"] = rpt._get_url(
                                page_name=r["Object Name"]
                            )
                elif scope in ["Visual", "Visual Filter"]:
                    if not df_output.empty:
                        for idx, r in df_output.iterrows():
                            df_output.loc[idx, "URL"] = rpt._get_url(
                                page_name=r["Object Name"].split("[")[0][1:-1],
                                visual_name=r["Object Name"].split("[")[1].rstrip("]"),
                            )
                else:
                    df_output["URL"] = rpt._get_url()
                df_outputs.append(df_output)

            return df_outputs

    # flatten list of lists
    flatten_dfs = [
        df for dfs in rules.apply(execute_rule, axis=1).tolist() for df in dfs
    ]

    finalDF = pd.concat(flatten_dfs, ignore_index=True)
    finalDF = finalDF[
        [
            "Category",
            "Rule Name",
            "Object Type",
            "Object Name",
            "Severity",
            "Description",
            "URL",
            # "Report URL",
        ]
    ]

    if return_dataframe:
        return finalDF

    if export:
        if not lakehouse_attached():
            raise ValueError(
                f"{icons.red_dot} In order to export the BPA results, a lakehouse must be attached to the notebook."
            )

        now = datetime.datetime.now()
        delta_table_name = "reportbparesults"
        lakeT = get_lakehouse_tables()
        lakeT_filt = lakeT[lakeT["Table Name"] == delta_table_name]

        if len(lakeT_filt) == 0:
            runId = 1
        else:
            max_run_id = _get_column_aggregate(table_name=delta_table_name)
            runId = max_run_id + 1

        (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
        (report_name, report_id) = resolve_item_name_and_id(
            item=report, type="Report", workspace=workspace_id
        )

        export_df = finalDF.copy()
        capacity_id, capacity_name = resolve_workspace_capacity(workspace=workspace_id)
        export_df["Capacity Name"] = capacity_name
        export_df["Capacity Id"] = capacity_id
        export_df["Workspace Name"] = workspace_name
        export_df["Workspace Id"] = workspace_id
        export_df["Report Name"] = report_name
        export_df["Report Id"] = report_id
        export_df["RunId"] = runId
        export_df["Timestamp"] = now
        export_df["RunId"] = export_df["RunId"].astype(int)

        export_df = export_df[
            [
                "Capacity Name",
                "Capacity Id",
                "Workspace Name",
                "Workspace Id",
                "Report Name",
                "Report Id",
                "Category",
                "Rule Name",
                "Object Type",
                "Object Name",
                "Severity",
                "Description",
                "URL",
            ]
        ]

        save_as_delta_table(
            dataframe=export_df,
            delta_table_name=delta_table_name,
            write_mode="append",
            merge_schema=True,
        )

        return

    finalDF.replace(
        {"Warning": icons.warning, "Error": icons.error, "Info": icons.info},
        inplace=True,
    )

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
                # "Report URL",
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
            if pd.notnull(row["URL"]):
                content_html += (
                    f'<td><a href="{row["URL"]}">{row["Object Name"]}</a></td>'
                )
            else:
                content_html += f'<td>{row["Object Name"]}</td>'
            content_html += f'<td style="text-align: center;">{row["Severity"]}</td>'
            content_html += "</tr>"
        content_html += "</table>"

        content_html += "</div>"
    tab_html += "</div>"

    # Display the tabs, tab contents, and run the script
    return display(HTML(styles + tab_html + content_html + script))
