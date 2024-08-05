import sempy.fabric as fabric
from typing import Optional
import pandas as pd
import re
from pyspark.sql import SparkSession
import datetime
from sempy._utils._log import log
from sempy_labs.report import ReportWrapper, report_bpa_rules
from sempy_labs._helper_functions import (
    format_dax_object_name,
    save_as_delta_table,
    resolve_report_id,
    resolve_lakehouse_name,
)
from sempy_labs.lakehouse import get_lakehouse_tables, lakehouse_attached
import sempy_labs._icons as icons


@log
def run_report_bpa(
    report: str,
    rules: Optional[pd.DataFrame] = None,
    workspace: Optional[str] = None,
    language: Optional[str] = None,
    return_dataframe: Optional[bool] = False,
    export: Optional[bool] = False,
):

    rpt = ReportWrapper(report=report, workspace=workspace, readonly=True)
    dfCV = rpt.list_custom_visuals(extended=True)
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
    dfV = rpt.list_visuals(extended=True)
    dfV["Visual Full Name"] = format_dax_object_name(
        dfV["Page Display Name"], dfV["Visual Name"]
    )

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
                violation_func = lambda violations: violations[violation_cols_or_func]
            else:
                violation_func = violation_cols_or_func

            # build output data frame
            df_output = violation_func(df_violations).copy()

            df_output.columns = ["Object Name"]
            df_output["Rule Name"] = row["Rule Name"]
            df_output["Category"] = row["Category"]
            df_output["Object Type"] = scope
            df_output["Severity"] = row["Severity"]
            df_output["Description"] = row["Description"]
            # df_output['URL'] = row['URL']

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
            # "Page Name",
            "Severity",
            "Description",
            # "URL",
        ]
    ]

    dfR = fabric.list_reports(workspace=workspace)
    dfR_filt = dfR[dfR["Name"] == report]
    web_url = dfR_filt["Web Url"].iloc[0]

    # for i, r in finalDF.iterrows():
    #    object_type = r["Object Type"]
    #    object_name = r["Object Name"]
    #    if object_type in ["Page", "Page Filter"]:
    #        page_id, page_display, page_file = rpt.resolve_page_name(
    #            page_name=object_name
    #        )
    #        r["Page Name"] = page_id
    #    elif object_type in ["Visual", "Visual Filter"]:
    #        pattern = r"'(.*?)'|\[(.*?)\]"
    #        matches = re.findall(pattern, object_name)
    #        page_name = matches[0][0]
    #        visual_id = matches[1][1]
    #        page_id, page_display, visual_name, visual_path = rpt.resolve_visual_name(
    #            page_name=page_name, visual_name=visual_id
    #        )
    #        r["Page Name"] = page_id

    if return_dataframe:
        return finalDF

    if export:
        if not lakehouse_attached():
            raise ValueError(
                f"{icons.red_dot} In order to export the BPA results, a lakehouse must be attached to the notebook."
            )

        now = datetime.datetime.now()
        delta_table_name = "reportbparesults"
        lakehouse_id = fabric.get_lakehouse_id()
        lake_workspace = fabric.get_workspace_id()
        lakehouse = resolve_lakehouse_name(
            lakehouse_id=lakehouse_id, workspace=lake_workspace
        )

        lakeT = get_lakehouse_tables(lakehouse=lakehouse, workspace=lake_workspace)
        lakeT_filt = lakeT[lakeT["Table Name"] == delta_table_name]

        spark = SparkSession.builder.getOrCreate()
        query = f"SELECT MAX(RunId) FROM {lakehouse}.{delta_table_name}"

        if len(lakeT_filt) == 0:
            runId = 1
        else:
            dfSpark = spark.sql(query)
            maxRunId = dfSpark.collect()[0][0]
            runId = maxRunId + 1

        export_df = finalDF.copy()
        dfW = fabric.list_workspaces(filter=f"name eq '{workspace}'")
        capacity_id = dfW["Capacity Id"].iloc[0]
        dfC = fabric.list_capacities()
        dfC_filt = dfC[dfC["Id"] == capacity_id]
        capacity_name = dfC_filt["Display Name"].iloc[0]
        export_df["Capacity Name"] = capacity_name
        export_df["Capacity Id"] = capacity_id
        export_df["Workspace Name"] = workspace
        export_df["Workspace Id"] = fabric.resolve_workspace_id(workspace)
        export_df["Report Name"] = report
        export_df["Report Id"] = resolve_report_id(report, workspace)
        export_df["RunId"] = runId
        export_df["Timestamp"] = now
        export_df["RunId"] = export_df["RunId"].astype(int)

        export_df = [
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
                # "Page Name",
                "Severity",
                "Description",
                # "URL",
            ]
        ]
        save_as_delta_table(
            dataframe=export_df,
            delta_table_name=delta_table_name,
            write_mode="append",
            merge_schema=True,
        )

    finalDF.replace(
        {"Warning": icons.warning, "Error": icons.error, "Info": icons.info},
        inplace=True,
    )

    return finalDF
