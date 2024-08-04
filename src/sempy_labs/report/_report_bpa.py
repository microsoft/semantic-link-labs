from typing import Optional
import pandas as pd
from sempy._utils._log import log
from sempy_labs.report import ReportWrapper
from sempy_labs.report._report_bpa_rules import report_bpa_rules
from sempy_labs._helper_functions import format_dax_object_name


@log
def run_report_bpa(
    report: str,
    rules: Optional[pd.DataFrame] = None,
    workspace: Optional[str] = None,
    language: Optional[str] = None,
):

    rpt = ReportWrapper(report=report, workspace=workspace, readonly=True)
    dfCV = rpt.list_custom_visuals(extened=True)
    dfP = rpt.list_pages()
    dfRF = rpt.list_report_filters()
    dfPF = rpt.list_page_filters()
    dfVF = rpt.list_visual_filters()
    dfRLM = rpt.list_report_level_measures()
    dfV = rpt.list_visuals(extended=True)

    if rules is None:
        rules = report_bpa_rules()

    scope_to_dataframe = {
        "Custom Visual": (dfCV, ["Custom Visual Display Name"]),
        "Page": (dfP, ["Page Display Name"]),
        "Visual": (dfV, format_dax_object_name(["Page Display Name"], ["Visual Name"])),
        "Report Filter": (
            dfRF,
            format_dax_object_name(["Table Name"], ["Object Name"]),
        ),
        "Page Filter": (
            dfPF,
            f"{['Page Display Name']} : {format_dax_object_name(['Table Name'], ['Object Name'])}",
        ),
        "Visual Filter": (
            dfVF,
            f"{format_dax_object_name(['Page Display Name'], ['Visual Name'])} : {format_dax_object_name(['Table Name'], ['Object Name'])}",
        ),
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

    return finalDF
