import sempy.fabric as fabric
import pandas as pd
import datetime
from pyspark.sql import SparkSession
from sempy_labs._helper_functions import resolve_lakehouse_name, save_as_delta_table
from sempy_labs.lakehouse import get_lakehouse_tables, lakehouse_attached
from sempy_labs._model_bpa import run_model_bpa
from typing import Optional, List
from sempy._utils._log import log
import sempy_labs._icons as icons


@log
def run_model_bpa_bulk(
    rules: Optional[pd.DataFrame] = None,
    extended: Optional[bool] = False,
    language: Optional[str] = None,
    workspace: Optional[str | List[str]] = None,
):
    """
    Runs the semantic model Best Practice Analyzer across all semantic models in a workspace (or all accessible workspaces).
    Saves (appends) the results to the 'modelbparesults' delta table in the lakehouse attached to the notebook.
    Default semantic models are skipped in this analysis.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    rules : pandas.DataFrame, default=None
        A pandas dataframe containing rules to be evaluated. Based on the format of the dataframe produced by the model_bpa_rules function.
    extended : bool, default=False
        If True, runs the set_vertipaq_annotations function to collect Vertipaq Analyzer statistics to be used in the analysis of the semantic model.
    language : str, default=None
        The language (code) in which the rules will appear. For example, specifying 'it-IT' will show the Rule Name, Category and Description in Italian.
        Defaults to None which resolves to English.
    workspace : str | List[str], default=None
        The workspace or list of workspaces to scan.
        Defaults to None which scans all accessible workspaces.

    Returns
    -------
    """

    if not lakehouse_attached():
        raise ValueError(
            "No lakehouse is attached to this notebook. Must attach a lakehouse to the notebook."
        )

    cols = [
        "Workspace Name",
        "Dataset Name",
        "Configured By",
        "Rule Name",
        "Category",
        "Severity",
        "Object Type",
        "Object Name",
        "Description",
        "URL",
        "RunId",
        "Timestamp",
    ]
    now = datetime.datetime.now()
    output_table = "modelbparesults"
    spark = SparkSession.builder.getOrCreate()
    lakehouse_workspace = fabric.resolve_workspace_name()
    lakehouse_id = fabric.get_lakehouse_id()
    lakehouse = resolve_lakehouse_name(
        lakehouse_id=lakehouse_id, workspace=lakehouse_workspace
    )
    lakeT = get_lakehouse_tables(lakehouse=lakehouse, workspace=lakehouse_workspace)
    lakeT_filt = lakeT[lakeT["Table Name"] == output_table]
    query = f"SELECT MAX(RunId) FROM {lakehouse}.{output_table}"
    if len(lakeT_filt) == 0:
        runId = 1
    else:
        dfSpark = spark.sql(query)
        maxRunId = dfSpark.collect()[0][0]
        runId = maxRunId + 1

    if isinstance(workspace, str):
        workspace = [workspace]

    dfW = fabric.list_workspaces()
    for i, r in dfW.iterrows():
        wksp = r["Name"]
        if workspace is None or wksp in workspace:
            df = pd.DataFrame(columns=cols)
            dfD = fabric.list_datasets(workspace=wksp, mode="rest")

            if len(dfD) > 0:
                dfI = fabric.list_items(workspace=wksp)

                # Exclude default semantic models
                filtered_df = dfI.groupby("Display Name").filter(
                    lambda x: set(["Warehouse", "SemanticModel"]).issubset(
                        set(x["Type"])
                    )
                    or set(["Lakehouse", "SemanticModel"]).issubset(set(x["Type"]))
                )
                default_semantic_models = filtered_df["Display Name"].unique().tolist()
                # Skip BPAModel :)
                skip_models = default_semantic_models + ["BPAModel"]
                dfD_filt = dfD[~dfD["Dataset Name"].isin(skip_models)]

                if len(dfD_filt) > 0:
                    for i2, r2 in dfD_filt.iterrows():
                        dataset_name = r2["Dataset Name"]
                        config_by = r2["Configured By"]
                        print(
                            f"{icons.in_progress} Collecting BPA stats for the '{dataset_name}' semantic model within the '{wksp}' workspace."
                        )
                        try:
                            bpa_df = run_model_bpa(
                                dataset=dataset_name,
                                workspace=wksp,
                                language=language,
                                return_dataframe=True,
                                rules=rules,
                                extended=extended,
                            )
                            bpa_df["Workspace Name"] = wksp
                            bpa_df["Dataset Name"] = dataset_name
                            bpa_df["Configured By"] = config_by
                            bpa_df["Timestamp"] = now
                            bpa_df["RunId"] = runId
                            bpa_df = bpa_df[cols]

                            bpa_df["RunId"] = bpa_df["RunId"].astype("int")

                            df = pd.concat([df, bpa_df], ignore_index=True)
                            print(
                                f"{icons.green_dot} Collected BPA stats for the '{dataset_name}' semantic model within the '{wksp}' workspace."
                            )
                        except Exception:
                            print(
                                f"{icons.red_dot} BPA Issue: The '{dataset_name}' semantic model within the '{wksp}' workspace."
                            )

                    df["Severity"].replace(icons.warning, "Warning", inplace=True)
                    df["Severity"].replace(icons.error, "Error", inplace=True)
                    df["Severity"].replace(icons.info, "Info", inplace=True)

                    # Append save results individually for each workspace (so as not to create a giant dataframe)
                    print(
                        f"{icons.in_progress} Saving the BPA results of the '{wksp}' workspace to the '{output_table}' within the '{lakehouse}' lakehouse within the '{lakehouse_workspace}' workspace..."
                    )
                    save_as_delta_table(
                        dataframe=df,
                        delta_table_name=output_table,
                        write_mode="append",
                        merge_schema=True,
                    )
                    print(
                        f"{icons.green_dot} Saved BPA results to the '{output_table}' delta table."
                    )

    print(f"{icons.green_dot} Bulk BPA scan complete.")


def create_model_bpa_semantic_model(
    dataset: Optional[str] = "BPAModel",
    lakehouse: Optional[str] = None,
    lakehouse_workspace: Optional[str] = None,
):
    """
    Dynamically generates a Direct Lake semantic model based on the 'modelbparesults' delta table which contains the Best Practice Analyzer results.
    This semantic model used in combination with the corresponding Best Practice Analyzer report can be used to analyze multiple semantic models
    on multiple workspaces at once (and over time).

    The semantic model is always created within the same workspace as the lakehouse.

    Parameters
    ----------
    dataset : str, default='BPAModel'
        Name of the semantic model to be created.
    lakehouse : str, default=None
        Name of the Fabric lakehouse which contains the 'modelbparesults' delta table.
        Defaults to None which resolves to the default lakehouse attached to the notebook.
    lakehouse_workspace : str, default=None
        The workspace in which the lakehouse resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    """

    from sempy_labs._helper_functions import resolve_lakehouse_name
    from sempy_labs.directlake import (
        get_shared_expression,
        add_table_to_direct_lake_semantic_model,
    )
    from sempy_labs import create_blank_semantic_model, refresh_semantic_model
    from sempy_labs.tom import connect_semantic_model

    lakehouse_workspace = fabric.resolve_workspace_name(lakehouse_workspace)

    if lakehouse is None:
        lakehouse_id = fabric.get_lakehouse_id()
        lakehouse = resolve_lakehouse_name(
            lakehouse_id=lakehouse_id, workspace=lakehouse_workspace
        )

    # Generate the shared expression based on the lakehouse and lakehouse workspace
    expr = get_shared_expression(lakehouse=lakehouse, workspace=lakehouse_workspace)

    # Create blank model
    create_blank_semantic_model(dataset=dataset, workspace=lakehouse_workspace)
    table_exists = False
    with connect_semantic_model(
        dataset=dataset, readonly=False, workspace=lakehouse_workspace
    ) as tom:
        t_name = "BPAResults"
        t_name_full = f"'{t_name}'"
        # Create the shared expression
        if not any(e.Name == "DatabaseQuery" for e in tom.model.Expressions):
            tom.add_expression(name="DatabaseQuery", expression=expr)
        # Add the table to the model
        if any(t.Name == t_name for t in tom.model.Tables):
            table_exists = True
    if not table_exists:
        add_table_to_direct_lake_semantic_model(
            dataset=dataset,
            table_name=t_name,
            lakehouse_table_name="modelbparesults",
            workspace=lakehouse_workspace,
            refresh=False,
        )
    with connect_semantic_model(
        dataset=dataset, readonly=False, workspace=lakehouse_workspace
    ) as tom:
        # Fix column names
        for c in tom.all_columns():
            if c.Name == "Dataset_Name":
                c.Name = "Model"
            elif c.Name == "Workspace_Name":
                c.Name = "Workspace"
            elif c.Name == "Configured_By":
                c.Name = "Model Owner"
            elif c.Name == "URL":
                c.DataCategory = "WebURL"
            elif c.Name == "RunId":
                tom.set_summarize_by(
                    table_name=c.Parent.Name, column_name=c.Name, value="None"
                )
            c.Name = c.Name.replace("_", " ")

        # Implement pattern for base measures
        def get_expr(table_name, calculation):
            return f"IF(HASONEFILTER({table_name}[RunId]),{calculation},CALCULATE({calculation},FILTER(VALUES({table_name}[RunId]),{table_name}[RunId] = [Max Run Id])))"

        # Add measures
        if not any(m.Name == "Max Run Id" for m in tom.all_measures()):
            tom.add_measure(
                table_name=t_name,
                measure_name="Max Run Id",
                expression=f"CALCULATE(MAX({t_name_full}[RunId]),{t_name_full}[RunId])",
                format_string="#,0",
            )
        if not any(m.Name == "Models" for m in tom.all_measures()):
            calc = f"COUNTROWS(DISTINCT({t_name_full}[Model]))"
            tom.add_measure(
                table_name=t_name,
                measure_name="Models",
                expression=get_expr(t_name_full, calc),
                format_string="#,0",
            )
        if not any(m.Name == "Workspaces" for m in tom.all_measures()):
            calc = f"COUNTROWS(DISTINCT({t_name_full}[Workspace]))"
            tom.add_measure(
                table_name=t_name,
                measure_name="Workspaces",
                expression=get_expr(t_name_full, calc),
                format_string="#,0",
            )
        if not any(m.Name == "Violations" for m in tom.all_measures()):
            calc = f"COUNTROWS({t_name_full})"
            tom.add_measure(
                table_name=t_name,
                measure_name="Violations",
                expression=get_expr(t_name_full, calc),
                format_string="#,0",
            )
        if not any(m.Name == "Error Violations" for m in tom.all_measures()):
            tom.add_measure(
                table_name=t_name,
                measure_name="Error Violations",
                expression=f'CALCULATE([Violations],{t_name_full}[Severity]="Error")',
                format_string="#,0",
            )
        if not any(m.Name == "Rules Violated" for m in tom.all_measures()):
            calc = f"COUNTROWS(DISTINCT({t_name_full}[Rule Name]))"
            tom.add_measure(
                table_name=t_name,
                measure_name="Rules Violated",
                expression=get_expr(t_name_full, calc),
                format_string="#,0",
            )
        if not any(m.Name == "Rule Severity" for m in tom.all_measures()):
            tom.add_measure(
                table_name=t_name,
                measure_name="Rule Severity",
                expression=f"IF(ISFILTERED({t_name_full}[Rule Name]),IF( HASONEVALUE({t_name_full}[Rule Name]),MIN({t_name_full}[Severity])))",
            )
        # tom.add_measure(table_name=t_name, measure_name='Rules Followed', expression="[Rules] - [Rules Violated]")

    # Refresh the model
    refresh_semantic_model(dataset=dataset, workspace=lakehouse_workspace)
