import sempy.fabric as fabric
import pandas as pd
import datetime
from sempy_labs._helper_functions import (
    save_as_delta_table,
    resolve_workspace_capacity,
    retry,
    _get_column_aggregate,
    resolve_workspace_id,
    resolve_lakehouse_name_and_id,
)
from sempy_labs.lakehouse import (
    get_lakehouse_tables,
    lakehouse_attached,
)
from sempy_labs._model_bpa import run_model_bpa
from typing import Optional, List
from sempy._utils._log import log
import sempy_labs._icons as icons
from uuid import UUID


@log
def run_model_bpa_bulk(
    rules: Optional[pd.DataFrame] = None,
    extended: bool = False,
    language: Optional[str] = None,
    workspace: Optional[str | UUID | List[str | UUID]] = None,
    skip_models: Optional[str | List[str]] = ["ModelBPA", "Fabric Capacity Metrics"],
    skip_models_in_workspace: Optional[dict] = None,
):
    """
    Runs the semantic model Best Practice Analyzer across all semantic models in a workspace (or all accessible workspaces).
    Saves (appends) the results to the 'modelbparesults' delta table in the lakehouse attached to the notebook.
    Default semantic models are skipped in this analysis.

    Parameters
    ----------
    rules : pandas.DataFrame, default=None
        A pandas dataframe containing rules to be evaluated. Based on the format of the dataframe produced by the model_bpa_rules function.
    extended : bool, default=False
        If True, runs the set_vertipaq_annotations function to collect Vertipaq Analyzer statistics to be used in the analysis of the semantic model.
    language : str, default=None
        The language (code) in which the rules will appear. For example, specifying 'it-IT' will show the Rule Name, Category and Description in Italian.
        Defaults to None which resolves to English.
    workspace : str | uuid.UUID | List[str | uuid.UUID], default=None
        The workspace or list of workspaces to scan. Supports both the workspace name and the workspace id.
        Defaults to None which scans all accessible workspaces.
    skip_models : str | List[str], default=['ModelBPA', 'Fabric Capacity Metrics']
        The semantic models to always skip when running this analysis.
    skip_models_in_workspace : dict, default=None
        A dictionary showing specific semantic models within specific workspaces to skip. See the example below:
        {
            "Workspace A": ["Dataset1", "Dataset2"],
            "Workspace B": ["Dataset5", "Dataset 8"],
        }
    """

    if not lakehouse_attached():
        raise ValueError(
            f"{icons.red_dot} No lakehouse is attached to this notebook. Must attach a lakehouse to the notebook."
        )

    if isinstance(skip_models, str):
        skip_models = [skip_models]

    skip_models.extend(["ModelBPA", "Fabric Capacity Metrics"])

    now = datetime.datetime.now()
    output_table = "modelbparesults"
    lakeT = get_lakehouse_tables()
    lakeT_filt = lakeT[lakeT["Table Name"] == output_table]
    if lakeT_filt.empty:
        runId = 1
    else:
        max_run_id = _get_column_aggregate(table_name=output_table)
        runId = max_run_id + 1

    if isinstance(workspace, str):
        workspace = [workspace]

    dfW = fabric.list_workspaces("type ne 'AdminInsights'")
    if workspace is None:
        dfW_filt = dfW.copy()
    else:
        dfW_filt = dfW[(dfW["Name"].isin(workspace)) | (dfW["Id"].isin(workspace))]

    if dfW_filt.empty:
        raise ValueError(
            f"{icons.red_dot} There are no valid workspaces to assess. This is likely due to not having proper permissions to the workspace(s) entered in the 'workspace' parameter."
        )

    for _, r in dfW_filt.iterrows():
        wksp = r["Name"]
        wksp_id = r["Id"]
        capacity_id, capacity_name = resolve_workspace_capacity(workspace=wksp)
        df = pd.DataFrame(columns=list(icons.bpa_schema.keys()))
        dfD = fabric.list_datasets(workspace=wksp, mode="rest")

        # Skip models in workspace
        if skip_models_in_workspace is not None and isinstance(
            skip_models_in_workspace, dict
        ):
            skip_models_wkspc = skip_models_in_workspace.get(wksp)
            dfD = dfD[~dfD["Dataset Name"].isin(skip_models_wkspc)]

        # Exclude default semantic models
        if not dfD.empty:
            dfI = fabric.list_items(workspace=wksp)
            filtered_df = dfI.groupby("Display Name").filter(
                lambda x: set(["Warehouse", "SemanticModel"]).issubset(set(x["Type"]))
                or set(["Lakehouse", "SemanticModel"]).issubset(set(x["Type"]))
            )
            default_semantic_models = filtered_df["Display Name"].unique().tolist()
            skip_models.extend(default_semantic_models)
            dfD_filt = dfD[~dfD["Dataset Name"].isin(skip_models)]

            if not dfD_filt.empty:
                for _, r2 in dfD_filt.iterrows():
                    dataset_id = r2["Dataset Id"]
                    dataset_name = r2["Dataset Name"]
                    config_by = r2["Configured By"]
                    print(
                        f"{icons.in_progress} Collecting Model BPA stats for the '{dataset_name}' semantic model within the '{wksp}' workspace."
                    )
                    try:
                        bpa_df = run_model_bpa(
                            dataset=dataset_id,
                            workspace=wksp,
                            language=language,
                            return_dataframe=True,
                            rules=rules,
                            extended=extended,
                        )
                        bpa_df["Capacity Name"] = capacity_name
                        bpa_df["Capacity Id"] = capacity_id
                        bpa_df["Workspace Name"] = wksp
                        bpa_df["Workspace Id"] = wksp_id
                        bpa_df["Dataset Name"] = dataset_name
                        bpa_df["Dataset Id"] = dataset_id
                        bpa_df["Configured By"] = config_by
                        bpa_df["Timestamp"] = now
                        bpa_df["RunId"] = runId
                        bpa_df = bpa_df[list(icons.bpa_schema.keys())]

                        bpa_df["RunId"] = bpa_df["RunId"].astype("int")

                        if df.empty:
                            df = bpa_df
                        elif not bpa_df.empty:
                            df = pd.concat([df, bpa_df], ignore_index=True)
                        print(
                            f"{icons.green_dot} Collected Model BPA stats for the '{dataset_name}' semantic model within the '{wksp}' workspace."
                        )
                    except Exception as e:
                        print(
                            f"{icons.red_dot} Model BPA failed for the '{dataset_name}' semantic model within the '{wksp}' workspace."
                        )
                        print(e)

                if df.empty:
                    print(
                        f"{icons.yellow_dot} No BPA results to save for the '{wksp}' workspace."
                    )
                else:
                    df["Severity"].replace(icons.severity_mapping, inplace=True)

                    # Append save results individually for each workspace (so as not to create a giant dataframe)
                    print(
                        f"{icons.in_progress} Saving the Model BPA results of the '{wksp}' workspace to the '{output_table}' within the lakehouse attached to this notebook..."
                    )

                    schema = {
                        key.replace(" ", "_"): value
                        for key, value in icons.bpa_schema.items()
                    }

                    save_as_delta_table(
                        dataframe=df,
                        delta_table_name=output_table,
                        write_mode="append",
                        schema=schema,
                        merge_schema=True,
                    )
                    print(
                        f"{icons.green_dot} Saved BPA results to the '{output_table}' delta table."
                    )

    print(f"{icons.green_dot} Bulk BPA scan complete.")


@log
def create_model_bpa_semantic_model(
    dataset: Optional[str] = icons.model_bpa_name,
    lakehouse: Optional[str | UUID] = None,
    lakehouse_workspace: Optional[str | UUID] = None,
):
    """
    Dynamically generates a Direct Lake semantic model based on the 'modelbparesults' delta table which contains the Best Practice Analyzer results.
    This semantic model used in combination with the corresponding Best Practice Analyzer report can be used to analyze multiple semantic models
    on multiple workspaces at once (and over time).

    The semantic model is always created within the same workspace as the lakehouse.

    Parameters
    ----------
    dataset : str, default='ModelBPA'
        Name of the semantic model to be created.
    lakehouse : str | uuid.UUID, default=None
        Name of the Fabric lakehouse which contains the 'modelbparesults' delta table.
        Defaults to None which resolves to the default lakehouse attached to the notebook.
    lakehouse_workspace : str | uuid.UUID, default=None
        The workspace in which the lakehouse resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    from sempy_labs.directlake import (
        generate_shared_expression,
        add_table_to_direct_lake_semantic_model,
    )
    from sempy_labs import create_blank_semantic_model, refresh_semantic_model
    from sempy_labs.tom import connect_semantic_model

    lakehouse_workspace_id = resolve_workspace_id(workspace=lakehouse_workspace)
    (lakehouse_id, lakehouse_name) = resolve_lakehouse_name_and_id(
        lakehouse=lakehouse, workspace=lakehouse_workspace_id
    )

    # Generate the shared expression based on the lakehouse and lakehouse workspace
    expr = generate_shared_expression(
        item_name=lakehouse_name,
        item_type="Lakehouse",
        workspace=lakehouse_workspace_id,
    )

    # Create blank model
    create_blank_semantic_model(
        dataset=dataset, workspace=lakehouse_workspace_id, overwrite=True
    )

    @retry(
        sleep_time=1,
        timeout_error_message=f"{icons.red_dot} Function timed out after 1 minute",
    )
    def dyn_connect():
        with connect_semantic_model(
            dataset=dataset, readonly=True, workspace=lakehouse_workspace_id
        ) as tom:

            tom.model

    dyn_connect()
    icons.sll_tags.append("ModelBPABulk")
    table_exists = False
    with connect_semantic_model(
        dataset=dataset, readonly=False, workspace=lakehouse_workspace_id
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
            workspace=lakehouse_workspace_id,
            refresh=False,
        )
    with connect_semantic_model(
        dataset=dataset, readonly=False, workspace=lakehouse_workspace_id
    ) as tom:
        # Fix column names
        for c in tom.all_columns():
            if c.Name == "Dataset_Name":
                c.Name = "Model"
            elif c.Name == "Dataset_Id":
                c.Name = "Model Id"
            elif c.Name == "Workspace_Name":
                c.Name = "Workspace"
            elif c.Name == "Capacity_Name":
                c.Name = "Capacity"
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
        int_format = "#,0"
        m_name = "Max Run Id"
        if not any(m.Name == m_name for m in tom.all_measures()):
            tom.add_measure(
                table_name=t_name,
                measure_name=m_name,
                expression=f"CALCULATE(MAX({t_name_full}[RunId]),{t_name_full}[RunId])",
                format_string=int_format,
            )
        m_name = "Capacities"
        if not any(m.Name == m_name for m in tom.all_measures()):
            calc = f"COUNTROWS(DISTINCT({t_name_full}[Capacity]))"
            tom.add_measure(
                table_name=t_name,
                measure_name=m_name,
                expression=get_expr(t_name_full, calc),
                format_string=int_format,
            )
        m_name = "Models"
        if not any(m.Name == m_name for m in tom.all_measures()):
            calc = f"COUNTROWS(DISTINCT({t_name_full}[Model]))"
            tom.add_measure(
                table_name=t_name,
                measure_name=m_name,
                expression=get_expr(t_name_full, calc),
                format_string=int_format,
            )
        m_name = "Workspaces"
        if not any(m.Name == m_name for m in tom.all_measures()):
            calc = f"COUNTROWS(DISTINCT({t_name_full}[Workspace]))"
            tom.add_measure(
                table_name=t_name,
                measure_name=m_name,
                expression=get_expr(t_name_full, calc),
                format_string=int_format,
            )
        m_name = "Violations"
        if not any(m.Name == m_name for m in tom.all_measures()):
            calc = f"COUNTROWS({t_name_full})"
            tom.add_measure(
                table_name=t_name,
                measure_name=m_name,
                expression=get_expr(t_name_full, calc),
                format_string=int_format,
            )
        m_name = "Error Violations"
        if not any(m.Name == m_name for m in tom.all_measures()):
            tom.add_measure(
                table_name=t_name,
                measure_name=m_name,
                expression=f'CALCULATE([Violations],{t_name_full}[Severity]="Error")',
                format_string=int_format,
            )
        m_name = "Rules Violated"
        if not any(m.Name == m_name for m in tom.all_measures()):
            calc = f"COUNTROWS(DISTINCT({t_name_full}[Rule Name]))"
            tom.add_measure(
                table_name=t_name,
                measure_name=m_name,
                expression=get_expr(t_name_full, calc),
                format_string=int_format,
            )
        m_name = "Rule Severity"
        if not any(m.Name == m_name for m in tom.all_measures()):
            tom.add_measure(
                table_name=t_name,
                measure_name=m_name,
                expression=f"IF(ISFILTERED({t_name_full}[Rule Name]),IF( HASONEVALUE({t_name_full}[Rule Name]),MIN({t_name_full}[Severity])))",
            )
        # tom.add_measure(table_name=t_name, measure_name='Rules Followed', expression="[Rules] - [Rules Violated]")

    # Refresh the model
    refresh_semantic_model(dataset=dataset, workspace=lakehouse_workspace_id)
