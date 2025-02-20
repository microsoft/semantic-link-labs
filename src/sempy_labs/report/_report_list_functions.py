import sempy.fabric as fabric
from typing import Optional
import pandas as pd
from sempy_labs._helper_functions import (
    format_dax_object_name,
    resolve_workspace_name_and_id,
    resolve_dataset_name_and_id,
)
from sempy_labs.report._reportwrapper import ReportWrapper
from sempy_labs._list_functions import list_reports_using_semantic_model
from uuid import UUID


def list_unused_objects_in_reports(
    dataset: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Shows a list of all columns in the semantic model which are not used in any related Power BI reports (including dependencies).
    Note: As with all functions which rely on the ReportWrapper, this function requires the report to be in the 'PBIR' format.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of all columns in the semantic model which are not used in any related Power BI reports (including dependencies).
    """

    # TODO: what about relationships/RLS?

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    fabric.refresh_tom_cache(workspace=workspace)

    dfR = _list_all_report_semantic_model_objects(
        dataset=dataset_id, workspace=workspace_id
    )
    dfR_filt = (
        dfR[dfR["Object Type"] == "Column"][["Table Name", "Object Name"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dfR_filt["Column Object"] = format_dax_object_name(
        dfR_filt["Table Name"], dfR_filt["Object Name"]
    )

    dfC = fabric.list_columns(dataset=dataset_id, workspace=workspace_id)
    dfC["Column Object"] = format_dax_object_name(dfC["Table Name"], dfC["Column Name"])

    df = dfC[~(dfC["Column Object"].isin(dfR_filt["Column Object"].values))]
    df = df.drop("Column Object", axis=1)

    return df


def _list_all_report_semantic_model_objects(
    dataset: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Shows a unique list of all semantic model objects (columns, measures, hierarchies) which are used in all reports which leverage the semantic model.
    Note: As with all functions which rely on the ReportWrapper, this function requires the report to be in the 'PBIR' format.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    dfR = list_reports_using_semantic_model(dataset=dataset_id, workspace=workspace_id)
    dfs = []

    for _, r in dfR.iterrows():
        report_name = r["Report Name"]
        report_workspace = r["Report Workspace Name"]

        rpt = ReportWrapper(report=report_name, workspace=report_workspace)

        new_data = rpt._list_all_semantic_model_objects()
        new_data["Report Name"] = report_name
        new_data["Report Workspace"] = report_workspace
        dfs.append(new_data)

    df = pd.concat(dfs, ignore_index=True)

    colName = "Report Name"
    df.insert(2, colName, df.pop(colName))
    colName = "Report Workspace"
    df.insert(3, colName, df.pop(colName))

    return df
