import sempy
import sempy.fabric as fabric
from sempy_labs._helper_functions import resolve_dataset_id, resolve_report_id
from typing import List, Optional, Union
from sempy._utils._log import log
import sempy_labs._icons as icons


@log
def report_rebind(
    report: str,
    dataset: str,
    report_workspace: Optional[str] = None,
    dataset_workspace: Optional[str] = None,
):
    """
    Rebinds a report to a semantic model.

    Parameters
    ----------
    report : str
        Name of the Power BI report.
    dataset : str
        Name of the semantic model.
    report_workspace : str, default=None
        The name of the Fabric workspace in which the report resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    dataset_workspace : str, default=None
        The name of the Fabric workspace in which the semantic model resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------

    """

    if report_workspace == None:
        report_workspace_id = fabric.get_workspace_id()
        report_workspace = fabric.resolve_workspace_name(report_workspace_id)
    else:
        report_workspace_id = fabric.resolve_workspace_id(report_workspace)
    if dataset_workspace == None:
        dataset_workspace = report_workspace

    client = fabric.PowerBIRestClient()

    reportId = resolve_report_id(report=report, workspace=report_workspace)
    datasetId = resolve_dataset_id(dataset=dataset, workspace=dataset_workspace)

    # Prepare API
    request_body = {"datasetId": datasetId}

    response = client.post(
        f"/v1.0/myorg/groups/{report_workspace_id}/reports/{reportId}/Rebind",
        json=request_body,
    )

    if response.status_code == 200:
        print(
            f"{icons.green_dot} The '{report}' report has been successfully rebinded to the '{dataset}' semantic model."
        )
    else:
        print(
            f"{icons.red_dot} The '{report}' report within the '{report_workspace}' workspace failed to rebind to the '{dataset}' semantic model within the '{dataset_workspace}' workspace."
        )


@log
def report_rebind_all(
    dataset: str,
    new_dataset: str,
    dataset_workspace: Optional[str] = None,
    new_dataset_workpace: Optional[str] = None,
    report_workspace: Optional[str] = None,
):
    """
    Rebinds all reports in a workspace which are bound to a specific semantic model to a new semantic model.

    Parameters
    ----------
    report : str
        Name of the Power BI report.
    dataset : str
        Name of the semantic model currently binded to the reports.
    new_dataset : str
        Name of the semantic model to rebind to the reports.
    dataset_workspace : str, default=None
        The name of the Fabric workspace in which the original semantic model resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    new_dataset_workspace : str, default=None
        The name of the Fabric workspace in which the new semantic model resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    report_workspace : str, default=None
        The name of the Fabric workspace in which the report resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------

    """

    if dataset_workspace == None:
        dataset_workspace_id = fabric.get_workspace_id()
        dataset_workspace = fabric.resolve_workspace_name(dataset_workspace_id)
    else:
        dataset_workspace_id = fabric.resolve_workspace_id(dataset_workspace)

    if new_dataset_workpace == None:
        new_dataset_workpace = dataset_workspace

    if report_workspace == None:
        report_workspace = dataset_workspace

    datasetId = resolve_dataset_id(dataset, dataset_workspace)

    dfRep = fabric.list_reports(workspace=report_workspace)
    dfRep_filt = dfRep[dfRep["Dataset Id"] == datasetId]

    for i, r in dfRep_filt.iterrows():
        rptName = r["Name"]
        report_rebind(
            report=rptName,
            dataset=new_dataset,
            report_workspace=report_workspace,
            dataset_workspace=new_dataset_workpace,
        )
