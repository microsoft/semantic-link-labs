import sempy.fabric as fabric
from sempy_labs._helper_functions import resolve_dataset_id, resolve_report_id
from typing import Optional, List
from sempy._utils._log import log
import sempy_labs._icons as icons
from sempy.fabric.exceptions import FabricHTTPException


@log
def report_rebind(
    report: str | List[str],
    dataset: str,
    report_workspace: Optional[str] = None,
    dataset_workspace: Optional[str] = None,
):
    """
    Rebinds a report to a semantic model.

    Parameters
    ----------
    report : str | List[str]
        Name(s) of the Power BI report(s).
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

    if report_workspace is None:
        report_workspace_id = fabric.get_workspace_id()
        report_workspace = fabric.resolve_workspace_name(report_workspace_id)
    else:
        report_workspace_id = fabric.resolve_workspace_id(report_workspace)
    if dataset_workspace is None:
        dataset_workspace = report_workspace

    client = fabric.PowerBIRestClient()

    if isinstance(report, str):
        report = [report]

    for rpt in report:
        reportId = resolve_report_id(report=rpt, workspace=report_workspace)
        datasetId = resolve_dataset_id(dataset=dataset, workspace=dataset_workspace)

        # Prepare API
        request_body = {"datasetId": datasetId}

        response = client.post(
            f"/v1.0/myorg/groups/{report_workspace_id}/reports/{reportId}/Rebind",
            json=request_body,
        )

        if response.status_code != 200:
            raise FabricHTTPException(response)
        print(
            f"{icons.green_dot} The '{rpt}' report has been successfully rebinded to the '{dataset}' semantic model."
        )


@log
def report_rebind_all(
    dataset: str,
    new_dataset: str,
    dataset_workspace: Optional[str] = None,
    new_dataset_workpace: Optional[str] = None,
    report_workspace: Optional[str | List[str]] = None,
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
    report_workspace : str | List[str], default=None
        The name(s) of the Fabric workspace(s) in which the report(s) reside(s).
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------

    """

    dataset_workspace = fabric.resolve_workspace_name()

    if new_dataset_workpace is None:
        new_dataset_workpace = dataset_workspace

    if report_workspace is None:
        report_workspace = dataset_workspace

    if isinstance(report_workspace, str):
        report_workspace = [report_workspace]

    datasetId = resolve_dataset_id(dataset, dataset_workspace)

    for rw in report_workspace:
        dfRep = fabric.list_reports(workspace=rw)
        dfRep_filt = dfRep[dfRep["Dataset Id"] == datasetId]
        for i, r in dfRep_filt.iterrows():
            rptName = r["Name"]
            report_rebind(
                report=rptName,
                dataset=new_dataset,
                report_workspace=rw,
                dataset_workspace=new_dataset_workpace,
            )
