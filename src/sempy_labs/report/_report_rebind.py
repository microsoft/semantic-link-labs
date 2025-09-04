from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_item_name_and_id,
    _base_api,
    resolve_dataset_name_and_id,
)
from typing import Optional, List
from sempy._utils._log import log
import sempy_labs._icons as icons
from uuid import UUID


@log
def report_rebind(
    report: str | UUID | List[str | UUID],
    dataset: str | UUID,
    report_workspace: Optional[str | UUID] = None,
    dataset_workspace: Optional[str | UUID] = None,
):
    """
    Rebinds a report to a semantic model.

    This is a wrapper function for the following API: `Reports - Rebind Report In Group <https://learn.microsoft.com/rest/api/power-bi/reports/rebind-report-in-group>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    report : str | uuid.UUID | List[str | uuid.UUID]
        Name(s) or ID(s) of the Power BI report(s).
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    report_workspace : str | uuid.UUID, default=None
        The name or ID of the Fabric workspace in which the report resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    dataset_workspace : str | uuid.UUID, default=None
        The name or ID of the Fabric workspace in which the semantic model resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (report_workspace_name, report_workspace_id) = resolve_workspace_name_and_id(
        report_workspace
    )

    if dataset_workspace is None:
        dataset_workspace = report_workspace_name

    (dataset_workspace_name, dataset_workspace_id) = resolve_workspace_name_and_id(
        dataset_workspace
    )
    if isinstance(report, str):
        report = [report]

    for rpt in report:
        (report_name, report_id) = resolve_item_name_and_id(
            item=rpt, type="Report", workspace=report_workspace_id
        )
        (dataset_name, dataset_id) = resolve_item_name_and_id(
            item=dataset, type="SemanticModel", workspace=dataset_workspace
        )

        payload = {"datasetId": dataset_id}

        _base_api(
            request=f"v1.0/myorg/groups/{report_workspace_id}/reports/{report_id}/Rebind",
            method="post",
            payload=payload,
            client="fabric_sp",
        )

        print(
            f"{icons.green_dot} The '{report_name}' report within the '{report_workspace_name}' workspace has been successfully rebinded to the '{dataset_name}' semantic model within the '{dataset_workspace_name}' workspace."
        )


@log
def report_rebind_all(
    dataset: str | UUID,
    new_dataset: str | UUID,
    dataset_workspace: Optional[str | UUID] = None,
    new_dataset_workspace: Optional[str | UUID] = None,
    report_workspace: Optional[str | UUID | List[str | UUID]] = None,
):
    """
    Rebinds all reports across the provided report workspaces which are bound to a specific semantic model to a new semantic model.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name of the semantic model currently binded to the reports.
    new_dataset : str | uuid.UUID
        Name of the semantic model to rebind to the reports.
    dataset_workspace : str | uuid.UUID, default=None
        The name of the Fabric workspace in which the original semantic model resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    new_dataset_workspace : str | uuid.UUID, default=None
        The name of the Fabric workspace in which the new semantic model resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    report_workspace : str | uuid.UUID | List[str | uuid.UUID], default=None
        The name(s) or IDs of the Fabric workspace(s) in which the report(s) reside(s).
        Defaults to None which finds all reports in all workspaces which use the semantic model and rebinds them to
        the new semantic model.
    """

    from sempy_labs._list_functions import list_reports_using_semantic_model

    (dataset_name, dataset_id) = resolve_dataset_name_and_id(
        dataset=dataset, workspace=dataset_workspace
    )
    (new_dataset_name, new_dataset_id) = resolve_dataset_name_and_id(
        dataset=new_dataset, workspace=new_dataset_workspace
    )

    if dataset_id == new_dataset_id:
        raise ValueError(
            f"{icons.red_dot} The 'dataset' and 'new_dataset' parameters are both set to '{dataset}'. These parameters must be set to different values."
        )
    (dataset_workspace_name, dataset_workspace_id) = resolve_workspace_name_and_id(
        workspace=dataset_workspace
    )

    if isinstance(report_workspace, str):
        report_workspace = [report_workspace]

    dfR = list_reports_using_semantic_model(
        dataset=dataset, workspace=dataset_workspace
    )

    if dfR.empty:
        print(
            f"{icons.info} The '{dataset_name}' semantic model within the '{dataset_workspace_name}' workspace has no dependent reports."
        )
        return

    if report_workspace is None:
        dfR_filt = dfR.copy()
    else:
        dfR_filt = dfR[
            (dfR["Report Workspace Name"].isin(report_workspace))
            | (dfR["Report Workspace ID"].isin(report_workspace))
        ]

    if dfR_filt.empty:
        print(
            f"{icons.info} No reports found for the '{dataset_name}' semantic model within the '{dataset_workspace_name}' workspace."
        )
        return

    for _, r in dfR_filt.iterrows():
        rpt_name = r["Report Name"]
        rpt_wksp = r["Report Workspace Name"]

        report_rebind(
            report=rpt_name,
            dataset=new_dataset,
            report_workspace=rpt_wksp,
            dataset_workspace=new_dataset_workspace,
        )
