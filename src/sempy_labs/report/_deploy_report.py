import sempy.fabric as fabric
from uuid import UUID
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_item_name_and_id,
    resolve_workspace_name_and_id,
    _base_api,
)
import sempy_labs._icons as icons
from sempy_labs.report._report_rebind import report_rebind


def deploy_report(
    source_report: str | UUID,
    target_report: str,
    source_workspace: Optional[str | UUID] = None,
    target_workspace: Optional[str | UUID] = None,
    overwrite: bool = False,
    rebind_to_source_report: bool = True,
):
    """
    Copies a Power BI report to a new location.

    Parameters
    ----------
    source_report : str | uuid.UUID
        Name or ID of the Power BI report.
    target_report : str
        Name of the new Power BI report.
    source_workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID of the source report.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    target_workspace : str | uuid.UUID, default=None
        The name or ID of the Fabric workspace to place the target report.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    overwrite : bool, default=False
        If True, will overwrite the existing report in the target workspace.
    rebind_to_source_report : bool, default=True
        If True, rebinds a newly created report to the source report's semantic model. This only takes effect when a new report is created, not when a report is updated.
    """

    (source_workspace_name, source_workspace_id) = resolve_workspace_name_and_id(
        source_workspace
    )
    (source_report_name, source_report_id) = resolve_item_name_and_id(source_report)
    (target_workspace_name, target_workspace_id) = resolve_workspace_name_and_id(
        target_workspace
    )

    report_definition = _base_api(
        request=f"/v1/workspaces/{source_workspace_id}/reports/{source_report_id}/getDefinition",
        method="post",
        lro_return_json=True,
        status_codes=None,
    )

    dfR = fabric.list_reports(workspace=target_workspace)
    dfR_filt = dfR[dfR["Name"] == target_report]
    if not dfR_filt.empty:
        if not overwrite:
            print(
                f"{icons.warning} The '{target_report}' report already exists in the '{target_workspace_name}' workspace and the 'overwrite' parameter is set to False."
            )
            return
        else:
            # Update the report
            target_report_id = dfR_filt["Id"].iloc[0]
            _base_api(
                request=f"/v1/workspaces/{target_workspace_id}/reports/{target_report_id}/updateDefinition",
                method="post",
                lro_return_status_code=True,
                status_codes=None,
                payload=report_definition,
            )
            print(
                f"{icons.green_dot} The '{target_report}' report has been updated in the '{target_workspace_name}' workspace."
            )
    else:
        # Create the report
        report_definition["displayName"] = target_report
        _base_api(
            request=f"/v1/workspaces/{target_workspace_id}/reports",
            method="post",
            lro_return_status_code=True,
            status_codes=[201, 202],
            payload=report_definition,
        )
        print(
            f"{icons.green_dot} The '{target_report}' report has been created in the '{target_workspace_name}' workspace."
        )

        # Rebind the newly created report to the semantic model used by the source report
        if rebind_to_source_report:
            dfR = fabric.list_reports(workspace=source_workspace)
            dfR_filt = dfR[dfR["Name"] == source_report_name]
            dataset_id = dfR_filt["Dataset Id"].iloc[0]
            dataset_workspace_id = dfR_filt["Dataset Workspace Id"].iloc[0]
            report_rebind(
                report=target_report,
                dataset=dataset_id,
                report_workspace=target_workspace,
                dataset_workspace=dataset_workspace_id,
            )
