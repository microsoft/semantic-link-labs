from uuid import UUID
from typing import Optional
import pandas as pd
from sempy_labs._helper_functions import (
    _base_api,
    _create_dataframe,
    resolve_workspace_id,
    resolve_item_id,
)
from sempy._utils._log import log


@log
def list_reports_base(report: Optional[str | UUID] = None, workspace: Optional[str | UUID] = None) -> pd.DataFrame:

    columns = {
        "Report Name": "string",
        "Report Id": "string",
        "Format": "string",
        "Web Url": "string",
        "Embed Url": "string",
        "Is From PBIX": "bool",
        "Is Owned By Me": "bool",
        "Dataset Id": "string",
        "Dataset Workspace Id": "string",
        "Users": "list",
        "Subscriptions": "list",
    }

    workspace_id = resolve_workspace_id(workspace)
    url = f"/v1.0/myorg/groups/{workspace_id}/reports"

    if report is not None:
        report_id = resolve_item_id(item=report, type="Report", workspace=workspace_id)
        url += f"/{report_id}"

    response_json = _base_api(url).json()

    fields = {
        "Report Name": "name",
        "Report Id": "id",
        "Format": "format",
        "Web Url": "webUrl",
        "Embed Url": "embedUrl",
        "Is From PBIX": "isFromPbix",
        "Is Owned By Me": "isOwnedByMe",
        "Dataset Id": "datasetId",
        "Dataset Workspace Id": "datasetWorkspaceId",
        "Users": "users",
        "Subscriptions": "subscriptions",
    }

    items = [response_json] if report is not None else response_json.get("value", [])

    rows = [
        {
            col: item.get(src, [] if columns[col] == "list" else None)
            for col, src in fields.items()
        }
        for item in items
    ]

    df = pd.DataFrame(rows, columns=columns.keys()) if rows else _create_dataframe(columns=columns)

    return df


@log
def list_reports(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Returns a list of reports from the specified workspace.

    This is a wrapper function for the following API: `Reports - Get Reports In Group <https://learn.microsoft.com/rest/api/power-bi/reports/get-reports-in-group>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The name or ID of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the list of reports.
    """

    return list_reports_base(workspace=workspace)

@log
def get_report(report: str | UUID, workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Returns the properties of a specific report from the specified workspace.

    This is a wrapper function for the following API: `Reports - Get Report In Group <https://learn.microsoft.com/rest/api/power-bi/reports/get-report-in-group>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    report : str | uuid.UUID
        Name or ID of the Power BI report.
    workspace : str | uuid.UUID, default=None
        The name or ID of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the properties of the report.
    """

    return list_reports_base(report=report, workspace=workspace)

