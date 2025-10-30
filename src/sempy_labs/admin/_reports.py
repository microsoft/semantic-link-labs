import pandas as pd
from typing import Optional
from sempy_labs._helper_functions import (
    _base_api,
    _create_dataframe,
    _update_dataframe_datatypes,
    _is_valid_uuid,
)
from uuid import UUID
import sempy_labs._icons as icons
from sempy._utils._log import log


@log
def list_reports(
    top: Optional[int] = None,
    skip: Optional[int] = None,
    filter: Optional[str] = None,
) -> pd.DataFrame:
    """
    Shows a list of reports for the organization.

    This is a wrapper function for the following API: `Admin - Reports GetReportsAsAdmin <https://learn.microsoft.com/rest/api/power-bi/admin/reports-get-reports-as-admin>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    top : int, default=None
        Returns only the first n results.
    skip : int, default=None
        Skips the first n results.
    filter : str, default=None
        Returns a subset of a results based on Odata filter query parameter condition.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of reports for the organization.
    """

    columns = {
        "Report Id": "string",
        "Report Name": "string",
        "Type": "string",
        "Web URL": "string",
        "Embed URL": "string",
        "Dataset Id": "string",
        "Created Date": "datetime_coerce",
        "Modified Date": "datetime_coerce",
        "Created By": "string",
        "Modified By": "string",
        "Sensitivity Label Id": "string",
        "Users": "string",
        "Subscriptions": "string",
        "Workspace Id": "string",
        "Report Flags": "int",
    }

    df = _create_dataframe(columns=columns)

    url = "/v1.0/myorg/admin/reports?"
    if top is not None:
        url += f"$top={top}&"
    if skip is not None:
        url += f"$skip={skip}&"
    if filter is not None:
        url += f"$filter={filter}&"

    url.rstrip("$").rstrip("?")
    response = _base_api(request=url, client="fabric_sp")

    rows = []
    for v in response.json().get("value", []):
        rows.append(
            {
                "Report Id": v.get("id"),
                "Report Name": v.get("name"),
                "Type": v.get("reportType"),
                "Web URL": v.get("webUrl"),
                "Embed URL": v.get("embedUrl"),
                "Dataset Id": v.get("datasetId"),
                "Created Date": v.get("createdDateTime"),
                "Modified Date": v.get("modifiedDateTime"),
                "Created By": v.get("createdBy"),
                "Modified By": v.get("modifiedBy"),
                "Sensitivity Label Id": v.get("sensitivityLabel", {}).get("labelId"),
                "Users": v.get("users"),
                "Subscriptions": v.get("subscriptions"),
                "Workspace Id": v.get("workspaceId"),
                "Report Flags": v.get("reportFlags"),
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
def _resolve_report_id(report: str | UUID) -> str:
    if _is_valid_uuid(report):
        return report
    else:
        df = list_reports()
        df_filt = df[df["Report Name"] == report]
        if df_filt.empty:
            raise ValueError(f"{icons.red_dot} The '{report}' report does not exist.")
        return df_filt["Report Id"].iloc[0]


@log
def list_report_users(report: str | UUID) -> pd.DataFrame:
    """
    Shows a list of users that have access to the specified report.

    This is a wrapper function for the following API: `Admin - Reports GetDatasetUsersAsAdmin <https://learn.microsoft.com/rest/api/power-bi/admin/reports-get-report-users-as-admin>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    report : str | uuid.UUID
        The name or ID of the report.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of users that have access to the specified report.
    """

    report_id = _resolve_report_id(report)

    columns = {
        "User Name": "string",
        "Email Address": "string",
        "Report User Access Right": "string",
        "Identifier": "string",
        "Graph Id": "string",
        "Principal Type": "string",
    }

    df = _create_dataframe(columns=columns)

    url = f"/v1.0/myorg/admin/reports/{report_id}/users"
    response = _base_api(request=url, client="fabric_sp")

    rows = []
    for v in response.json().get("value", []):
        rows.append(
            {
                "User Name": v.get("displayName"),
                "Email Address": v.get("emailAddress"),
                "Report User Access Right": v.get("reportUserAccessRight"),
                "Identifier": v.get("identifier"),
                "Graph Id": v.get("graphId"),
                "Principal Type": v.get("principalType"),
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
def list_report_subscriptions(report: str | UUID) -> pd.DataFrame:
    """
    Shows a list of report subscriptions along with subscriber details. This is a preview API call.

    This is a wrapper function for the following API: `Admin - Reports GetReportSubscriptionsAsAdmin <https://learn.microsoft.com/rest/api/power-bi/admin/reports-get-report-subscriptions-as-admin>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    report : str | uuid.UUID
        The name or ID of the report.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of report subscriptions along with subscriber details. This is a preview API call.
    """

    report_id = _resolve_report_id(report)

    columns = {
        "Subscription Id": "string",
        "Title": "string",
        "Artifact Id": "string",
        "Artifact Name": "string",
        "Sub Artifact Name": "string",
        "Artifact Type": "string",
        "Is Enabled": "bool",
        "Frequency": "string",
        "Start Date": "datetime",
        "End Date": "string",
        "Link To Content": "bool",
        "Preview Image": "bool",
        "Attachment Format": "string",
        "Users": "list",
    }

    df = _create_dataframe(columns=columns)

    response = _base_api(
        request=f"/v1.0/myorg/admin/reports/{report_id}/subscriptions",
        client="fabric_sp",
    )

    rows = []
    for v in response.json().get("value", []):
        rows.append(
            {
                "Subscription Id": v.get("id"),
                "Title": v.get("title"),
                "Artifact Id": v.get("artifactId"),
                "Artifact Name": v.get("artifactDisplayName"),
                "Sub Artifact Name": v.get("subArtifactDisplayName"),
                "Artifact Type": v.get("artifactType"),
                "Is Enabled": v.get("isEnabled"),
                "Frequency": v.get("frequency"),
                "Start Date": v.get("startDate"),
                "End Date": v.get("endDate"),
                "Link To Content": v.get("linkToContent"),
                "Preview Image": v.get("previewImage"),
                "Attachment Format": v.get("attachmentFormat"),
                "Users": v.get("users", []),
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df
