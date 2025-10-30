import pandas as pd
from typing import Optional
from sempy_labs._helper_functions import (
    _build_url,
    _base_api,
    _create_dataframe,
    _update_dataframe_datatypes,
    _is_valid_uuid,
)
from uuid import UUID
import sempy_labs._icons as icons
from sempy._utils._log import log


@log
def list_apps(
    top: Optional[int] = 1000,
    skip: Optional[int] = None,
) -> pd.DataFrame:
    """
    Shows a list of apps in the organization.

    This is a wrapper function for the following API: `Admin - Apps GetAppsAsAdmin <https://learn.microsoft.com/rest/api/power-bi/admin/apps-get-apps-as-admin>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    top : int, default=1000
        Returns only the first n results.
    skip : int, default=None
        Skips the first n results.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of apps in the organization.
    """

    columns = {
        "App Name": "string",
        "App Id": "string",
        "Description": "string",
        "Published By": "string",
        "Last Update": "datetime_coerce",
    }

    df = _create_dataframe(columns=columns)

    params = {}
    url = "/v1.0/myorg/admin/apps"

    params["$top"] = top

    if skip is not None:
        params["$skip"] = skip

    url = _build_url(url, params)
    response = _base_api(request=url, client="fabric_sp")

    rows = []
    for v in response.json().get("value", []):
        rows.append(
            {
                "App Name": v.get("name"),
                "App Id": v.get("id"),
                "Description": v.get("description"),
                "Published By": v.get("publishedBy"),
                "Last Update": v.get("lastUpdate"),
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
def _resolve_app_id(app: str | UUID) -> str:
    if _is_valid_uuid(app):
        return app
    else:
        df = list_apps()
        df_filt = df[df["App Name"] == app]
        if df_filt.empty:
            raise ValueError(f"{icons.red_dot} The '{app}' app does not exist.")
        return df_filt["App Id"].iloc[0]


@log
def list_app_users(app: str | UUID) -> pd.DataFrame:
    """
    Shows a list of users that have access to the specified app.

    This is a wrapper function for the following API: `Admin - Apps GetAppUsersAsAdmin <https://learn.microsoft.com/rest/api/power-bi/admin/apps-get-app-users-as-admin>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    app : str | uuid.UUID
        The name or ID of the app.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of users that have access to the specified app.
    """

    app_id = _resolve_app_id(app)

    columns = {
        "User Name": "string",
        "Email Address": "string",
        "App User Access Right": "string",
        "Identifier": "string",
        "Graph Id": "string",
        "Principal Type": "string",
    }

    df = _create_dataframe(columns=columns)

    url = f"/v1.0/myorg/admin/apps/{app_id}/users"
    response = _base_api(request=url, client="fabric_sp")

    rows = []
    for v in response.json().get("value", []):
        rows.append(
            {
                "User Name": v.get("displayName"),
                "Email Address": v.get("emailAddress"),
                "App User Access Right": v.get("appUserAccessRight"),
                "Identifier": v.get("identifier"),
                "Graph Id": v.get("graphId"),
                "Principal Type": v.get("principalType"),
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df
