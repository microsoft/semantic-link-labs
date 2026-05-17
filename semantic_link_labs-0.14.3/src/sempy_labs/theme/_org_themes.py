import pandas as pd
from uuid import UUID
from sempy_labs._helper_functions import (
    _is_valid_uuid,
    _base_api,
    _update_dataframe_datatypes,
    _create_dataframe,
)


_pbi_url_prefix = None
_theme_url_prefix = "metadata/v202409/organization/themes"


def init_pbi_url_prefix():
    global _pbi_url_prefix
    if _pbi_url_prefix:
        return

    response = _base_api("/v1.0/myorg/capacities")
    _pbi_url_prefix = response.json().get("@odata.context").split("/v1.0/myorg")[0]


def list_org_themes() -> pd.DataFrame:
    """
    Lists all `organizational themes <https://www.linkedin.com/pulse/organizational-themes-preview-pbicorevisuals-j7jxe/>`_ in Power BI. Note that this uses an internal API and may break at any time.

    Returns
    -------
    pandas.DataFrame
        A dataframe containing the details of the organizational themes.
    """

    init_pbi_url_prefix()

    columns = {
        "Theme Id": "str",
        "Theme Name": "str",
        "Description": "str",
        "Enabled In Theme Gallery": "bool",
        "Update Time": "datetime",
        "For Copilot": "bool",
        "Theme Json": "str",
    }

    df = _create_dataframe(columns=columns)

    response = _base_api(request=f"{_pbi_url_prefix}/{_theme_url_prefix}")

    result = response.json().get("orgThemes")
    if result:
        df = pd.json_normalize(result)
        df.rename(
            columns={
                "objectId": "Theme Id",
                "displayName": "Theme Name",
                "description": "Description",
                "enabledInThemeGallery": "Enabled In Theme Gallery",
                "updateTime": "Update Time",
                "forCopilot": "For Copilot",
                "themeJson": "Theme Json",
            },
            inplace=True,
        )
        _update_dataframe_datatypes(df, columns)

    return df


def resolve_theme_id(theme: str | UUID):
    """
    Resolves the ID of an organization theme by its name or ID.

    Parameters
    ----------
    theme : str | uuid.UUID
        The name or ID of the organization theme.

    Returns
    -------
    uuid.UUID
        The ID of the organization theme.
    """

    if _is_valid_uuid(theme):
        return theme
    else:
        df = list_org_themes()
        df_filt = df[df["Theme Name"] == theme]
        if df_filt.empty:
            raise ValueError("Invalid theme")
        else:
            return df_filt["Theme Id"].iloc[0]


def get_org_theme_json(theme: str | UUID) -> dict:
    """
    Retrieves the JSON representation of an organization theme by its name or ID.

    Parameters
    ----------
    theme : str | uuid.UUID
        The name or ID of the organization theme.

    Returns
    -------
    dict
        The JSON representation of the organization theme.
    """

    init_pbi_url_prefix()

    theme_id = resolve_theme_id(theme)
    response = _base_api(request=f"{_pbi_url_prefix}/{_theme_url_prefix}/{theme_id}")
    return response.json().get("themeJson", {})


def delete_org_theme(theme: str | UUID) -> None:
    """
    Deletes an organization theme by its name or ID.
    """
    init_pbi_url_prefix()

    theme_id = resolve_theme_id(theme)
    _base_api(
        request=f"{_pbi_url_prefix}/{_theme_url_prefix}/{theme_id}",
        method="delete",
        status_codes=204,
    )
