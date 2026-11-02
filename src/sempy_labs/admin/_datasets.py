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
def list_datasets(
    top: Optional[int] = None,
    filter: Optional[str] = None,
    skip: Optional[int] = None,
) -> pd.DataFrame:
    """
    Shows a list of datasets for the organization.

    This is a wrapper function for the following API: `Admin - Datasets GetDatasetsAsAdmin <https://learn.microsoft.com/rest/api/power-bi/admin/datasets-get-datasets-as-admin>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    top : int, default=None
        Returns only the first n results.
    filter : str, default=None
        Returns a subset of a results based on Odata filter query parameter condition.
    skip : int, default=None
        Skips the first n results.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of datasets for the organization.
    """

    columns = {
        "Dataset Id": "string",
        "Dataset Name": "string",
        "Web URL": "string",
        "Add Rows API Enabled": "bool",
        "Configured By": "string",
        "Is Refreshable": "bool",
        "Is Effective Identity Required": "bool",
        "Is Effective Identity Roles Required": "bool",
        "Target Storage Mode": "string",
        "Created Date": "datetime",
        "Content Provider Type": "string",
        "Create Report Embed URL": "string",
        "QnA Embed URL": "string",
        "Upstream Datasets": "list",
        "Users": "list",
        "Is In Place Sharing Enabled": "bool",
        "Workspace Id": "string",
        "Auto Sync Read Only Replicas": "bool",
        "Max Read Only Replicas": "int",
    }

    df = _create_dataframe(columns=columns)

    params = {}
    url = "/v1.0/myorg/admin/datasets"

    if top is not None:
        params["$top"] = top

    if filter is not None:
        params["$filter"] = filter

    if skip is not None:
        params["$skip"] = skip

    url = _build_url(url, params)
    response = _base_api(request=url, client="fabric_sp")

    rows = []
    for v in response.json().get("value", []):
        rows.append(
            {
                "Dataset Id": v.get("id"),
                "Dataset Name": v.get("name"),
                "Web URL": v.get("webUrl"),
                "Add Rows API Enabled": v.get("addRowsAPIEnabled"),
                "Configured By": v.get("configuredBy"),
                "Is Refreshable": v.get("isRefreshable"),
                "Is Effective Identity Required": v.get("isEffectiveIdentityRequired"),
                "Is Effective Identity Roles Required": v.get(
                    "isEffectiveIdentityRolesRequired"
                ),
                "Target Storage Mode": v.get("targetStorageMode"),
                "Created Date": pd.to_datetime(v.get("createdDate")),
                "Content Provider Type": v.get("contentProviderType"),
                "Create Report Embed URL": v.get("createReportEmbedURL"),
                "QnA Embed URL": v.get("qnaEmbedURL"),
                "Upstream Datasets": v.get("upstreamDatasets", []),
                "Users": v.get("users", []),
                "Is In Place Sharing Enabled": v.get("isInPlaceSharingEnabled"),
                "Workspace Id": v.get("workspaceId"),
                "Auto Sync Read Only Replicas": v.get("queryScaleOutSettings", {}).get(
                    "autoSyncReadOnlyReplicas"
                ),
                "Max Read Only Replicas": v.get("queryScaleOutSettings", {}).get(
                    "maxReadOnlyReplicas"
                ),
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
def _resolve_dataset_id(dataset: str | UUID) -> str:
    if _is_valid_uuid(dataset):
        return dataset
    else:
        df = list_datasets()
        df_filt = df[df["Dataset Name"] == dataset]
        if df_filt.empty:
            raise ValueError(f"{icons.red_dot} The '{dataset}' dataset does not exist.")
        return df_filt["Dataset Id"].iloc[0]


@log
def list_dataset_users(dataset: str | UUID) -> pd.DataFrame:
    """
    Shows a list of users that have access to the specified dataset.

    This is a wrapper function for the following API: `Admin - Datasets GetDatasetUsersAsAdmin <https://learn.microsoft.com/rest/api/power-bi/admin/datasets-get-dataset-users-as-admin>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    dataset : str | uuid.UUID
        The name or ID of the dataset.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of users that have access to the specified dataset.
    """

    dataset_id = _resolve_dataset_id(dataset)

    columns = {
        "User Name": "string",
        "Email Address": "string",
        "Dataset User Access Right": "string",
        "Identifier": "string",
        "Graph Id": "string",
        "Principal Type": "string",
    }

    df = _create_dataframe(columns=columns)

    url = f"/v1.0/myorg/admin/datasets/{dataset_id}/users"
    response = _base_api(request=url, client="fabric_sp")

    rows = []
    for v in response.json().get("value", []):
        rows.append(
            {
                "User Name": v.get("displayName"),
                "Email Address": v.get("emailAddress"),
                "Dataset User Access Right": v.get("datasetUserAccessRight"),
                "Identifier": v.get("identifier"),
                "Graph Id": v.get("graphId"),
                "Principal Type": v.get("principalType"),
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df
