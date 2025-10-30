from sempy_labs._helper_functions import (
    _base_api,
    _create_dataframe,
    _update_dataframe_datatypes,
)
from uuid import UUID
import pandas as pd
from sempy._utils._log import log


@log
def list_access_entities(
    user_email_address: str,
) -> pd.DataFrame:
    """
    Shows a list of permission details for Fabric and Power BI items the specified user can access.

    This is a wrapper function for the following API: `Users - List Access Entities <https://learn.microsoft.com/rest/api/fabric/admin/users/list-access-entities>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    user_email_address : str
        The user's email address.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of permission details for Fabric and Power BI items the specified user can access.
    """

    columns = {
        "Item Id": "string",
        "Item Name": "string",
        "Item Type": "string",
        "Permissions": "string",
        "Additional Permissions": "string",
    }
    df = _create_dataframe(columns=columns)

    responses = _base_api(
        request=f"/v1/admin/users/{user_email_address}/access",
        client="fabric_sp",
        uses_pagination=True,
    )

    rows = []
    for r in responses:
        for v in r.get("accessEntities", []):
            rows.append(
                {
                    "Item Id": v.get("id"),
                    "Item Name": v.get("displayName"),
                    "Item Type": v.get("itemAccessDetails", {}).get("type"),
                    "Permissions": v.get("itemAccessDetails", {}).get("permissions"),
                    "Additional Permissions": v.get("itemAccessDetails", {}).get(
                        "additionalPermissions"
                    ),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def list_user_subscriptions(user: str | UUID) -> pd.DataFrame:
    """
    Shows a list of subscriptions for the specified user. This is a preview API call.

    This is a wrapper function for the following API: `Admin - Users GetUserSubscriptionsAsAdmin <https://learn.microsoft.com/rest/api/power-bi/admin/users-get-user-subscriptions-as-admin>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    user : str | uuid.UUID
        The graph ID or user principal name (UPN) of the user.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of subscriptions for the specified user. This is a preview API call.
    """

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
        "Users": "string",
    }

    df = _create_dataframe(columns=columns)

    responses = _base_api(
        request=f"/v1.0/myorg/admin/users/{user}/subscriptions",
        client="fabric_sp",
        uses_pagination=True,
    )

    rows = []
    for r in responses:
        for v in r.get("subscriptionEntities", []):
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
                    "Users": str(v.get("users")),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df
