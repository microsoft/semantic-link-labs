import requests
import pandas as pd
from uuid import UUID
from sempy_labs._authentication import _get_headers
from sempy.fabric._token_provider import TokenProvider
from sempy.fabric.exceptions import FabricHTTPException


def _ms_graph_base(api_name, token_provider):

    headers = _get_headers(token_provider, audience="graph")
    url = f"https://graph.microsoft.com/v1.0/{api_name}"

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        raise FabricHTTPException(response)

    return response.json()


def resolve_user_id(user: str | UUID, token_provider: TokenProvider) -> UUID:
    """
    Resolves the user ID from the user principal name or ID.

    Parameters
    ----------
    user : str | uuid.UUID
        The user principal name.
    token_provider : TokenProvider

    Returns
    -------
    uuid.UUID
        The user ID.
    """

    result = _ms_graph_base(f"users/{user}", token_provider)

    return result.get("id")


def get_user(user: str | UUID, token_provider: TokenProvider) -> pd.DataFrame:
    """
    Shows properties of a given user.

    This is a wrapper function for the following API: `Get a user <https://learn.microsoft.com/graph/api/user-get>`_.

    Parameters
    ----------
    user : str | uuid.UUID
        The user ID or user principal name.
    token_provider : TokenProvider

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing properties of a given user.
    """

    result = _ms_graph_base(f"users/{user}", token_provider)

    new_data = {
        "User Id": result.get("id"),
        "User Principal Name": result.get("userPrincipalName"),
        "User Name": result.get("displayName"),
        "Mail": result.get("mail"),
        "Job Title": result.get("jobTitle"),
        "Office Location": result.get("officeLocation"),
        "Mobile Phone": result.get("mobilePhone"),
        "Business Phones": result.get("businessPhones"),
        "Preferred Language": result.get("preferredLanguage"),
        "Surname": result.get("surname"),
    }

    return pd.DataFrame([new_data])


def list_users(token_provider: TokenProvider) -> pd.DataFrame:
    """
    Shows a list of users and their properties.

    This is a wrapper function for the following API: `List users <https://learn.microsoft.com/graph/api/user-list>`_.

    Parameters
    ----------
    token_provider : TokenProvider

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of users and their properties.
    """

    result = _ms_graph_base("users", token_provider)

    df = pd.DataFrame(
        columns=[
            "User Id",
            "User Principal Name",
            "User Name",
            "Mail",
            "Job Title",
            "Office Location",
            "Mobile Phone",
            "Business Phones",
            "Preferred Language",
            "Surname",
        ]
    )

    for v in result.get("value"):
        new_data = {
            "User Id": v.get("id"),
            "User Principal Name": v.get("userPrincipalName"),
            "User Name": v.get("displayName"),
            "Mail": v.get("mail"),
            "Job Title": v.get("jobTitle"),
            "Office Location": v.get("officeLocation"),
            "Mobile Phone": v.get("mobilePhone"),
            "Business Phones": v.get("businessPhones"),
            "Preferred Language": v.get("preferredLanguage"),
            "Surname": v.get("surname"),
        }

        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def list_groups(token_provider: TokenProvider) -> pd.DataFrame:
    """
    Shows a list of groups and their properties.

    This is a wrapper function for the following API: `List groups <https://learn.microsoft.com/graph/api/group-list>`_.

    Parameters
    ----------
    token_provider : TokenProvider

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of groups and their properties.
    """

    result = _ms_graph_base("groups", token_provider)

    df = pd.DataFrame(
        columns=[
            "Group Id",
            "Group Name",
            "Mail",
            "Description",
            "Classification",
            "Mail Enabled",
            "Security Enabled",
            "Visibility",
        ]
    )

    for v in result.get("value"):
        new_data = {
            "Group Id": v.get("id"),
            "Group Name": v.get("displayName"),
            "Mail": v.get("mail"),
            "Description": v.get("description"),
            "Classification": v.get("classification"),
            "Mail Enabled": v.get("mailEnabled"),
            "Security Enabled": v.get("securityEnabled"),
            "Created DateTime": v.get("createdDateTime"),
            "Expiration DateTime": v.get("expirationDateTime"),
            "Visibility": v.get("visibility"),
            "Security Identifier": v.get("securityIdentifier"),
        }

        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    bool_cols = ["Mail Enabled", "Security Enabled"]
    df[bool_cols] = df[bool_cols].astype(bool)
    df["Created DateTime"] = pd.to_datetime(df["Created DateTime"])

    return df


def _get_group(group_id: UUID, token_provider: TokenProvider) -> pd.DataFrame:
    """
    Shows a list of groups and their properties.

    This is a wrapper function for the following API: `Get group <https://learn.microsoft.com/graph/api/group-get>`_.

    Parameters
    ----------
    group_id : uuid.UUID
        The group ID.
    token_provider : TokenProvider

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of groups and their properties.
    """

    result = _ms_graph_base(f"groups/{group_id}", token_provider)

    df = pd.DataFrame(
        columns=[
            "Group Id",
            "Group Name",
            "Mail",
            "Description",
            "Classification",
            "Mail Enabled",
            "Security Enabled",
            "Visibility",
        ]
    )

    for v in result.get("value"):
        new_data = {
            "Group Id": v.get("id"),
            "Group Name": v.get("displayName"),
            "Mail": v.get("mail"),
            "Description": v.get("description"),
            "Classification": v.get("classification"),
            "Mail Enabled": v.get("mailEnabled"),
            "Security Enabled": v.get("securityEnabled"),
            "Created DateTime": v.get("createdDateTime"),
            "Expiration DateTime": v.get("expirationDateTime"),
            "Visibility": v.get("visibility"),
            "Security Identifier": v.get("securityIdentifier"),
        }

        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    bool_cols = ["Mail Enabled", "Security Enabled"]
    df[bool_cols] = df[bool_cols].astype(bool)
    df["Created DateTime"] = pd.to_datetime(df["Created DateTime"])

    return df
