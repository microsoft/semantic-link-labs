import pandas as pd
from uuid import UUID
from sempy.fabric._token_provider import TokenProvider
from sempy_labs._helper_functions import _is_valid_uuid
import sempy_labs._icons as icons
from typing import List, Literal
from sempy_labs.graph._util import _ms_graph_base


def resolve_group_id(group: str | UUID, token_provider: TokenProvider) -> UUID:
    """
    Resolves the group ID from the group name or ID.

    Parameters
    ----------
    group : str | uuid.UUID
        The group name.
    token_provider : TokenProvider
        The token provider for authentication, created by using the ServicePrincipalTokenProvider class.

    Returns
    -------
    uuid.UUID
        The group ID.
    """
    if _is_valid_uuid(group):
        group_id = group
    else:
        dfG = list_groups(token_provider=token_provider)
        dfG_filt = dfG[dfG["Group Name"] == group]
        if dfG_filt.empty:
            raise ValueError(f"{icons.red_dot} The '{group}' group does not exist.")
        group_id = dfG_filt["Group Id"].iloc[0]

    return group_id


def list_groups(token_provider: TokenProvider) -> pd.DataFrame:
    """
    Shows a list of groups and their properties.

    This is a wrapper function for the following API: `List groups <https://learn.microsoft.com/graph/api/group-list>`_.

    Parameters
    ----------
    token_provider : TokenProvider
        The token provider for authentication, created by using the ServicePrincipalTokenProvider class.

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
            "Created Date Time",
            "Expiration Date Time",
            "Deleted Date Time",
            "Renewed Date Time",
            "Visibility",
            "Security Identifier",
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
            "Created Date Time": v.get("createdDateTime"),
            "Expiration Date Time": v.get("expirationDateTime"),
            "Renewed Date Time": v.get("renewedDateTime"),
            "Deleted Date Time": v.get("deletedDateTime"),
            "Visibility": v.get("visibility"),
            "Security Identifier": v.get("securityIdentifier"),
        }

        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    bool_cols = ["Mail Enabled", "Security Enabled"]
    df[bool_cols] = df[bool_cols].astype(bool)
    df["Created Date Time"] = pd.to_datetime(df["Created Date Time"])

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
            "Created Date Time",
            "Expiration Date Time",
            "Deleted Date Time",
            "Renewed Date Time",
            "Visibility",
            "Security Identifier",
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
            "Created Date Time": v.get("createdDateTime"),
            "Expiration Date Time": v.get("expirationDateTime"),
            "Deleted Date Time": v.get("deletedDateTime"),
            "Renewed Date Time": v.get("renewedDateTime"),
            "Visibility": v.get("visibility"),
            "Security Identifier": v.get("securityIdentifier"),
        }

        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    bool_cols = ["Mail Enabled", "Security Enabled"]
    df[bool_cols] = df[bool_cols].astype(bool)
    df["Created Date Time"] = pd.to_datetime(df["Created Date Time"])

    return df


def list_group_members(
    group: str | UUID, token_provider: TokenProvider
) -> pd.DataFrame:
    """
    Shows a list of the members of a group.

    This is a wrapper function for the following API: `List group members <https://learn.microsoft.com/graph/api/group-list-members>`_.

    Parameters
    ----------
    group : str | uuid.UUID
        The group name or ID.
    token_provider : TokenProvider
        The token provider for authentication, created by using the ServicePrincipalTokenProvider class.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of the members of a group.
    """

    if _is_valid_uuid(group):
        group_id = group
    else:
        group_id = resolve_group_id(group, token_provider)

    result = _ms_graph_base(f"groups/{group_id}/members", token_provider)

    df = pd.DataFrame(
        columns=[
            "Member Id",
            "Member Name",
            "User Principal Name",
            "Mail",
            "Job Title",
            "Office Location",
            "Mobile Phone",
            "Business Phones",
            "Preferred Language",
            "Given Name",
            "Surname",
        ]
    )

    for v in result.get("value"):
        new_data = {
            "Member Id": v.get("id"),
            "Member Name": v.get("displayName"),
            "User Principal Name": v.get("userPrincipalName"),
            "Mail": v.get("mail"),
            "Job Title": v.get("jobTitle"),
            "Office Location": v.get("officeLocation"),
            "Mobile Phone": v.get("mobilePhone"),
            "Business Phones": str(v.get("businessPhones")),
            "Preferred Language": v.get("preferredLanguage"),
            "Given Name": v.get("givenName"),
            "Surname": v.get("surname"),
        }

        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def list_group_owners(group: str | UUID, token_provider: TokenProvider) -> pd.DataFrame:
    """
    Shows a list of the owners of a group.

    This is a wrapper function for the following API: `List group owners <https://learn.microsoft.com/graph/api/group-list-owners>`_.

    Parameters
    ----------
    group : str | uuid.UUID
        The group name or ID.
    token_provider : TokenProvider
        The token provider for authentication, created by using the ServicePrincipalTokenProvider class.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of the owners of a group.
    """

    if _is_valid_uuid(group):
        group_id = group
    else:
        group_id = resolve_group_id(group, token_provider)

    result = _ms_graph_base(f"groups/{group_id}/members", token_provider)

    df = pd.DataFrame(
        columns=[
            "Owner Id",
            "Owner Name",
            "User Principal Name",
            "Mail",
            "Job Title",
            "Office Location",
            "Mobile Phone",
            "Business Phones",
            "Preferred Language",
            "Given Name",
            "Surname",
        ]
    )

    for v in result.get("value"):
        new_data = {
            "Owner Id": v.get("id"),
            "Owner Name": v.get("displayName"),
            "User Principal Name": v.get("userPrincipalName"),
            "Mail": v.get("mail"),
            "Job Title": v.get("jobTitle"),
            "Office Location": v.get("officeLocation"),
            "Mobile Phone": v.get("mobilePhone"),
            "Business Phones": str(v.get("businessPhones")),
            "Preferred Language": v.get("preferredLanguage"),
            "Given Name": v.get("givenName"),
            "Surname": v.get("surname"),
        }

        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def _base_add_to_group(
    group: str | UUID,
    object: str | UUID,
    token_provider: TokenProvider,
    object_type: Literal["members", "owners"],
):

    from sempy_labs.graph._users import resolve_user_id

    object_list = []

    if isinstance(object, str):
        object = [object]

    if _is_valid_uuid(group):
        group_id = group
    else:
        group_id = resolve_group_id(group, token_provider)

    for m in object:
        if _is_valid_uuid(m):
            member_id = m
        else:
            member_id = resolve_user_id(m, token_provider)
        if object_type == "members":
            object_list.append(
                f"https://graph.microsoft.com/v1.0/directoryObjects/{member_id}"
            )
        else:
            object_list.append(f"https://graph.microsoft.com/v1.0/users/{member_id}")

    url = f"groups/{group_id}/{object_type}/$ref"

    # Must submit one request for each owner. Members can be sent in a single request.
    if object_type == "members":
        payload = {"members@odata.bind": object_list}
        _ms_graph_base(
            api_name=url,
            token_provider=token_provider,
            payload=payload,
            status_success_code=204,
            return_json=False,
        )
    else:
        for o in object_list:
            payload = {"odata.id": o}
            _ms_graph_base(
                api_name=url,
                token_provider=token_provider,
                payload=payload,
                status_success_code=204,
                return_json=False,
            )

    print(
        f"{icons.green_dot} The {object} {object_type[:-1]}(s) have been added to the '{group}' group."
    )


def add_group_members(
    group: str | UUID,
    user: str | UUID | List[str | UUID],
    token_provider: TokenProvider,
):
    """
    Adds a member to a group.

    This is a wrapper function for the following API: `Add members <https://learn.microsoft.com/graph/api/group-post-members>`_.

    Parameters
    ----------
    group : str | uuid.UUID
        The group name or ID.
    user : str | uuid.UUID
        The user ID or user principal name.
    token_provider : TokenProvider
        The token provider for authentication, created by using the ServicePrincipalTokenProvider class.
    """

    _base_add_to_group(
        group=group, object=user, token_provider=token_provider, object_type="members"
    )


def add_group_owners(
    group: str | UUID,
    user: str | UUID | List[str | UUID],
    token_provider: TokenProvider,
):
    """
    Adds an owner to a group.

    This is a wrapper function for the following API: `Add owners <https://learn.microsoft.com/graph/api/group-post-owners>`_.

    Parameters
    ----------
    group : str | uuid.UUID
        The group name or ID.
    user : str | uuid.UUID
        The user ID or user principal name.
    token_provider : TokenProvider
        The token provider for authentication, created by using the ServicePrincipalTokenProvider class.
    """

    _base_add_to_group(
        group=group, object=user, token_provider=token_provider, object_type="owners"
    )
