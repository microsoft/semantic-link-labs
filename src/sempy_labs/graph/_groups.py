import pandas as pd
from uuid import UUID
from .._helper_functions import (
    _is_valid_uuid,
    _base_api,
    _create_dataframe,
    _update_dataframe_datatypes,
)
from sempy._utils._log import log
import sempy_labs._icons as icons
from typing import List, Literal


@log
def resolve_group_id(group: str | UUID) -> UUID:
    """
    Resolves the group ID from the group name or ID.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    group : str | uuid.UUID
        The group name or ID.

    Returns
    -------
    uuid.UUID
        The group ID.
    """
    if _is_valid_uuid(group):
        group_id = group
    else:
        dfG = list_groups()
        dfG_filt = dfG[dfG["Group Name"] == group]
        if dfG_filt.empty:
            raise ValueError(f"{icons.red_dot} The '{group}' group does not exist.")
        group_id = dfG_filt["Group Id"].iloc[0]

    return group_id


@log
def list_groups() -> pd.DataFrame:
    """
    Shows a list of groups and their properties.

    This is a wrapper function for the following API: `List groups <https://learn.microsoft.com/graph/api/group-list>`_.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of groups and their properties.
    """

    result = _base_api(request="groups", client="graph").json()

    columns = {
        "Group Id": "string",
        "Group Name": "string",
        "Mail": "string",
        "Description": "string",
        "Classification": "string",
        "Mail Enabled": "bool",
        "Security Enabled": "bool",
        "Created Date Time": "datetime",
        "Expiration Date Time": "string",
        "Deleted Date Time": "string",
        "Renewed Date Time": "string",
        "Visibility": "string",
        "Security Identifier": "string",
    }

    df = _create_dataframe(columns=columns)

    rows = []
    for v in result.get("value"):
        rows.append(
            {
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
        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
def _get_group(group_id: UUID) -> pd.DataFrame:
    """
    Shows a list of groups and their properties.

    This is a wrapper function for the following API: `Get group <https://learn.microsoft.com/graph/api/group-get>`_.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    group_id : uuid.UUID
        The group ID.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of groups and their properties.
    """

    result = _base_api(request=f"groups/{group_id}", client="graph").json()

    columns = {
        "Group Id": "string",
        "Group Name": "string",
        "Mail": "string",
        "Description": "string",
        "Classification": "string",
        "Mail Enabled": "bool",
        "Security Enabled": "bool",
        "Created Date Time": "datetime",
        "Expiration Date Time": "string",
        "Deleted Date Time": "string",
        "Renewed Date Time": "string",
        "Visibility": "string",
        "Security Identifier": "string",
    }
    df = _create_dataframe(columns=columns)

    rows = []
    for v in result.get("value"):
        rows.append(
            {
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
        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
def list_group_members(group: str | UUID) -> pd.DataFrame:
    """
    Shows a list of the members of a group.

    This is a wrapper function for the following API: `List group members <https://learn.microsoft.com/graph/api/group-list-members>`_.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    group : str | uuid.UUID
        The group name or ID.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of the members of a group.
    """

    group_id = resolve_group_id(group)

    result = _base_api(request=f"groups/{group_id}/members", client="graph").json()

    columns = {
        "Member Id": "string",
        "Member Name": "string",
        "User Principal Name": "string",
        "Mail": "string",
        "Job Title": "string",
        "Office Location": "string",
        "Mobile Phone": "string",
        "Business Phones": "string",
        "Preferred Language": "string",
        "Given Name": "string",
        "Surname": "string",
    }

    df = _create_dataframe(columns=columns)

    rows = []
    for v in result.get("value"):
        rows.append(
            {
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
        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def list_group_owners(group: str | UUID) -> pd.DataFrame:
    """
    Shows a list of the owners of a group.

    This is a wrapper function for the following API: `List group owners <https://learn.microsoft.com/graph/api/group-list-owners>`_.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    group : str | uuid.UUID
        The group name or ID.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of the owners of a group.
    """

    group_id = resolve_group_id(group)

    result = _base_api(request=f"groups/{group_id}/owners", client="graph").json()

    columns = {
        "Owner Id": "string",
        "Owner Name": "string",
        "User Principal Name": "string",
        "Mail": "string",
        "Job Title": "string",
        "Office Location": "string",
        "Mobile Phone": "string",
        "Business Phones": "string",
        "Preferred Language": "string",
        "Given Name": "string",
        "Surname": "string",
    }

    df = _create_dataframe(columns=columns)

    rows = []
    for v in result.get("value"):
        rows.append(
            {
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
        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def _base_add_to_group(
    group: str | UUID,
    object: str | UUID,
    object_type: Literal["members", "owners"],
):

    from sempy_labs.graph._users import resolve_user_id

    object_list = []

    if isinstance(object, str):
        object = [object]

    group_id = resolve_group_id(group)
    url = f"groups/{group_id}/{object_type}/$ref"

    for m in object:
        if _is_valid_uuid(m):
            member_id = m
        else:
            member_id = resolve_user_id(m)
        if object_type == "members":
            object_list.append(
                f"https://graph.microsoft.com/v1.0/directoryObjects/{member_id}"
            )
        else:
            object_list.append(f"https://graph.microsoft.com/v1.0/users/{member_id}")

    # Must submit one request for each owner. Members can be sent in a single request.
    if object_type == "members":
        payload = {"members@odata.bind": object_list}

        _base_api(
            request=url,
            client="graph",
            payload=payload,
            method="post",
            status_codes=204,
        )

    else:
        for o in object_list:
            payload = {"odata.id": o}
            _base_api(
                request=url,
                client="graph",
                payload=payload,
                method="post",
                status_codes=204,
            )

    print(
        f"{icons.green_dot} The {object} {object_type[:-1]}(s) have been added to the '{group}' group."
    )


@log
def add_group_members(
    group: str | UUID,
    user: str | UUID | List[str | UUID],
):
    """
    Adds a member to a group.

    This is a wrapper function for the following API: `Add members <https://learn.microsoft.com/graph/api/group-post-members>`_.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    group : str | uuid.UUID
        The group name or ID.
    user : str | uuid.UUID
        The user ID or user principal name.
    """

    _base_add_to_group(group=group, object=user, object_type="members")


@log
def add_group_owners(
    group: str | UUID,
    user: str | UUID | List[str | UUID],
):
    """
    Adds an owner to a group.

    This is a wrapper function for the following API: `Add owners <https://learn.microsoft.com/graph/api/group-post-owners>`_.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    group : str | uuid.UUID
        The group name or ID.
    user : str | uuid.UUID
        The user ID or user principal name.
    """

    _base_add_to_group(group=group, object=user, object_type="owners")


@log
def renew_group(group: str | UUID):
    """
    Renews the group.

    This is a wrapper function for the following API: `Renew group <https://learn.microsoft.com/graph/api/group-post-renew>`_.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    group : str | uuid.UUID
        The group name or ID.
    """

    group_id = resolve_group_id(group)

    _base_api(
        request=f"groups/{group_id}/renew",
        client="graph",
        method="post",
        status_codes=204,
    )

    print(f"{icons.green_dot} The '{group}' group has been renewed.")
