import pandas as pd
from uuid import UUID
from sempy_labs._helper_functions import (
    _is_valid_uuid,
    _base_api,
    _create_dataframe,
    _update_dataframe_datatypes,
)
from sempy._utils._log import log
import sempy_labs._icons as icons
from typing import List, Literal, Optional


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

    result = _base_api(request="groups", client="graph", uses_pagination=True)

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
    for r in result:
        for v in r.get("value", []):
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

    result = _base_api(
        request=f"groups/{group_id}/members", client="graph", uses_pagination=True
    )

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
    for r in result:
        for v in r.get("value", []):
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
def list_group_transitive_members(group: str | UUID) -> pd.DataFrame:
    """
    Shows a list of the members of a group. This operation is transitive and returns a flat list of all nested members.

    This is a wrapper function for the following API: `List group transitive members <https://learn.microsoft.com/graph/api/group-list-transitivemembers>`_.

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

    result = _base_api(
        request=f"groups/{group_id}/transitiveMembers",
        client="graph",
        uses_pagination=True,
    )

    columns = {
        "Member Id": "string",
        "Organization Id": "string",
        "Description": "string",
        "Member Name": "string",
        "Group Types": "list",
        "Mail": "string",
        "Mail Enabled": "bool",
        "Mail Nickname": "string",
    }

    df = _create_dataframe(columns=columns)

    rows = []
    for r in result:
        for v in r.get("value", []):
            rows.append(
                {
                    "Member Id": v.get("id"),
                    "Organization Id": v.get("organizationId"),
                    "Description": v.get("description"),
                    "Member Name": v.get("displayName"),
                    "Group Types": v.get("groupTypes"),
                    "Mail": v.get("mail"),
                    "Mail Enabled": v.get("mailEnabled"),
                    "Mail Nickname": v.get("mailNickname"),
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

    result = _base_api(
        request=f"groups/{group_id}/owners", client="graph", uses_pagination=True
    )

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
    for r in result:
        for v in r.get("value", []):
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


@log
def create_group(
    display_name: str,
    description: Optional[str] = None,
    mail_enabled: bool = False,
    security_enabled: bool = True,
    mail_nickname: str = None,
    owners: Optional[str | UUID | List[str | UUID]] = None,
    members: Optional[str | UUID | List[str | UUID]] = None,
):
    """
    Creates a new group.

    This is a wrapper function for the following API: `Create group <https://learn.microsoft.com/graph/api/group-post-groups>`_.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    display_name : str
        The name of the group.
    description : str, optional
        The description of the group.
    mail_enabled : bool, default=False
        Whether the group is mail-enabled.
    security_enabled : bool, default=True
        Whether the group is security-enabled.
    mail_nickname : str, default=None
        The mail alias for the group.
    owners : str | uuid.UUID | List[str | uuid.UUID], default=None
        The owners of the group.
    members : str | uuid.UUID | List[str | uuid.UUID], default=None
        The members of the group.
    """
    from sempy_labs.graph._users import resolve_user_id

    payload = {
        "displayName": display_name,
        "description": description,
        "mailEnabled": mail_enabled,
        "securityEnabled": security_enabled,
        "mailNickname": mail_nickname,
    }

    if owners:
        if isinstance(owners, str):
            owners = [owners]
        user_list = []
        for o in owners:
            user_id = resolve_user_id(o)
            user_list.append(f"https://graph.microsoft.com/v1.0/users/{user_id}")
        payload["owners@odata.bind"] = user_list
    if members:
        if isinstance(members, str):
            members = [members]
        user_list = []
        for m in members:
            user_id = resolve_user_id(m)
            user_list.append(f"https://graph.microsoft.com/v1.0/users/{user_id}")
        payload["members@odata.bind"] = user_list

    _base_api(
        request="groups",
        client="graph",
        payload=payload,
        method="post",
        status_codes=201,
    )

    print(f"{icons.green_dot} The '{display_name}' group has been created.")


@log
def delete_group(group: str | UUID):
    """
    Deletes a group.

    This is a wrapper function for the following API: `Delete group <https://learn.microsoft.com/graph/api/group-delete>`_.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    group : str | uuid.UUID
        The group name or ID.
    """

    group_id = resolve_group_id(group)

    _base_api(
        request=f"groups/{group_id}",
        client="graph",
        status_codes=204,
        method="delete",
    )

    print(f"{icons.green_dot} The '{group}' group has been deleted successfully.")


@log
def update_group(
    group: str | UUID,
    display_name: Optional[str] = None,
    mail_nickname: Optional[str] = None,
    description: Optional[str] = None,
    security_enabled: Optional[bool] = None,
):
    """
    Updates a group's properties.

    This is a wrapper function for the following API: `Update group <https://learn.microsoft.com/en-us/graph/api/group-update>`_.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    group : str | uuid.UUID
        The group name or ID.
    display_name : str, default=None
        The new display name for the group.
    mail_nickname : str, default=None
        The new mail nickname for the group.
    description : str, default=None
        The new description for the group.
    security_enabled : bool, default=None
        Whether the group is security-enabled.
    """

    group_id = resolve_group_id(group)

    payload = {}
    if display_name:
        payload["displayName"] = display_name
    if mail_nickname:
        payload["mailNickname"] = mail_nickname
    if description:
        payload["description"] = description
    if security_enabled is not None and isinstance(security_enabled, bool):
        payload["securityEnabled"] = security_enabled

    if not payload:
        print(f"{icons.info} No properties to update.")
        return

    _base_api(
        request=f"groups/{group_id}",
        client="graph",
        status_codes=204,
        payload=payload,
        method="patch",
    )

    print(f"{icons.green_dot} The '{group}' group has been updated successfully.")
