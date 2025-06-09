import pandas as pd
from uuid import UUID
from sempy._utils._log import log
from sempy_labs._helper_functions import (
    _base_api,
    _create_dataframe,
    _update_dataframe_datatypes,
)
from typing import List, Optional
from sempy_labs.graph._users import resolve_user_id
import sempy_labs._icons as icons


@log
def list_teams() -> pd.DataFrame:
    """
    Shows a list of teams and their properties.

    This is a wrapper function for the following API: `List teams <https://learn.microsoft.com/graph/api/teams-list>`_.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of teams and their properties.
    """

    result = _base_api(request="teams", client="graph").json()

    columns = {
        "Team Id": "str",
        "Team Name": "str",
        "Description": "str",
        "Creation Date Time": "datetime",
        "Classification": "str",
        "Specialization": "str",
        "Visibility": "str",
        "Web Url": "str",
        "Archived": "bool",
        "Favorite By Me": "bool",
        "Discoverable By Me": "bool",
        "Member Count": "int_fillna",
    }

    df = _create_dataframe(columns=columns)

    for v in result.get("value"):
        new_data = {
            "Team Id": v.get("id"),
            "Team Name": v.get("displayName"),
            "Description": v.get("description"),
            "Creation Date Time": v.get("createdDateTime"),
            "Classification": v.get("classification"),
            "Specialization": v.get("specialization"),
            "Visibility": v.get("visibility"),
            "Web Url": v.get("webUrl"),
            "Archived": v.get("isArchived"),
            "Favorite By Me": v.get("isFavoriteByMe"),
            "Discoverable By Me": v.get("isDiscoverableByMe"),
            "Member Count": v.get("memberCount"),
        }

        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


def list_teams_chats(user: str | UUID) -> pd.DataFrame:
    """
    Shows a list of teams chats for a given user.

    This is a wrapper function for the following API: `List teams <>`_.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    user : str | uuid.UUID
        The user name or ID.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of teams and their properties.
    """

    user_id = resolve_user_id(user=user)
    result = _base_api(request=f"users/{user_id}/chats", client="graph").json()

    columns = {
        "Chat Id": "str",
        "Type": "str",
        "Topic": "str",
        "Created Date Time": "datetime",
        "Last Updated Date Time": "datetime",
    }

    df = _create_dataframe(columns=columns)

    for v in result.get("value"):
        new_data = {
            "Chat Id": v.get("id"),
            "Type": v.get("chatType"),
            "Topic": v.get("topic"),
            "Created Date Time": v.get("createdDateTime"),
            "Last Updated Date Time": v.get("lastUpdatedDateTime"),
        }

        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def send_teams_message(chat_id: str, message: str):
    """
    Sends a teams message.

    This is a wrapper function for the following API: `Send message in a chat <https://learn.microsoft.com/graph/api/chat-post-messages>`_.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    chat_id : str | uuid.UUID
        The chat ID.
    message : str
        The message to send.
    """

    payload = {
        "body": {
            "content": message,
        }
    }

    _base_api(
        request=f"chats/{chat_id}/messages",
        client="graph",
        method="post",
        payload=payload,
        status_codes=201,
    )


def create_teams_chat(members: List[str | UUID], title: Optional[str] = None):
    """
    Creates a new teams chat.

    This is a wrapper function for the following API: `Create chat <https://learn.microsoft.com/graph/api/chat-post>`_.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    members : List[str | uuid.UUID]
        The members (users) to add to the chat.
    title : str, default=None
        The title of the chat. Only applicable to group chats (more than 2 members).
    """

    if len(members) < 2:
        raise ValueError(
            f"{icons.red_dot} At least two members are required to create a chat."
        )
    elif len(members) == 2:
        chat_type = "oneOnOne"
    else:
        chat_type = "group"

    for i, member in enumerate(members):
        members[i] = resolve_user_id(user=member)

    payload = {
        "chatType": chat_type,
    }

    if chat_type == "group" and title is not None:
        payload["topic"] = title

    member_payload = []
    for user_id in members:
        member_payload.append(
            {
                "@odata.type": "#microsoft.graph.aadUserConversationMember",
                "roles": ["owner"],
                "user@odata.bind": f"https://graph.microsoft.com/v1.0/users('{user_id}')",
            }
        )

    payload["members"] = member_payload

    _base_api(
        request="chats",
        client="graph",
        method="post",
        payload=payload,
        status_codes=201,
    )


def get_teams_chat_members(
    chat_id: UUID, user: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Shows a list of teams chat members.

    This is a wrapper function for the following API: `List chat members <https://learn.microsoft.com/graph/api/chatmembers-list>`_.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    chat_id : uuid.UUID
        The chat ID.
    user : str | uuid.UUID, default=None
        The user name or ID.
        If specified, shows the members for a given user.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of teams chat members.
    """

    if user is None:
        url = f"chats/{chat_id}/members"
    else:
        user_id = resolve_user_id(user=user)
        url = f"users/{user_id}/chats/{chat_id}/members"

    columns = {
        "Chat Id": "str",
        "Chat Name": "str",
        "Roles": "str",
        "User Id": "str",
        "Email": "str",
        "Tenant Id": "str",
    }

    df = _create_dataframe(columns=columns)

    result = _base_api(
        request=url,
        client="graph",
    )

    for v in result.get("value"):
        new_data = {
            "Chat Id": v.get("id"),
            "Chat Name": v.get("displayName"),
            "Roles": v.get("roles"),
            "User Id": v.get("userId"),
            "Email": v.get("email"),
            "Tenant Id": v.get("tenantId"),
        }

        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def list_teams_chat_members(user: str | UUID) -> pd.DataFrame:
    """
    Shows the members for each teams chat for a given user.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    user : str | uuid.UUID
        The user name or ID.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the members for each teams chat for a given user.
    """

    columns = {
        "Chat Id": "str",
        "Chat Name": "str",
        "Roles": "str",
        "User Id": "str",
        "Email": "str",
        "Tenant Id": "str",
    }

    df = _create_dataframe(columns=columns)

    dfT = list_teams_chats(user=user)
    dfs = []

    for _, r in dfT.iterrows():
        chat_id = r["Chat Id"]
        dfM = get_teams_chat_members(chat_id=chat_id, user=user)

        if not dfM.empty:
            dfs.append(dfM)

    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        return df
