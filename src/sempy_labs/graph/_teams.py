import pandas as pd
from uuid import UUID
from sempy_labs._helper_functions import (
    _base_api,
    _create_dataframe,
    _update_dataframe_datatypes,
)


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


def list_chats(user: str | UUID) -> pd.DataFrame:
    """
    In progress...
    """

    from sempy_labs.graph._users import resolve_user_id

    user_id = resolve_user_id(user=user)
    result = _base_api(request=f"users/{user_id}/chats", client="graph").json()

    columns = {
        "Chat Id": "str",
        "Type": "str",
        "Members": "str",
    }

    df = _create_dataframe(columns=columns)

    for v in result.get("value"):
        new_data = {
            "Chat Id": v.get("id"),
            "Type": v.get("chatType"),
            "Members": v.get("members"),
        }

        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def send_teams_message(chat_id: str, message: str):
    """
    In progress...
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
