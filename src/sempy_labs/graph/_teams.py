import pandas as pd
from sempy.fabric._token_provider import TokenProvider
from sempy_labs.graph._util import _ms_graph_base


def list_teams(token_provider: TokenProvider) -> pd.DataFrame:
    """
    Shows a list of teams and their properties.

    This is a wrapper function for the following API: `List teams <https://learn.microsoft.com/graph/api/team-list>`_.

    Parameters
    ----------
    token_provider : TokenProvider
        The token provider for authentication, created by using the ServicePrincipalTokenProvider class.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of teams and their properties.
    """

    result = _ms_graph_base(api_name="teams", token_provider=token_provider)

    df = pd.DataFrame(
        columns=[
            "Team Id",
            "Team Name",
            "Description",
            "Creation Date Time",
            "Classification",
            "Specialization",
            "Visibility",
            "Web Url",
            "Archived",
            "Favorite By Me",
            "Discoverable By Me",
            "Member Count",
        ]
    )

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

    bool_cols = ["Archived", "Favorite By Me", "Discoverable By Me"]
    df[bool_cols] = df[bool_cols].astype(bool)
    df["Creation Date Time"] = pd.to_datetime(df["Creation Date Time"])

    return df
