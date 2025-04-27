from sempy_labs._helper_functions import (
    _base_api,
    _create_dataframe,
    _update_dataframe_datatypes,
)
import pandas as pd


def list_tags() -> pd.DataFrame:
    """
    Shows a list of all the tenant's tags.

    This is a wrapper function for the following API: `Items - List Tags <https://learn.microsoft.com/rest/api/fabric/core/tags/list-tags>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of all the tenant's tags.
    """

    columns = {
        "Tag Name": "string",
        "Tag Id": "string",
    }
    df = _create_dataframe(columns=columns)

    responses = _base_api(
        request="/v1/tags",
        uses_pagination=True,
        client="fabric_sp",
    )

    dfs = []

    for r in responses:
        for v in r.get("value", []):
            new_data = {
                "Tag Name": v.get("displayName"),
                "Tag Id": v.get("id"),
            }
            dfs.append(pd.DataFrame(new_data, index=[0]))

    if dfs:
        df = pd.concat(dfs, ignore_index=True)
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df
