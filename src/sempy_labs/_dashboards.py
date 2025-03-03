from typing import Optional
from uuid import UUID
import pandas as pd
from sempy_labs._helper_functions import (
    _create_dataframe,
    _base_api,
    resolve_workspace_name_and_id,
    _update_dataframe_datatypes,
)


def list_dashboards(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows a list of the dashboards within a workspace.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the dashboards within a workspace.
    """

    columns = {
        "Dashboard ID": "string",
        "Dashboard Name": "string",
        "Read Only": "bool",
        "Web URL": "string",
        "Embed URL": "string",
        "Data Classification": "string",
        "Users": "string",
        "Subscriptions": "string",
    }
    df = _create_dataframe(columns=columns)

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    response = _base_api(request=f"/v1.0/myorg/groups/{workspace_id}/dashboards")

    for v in response.json().get("value", []):
        new_data = {
            "Dashboard ID": v.get("id"),
            "Dashboard Name": v.get("displayName"),
            "Read Only": v.get("isReadOnly"),
            "Web URL": v.get("webUrl"),
            "Embed URL": v.get("embedUrl"),
            "Data Classification": v.get("dataClassification"),
            "Users": v.get("users"),
            "Subscriptions": v.get("subscriptions"),
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df
