from typing import Optional
from uuid import UUID
import pandas as pd
from sempy_labs._helper_functions import (
    _create_dataframe,
    _base_api,
    resolve_workspace_id,
    _update_dataframe_datatypes,
)
from sempy._utils._log import log


@log
def list_dashboards(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows a list of the dashboards within a workspace.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

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

    workspace_id = resolve_workspace_id(workspace)

    response = _base_api(
        request=f"/v1.0/myorg/groups/{workspace_id}/dashboards", client="fabric_sp"
    )

    rows = []
    for v in response.json().get("value", []):
        rows.append(
            {
                "Dashboard ID": v.get("id"),
                "Dashboard Name": v.get("displayName"),
                "Read Only": v.get("isReadOnly"),
                "Web URL": v.get("webUrl"),
                "Embed URL": v.get("embedUrl"),
                "Data Classification": v.get("dataClassification"),
                "Users": v.get("users"),
                "Subscriptions": v.get("subscriptions"),
            }
        )
    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df
