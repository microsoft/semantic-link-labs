import pandas as pd
from uuid import UUID
from typing import Optional
from sempy._utils._log import log
from sempy_labs._helper_functions import (
    _base_api,
    resolve_item_name_and_id,
    resolve_workspace_name_and_id,
)
import sempy_labs._icons as icons


@log
def refresh_graph(
    graph_model: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Refreshes the graph model.

    This is a wrapper function for the following API: `Background Jobs - Run On Demand Refresh Graph <https://learn.microsoft.com/rest/api/fabric/graphmodel/background-jobs/run-on-demand-refresh-graph>`_.

    Parameters
    ----------
    graph_model : str | uuid.UUID
        The graph model name or ID.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the result of the refresh operation.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (item_name, item_id) = resolve_item_name_and_id(
        item=graph_model, type="GraphModel", workspace=workspace_id
    )

    print(
        f"{icons.in_progress} The refresh graph job for the '{item_name}' graph model within the '{workspace_name}' workspace has been initiated."
    )

    df = _base_api(
        request=f"/v1/workspaces/{workspace_id}/GraphModels/{item_id}/jobs/instances?jobType=RefreshGraph",
        method="post",
        lro_return_df=True,
    )
    status = df["Status"].iloc[0]

    if status == "Completed":
        print(
            f"{icons.green_dot} The refresh graph job for the '{item_name}' graph model within the '{workspace_name}' workspace has succeeded."
        )
    else:
        print(status)
        print(
            f"{icons.red_dot} The refresh graph job for the '{item_name}' graph model within the '{workspace_name}' workspace has failed."
        )

    return df
