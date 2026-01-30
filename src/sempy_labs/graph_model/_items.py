import pandas as pd
from uuid import UUID
from typing import Optional
from sempy._utils._log import log
from sempy_labs._helper_functions import (
    _base_api,
    _create_dataframe,
    resolve_item_id,
    resolve_workspace_id,
    resolve_item_name_and_id,
    resolve_workspace_name_and_id,
)
import sempy_labs._icons as icons


@log
def list_graph_models(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows the graph models within a workspace.

    This is a wrapper function for the following API: `Items - List Graph Models <https://learn.microsoft.com/rest/api/fabric/graphmodel/items/list-graph-models>`_.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the graph models within a workspace.
    """

    columns = {
        "Graph Model Name": "string",
        "Graph Model Id": "string",
        "Description": "string",
        "OneLake Root Path": "string",
    }
    df = _create_dataframe(columns=columns)

    workspace_id = resolve_workspace_id(workspace)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/GraphModels",
        uses_pagination=True,
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            rows.append(
                {
                    "Graph Model Name": v.get("displayName"),
                    "Graph Model Id": v.get("id"),
                    "Description": v.get("description"),
                    "OneLake Root Path": v.get("properties", {}).get("oneLakeRootPath"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def execute_query(
    graph_model: str | UUID, query: str, workspace: Optional[str | UUID] = None
) -> dict:
    """
    Executes a query on the specified graph model.

    This is a wrapper function for the following API: `Items - ExecuteQuery <https://learn.microsoft.com/rest/api/fabric/graphmodel/items/execute-query(preview)>`_.

    Parameters
    ----------
    graph_model : str | uuid.UUID
        The graph model name or ID.
    query : str
        The query string.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    dict
        The response from the API.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (item_name, item_id) = resolve_item_name_and_id(
        item=graph_model, type="GraphModel", workspace=workspace_id
    )

    payload = {
        "query": query,
    }
    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/GraphModels/{item_id}/executeQuery?preview=True",
        method="post",
        payload=payload,
    )

    print(
        f"{icons.green_dot} Executed query on Graph Model '{item_name}' in workspace '{workspace_name}' successfully."
    )

    return response.json()


@log
def get_queryable_graph_type(
    graph_model: str | UUID, workspace: Optional[str | UUID] = None
) -> dict:
    """
    Gets the current queryable graph type.

    This is a wrapper function for the following API: `Items - GetQueryableGraphType <https://learn.microsoft.com/rest/api/fabric/graphmodel/items/get-queryable-graph-type(preview)>`_.

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
    dict
        A dictionary showing the current queryable graph type.
    """

    workspace_id = resolve_workspace_id(workspace)
    item_id = resolve_item_id(
        item=graph_model, type="GraphModel", workspace=workspace_id
    )

    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/GraphModels/{item_id}/getQueryableGraphType?preview=True"
    )

    return response.json()
