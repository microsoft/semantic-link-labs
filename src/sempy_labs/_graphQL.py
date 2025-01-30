import pandas as pd
from uuid import UUID
from typing import Optional
from sempy_labs._helper_functions import (
    _base_api,
    _create_dataframe,
    resolve_workspace_name_and_id,
    _print_success,
)


def list_graphql_apis(workspace: Optional[str | UUID]) -> pd.DataFrame:
    """
    Shows the Graph QL APIs within a workspace.

    This is a wrapper function for the following API: `Items - List GraphQLApis <https://learn.microsoft.com/rest/api/fabric/graphqlapi/items/list-graphqlapi-s>`_.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the GraphQL APIs within a workspace.
    """

    columns = {
        "GraphQL API Name": "string",
        "GraphQL API Id": "string",
        "Description": "string",
    }
    df = _create_dataframe(columns=columns)

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/GraphQLApis", uses_pagination=True
    )

    for r in responses:
        for v in r.get("value", []):
            new_data = {
                "GraphQL API Name": v.get("displayName"),
                "GraphQL API Id": v.get("id"),
                "Description": v.get("description"),
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def create_graphql_api(
    name: str, description: Optional[str] = None, workspace: Optional[str | UUID] = None
):
    """
    Creates a GraphQL API.

    This is a wrapper function for the following API: `Items - Create GraphQLApi <https://learn.microsoft.com/rest/api/fabric/graphqlapi/items/create-graphqlapi>`_.

    Parameters
    ----------
    name: str
        Name of the GraphQL API.
    description : str, default=None
        A description of the GraphQL API.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    payload = {"displayName": name}

    if description:
        payload["description"] = description

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/GraphQLApis",
        method="post",
        status_codes=[201, 202],
        payload=payload,
        lro_return_status_code=True,
    )
    _print_success(
        item_name=name,
        item_type="GraphQL API",
        workspace_name=workspace_name,
        action="created",
    )
