import pandas as pd
from uuid import UUID
from typing import Optional
from sempy_labs._helper_functions import (
    _base_api,
    _create_dataframe,
    resolve_workspace_name_and_id,
    create_item,
)


def list_graphql_apis(workspace: Optional[str | UUID]) -> pd.DataFrame:
    """
    Shows the Graph QL APIs within a workspace.

    This is a wrapper function for the following API: `Items - List GraphQLApis <https://learn.microsoft.com/rest/api/fabric/graphqlapi/items/list-graphqlapi-s>`_.

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
        request=f"/v1/workspaces/{workspace_id}/GraphQLApis",
        uses_pagination=True,
        client="fabric_sp",
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

    create_item(
        name=name, description=description, type="GraphQLApi", workspace=workspace
    )
