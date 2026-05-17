import pandas as pd
from uuid import UUID
from typing import Optional
from sempy._utils._log import log
import sempy_labs.graphql as graphql


@log
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

    return graphql.list_graphql_apis(workspace=workspace)


@log
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

    graphql.create_graphql_api(name=name, description=description, workspace=workspace)
