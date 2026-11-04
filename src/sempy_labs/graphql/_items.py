import pandas as pd
from uuid import UUID
from typing import Optional
from sempy_labs._helper_functions import (
    _base_api,
    _create_dataframe,
    _update_dataframe_datatypes,
    _get_item_definition,
    _conv_b64,
    resolve_workspace_id,
    resolve_workspace_name_and_id,
    resolve_item_id,
    resolve_item_name_and_id,
    create_item,
    delete_item,
)
from sempy._utils._log import log
import sempy_labs._icons as icons


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

    columns = {
        "GraphQL API Name": "string",
        "GraphQL API Id": "string",
        "Description": "string",
    }
    df = _create_dataframe(columns=columns)

    workspace_id = resolve_workspace_id(workspace)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/GraphQLApis",
        uses_pagination=True,
        client="fabric_sp",
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            rows.append(
                {
                    "GraphQL API Name": v.get("displayName"),
                    "GraphQL API Id": v.get("id"),
                    "Description": v.get("description"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


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

    create_item(
        name=name, description=description, type="GraphQLApi", workspace=workspace
    )


@log
def delete_graphql_api(graphql_api: str | UUID, workspace: Optional[str | UUID] = None):
    """
    Deletes a GraphQL API.

    This is a wrapper function for the following API: `Items - Delete GraphQLApi <https://learn.microsoft.com/rest/api/fabric/graphqlapi/items/delete-graphqlapi>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    graphql_api : str | uuid.UUID
        Name or ID of the GraphQL API.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    delete_item(item=graphql_api, type="GraphQLApi", workspace=workspace)


@log
def get_graphql_api(
    graphql_api: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Returns properties of the specified GraphQL API.

    This is a wrapper function for the following API: `Items - Get GraphQLApi <https://learn.microsoft.com/rest/api/fabric/graphqlapi/items/get-graphqlapi>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    graphql_api : str | uuid.UUID
        Name or ID of the GraphQL API.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the properties of the GraphQL API.
    """

    columns = {
        "GraphQL API Name": "string",
        "GraphQL API Id": "string",
        "Description": "string",
        "Folder Id": "string",
        "Sensitivity Label Id": "string",
        "Default Identity Type": "string",
        "Default Identity Id": "string",
        "Default Identity Name": "string",
    }
    df = _create_dataframe(columns=columns)

    workspace_id = resolve_workspace_id(workspace)
    graphql_api_id = resolve_item_id(
        item=graphql_api, type="GraphQLApi", workspace=workspace_id
    )

    result = _base_api(
        request=f"/v1/workspaces/{workspace_id}/GraphQLApis/{graphql_api_id}",
        client="fabric_sp",
    ).json()

    identity = result.get("defaultIdentity", {})

    df = pd.DataFrame(
        [
            {
                "GraphQL API Name": result.get("displayName"),
                "GraphQL API Id": result.get("id"),
                "Description": result.get("description"),
                "Folder Id": result.get("folderId"),
                "Sensitivity Label Id": result.get("sensitivityLabel", {}).get("id"),
                "Default Identity Type": identity.get("type"),
                "Default Identity Id": identity.get("id"),
                "Default Identity Name": identity.get("displayName"),
            }
        ],
        columns=list(columns.keys()),
    )

    _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
def update_graphql_api(
    graphql_api: str | UUID,
    name: Optional[str] = None,
    description: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Updates the properties of the specified GraphQL API.

    This is a wrapper function for the following API: `Items - Update GraphQLApi <https://learn.microsoft.com/rest/api/fabric/graphqlapi/items/update-graphqlapi>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    graphql_api : str | uuid.UUID
        Name or ID of the GraphQL API.
    name : str, default=None
        New name of the GraphQL API.
    description : str, default=None
        New description of the GraphQL API.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    item_name, item_id = resolve_item_name_and_id(
        item=graphql_api, type="GraphQLApi", workspace=workspace_id
    )

    payload = {}
    if name:
        payload["displayName"] = name
    if description is not None:
        payload["description"] = description

    if not payload:
        print(
            f"{icons.info} No updates were provided for the '{item_name}' GraphQL API within the '{workspace_name}' workspace."
        )
        return

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/GraphQLApis/{item_id}",
        method="patch",
        payload=payload,
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The '{item_name}' GraphQL API within the '{workspace_name}' workspace has been updated."
    )


@log
def get_graphql_api_definition(
    graphql_api: str | UUID,
    workspace: Optional[str | UUID] = None,
    return_dataframe: bool = False,
) -> dict | pd.DataFrame:
    """
    Gets the definition of the specified GraphQL API.

    This is a wrapper function for the following API: `Items - Get GraphQLApi Definition <https://learn.microsoft.com/rest/api/fabric/graphqlapi/items/get-graphqlapi-definition>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    graphql_api : str | uuid.UUID
        Name or ID of the GraphQL API.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    return_dataframe : bool, default=False
        If True, returns a pandas dataframe of the definition parts.
        If False, returns the decoded contents of 'graphql-definition.json' as a dictionary.

    Returns
    -------
    dict | pandas.DataFrame
        The GraphQL API definition in .json format or as a pandas dataframe.
    """

    return _get_item_definition(
        item=graphql_api,
        type="GraphQLApi",
        workspace=workspace,
        return_dataframe=return_dataframe,
    )


@log
def update_graphql_api_definition(
    graphql_api: str | UUID,
    definition: dict,
    workspace: Optional[str | UUID] = None,
):
    """
    Overrides the definition of an existing GraphQL API.

    This is a wrapper function for the following API: `Items - Update GraphQLApi Definition <https://learn.microsoft.com/rest/api/fabric/graphqlapi/items/update-graphqlapi-definition>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    graphql_api : str | uuid.UUID
        Name or ID of the GraphQL API.
    definition : dict
        The contents of the 'graphql-definition.json' file (not Base64-encoded), in the format
        returned by :func:`sempy_labs.graphql.get_graphql_api_definition`.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    item_name, item_id = resolve_item_name_and_id(
        item=graphql_api, type="GraphQLApi", workspace=workspace_id
    )

    payload = {
        "definition": {
            "parts": [
                {
                    "path": "graphql-definition.json",
                    "payload": _conv_b64(definition),
                    "payloadType": "InlineBase64",
                }
            ]
        }
    }

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/GraphQLApis/{item_id}/updateDefinition",
        method="post",
        payload=payload,
        client="fabric_sp",
        status_codes=None,
        lro_return_status_code=True,
    )

    print(
        f"{icons.green_dot} The definition of the '{item_name}' GraphQL API within the '{workspace_name}' workspace has been updated."
    )
