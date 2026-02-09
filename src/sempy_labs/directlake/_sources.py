import re
from uuid import UUID
from typing import Optional, List
from sempy_labs._helper_functions import (
    _base_api,
    resolve_workspace_name,
)
from sempy._utils._log import log


@log
def _extract_expression_list(expression):
    """
    Finds the pattern for DL/SQL & DL/OL expressions in the semantic model.
    """

    pattern_sql = r'Sql\.Database\s*\(\s*"([^"]+)"\s*,\s*"([^"]+)"\s*\)'
    pattern_no_sql = (
        r'AzureStorage\.DataLake\(".*?/([0-9a-fA-F\-]{36})/([0-9a-fA-F\-]{36})"'
    )

    match_sql = re.search(pattern_sql, expression)
    match_no_sql = re.search(pattern_no_sql, expression)

    result = []
    if match_sql:
        value_1, value_2 = match_sql.groups()
        result = [value_1, value_2, True]
    elif match_no_sql:
        value_1, value_2 = match_no_sql.groups()
        result = [value_1, value_2, False]

    return result


@log
def _get_direct_lake_expressions(
    dataset: str | UUID, workspace: Optional[str | UUID] = None
) -> dict:
    """
    Extracts a dictionary of all Direct Lake expressions from a semantic model.
    """

    from sempy_labs.tom import connect_semantic_model

    result = {}

    with connect_semantic_model(dataset=dataset, workspace=workspace) as tom:
        for e in tom.model.Expressions:
            expr_name = e.Name
            expr = e.Expression

            list_values = _extract_expression_list(expr)
            if list_values:
                result[expr_name] = list_values

    return result


@log
def get_direct_lake_sources(
    dataset: str | UUID, workspace: Optional[str | UUID] = None
) -> List[dict]:
    """
    Retrieves a list of the Direct Lake sources used in a semantic model, including their type, workspace, and whether they use a SQL endpoint.

    Parameters:
    dataset : str | uuid.UUID
        The name or UUID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns:
    typing.List[dict]
        A list of dictionaries, each containing details about a Direct Lake source used in the semantic model.
        Example:
        [
            {
                "itemId": "123e4567-e89b-12d3-a456-426614174000",
                "itemName": "My Lakehouse",
                "itemType": "Lakehouse",
                "workspaceId": "123e4567-e89b-12d3-a456-426614174001",
                "workspaceName": "Workspace A",
                "usesSqlEndpoint": False,
            },
            {
                "itemId": "123e4567-e89b-12d3-a456-426614174002",
                "itemName": "My Data Source",
                "itemType": "Warehouse",
                "workspaceId": "123e4567-e89b-12d3-a456-426614174001",
                "workspaceName": "Workspace A",
                "usesSqlEndpoint": True,
            }
        ]

    """

    sql = "SqlAnalyticsEndpoint"
    sources = []
    expr = _get_direct_lake_expressions(dataset=dataset, workspace=workspace)
    for name, items in expr.items():
        artifact_id = items[1]
        uses_sql_endpoint = items[2]
        result = _base_api(
            request=f"metadata/artifacts/{artifact_id}", client="internal"
        ).json()
        item_type = result.get("artifactType")
        item_name = result.get("displayName")
        item_workspace_id = result.get("folderObjectId")
        parent_artifact_id = result.get("parentArtifactObjectId")

        type = "Lakehouse" if item_type == sql else item_type
        item_id = parent_artifact_id if item_type == sql else artifact_id

        sources.append(
            {
                "itemId": item_id,
                "itemName": item_name,
                "itemType": type,
                "workspaceId": item_workspace_id,
                "workspaceName": resolve_workspace_name(item_workspace_id),
                "usesSqlEndpoint": uses_sql_endpoint,
            }
        )

    return sources
