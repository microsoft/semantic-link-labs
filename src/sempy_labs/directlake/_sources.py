from uuid import UUID
from typing import Optional, List
from sempy._utils._log import log


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
    from sempy_labs.tom import connect_semantic_model

    with connect_semantic_model(dataset=dataset, workspace=workspace) as tom:
        return tom.get_direct_lake_sources()
