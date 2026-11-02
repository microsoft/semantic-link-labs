import pandas as pd
import sempy_labs._icons as icons
from typing import Optional, Literal
from sempy_labs._helper_functions import (
    resolve_workspace_id,
    resolve_workspace_name_and_id,
    _base_api,
    _create_dataframe,
)
from uuid import UUID
from sempy._utils._log import log


@log
def list_workspace_relations(
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Shows a list of the workspace relations for a given workspace.

    This is a wrapper function for the following API: `Workspace Relations - List Workspace Relations <https://learn.microsoft.com/rest/api/fabric/core/workspace-relations/list-workspace-relations>`_.

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
        A pandas dataframe showing the workspace relations of a given workspace.
    """

    workspace_id = resolve_workspace_id(workspace)

    columns = {
        "Workspace Relation Id": "string",
        "Workspace Id": "string",
        "Related Workspace Id": "string",
        "Relation Type": "string",
    }
    df = _create_dataframe(columns=columns)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/git/workspaceRelations",
        uses_pagination=True,
        client="fabric_sp",
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            rows.append(
                {
                    "Workspace Relation Id": v.get("id"),
                    "Workspace Id": v.get("workspaceId"),
                    "Related Workspace Id": v.get("relatedWorkspaceId"),
                    "Relation Type": v.get("relationType"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def create_workspace_relation(
    related_workspace: str | UUID,
    relation_type: Literal["Base", "Branch", "RelatedWorkspace"],
    workspace: Optional[str | UUID] = None,
):
    """
    Creates a branching relation between a branch workspace and its base workspace.

    This is a wrapper function for the following API: `Workspace Relations - Create Workspace Relation
    <https://learn.microsoft.com/rest/api/fabric/core/workspace-relations/create-workspace-relation>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    related_workspace : str | uuid.UUID
        The name or ID of the related Fabric workspace.
    relation_type : typing.Literal["Base", "Branch", "RelatedWorkspace"]
        The type of the related workspace to create in the relation. 'Base' indicates that
        the related workspace is the base (source) workspace, 'Branch' indicates that the
        related workspace is a branch workspace.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    related_workspace_id = resolve_workspace_id(related_workspace)

    relation_types = ["Base", "Branch", "RelatedWorkspace"]
    if relation_type not in relation_types:
        raise ValueError(
            f"{icons.red_dot} Invalid relation type. Valid options: {relation_types}."
        )

    payload = {
        "relatedWorkspaceId": related_workspace_id,
        "relationType": relation_type,
    }

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/git/workspaceRelations",
        method="post",
        payload=payload,
        status_codes=201,
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} A '{relation_type}' workspace relation to the '{related_workspace_id}' workspace has been created within the '{workspace_name}' workspace."
    )


@log
def delete_workspace_relation(
    workspace_relation_id: UUID,
    workspace: Optional[str | UUID] = None,
):
    """
    Deletes an existing workspace relation between two workspaces.

    The specified workspace can be either a base workspace or a branch workspace.

    This is a wrapper function for the following API: `Workspace Relations - Delete Workspace Relation
    <https://learn.microsoft.com/rest/api/fabric/core/workspace-relations/delete-workspace-relation>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    workspace_relation_id : uuid.UUID
        The ID of the workspace relation (see :func:`~sempy_labs.workspace.list_workspace_relations`).
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/git/workspaceRelations/{workspace_relation_id}",
        method="delete",
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The '{workspace_relation_id}' workspace relation has been deleted from the '{workspace_name}' workspace."
    )
