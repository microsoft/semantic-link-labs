import sempy.fabric as fabric
import pandas as pd
import sempy_labs._icons as icons
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    _base_api,
    _create_dataframe,
)
from uuid import UUID


def list_kql_querysets(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows the KQL querysets within a workspace.

    This is a wrapper function for the following API: `Items - List KQL Querysets <https://learn.microsoft.com/rest/api/fabric/kqlqueryset/items/list-kql-querysets>`_.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the KQL querysets within a workspace.
    """

    columns = {
        "KQL Queryset Name": "string",
        "KQL Queryset Id": "string",
        "Description": "string",
    }
    df = _create_dataframe(columns=columns)

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    responses = _base_api(
        request=f"v1/workspaces/{workspace_id}/kqlQuerysets", uses_pagination=True
    )

    for r in responses:
        for v in r.get("value", []):
            new_data = {
                "KQL Queryset Name": v.get("displayName"),
                "KQL Queryset Id": v.get("id"),
                "Description": v.get("description"),
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def create_kql_queryset(
    name: str, description: Optional[str] = None, workspace: Optional[str | UUID] = None
):
    """
    Creates a KQL queryset.

    This is a wrapper function for the following API: `Items - Create KQL Queryset <https://learn.microsoft.com/rest/api/fabric/kqlqueryset/items/create-kql-queryset>`_.

    Parameters
    ----------
    name: str
        Name of the KQL queryset.
    description : str, default=None
        A description of the environment.
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
        request=f"v1/workspaces/{workspace_id}/kqlQuerysets",
        method="post",
        payload=payload,
        status_codes=[201, 202],
        lro_return_status_code=True,
    )

    print(
        f"{icons.green_dot} The '{name}' KQL queryset has been created within the '{workspace_name}' workspace."
    )


def delete_kql_queryset(name: str, workspace: Optional[str | UUID] = None):
    """
    Deletes a KQL queryset.

    This is a wrapper function for the following API: `Items - Delete KQL Queryset <https://learn.microsoft.com/rest/api/fabric/kqlqueryset/items/delete-kql-queryset>`_.

    Parameters
    ----------
    name: str
        Name of the KQL queryset.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    kql_database_id = fabric.resolve_item_id(
        item_name=name, type="KQLQueryset", workspace=workspace_id
    )

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/kqlQuerysets/{kql_database_id}",
        method="delete",
    )
    print(
        f"{icons.green_dot} The '{name}' KQL queryset within the '{workspace_name}' workspace has been deleted."
    )
