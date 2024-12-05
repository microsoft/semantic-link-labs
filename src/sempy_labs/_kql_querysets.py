import sempy.fabric as fabric
import pandas as pd
import sempy_labs._icons as icons
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    lro,
    pagination,
)
from sempy.fabric.exceptions import FabricHTTPException


def list_kql_querysets(
    workspace: Optional[str] = None,
    token_provider: Optional[str] = None,
) -> pd.DataFrame:
    f"""
    Shows the KQL querysets within a workspace.

    This is a wrapper function for the following API: `Items - List KQL Querysets <https://learn.microsoft.com/rest/api/fabric/kqlqueryset/items/list-kql-querysets>`_.

    Parameters
    ----------
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    token_provider : str, default=None
        {icons.token_provider_desc}

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the KQL querysets within a workspace.
    """

    df = pd.DataFrame(
        columns=[
            "KQL Queryset Name",
            "KQL Queryset Id",
            "Description",
        ]
    )

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient(token_provider=token_provider)
    response = client.get(f"/v1/workspaces/{workspace_id}/kqlQuerysets")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

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
    name: str,
    description: Optional[str] = None,
    workspace: Optional[str] = None,
    token_provider: Optional[str] = None,
):
    f"""
    Creates a KQL queryset.

    This is a wrapper function for the following API: `Items - Create KQL Queryset <https://learn.microsoft.com/rest/api/fabric/kqlqueryset/items/create-kql-queryset>`_.

    Parameters
    ----------
    name: str
        Name of the KQL queryset.
    description : str, default=None
        A description of the environment.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    token_provider : str, default=None
        {icons.token_provider_desc}
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    request_body = {"displayName": name}

    if description:
        request_body["description"] = description

    client = fabric.FabricRestClient(token_provider=token_provider)
    response = client.post(
        f"/v1/workspaces/{workspace_id}/kqlQuerysets", json=request_body
    )

    lro(client, response, status_codes=[201, 202])

    print(
        f"{icons.green_dot} The '{name}' KQL queryset has been created within the '{workspace}' workspace."
    )


def delete_kql_queryset(
    name: str,
    workspace: Optional[str] = None,
    token_provider: Optional[str] = None,
):
    f"""
    Deletes a KQL queryset.

    This is a wrapper function for the following API: `Items - Delete KQL Queryset <https://learn.microsoft.com/rest/api/fabric/kqlqueryset/items/delete-kql-queryset>`_.

    Parameters
    ----------
    name: str
        Name of the KQL queryset.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    token_provider : str, default=None
        {icons.token_provider_desc}
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)
    kql_database_id = fabric.resolve_item_id(
        item_name=name, type="KQLQueryset", workspace=workspace
    )

    client = fabric.FabricRestClient(token_provider=token_provider)
    response = client.delete(
        f"/v1/workspaces/{workspace_id}/kqlQuerysets/{kql_database_id}"
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)
    print(
        f"{icons.green_dot} The '{name}' KQL queryset within the '{workspace}' workspace has been deleted."
    )
