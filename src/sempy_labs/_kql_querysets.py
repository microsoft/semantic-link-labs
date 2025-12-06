import pandas as pd
import sempy_labs._icons as icons
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_id,
    _base_api,
    _create_dataframe,
    delete_item,
    create_item,
)
from uuid import UUID
from sempy._utils._log import log


@log
def list_kql_querysets(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows the KQL querysets within a workspace.

    This is a wrapper function for the following API: `Items - List KQL Querysets <https://learn.microsoft.com/rest/api/fabric/kqlqueryset/items/list-kql-querysets>`_.

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
        A pandas dataframe showing the KQL querysets within a workspace.
    """

    columns = {
        "KQL Queryset Name": "string",
        "KQL Queryset Id": "string",
        "Description": "string",
    }
    df = _create_dataframe(columns=columns)

    workspace_id = resolve_workspace_id(workspace)

    responses = _base_api(
        request=f"v1/workspaces/{workspace_id}/kqlQuerysets",
        uses_pagination=True,
        client="fabric_sp",
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            rows.append(
                {
                    "KQL Queryset Name": v.get("displayName"),
                    "KQL Queryset Id": v.get("id"),
                    "Description": v.get("description"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def create_kql_queryset(
    name: str, description: Optional[str] = None, workspace: Optional[str | UUID] = None
):
    """
    Creates a KQL queryset.

    This is a wrapper function for the following API: `Items - Create KQL Queryset <https://learn.microsoft.com/rest/api/fabric/kqlqueryset/items/create-kql-queryset>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

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

    create_item(
        name=name, description=description, type="KQLQueryset", workspace=workspace
    )


@log
def delete_kql_queryset(
    kql_queryset: str | UUID, workspace: Optional[str | UUID] = None, **kwargs
):
    """
    Deletes a KQL queryset.

    This is a wrapper function for the following API: `Items - Delete KQL Queryset <https://learn.microsoft.com/rest/api/fabric/kqlqueryset/items/delete-kql-queryset>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    kql_queryset: str | uuid.UUID
        Name or ID of the KQL queryset.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    if "name" in kwargs:
        kql_queryset = kwargs["name"]
        print(
            f"{icons.warning} The 'name' parameter is deprecated. Please use 'kql_queryset' instead."
        )

    delete_item(item=kql_queryset, type="KQLQueryset", workspace=workspace)
