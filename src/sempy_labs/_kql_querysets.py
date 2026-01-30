import pandas as pd
from typing import Optional
from uuid import UUID
from sempy._utils._log import log
import sempy_labs.kql_queryset as kqlq


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

    return kqlq.list_kql_querysets(workspace=workspace)


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

    kqlq.create_kql_queryset(name=name, description=description, workspace=workspace)


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

    kqlq.delete_kql_queryset(kql_queryset=kql_queryset, workspace=workspace)
