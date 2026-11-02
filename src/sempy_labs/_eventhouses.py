import pandas as pd
from typing import Optional
from uuid import UUID
from sempy._utils._log import log
import sempy_labs.eventhouse as eh


@log
def create_eventhouse(
    name: str,
    definition: Optional[dict],
    description: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Creates a Fabric eventhouse.

    This is a wrapper function for the following API: `Items - Create Eventhouse <https://learn.microsoft.com/rest/api/fabric/environment/items/create-eventhouse>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    name: str
        Name of the eventhouse.
    definition : dict
        The definition (EventhouseProperties.json) of the eventhouse.
    description : str, default=None
        A description of the environment.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    eh.create_eventhouse(
        name=name,
        definition=definition,
        description=description,
        workspace=workspace,
    )


@log
def list_eventhouses(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows the eventhouses within a workspace.

    This is a wrapper function for the following API: `Items - List Eventhouses <https://learn.microsoft.com/rest/api/fabric/environment/items/list-eventhouses>`_.

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
        A pandas dataframe showing the eventhouses within a workspace.
    """

    return eh.list_eventhouses(workspace=workspace)


@log
def delete_eventhouse(name: str, workspace: Optional[str | UUID] = None):
    """
    Deletes a Fabric eventhouse.

    This is a wrapper function for the following API: `Items - Delete Eventhouse <https://learn.microsoft.com/rest/api/fabric/environment/items/delete-eventhouse>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    name: str
        Name of the eventhouse.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    eh.delete_eventhouse(name=name, workspace=workspace)


@log
def get_eventhouse_definition(
    eventhouse: str | UUID,
    workspace: Optional[str | UUID] = None,
    return_dataframe: bool = False,
) -> dict | pd.DataFrame:
    """
    Gets the eventhouse definition.

    This is a wrapper function for the following API: `Items - Get Eventhouse Definition <https://learn.microsoft.com/rest/api/fabric/eventhouse/items/get-eventhouse-definition>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    eventhouse : str
        Name of the eventhouse.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID in which the eventhouse resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    return_dataframe : bool, default=False
        If True, returns a dataframe. If False, returns a json dictionary.

    Returns
    -------
    dict | pandas.DataFrame
        The eventhouse definition in .json format or as a pandas dataframe.
    """

    return eh.get_eventhouse_definition(
        eventhouse=eventhouse,
        workspace=workspace,
        return_dataframe=return_dataframe,
    )
