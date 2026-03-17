import pandas as pd
from typing import Optional
from uuid import UUID
import sempy_labs.data_pipeline as dp
from sempy._utils._log import log


@log
def list_data_pipelines(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows the data pipelines within a workspace.

    This is a wrapper function for the following API: `Items - List Data Pipelines <https://learn.microsoft.com/rest/api/fabric/datapipeline/items/list-data-pipelines>`_.

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
        A pandas dataframe showing the data pipelines within a workspace.
    """

    return dp.list_data_pipelines(workspace=workspace)


@log
def create_data_pipeline(
    name: str, description: Optional[str] = None, workspace: Optional[str | UUID] = None
):
    """
    Creates a Fabric data pipeline.

    This is a wrapper function for the following API: `Items - Create Data Pipeline <https://learn.microsoft.com/rest/api/fabric/datapipeline/items/create-data-pipeline>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    name: str
        Name of the data pipeline.
    description : str, default=None
        A description of the environment.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    dp.create_data_pipeline(name=name, description=description, workspace=workspace)


@log
def delete_data_pipeline(name: str | UUID, workspace: Optional[str | UUID] = None):
    """
    Deletes a Fabric data pipeline.

    This is a wrapper function for the following API: `Items - Delete Data Pipeline <https://learn.microsoft.com/rest/api/fabric/datapipeline/items/delete-data-pipeline>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    name: str | uuid.UUID
        Name or ID of the data pipeline.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    dp.delete_data_pipeline(name=name, workspace=workspace)


@log
def get_data_pipeline_definition(
    name: str | UUID, workspace: Optional[str | UUID] = None, decode: bool = True
) -> dict | pd.DataFrame:
    """
    Obtains the definition of a data pipeline.

    Parameters
    ----------
    name : str or uuid.UUID
        The name or ID of the data pipeline.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    decode : bool, default=True
        decode : bool, default=True
        If True, decodes the data pipeline definition file into .json format.
        If False, obtains the data pipeline definition file a pandas DataFrame format.

    Returns
    -------
    dict | pandas.DataFrame
        A pandas dataframe showing the data pipelines within a workspace.
    """

    dp.get_data_pipeline_definition(
        name=name,
        workspace=workspace,
        decode=decode,
    )
