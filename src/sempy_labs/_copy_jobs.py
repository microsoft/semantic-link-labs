import pandas as pd
from typing import Optional
from uuid import UUID
import sempy_labs.copy_job as cj
from sempy._utils._log import log


@log
def list_copy_jobs(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Returns a list of copy jobs from the specified workspace.

    This is a wrapper function for the following API: `Items - List Copy Jobs <https://learn.microsoft.com/rest/api/fabric/copyjob/items/list-copy-jobs>`_.

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
        A pandas dataframe showing the copy jobs within a workspace.
    """

    return cj.list_copy_jobs(workspace=workspace)


@log
def create_copy_job(
    name: str,
    description: Optional[str] = None,
    definition: Optional[dict] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Creates a copy job in the specified workspace.

    This is a wrapper function for the following API: `Items - Create Copy Job <https://learn.microsoft.com/rest/api/fabric/copyjob/items/create-copy-job>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    name : str
        The display name of the copy job.
    description : str, default=None
        The description of the copy job. Maximum length is 256 characters.
    definition : dict, default=None
        The copy job public definition. Refer to `Copy job definition <https://learn.microsoft.com/rest/api/fabric/articles/item-management/definitions/copyjob-definition>`_ for more details.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    cj.create_copy_job(
        name=name,
        description=description,
        definition=definition,
        workspace=workspace,
    )


@log
def delete_copy_job(name: str | UUID, workspace: Optional[str | UUID] = None):
    """
    Deletes the specified copy job.

    This is a wrapper function for the following API: `Items - Delete Copy Job <https://learn.microsoft.com/rest/api/fabric/copyjob/items/delete-copy-job>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    name : str | uuid.UUID
        The name or ID of the copy job.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    cj.delete_copy_job(name=name, workspace=workspace)


@log
def get_copy_job(
    name: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Returns properties of the specified copy job.

    This is a wrapper function for the following API: `Items - Get Copy Job <https://learn.microsoft.com/rest/api/fabric/copyjob/items/get-copy-job>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    name : str | uuid.UUID
        The name or ID of the copy job.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the properties of the specified copy job.
    """

    return cj.get_copy_job(name=name, workspace=workspace)


@log
def update_copy_job(
    name: str | UUID,
    new_name: Optional[str] = None,
    description: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Updates the properties of the specified copy job.

    This is a wrapper function for the following API: `Items - Update Copy Job <https://learn.microsoft.com/rest/api/fabric/copyjob/items/update-copy-job>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    name : str | uuid.UUID
        The name or ID of the copy job.
    new_name : str, default=None
        The new display name of the copy job.
    description : str, default=None
        The new description of the copy job. Maximum length is 256 characters.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    cj.update_copy_job(
        name=name,
        new_name=new_name,
        description=description,
        workspace=workspace,
    )


@log
def get_copy_job_definition(
    name: str | UUID, workspace: Optional[str | UUID] = None, decode: bool = True
) -> dict | pd.DataFrame:
    """
    Returns the specified copy job public definition.

    This is a wrapper function for the following API: `Items - Get Copy Job Definition <https://learn.microsoft.com/rest/api/fabric/copyjob/items/get-copy-job-definition>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    name : str | uuid.UUID
        The name or ID of the copy job.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    decode : bool, default=True
        If True, decodes the copy job definition file into .json format.
        If False, returns the copy job definition as a pandas DataFrame.

    Returns
    -------
    dict | pandas.DataFrame
        If decode is True, returns a dictionary with the decoded copy job definition.
        If decode is False, returns a pandas DataFrame with the raw definition parts.
    """

    return cj.get_copy_job_definition(name=name, workspace=workspace, decode=decode)


@log
def update_copy_job_definition(
    name: str | UUID,
    definition: dict,
    update_metadata: bool = False,
    workspace: Optional[str | UUID] = None,
):
    """
    Overrides the definition for the specified copy job.

    This is a wrapper function for the following API: `Items - Update Copy Job Definition <https://learn.microsoft.com/rest/api/fabric/copyjob/items/update-copy-job-definition>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    name : str | uuid.UUID
        The name or ID of the copy job.
    definition : dict
        The copy job public definition. Refer to `Copy job definition <https://learn.microsoft.com/rest/api/fabric/articles/item-management/definitions/copyjob-definition>`_ for more details.
    update_metadata : bool, default=False
        When set to True and the .platform file is provided as part of the definition,
        the item's metadata is updated using the metadata in the .platform file.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    cj.update_copy_job_definition(
        name=name,
        definition=definition,
        update_metadata=update_metadata,
        workspace=workspace,
    )
