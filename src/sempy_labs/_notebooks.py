import pandas as pd
from typing import Optional, List
from sempy._utils._log import log
from os import PathLike
from uuid import UUID
import sempy_labs.notebook as nb


def _get_notebook_definition_base(
    notebook_name: str,
    workspace: Optional[str | UUID] = None,
    format: Optional[str] = None,
) -> pd.DataFrame:

    return nb.get_notebook_definition_base(
        notebook_name=notebook_name, workspace=workspace, format=format
    )


def get_notebook_type(
    notebook_name: str, workspace: Optional[str | UUID] = None
) -> str:

    return nb.get_notebook_type(notebook_name=notebook_name, workspace=workspace)


@log
def get_notebook_definition(
    notebook_name: str,
    workspace: Optional[str | UUID] = None,
    decode: bool = True,
    format: Optional[str] = None,
) -> str:
    """
    Obtains the notebook definition.

    This is a wrapper function for the following API: `Items - Get Notebook Definition <https://learn.microsoft.com/rest/api/fabric/notebook/items/get-notebook-definition>`_.

    Parameters
    ----------
    notebook_name : str
        The name of the notebook.
    workspace : str | uuid.UUID, default=None
        The name or ID of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    decode : bool, default=True
        If True, decodes the notebook definition file into .ipynb format.
        If False, obtains the notebook definition file in base64 format.
    format : str, default=None
        The only supported value is ipynb
        If provided the format will be in standard .ipynb otherwise the format will be in source code format which is GIT friendly ipynb

    Returns
    -------
    str
        The notebook definition.
    """

    return nb.get_notebook_definition(
        notebook_name=notebook_name,
        workspace=workspace,
        decode=decode,
        format=format,
    )


@log
def import_notebook_from_web(
    notebook_name: str,
    url: str,
    description: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
    overwrite: bool = False,
    folder: Optional[str | PathLike] = None,
):
    """
    Creates a new notebook within a workspace based on a Jupyter notebook hosted in the web.

    Note: When specifying a notebook from GitHub, please use the raw file path. Note that if the non-raw file path is specified, the url will be
    converted to the raw URL as the raw URL is needed to obtain the notebook content.

    Parameters
    ----------
    notebook_name : str
        The name of the notebook to be created.
    url : str
        The url of the Jupyter Notebook (.ipynb)
    description : str, default=None
        The description of the notebook.
        Defaults to None which does not place a description.
    workspace : str | uuid.UUID, default=None
        The name or ID of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    overwrite : bool, default=False
        If set to True, overwrites the existing notebook in the workspace if it exists.
    folder : str | os.PathLike, default=None
        The folder within the workspace where the notebook will be created.
        Defaults to None which places the notebook in the root of the workspace.
    """

    nb.import_notebook_from_web(
        notebook_name=notebook_name,
        url=url,
        description=description,
        workspace=workspace,
        overwrite=overwrite,
        folder=folder,
    )


@log
def create_notebook(
    name: str,
    notebook_content: str,
    type: str = "py",
    description: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
    format: Optional[str] = None,
    folder: Optional[str | PathLike] = None,
):
    """
    Creates a new notebook with a definition within a workspace.

    Parameters
    ----------
    name : str
        The name of the notebook to be created.
    notebook_content : str
        The Jupyter notebook content (not in Base64 format).
    type : str, default="py"
        The notebook type.
    description : str, default=None
        The description of the notebook.
        Defaults to None which does not place a description.
    workspace : str | uuid.UUID, default=None
        The name or ID of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    format : str, default=None
        If 'ipynb' is provided than notebook_content should be standard ipynb format
        otherwise notebook_content should be GIT friendly format
    folder : str | os.PathLike, default=None
        The folder within the workspace where the notebook will be created.
        Defaults to None which places the notebook in the root of the workspace.
    """

    nb.create_notebook(
        name=name,
        notebook_content=notebook_content,
        type=type,
        description=description,
        workspace=workspace,
        format=format,
        folder=folder,
    )


@log
def update_notebook_definition(
    name: str,
    notebook_content: str,
    workspace: Optional[str | UUID] = None,
    format: Optional[str] = None,
):
    """
    Updates an existing notebook with a new definition.

    Parameters
    ----------
    name : str
        The name of the notebook to be updated.
    notebook_content : str
        The Jupyter notebook content (not in Base64 format).
    workspace : str | uuid.UUID, default=None
        The name or ID of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    format : str, default=None
        If 'ipynb' is provided than notebook_content should be standard ipynb format
        otherwise notebook_content should be GIT friendly format
    """

    nb.update_notebook_definition(
        name=name,
        notebook_content=notebook_content,
        workspace=workspace,
        format=format,
    )


@log
def list_notebooks(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows the notebooks within a workspace.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the notebooks within a workspace.
    """

    return nb.list_notebooks(workspace=workspace)


@log
def search_notebooks(
    search_string: str,
    notebook: Optional[str | UUID] = None,
    workspace: Optional[str | UUID | List[str | UUID]] = None,
) -> pd.DataFrame:
    """
    Searches notebooks within a workspace or across multiple workspaces for a given search string.

    Parameters
    ----------
    search_string : str
        The string to search for within the notebook definitions.
    notebook : str | uuid.UUID, default=None
        The name or ID of a specific notebook to search within.
        Defaults to None which searches across all notebooks in the specified workspace(s).
    workspace : str | uuid.UUID | list, default=None
        The name or ID of the workspace or a list of workspaces to search within.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
        If a list is provided, it should contain workspace names or IDs.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the notebooks that contain the search string in their definitions.
        The dataframe includes the workspace name, workspace ID, notebook name, and notebook ID.
    """

    return nb.search_notebooks(
        search_string=search_string,
        notebook=notebook,
        workspace=workspace,
    )
