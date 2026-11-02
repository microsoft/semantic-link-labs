import pandas as pd
from typing import Optional
from uuid import UUID
from sempy._utils._log import log
import sempy_labs.ml_experiment as ml


@log
def list_ml_experiments(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows the ML experiments within a workspace.

    This is a wrapper function for the following API: `Items - List ML Experiments <https://learn.microsoft.com/rest/api/fabric/mlexperiment/items/list-ml-experiments>`_.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the ML models within a workspace.
    """

    return ml.list_ml_experiments(workspace=workspace)


@log
def create_ml_experiment(
    name: str, description: Optional[str] = None, workspace: Optional[str | UUID] = None
):
    """
    Creates a Fabric ML experiment.

    This is a wrapper function for the following API: `Items - Create ML Experiment <https://learn.microsoft.com/rest/api/fabric/mlexperiment/items/create-ml-experiment>`_.

    Parameters
    ----------
    name: str
        Name of the ML experiment.
    description : str, default=None
        A description of the ML experiment.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    ml.create_ml_experiment(name=name, description=description, workspace=workspace)


@log
def delete_ml_experiment(name: str, workspace: Optional[str | UUID] = None):
    """
    Deletes a Fabric ML experiment.

    This is a wrapper function for the following API: `Items - Delete ML Experiment <https://learn.microsoft.com/rest/api/fabric/mlexperiment/items/delete-ml-experiment>`_.

    Parameters
    ----------
    name: str
        Name of the ML experiment.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    ml.delete_ml_experiment(name=name, workspace=workspace)
