import pandas as pd
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_id,
    _base_api,
    delete_item,
    _create_dataframe,
    create_item,
)
from uuid import UUID
from sempy._utils._log import log


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

    columns = {
        "ML Experiment Name": "string",
        "ML Experiment Id": "string",
        "Description": "string",
    }
    df = _create_dataframe(columns=columns)

    workspace_id = resolve_workspace_id(workspace)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/mlExperiments",
        status_codes=200,
        uses_pagination=True,
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            model_id = v.get("id")
            modelName = v.get("displayName")
            desc = v.get("description")

            rows.append(
                {
                    "ML Experiment Name": modelName,
                    "ML Experiment Id": model_id,
                    "Description": desc,
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


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

    create_item(
        name=name, description=description, type="MLExperiment", workspace=workspace
    )


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

    delete_item(item=name, type="MLExperiment", workspace=workspace)
