import sempy.fabric as fabric
import pandas as pd
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    _base_api,
    _print_success,
    resolve_item_id,
)
from uuid import UUID


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

    df = pd.DataFrame(columns=["ML Experiment Name", "ML Experiment Id", "Description"])

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/mlExperiments",
        status_codes=200,
        uses_pagination=True,
    )

    for r in responses:
        for v in r.get("value", []):
            model_id = v.get("id")
            modelName = v.get("displayName")
            desc = v.get("description")

            new_data = {
                "ML Experiment Name": modelName,
                "ML Experiment Id": model_id,
                "Description": desc,
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


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
        request=f"/v1/workspaces/{workspace_id}/mlExperiments",
        method="post",
        json=payload,
        status_codes=[201, 202],
        lro_return_status_code=True,
    )
    _print_success(
        item_name=name,
        item_type="ML experiment",
        workspace_name=workspace_name,
        action="created",
    )


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

    item_id = resolve_item_id(item=name, type="MLExperiment", workspace=workspace)
    fabric.delete_item(item_id=item_id, workspace=workspace)
    _print_success(
        item_name=name,
        item_type="ML Experiment",
        workspace_name=workspace,
        action="deleted",
    )
