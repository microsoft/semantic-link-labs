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

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/mlExperiments")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

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

    request_body = {"displayName": name}

    if description:
        request_body["description"] = description

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/workspaces/{workspace_id}/mlExperiments", json=request_body
    )

    lro(client, response, status_codes=[201, 202])

    print(
        f"{icons.green_dot} The '{name}' ML experiment has been created within the '{workspace_name}' workspace."
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

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    item_id = fabric.resolve_item_id(
        item_name=name, type="MLExperiment", workspace=workspace_id
    )

    client = fabric.FabricRestClient()
    response = client.delete(f"/v1/workspaces/{workspace_id}/mlExperiments/{item_id}")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The '{name}' ML experiment within the '{workspace_name}' workspace has been deleted."
    )
