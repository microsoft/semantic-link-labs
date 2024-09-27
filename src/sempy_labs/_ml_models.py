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


def list_ml_models(workspace: Optional[str] = None) -> pd.DataFrame:
    """
    Shows the ML models within a workspace.

    Parameters
    ----------
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the ML models within a workspace.
    """

    df = pd.DataFrame(columns=["ML Model Name", "ML Model Id", "Description"])

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/mlModels")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    for r in responses:
        for v in r.get("value", []):
            model_id = v.get("id")
            modelName = v.get("displayName")
            desc = v.get("description")

            new_data = {
                "ML Model Name": modelName,
                "ML Model Id": model_id,
                "Description": desc,
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def create_ml_model(
    name: str, description: Optional[str] = None, workspace: Optional[str] = None
):
    """
    Creates a Fabric ML model.

    Parameters
    ----------
    name: str
        Name of the ML model.
    description : str, default=None
        A description of the environment.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    request_body = {"displayName": name}

    if description:
        request_body["description"] = description

    client = fabric.FabricRestClient()
    response = client.post(f"/v1/workspaces/{workspace_id}/mlModels", json=request_body)

    lro(client, response, status_codes=[201, 202])

    print(
        f"{icons.green_dot} The '{name}' ML model has been created within the '{workspace}' workspace."
    )


def delete_ml_model(name: str, workspace: Optional[str] = None):
    """
    Deletes a Fabric ML model.

    Parameters
    ----------
    name: str
        Name of the ML model.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    item_id = fabric.resolve_item_id(
        item_name=name, type="MLModel", workspace=workspace
    )

    client = fabric.FabricRestClient()
    response = client.delete(f"/v1/workspaces/{workspace_id}/mlModels/{item_id}")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The '{name}' ML model within the '{workspace}' workspace has been deleted."
    )
