import pandas as pd
from typing import Optional
from ._helper_functions import (
    resolve_item_name_and_id,
    resolve_workspace_id,
    _base_api,
    delete_item,
    _create_dataframe,
    create_item,
    resolve_workspace_name_and_id,
)
from uuid import UUID
from sempy._utils._log import log
import sempy_labs._icons as icons


@log
def list_ml_models(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows the ML models within a workspace.

    This is a wrapper function for the following API: `Items - List ML Models <https://learn.microsoft.com/rest/api/fabric/mlmodel/items/list-ml-models>`_.

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
        "ML Model Name": "string",
        "ML Model Id": "string",
        "Description": "string",
    }
    df = _create_dataframe(columns=columns)

    workspace_id = resolve_workspace_id(workspace)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/mlModels",
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
                    "ML Model Name": modelName,
                    "ML Model Id": model_id,
                    "Description": desc,
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def create_ml_model(
    name: str, description: Optional[str] = None, workspace: Optional[str | UUID] = None
):
    """
    Creates a Fabric ML model.

    This is a wrapper function for the following API: `Items - Create ML Model <https://learn.microsoft.com/rest/api/fabric/mlmodel/items/create-ml-model>`_.

    Parameters
    ----------
    name: str
        Name of the ML model.
    description : str, default=None
        A description of the ML model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    create_item(name=name, description=description, type="MLModel", workspace=workspace)


@log
def delete_ml_model(name: str | UUID, workspace: Optional[str | UUID] = None):
    """
    Deletes a Fabric ML model.

    This is a wrapper function for the following API: `Items - Delete ML Model <https://learn.microsoft.com/rest/api/fabric/mlmodel/items/delete-ml-model>`_.

    Parameters
    ----------
    name: str | uuid.UUID
        Name or ID of the ML model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    delete_item(item=name, type="MLModel", workspace=workspace)


@log
def activate_ml_model(
    ml_model: str | UUID, name: str, workspace: Optional[str | UUID] = None
):
    """
    Activates the specified model version endpoint.

    This is a wrapper function for the following API: `Endpoint - Activate ML Model Endpoint Version <https://learn.microsoft.com/rest/api/fabric/mlmodel/endpoint/activate-ml-model-endpoint-version>`_.

    Parameters
    ----------
    ml_model: str | uuid.UUID
        Name or ID of the ML model.
    name: str
        The ML model version name.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (model_name, model_id) = resolve_item_name_and_id(
        item=ml_model, type="MLModel", workspace=workspace
    )

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/mlmodels/{model_id}/endpoint/versions/{name}/activate",
        method="post",
        client="fabric_sp",
        lro_return_status_code=True,
        status_codes=[200, 202],
    )

    print(
        f"{icons.green_dot} The {model_name} model version {name} has been activated in the {workspace_name} workspace."
    )
