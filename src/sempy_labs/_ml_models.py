import pandas as pd
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    _base_api,
    delete_item,
    _create_dataframe,
    create_item,
)
from uuid import UUID


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

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/mlModels",
        status_codes=200,
        uses_pagination=True,
    )

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
