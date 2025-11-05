import pandas as pd
from typing import Any, Optional, List
from sempy_labs._helper_functions import (
    _update_dataframe_datatypes,
    resolve_item_id,
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
def delete_ml_model(ml_model: str | UUID, workspace: Optional[str | UUID] = None):
    """
    Deletes a Fabric ML model.

    This is a wrapper function for the following API: `Items - Delete ML Model <https://learn.microsoft.com/rest/api/fabric/mlmodel/items/delete-ml-model>`_.

    Parameters
    ----------
    ml_model: str | uuid.UUID
        Name or ID of the ML model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    delete_item(item=ml_model, type="MLModel", workspace=workspace)


@log
def activate_ml_model_endpoint_version(
    ml_model: str | UUID, name: str, workspace: Optional[str | UUID] = None
):
    """
    Activates the specified model version endpoint.

    This is a wrapper function for the following API: `Endpoint - Activate ML Model Endpoint Version <https://learn.microsoft.com/rest/api/fabric/mlmodel/endpoint/activate-ml-model-endpoint-version>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

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


@log
def deactivate_ml_model_endpoint_version(
    ml_model: str | UUID, name: str, workspace: Optional[str | UUID] = None
):
    """
    Deactivates the specified model version endpoint.

    This is a wrapper function for the following API: `Endpoint - Deactivate ML Model Endpoint Version <https://learn.microsoft.com/rest/api/fabric/mlmodel/endpoint/deactivate-ml-model-endpoint-version>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

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
        request=f"/v1/workspaces/{workspace_id}/mlmodels/{model_id}/endpoint/versions/{name}/deactivate",
        method="post",
        client="fabric_sp",
        lro_return_status_code=True,
        status_codes=[200, 202],
    )

    print(
        f"{icons.green_dot} The {model_name} model version {name} has been deactivated in the {workspace_name} workspace."
    )


@log
def deactivate_all_ml_model_endpoint_versions(
    ml_model: str | UUID, workspace: Optional[str | UUID] = None
):
    """
    Deactivates the specified machine learning model and its version's endpoints.

    This is a wrapper function for the following API: `Endpoint - Deactivate All ML Model Endpoint Versions <https://learn.microsoft.com/rest/api/fabric/mlmodel/endpoint/deactivate-all-ml-model-endpoint-versions>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    ml_model: str | uuid.UUID
        Name or ID of the ML model.
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
        request=f"/v1/workspaces/{workspace_id}/mlmodels/{model_id}/endpoint/versions/deactivateAll",
        method="post",
        client="fabric_sp",
        lro_return_status_code=True,
        status_codes=[200, 202],
    )

    print(
        f"{icons.green_dot} All endpoint versions of the {model_name} model within the {workspace_name} workspace have been deactivated."
    )


@log
def list_ml_model_endpoint_versions(
    ml_model: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Lists all machine learning model endpoint versions.

    This is a wrapper function for the following API: `Endpoint - List ML Model Endpoint Versions <https://learn.microsoft.com/rest/api/fabric/mlmodel/endpoint/list-ml-model-endpoint-versions>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    ml_model: str | uuid.UUID
        Name or ID of the ML model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the ML model endpoint versions within a workspace.
    """

    workspace_id = resolve_workspace_id(workspace)
    model_id = resolve_item_id(item=ml_model, type="MLModel", workspace=workspace)

    columns = {
        "Version Name": "string",
        "Status": "string",
        "Type": "string",
        "Name": "string",
        "Required": "bool",
        "Scale Rule": "string",
    }
    df = _create_dataframe(columns=columns)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/mlmodels/{model_id}/endpoint/versions",
        client="fabric_sp",
        uses_pagination=True,
    )

    rows = []
    for r in responses:
        for version in r.get("value", []):
            base = {
                "Version Name": version.get("versionName"),
                "Status": version.get("status"),
                "Scale Rule": version.get("scaleRule"),
            }
            for sig_type in ["inputSignature", "outputSignature"]:
                for entry in version.get(sig_type, []):
                    rows.append(
                        {
                            **base,
                            "Signature Type": (
                                "Input" if sig_type == "inputSignature" else "Output"
                            ),
                            "Name": entry.get("name"),
                            "Type": entry.get("type"),
                            "Required": entry.get("required"),
                        }
                    )
            # Handle versions with no signatures
            if "inputSignature" not in version and "outputSignature" not in version:
                rows.append(base)

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
def score_ml_model_endpoint(
    ml_model: str | UUID,
    inputs: List[List[Any]],
    orientation: str = "values",
    workspace: Optional[str | UUID] = None,
) -> dict:
    """
    Scores input data using the default version of the endpoint and returns results.

    This is a wrapper function for the following API: `Endpoint - Score ML Model Endpoint <https://learn.microsoft.com/rest/api/fabric/mlmodel/endpoint/score-ml-model-endpoint>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    ml_model: str | uuid.UUID
        Name or ID of the ML model.
    inputs: List[List[Any]]
        Machine learning inputs to score in the form of Pandas dataset arrays that can include strings, numbers, integers and booleans.
    orientation: str, default='values'
        `Orientation <https://learn.microsoft.com/en-us/rest/api/fabric/mlmodel/endpoint/score-ml-model-endpoint?tabs=HTTP#orientation>`_ of the input data.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    workspace_id = resolve_workspace_id(workspace)
    model_id = resolve_item_id(item=ml_model, type="MLModel", workspace=workspace)

    orientation = _validate_orientation(orientation)
    payload = {
        "formatType": "dataframe",
        "orientation": orientation,
        "inputs": inputs,
    }

    result = _base_api(
        request=f"/v1/workspaces/{workspace_id}/mlmodels/{model_id}/endpoint/score",
        method="post",
        client="fabric_sp",
        payload=payload,
        lro_return_json=True,
        status_codes=[200, 202],
    )

    return result


@log
def score_ml_model_endpoint_version(
    ml_model: str | UUID,
    name: str,
    inputs: List[List[Any]],
    orientation: str = "values",
    workspace: Optional[str | UUID] = None,
) -> dict:
    """
    Scores input data using the default version of the endpoint and returns results.

    This is a wrapper function for the following API: `Endpoint - Score ML Model Endpoint Version <https://learn.microsoft.com/rest/api/fabric/mlmodel/endpoint/score-ml-model-endpoint-version>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    ml_model: str | uuid.UUID
        Name or ID of the ML model.
    name: str
        The ML model version name.
    inputs: List[List[Any]]
        Machine learning inputs to score in the form of Pandas dataset arrays that can include strings, numbers, integers and booleans.
    orientation: str, default='values'
        `Orientation <https://learn.microsoft.com/en-us/rest/api/fabric/mlmodel/endpoint/score-ml-model-endpoint?tabs=HTTP#orientation>`_ of the input data.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    workspace_id = resolve_workspace_id(workspace)
    model_id = resolve_item_id(item=ml_model, type="MLModel", workspace=workspace)

    orientation = _validate_orientation(orientation)
    payload = {
        "formatType": "dataframe",
        "orientation": orientation,
        "inputs": inputs,
    }

    result = _base_api(
        request=f"/v1/workspaces/{workspace_id}/mlmodels/{model_id}/endpoint/versions/{name}/score",
        method="post",
        client="fabric_sp",
        payload=payload,
        lro_return_json=True,
        status_codes=[200, 202],
    )

    return result


def _validate_orientation(orientation: str):

    orientation = orientation.lower()
    if orientation not in ["split", "values", "record", "index", "table"]:
        raise ValueError(
            f"Invalid orientation '{orientation}'. Must be one of 'split', 'values', 'record', 'index', or 'table'."
        )
    return orientation
