import sempy.fabric as fabric
import pandas as pd
from typing import Optional
import sempy_labs._icons as icons
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    pagination,
    lro,
)
from sempy.fabric.exceptions import FabricHTTPException


def list_data_pipelines(workspace: Optional[str] = None) -> pd.DataFrame:
    """
    Shows the data pipelines within a workspace.

    Parameters
    ----------
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the data pipelines within a workspace.
    """

    # https://learn.microsoft.com/en-us/rest/api/fabric/datapipeline/items/list-data-pipelines?tabs=HTTP

    df = pd.DataFrame(columns=["Data Pipeline Name", "Data Pipeline ID", "Description"])

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/dataPipelines")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    for r in responses:
        for v in r.get("value", []):
            new_data = {
                "Data Pipeline Name": v.get("displayName"),
                "Data Pipeline Id": v.get("id"),
                "Description": v.get("description"),
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def create_data_pipeline(data_pipeline: str, description: Optional[str] = None, workspace: Optional[str] = None):
    """
    Creates a data pipeline within a workspace.

    Parameters
    ----------
    data_pipeline : str
        The data pipeline name.
    description : str, default=None
        The description of the data pipeline.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    # https://learn.microsoft.com/en-us/rest/api/fabric/datapipeline/items/create-data-pipeline?tabs=HTTP

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    payload = {'displayName': data_pipeline}

    if description is not None:
        payload['description'] = description

    client = fabric.FabricRestClient()
    response = client.post(f"/v1/workspaces/{workspace_id}/dataPipelines")

    lro(client, response, status_codes=[201, 202])

    print(f"{icons.green_dot} The '{data_pipeline}' data pipeline has been created within the '{workspace}' workspace.")


def delete_data_pipeline(data_pipeline: str, workspace: Optional[str] = None):
    """
    Deletes a data pipeline within a workspace.

    Parameters
    ----------
    data_pipeline : str
        The data pipeline name.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    # https://learn.microsoft.com/en-us/rest/api/fabric/datapipeline/items/delete-data-pipeline?tabs=HTTP

    from sempy_labs._helper_functions import resolve_data_pipeline_id

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)
    data_pipeline_id = resolve_data_pipeline_id(data_pipeline_name=data_pipeline, workspace=workspace)

    client = fabric.FabricRestClient()
    response = client.delete(f"/v1/workspaces/{workspace_id}/dataPipelines/{data_pipeline_id}")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(f"{icons.green_dot} The '{data_pipeline}' data pipeline within the '{workspace}' workspace has been deleted.")


def update_data_pipeline(data_pipeline: str, workspace: Optional[str] = None):

    # https://learn.microsoft.com/en-us/rest/api/fabric/datapipeline/items/update-data-pipeline?tabs=HTTP

    from sempy_labs._helper_functions import resolve_data_pipeline_id

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)
    data_pipeline_id = resolve_data_pipeline_id(data_pipeline_name=data_pipeline, workspace=workspace)
    print(data_pipeline_id)
