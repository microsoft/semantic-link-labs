import sempy.fabric as fabric
import pandas as pd
import sempy_labs._icons as icons
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    lro,
    pagination,
    _decode_b64,
)
from sempy.fabric.exceptions import FabricHTTPException
from uuid import UUID


def list_data_pipelines(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows the data pipelines within a workspace.

    This is a wrapper function for the following API: `Items - List Data Pipelines <https://learn.microsoft.com/rest/api/fabric/datapipeline/items/list-data-pipelines>`_.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the data pipelines within a workspace.
    """

    df = pd.DataFrame(columns=["Data Pipeline Name", "Data Pipeline ID", "Description"])

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/dataPipelines")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    for r in responses:
        for v in r.get("value", []):
            new_data = {
                "Data Pipeline Name": v.get("displayName"),
                "Data Pipeline ID": v.get("id"),
                "Description": v.get("description"),
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def create_data_pipeline(
    name: str, description: Optional[str] = None, workspace: Optional[str | UUID] = None
):
    """
    Creates a Fabric data pipeline.

    This is a wrapper function for the following API: `Items - Create Data Pipeline <https://learn.microsoft.com/rest/api/fabric/datapipeline/items/create-data-pipeline>`_.

    Parameters
    ----------
    name: str
        Name of the data pipeline.
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
        f"/v1/workspaces/{workspace_id}/dataPipelines", json=request_body
    )

    lro(client, response, status_codes=[201, 202])

    print(
        f"{icons.green_dot} The '{name}' data pipeline has been created within the '{workspace_name}' workspace."
    )


def delete_data_pipeline(name: str, workspace: Optional[str | UUID] = None):
    """
    Deletes a Fabric data pipeline.

    This is a wrapper function for the following API: `Items - Delete Data Pipeline <https://learn.microsoft.com/rest/api/fabric/datapipeline/items/delete-data-pipeline>`_.

    Parameters
    ----------
    name: str
        Name of the data pipeline.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    item_id = fabric.resolve_item_id(
        item_name=name, type="DataPipeline", workspace=workspace_id
    )

    client = fabric.FabricRestClient()
    response = client.delete(f"/v1/workspaces/{workspace_id}/dataPipelines/{item_id}")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The '{name}' data pipeline within the '{workspace_name}' workspace has been deleted."
    )


def get_data_pipeline_definition(
    name: str, workspace: Optional[str | UUID] = None, decode: bool = True
) -> dict | pd.DataFrame:
    """
    Obtains the definition of a data pipeline.

    Parameters
    ----------
    name : str
        The name of the data pipeline.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    decode : bool, default=True
        decode : bool, default=True
        If True, decodes the data pipeline definition file into .json format.
        If False, obtains the data pipeline definition file a pandas DataFrame format.

    Returns
    -------
    dict | pandas.DataFrame
        A pandas dataframe showing the data pipelines within a workspace.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    item_id = fabric.resolve_item_id(
        item_name=name, type="DataPipeline", workspace=workspace_id
    )

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/workspaces/{workspace_id}/dataPipelines/{item_id}/getDefinition"
    )
    result = lro(client, response).json()

    df = pd.json_normalize(result["definition"]["parts"])

    if not decode:
        return df
    content = df[df["path"] == "pipeline-content.json"]
    payload = content["payload"].iloc[0]

    return _decode_b64(payload)
