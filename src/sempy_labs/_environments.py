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


def create_environment(
    environment: str, description: Optional[str] = None, workspace: Optional[str] = None
):
    """
    Creates a Fabric environment.

    Parameters
    ----------
    environment: str
        Name of the environment.
    description : str, default=None
        A description of the environment.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    request_body = {"displayName": environment}

    if description:
        request_body["description"] = description

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/workspaces/{workspace_id}/environments", json=request_body
    )

    lro(client, response, status_codes=[201, 202])

    print(
        f"{icons.green_dot} The '{environment}' environment has been created within the '{workspace}' workspace."
    )


def list_environments(workspace: Optional[str] = None) -> pd.DataFrame:
    """
    Shows the environments within a workspace.

    Parameters
    ----------
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the environments within a workspace.
    """

    df = pd.DataFrame(columns=["Environment Name", "Environment Id", "Description"])

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/environments")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    for r in responses:
        for v in r.get("value", []):
            new_data = {
                "Environment Name": v.get("displayName"),
                "Environment Id": v.get("id"),
                "Description": v.get("description"),
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def delete_environment(environment: str, workspace: Optional[str] = None):
    """
    Deletes a Fabric environment.

    Parameters
    ----------
    environment: str
        Name of the environment.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    from sempy_labs._helper_functions import resolve_environment_id

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)
    environment_id = resolve_environment_id(
        environment=environment, workspace=workspace
    )

    client = fabric.FabricRestClient()
    response = client.delete(
        f"/v1/workspaces/{workspace_id}/environments/{environment_id}"
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The '{environment}' environment within the '{workspace}' workspace has been deleted."
    )


def publish_environment(environment: str, workspace: Optional[str] = None):
    """
    Publishes a Fabric environment.

    Parameters
    ----------
    environment: str
        Name of the environment.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    # https://learn.microsoft.com/en-us/rest/api/fabric/environment/spark-libraries/publish-environment?tabs=HTTP

    from sempy_labs._helper_functions import resolve_environment_id

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)
    environment_id = resolve_environment_id(
        environment=environment, workspace=workspace
    )

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/workspaces/{workspace_id}/environments/{environment_id}/staging/publish"
    )

    lro(client, response)

    print(
        f"{icons.green_dot} The '{environment}' environment within the '{workspace}' workspace has been published."
    )
