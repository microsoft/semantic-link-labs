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


def get_environment_id(workspace: str, environment: str) -> str:
    """
    Retrieves the environment ID for a given environment name within a specified workspace.

    This function searches for the environment by its name within
    the specified workspace.

    Parameters
    ----------
    workspace (str):
        The name of the workspace in which to search for the environment.
    environment (str):
        The name of the environment whose ID is to be retrieved.

    Returns
    -------
        str: The ID of the specified environment within the given workspace.

    Raises
    ------
        ValueError: If the specified environment does not exist within the workspace.
    """
    dfE = list_environments(workspace=workspace)
    dfE_filt = dfE[dfE["Environment Name"] == environment]
    if len(dfE_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{environment}' environment does not exist within the '{workspace}' workspace."
        )
    environment_id = dfE_filt["Environment Id"].iloc[0]

    return environment_id


def publish_environment(environment: str, workspace: Optional[str] = None) -> pd.DataFrame:
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

    Returns
    ------
        pd.DataFrame: A DataFrame containing the published details from the API response.
    """

    # Initialize FabricRestClient
    client = fabric.FabricRestClient()

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    environment_id = get_environment_id(workspace, environment)

    # Perform POST request using FabricRestClient
    response = client.post(
        f"/v1/workspaces/{workspace_id}/environments/{environment_id}/staging/publish")

    lro(client, response)

    response_data = response.json()

    # Convert the 'publishDetails' section into a DataFrame for structured processing
    if 'publishDetails' in response_data:
        publish_details_df = pd.json_normalize(response_data['publishDetails'])
    else:
        # Return an empty DataFrame with appropriate columns if 'publishDetails' is missing
        publish_details_df = pd.DataFrame(columns=['state', 'targetVersion', 'startTime',
                                                   'componentPublishInfo.sparkLibraries.state',
                                                   'componentPublishInfo.sparkSettings.state'])

    return publish_details_df
