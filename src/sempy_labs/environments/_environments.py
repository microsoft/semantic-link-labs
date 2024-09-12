import requests
import json
import sempy.fabric as fabric
from sempy.fabric.exceptions import FabricHTTPException
import sempy_labs._icons as icons

def list_environments(workspace_name: str) -> dict:
    """
        Retrieves the list of environments for a given workspace using the Fabric API.

        This function sends a GET request to the Fabric API to fetch all environments
        within the specified workspace. It returns a dictionary where each environments's
        display name is the key and the corresponding environments ID is the value.

        Args:
            workspace_name (str): The name of the workspace to retrieve environments from.

        Returns:
            dict: A dictionary with environments display names as keys and environments IDs as values.

        Example:
            get_environments("Some Workspace")

        Docs: https://learn.microsoft.com/en-us/rest/api/fabric/environment/items/list-environments?tabs=HTTP
    """

    workspace_id = fabric.resolve_workspace_id(workspace_name)
    client = fabric.FabricRestClient()

    try:
        response = client.get(f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/environments")

        if response.status_code != 200:
            raise FabricHTTPException(response)
    except FabricHTTPException as e:
        raise e
    except Exception as e:
        raise ValueError(
            f"{icons.red_dot} Failed to list environments for the '{workspace_name}' workspace."
        ) from e

    env_json = json.loads(response.content)

    environments_dict = {workspace['displayName']: workspace['id'] for workspace in env_json.get('value', [])}

    return environments_dict