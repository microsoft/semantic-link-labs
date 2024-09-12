
import sempy.fabric as fabric
import json
import sempy_labs._icons as icons
def create_environment(workspace_name: str, environment_name: str, environment_description: str = None):
    """
    Creates a new environment in the specified workspace using the Fabric API.

    This function sends a POST request to the Fabric API to create a new environment
    within a specified workspace. The environment's name is provided, and an optional
    description of the environment can be added.

    Args:
        workspace_name (str): The name of the workspace in which the environment is created.
        environment_name (str): The display name of the new environment.
        environment_description (str, optional): An optional description for the environment. Defaults to None.

    Example:
        create_environment("My Workspace", "My Environment", "Test environment")

    Docs: https://learn.microsoft.com/en-us/rest/api/fabric/environment/items/create-environment?tabs=HTTP
    """

    # Resolve the workspace ID based on the workspace name
    workspace_id = fabric.resolve_workspace_id(workspace_name)
    client = fabric.FabricRestClient()

    # Prepare the configuration for the new environment
    environment_conf = {
        "displayName": environment_name
    }

    if environment_description:
        environment_conf = environment_conf | {"description": environment_description}

    try:
        # Send a POST request to the Fabric API to create the environmen
        response = client.post(f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/environments/",
                            data=json.dumps(environment_conf))

        if response.status_code == 201:
            print(f"{icons.green_dot} The environment '{environment_name}' was created in the '{workspace_name}' workspace")
        else:
            print(response.status_code)
    except Exception as e:
        raise ValueError(
            f"{icons.red_dot} Failed to create an environment for the '{workspace_name}' workspace."
        ) from e


