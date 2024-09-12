import unittest
from unittest.mock import patch, MagicMock
import json
import sempy_labs._icons as icons
from sempy_labs.environments import create_environment


class TestCreateEnvironment(unittest.TestCase):

    @patch('sempy.fabric.FabricRestClient')
    @patch('sempy.fabric.resolve_workspace_id')
    def test_create_environment_success_with_description(self, mock_resolve_workspace_id, mock_fabric_client):
        """
        Test case for successfully creating an environment with a description.
        """
        # Mocking the workspace ID resolution
        mock_resolve_workspace_id.return_value = "test_workspace_id"

        # Mocking the FabricRestClient response
        mock_client_instance = MagicMock()
        mock_fabric_client.return_value = mock_client_instance
        mock_client_instance.post.return_value.status_code = 201

        # Call the function with description
        with patch('builtins.print') as mock_print:
            create_environment("Test Workspace", "Test Environment", "This is a test description")
            mock_print.assert_any_call(f"{icons.green_dot} The environment 'Test Environment' was created in the 'Test Workspace' workspace")

        # Assertions
        mock_resolve_workspace_id.assert_called_once_with("Test Workspace")
        mock_client_instance.post.assert_called_once_with(
            "https://api.fabric.microsoft.com/v1/workspaces/test_workspace_id/environments/",
            data=json.dumps({
                "displayName": "Test Environment",
                "description": "This is a test description"
            })
        )

    @patch('sempy.fabric.FabricRestClient')
    @patch('sempy.fabric.resolve_workspace_id')
    def test_create_environment_success_without_description(self, mock_resolve_workspace_id, mock_fabric_client):
        """
        Test case for successfully creating an environment without a description.
        """
        # Mocking the workspace ID resolution
        mock_resolve_workspace_id.return_value = "test_workspace_id"

        # Mocking the FabricRestClient response
        mock_client_instance = MagicMock()
        mock_fabric_client.return_value = mock_client_instance
        mock_client_instance.post.return_value.status_code = 201

        # Call the function without description
        with patch('builtins.print') as mock_print:
            create_environment("Test Workspace", "Test Environment")
            mock_print.assert_any_call(f"{icons.green_dot} The environment 'Test Environment' was created in the 'Test Workspace' workspace")

        # Assertions
        mock_resolve_workspace_id.assert_called_once_with("Test Workspace")
        mock_client_instance.post.assert_called_once_with(
            "https://api.fabric.microsoft.com/v1/workspaces/test_workspace_id/environments/",
            data=json.dumps({
                "displayName": "Test Environment"
            })
        )

    @patch('sempy.fabric.FabricRestClient')
    @patch('sempy.fabric.resolve_workspace_id')
    def test_create_environment_failure(self, mock_resolve_workspace_id, mock_fabric_client):
        """
        Test case for failing to create an environment with a non-201 status code.
        """
        # Mocking the workspace ID resolution
        mock_resolve_workspace_id.return_value = "test_workspace_id"

        # Mocking the FabricRestClient response
        mock_client_instance = MagicMock()
        mock_fabric_client.return_value = mock_client_instance
        mock_client_instance.post.return_value.status_code = 400

        # Capture printed output
        with patch('builtins.print') as mock_print:
            create_environment("Test Workspace", "Test Environment")
            mock_print.assert_any_call(400)

        mock_resolve_workspace_id.assert_called_once_with("Test Workspace")
        mock_client_instance.post.assert_called_once_with(
            "https://api.fabric.microsoft.com/v1/workspaces/test_workspace_id/environments/",
            data=json.dumps({"displayName": "Test Environment"})
        )

if __name__ == '__main__':
    unittest.main()
