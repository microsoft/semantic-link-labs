import unittest
from unittest.mock import patch, MagicMock
from sempy_labs.environments import list_environments
from sempy.fabric.exceptions import FabricHTTPException
import json

class TestListEnvironments(unittest.TestCase):
    @patch('sempy.fabric.FabricRestClient')
    @patch('sempy.fabric.resolve_workspace_id')
    def test_list_environments_success(self, mock_resolve_workspace_id, mock_fabric_client):
        """
        Test successful environment listing from the Fabric API.
        """
        # Mock the workspace_id to be returned by resolve_workspace_id
        mock_resolve_workspace_id.return_value = 'workspace-id-123'

        # Mock a successful response from the Fabric API
        mock_client_instance = MagicMock()
        mock_fabric_client.return_value = mock_client_instance
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = json.dumps({
            "value": [
                {"displayName": "Environment 1", "id": "env-1-id"},
                {"displayName": "Environment 2", "id": "env-2-id"}
            ]
        })
        mock_client_instance.get.return_value = mock_response

        # Call the function and assert the result
        result = list_environments("test_workspace")
        expected_result = {
            "Environment 1": "env-1-id",
            "Environment 2": "env-2-id"
        }
        self.assertEqual(result, expected_result)

        # Assert that resolve_workspace_id and the Fabric client were called
        mock_resolve_workspace_id.assert_called_once_with("test_workspace")
        mock_client_instance.get.assert_called_once_with(
            "https://api.fabric.microsoft.com/v1/workspaces/workspace-id-123/environments"
        )

    @patch('sempy.fabric.FabricRestClient')
    @patch('sempy.fabric.resolve_workspace_id')
    def test_list_environments_non_200_response(self, mock_resolve_workspace_id, mock_fabric_client):
        """
        Test handling of a non-200 status code from the Fabric API.
        """
        mock_resolve_workspace_id.return_value = 'workspace-id-123'

        # Mock a non-200 response
        mock_client_instance = MagicMock()
        mock_fabric_client.return_value = mock_client_instance
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_client_instance.get.return_value = mock_response

        with self.assertRaises(FabricHTTPException) as context:
            list_environments("test_workspace")

    @patch('sempy.fabric.FabricRestClient')
    @patch('sempy.fabric.resolve_workspace_id')
    def test_list_environments_exception_handling(self, mock_resolve_workspace_id, mock_fabric_client):
        """
        Test exception handling when an error occurs during the API request.
        """
        mock_resolve_workspace_id.return_value = 'workspace-id-123'

        # Mock an exception being raised
        mock_client_instance = MagicMock()
        mock_fabric_client.return_value = mock_client_instance
        mock_client_instance.get.side_effect = Exception("Something went wrong")

        with self.assertRaises(ValueError) as context:
            list_environments("test_workspace")

        # Assert that the raised ValueError contains the correct message
        self.assertIn("Failed to list environments", str(context.exception))


if __name__ == '__main__':
    unittest.main()
