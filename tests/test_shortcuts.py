import pandas as pd
from json import loads
from sempy_labs.lakehouse._shortcuts import create_shortcut_onelake
from unittest.mock import MagicMock, PropertyMock, patch


@patch("sempy.fabric.list_items")
@patch("sempy.fabric.resolve_workspace_id")
@patch("sempy.fabric.resolve_item_id")
@patch("sempy.fabric.FabricRestClient")
def test_create_shortcut_onelake(fabric_rest_client_mock, resolve_item_id_mock, resolve_workspace_id_mock, list_items_mock):
    # prepare mocks
    def resolve_workspace_id_mock_side_effect(workspace_name):
        if workspace_name == "source_workspace":
            return "00000000-0000-0000-0000-000000000001"

        if workspace_name == "destination_workspace":
            return "00000000-0000-0000-0000-000000000002"
        
        assert False, f"Unexpected workspace: {workspace_name}"

    resolve_workspace_id_mock.side_effect = resolve_workspace_id_mock_side_effect

    resolve_item_id_mock.return_value = "00000000-0000-0000-0000-00000000000A"

    def list_items_side_effect(type, workspace):
        assert type == "Lakehouse"

        if workspace == "source_workspace":
            return pd.DataFrame([{
                "Display Name": "source_lakehouse",
                "Id": "10000000-0000-0000-0000-000000000001"
            }])

        if workspace == "destination_workspace":
            return pd.DataFrame([{
                "Display Name": "destination_lakehouse",
                "Id": "20000000-0000-0000-0000-000000000002"
            }])
        
        assert False, f"Unexpected workspace: {workspace}"

    list_items_mock.side_effect = list_items_side_effect

    def post_side_effect(url, json):
        # TODO: we could validate the URL and JSON?

        response = MagicMock()
        type(response).status_code = PropertyMock(return_value=201)

        return response

    fabric_rest_client_mock.return_value.post.side_effect = post_side_effect

    # execute
    create_shortcut_onelake("table_name", "source_lakehouse", "source_workspace", "destination_lakehouse", "destination_workspace", "shortcut_name")
