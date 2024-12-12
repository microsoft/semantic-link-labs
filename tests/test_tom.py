import sempy.fabric
from unittest.mock import patch
from sempy_labs.tom import connect_semantic_model


@patch("sempy.fabric.resolve_item_id")
@patch("sempy.fabric.resolve_workspace_id")
@patch("sempy_labs._helper_functions.resolve_dataset_name_and_id")
@patch("sempy_labs._helper_functions.resolve_workspace_name_and_id")
@patch("sempy.fabric.create_tom_server")
def test_tom_wrapper(create_tom_server, resolve_workspace_name_and_id, resolve_dataset_name_and_id, resolve_workspace_id, resolve_item_id):

    sempy.fabric._client._utils._init_analysis_services()
    import Microsoft.AnalysisServices.Tabular as TOM

    resolve_workspace_name_and_id.return_value = ("my_workspace", "my_workspace_id")
    resolve_dataset_name_and_id.return_value = ("my_dataset", "my_dataset_id")
    resolve_workspace_id.return_value = "my_workspace_id"
    resolve_item_id.return_value = "my_dataset_id"

    # create dummy server, database and model
    tom_server = TOM.Server()

    db = TOM.Database()
    db.Name = "my_dataset"
    db.ID = "my_dataset_id"
    db.Model = TOM.Model()
    tom_server.Databases.Add(db)

    create_tom_server.return_value = tom_server

    # invoke the wrapper
    with connect_semantic_model(dataset="my_dataset_id", workspace="my_workspace") as tom:
        tom.add_table("my_table")

    # validate the result
    assert tom_server.Databases["my_dataset_id"].Model.Tables["my_table"] is not None
