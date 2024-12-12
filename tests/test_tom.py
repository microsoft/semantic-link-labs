import sempy.fabric
from unittest.mock import patch
from sempy_labs.tom import connect_semantic_model
from sempy_labs._helper_functions import resolve_workspace_name_and_id


@patch("sempy.fabric.create_tom_server")
def test_tom_wrapper(create_tom_server):

    sempy.fabric._client._utils._init_analysis_services()
    import Microsoft.AnalysisServices.Tabular as TOM

    # create dummy server, database and model
    tom_server = TOM.Server()

    db = TOM.Database()
    db.Name = "my_dataset"
    db.ID = "my_dataset" 
    db.Model = TOM.Model()
    tom_server.Databases.Add(db)

    create_tom_server.return_value = tom_server

    resolve_workspace_name_and_id.return_value = ("my_workspace", "my_workspace_id")

    # invoke the wrapper
    with connect_semantic_model("my_dataset") as tom:
        tom.add_table("my_table")

    # validate the result
    assert tom_server.Databases["my_dataset"].Model.Tables["my_table"] is not None
