import sempy
import sempy.fabric as fabric
import pandas as pd
from .HelperFunctions import resolve_dataset_id
from typing import List, Optional, Union



def run_dax(dataset: str, dax_query: str, user_name: Optional[str] = None, workspace: Optional[str] = None):

    """
    Runs a DAX query against a semantic model.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    dax_query : str
        The DAX query.
    user_name : str | None
        The user name (i.e. hello@goodbye.com).
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe holding the result of the DAX query.
    """

    #https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/execute-queries-in-group

    if workspace is None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

    dataset_id = resolve_dataset_id(dataset = dataset, workspace = workspace)    

    if user_name is None:
        request_body = {
    "queries": [
        {
        "query": dax_query
        }
    ]
    }
    else:
        request_body = {
    "queries": [
        {
        "query": dax_query
        }
    ],
    "impersonatedUserName": user_name
    }

    client = fabric.PowerBIRestClient()
    response = client.post(f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/executeQueries", json = request_body)
    data = response.json()['results'][0]['tables']
    column_names = data[0]['rows'][0].keys()
    data_rows = [row.values() for item in data for row in item['rows']]
    df = pd.DataFrame(data_rows, columns=column_names)
    
    return df