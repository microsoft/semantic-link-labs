import sempy
import sempy.fabric as fabric
from sempy_labs._helper_functions import resolve_dataset_id
from typing import List, Optional, Union
import sempy_labs._icons as icons


def clear_cache(dataset: str, workspace: Optional[str] = None):
    """
    Clears the cache of a semantic model.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    datasetID = resolve_dataset_id(dataset=dataset, workspace=workspace)

    xmla = f"""
            <ClearCache xmlns="http://schemas.microsoft.com/analysisservices/2003/engine">  
                <Object>  
                    <DatabaseID>{datasetID}</DatabaseID>  
                </Object>  
            </ClearCache>
            """
    fabric.execute_xmla(dataset=dataset, xmla_command=xmla, workspace=workspace)

    outputtext = f"{icons.green_dot} Cache cleared for the '{dataset}' semantic model within the '{workspace}' workspace."

    return outputtext
