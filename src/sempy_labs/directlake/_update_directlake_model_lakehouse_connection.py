import sempy
import sempy.fabric as fabric
from sempy_labs.directlake._get_shared_expression import get_shared_expression
from sempy_labs._helper_functions import (
    resolve_lakehouse_name,
    resolve_workspace_name_and_id,
)
from sempy_labs._tom import connect_semantic_model
from typing import List, Optional, Union


def update_direct_lake_model_lakehouse_connection(
    dataset: str,
    workspace: Optional[str] = None,
    lakehouse: Optional[str] = None,
    lakehouse_workspace: Optional[str] = None,
):
    """
    Remaps a Direct Lake semantic model's SQL Endpoint connection to a new lakehouse.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    workspace : str, default=None
        The Fabric workspace name in which the semantic model exists.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    lakehouse : str, default=None
        The Fabric lakehouse used by the Direct Lake semantic model.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    lakehouse_workspace : str, default=None
        The Fabric workspace used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------

    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    if lakehouse_workspace == None:
        lakehouse_workspace = workspace

    if lakehouse == None:
        lakehouse_id = fabric.get_lakehouse_id()
        lakehouse = resolve_lakehouse_name(lakehouse_id, lakehouse_workspace)

    # Check if lakehouse is valid
    dfI = fabric.list_items(workspace=lakehouse_workspace, type="Lakehouse")
    dfI_filt = dfI[(dfI["Display Name"] == lakehouse)]

    if len(dfI_filt) == 0:
        print(
            f"The '{lakehouse}' lakehouse does not exist within the '{lakehouse_workspace}' workspace. Therefore it cannot be used to support the '{dataset}' semantic model within the '{workspace}' workspace."
        )

    dfP = fabric.list_partitions(dataset=dataset, workspace=workspace)
    dfP_filt = dfP[dfP["Mode"] == "DirectLake"]

    if len(dfP_filt) == 0:
        print(
            f"The '{dataset}' semantic model is not in Direct Lake. This function is only applicable to Direct Lake semantic models."
        )
    else:
        with connect_semantic_model(
            dataset=dataset, readonly=False, workspace=workspace
        ) as tom:

            shEx = get_shared_expression(lakehouse, lakehouse_workspace)
            try:
                tom.model.Expressions["DatabaseQuery"].Expression = shEx
                print(
                    f"The expression in the '{dataset}' semantic model has been updated to point to the '{lakehouse}' lakehouse in the '{lakehouse_workspace}' workspace."
                )
            except:
                print(
                    f"ERROR: The expression in the '{dataset}' semantic model was not updated."
                )
