import sempy.fabric as fabric
from sempy_labs._helper_functions import (
    resolve_lakehouse_id,
    resolve_lakehouse_name,
    get_direct_lake_sql_endpoint,
)
from typing import Optional, Tuple
from uuid import UUID


def get_direct_lake_lakehouse(
    dataset: str,
    workspace: Optional[str] = None,
    lakehouse: Optional[str] = None,
    lakehouse_workspace: Optional[str] = None,
) -> Tuple[str, UUID]:
    """
    Identifies the lakehouse used by a Direct Lake semantic model.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    workspace : str, default=None
        The Fabric workspace name.
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
    str, uuid.UUID
        The lakehouse name and lakehouse ID.
    """

    workspace = fabric.resolve_workspace_name(workspace)

    if lakehouse_workspace is None:
        lakehouse_workspace = workspace

    if lakehouse is None:
        lakehouse_id = fabric.get_lakehouse_id()
        lakehouse = resolve_lakehouse_name(lakehouse_id, lakehouse_workspace)

    # dfP = fabric.list_partitions(dataset=dataset, workspace=workspace)
    # dfP_filt = dfP[dfP["Mode"] == "DirectLake"]

    # if len(dfP_filt) == 0:
    #    raise ValueError(
    #        f"{icons.red_dot} The '{dataset}' semantic model within the '{workspace}' workspace is not in Direct Lake mode."
    #    )

    sqlEndpointId = get_direct_lake_sql_endpoint(dataset, workspace)

    dfI = fabric.list_items(workspace=lakehouse_workspace, type="SQLEndpoint")
    dfI_filt = dfI[dfI["Id"] == sqlEndpointId]
    lakehouseName = dfI_filt["Display Name"].iloc[0]

    lakehouseId = resolve_lakehouse_id(lakehouseName, lakehouse_workspace)

    return lakehouseName, lakehouseId
