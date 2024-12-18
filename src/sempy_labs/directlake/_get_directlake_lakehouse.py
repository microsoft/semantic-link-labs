import sempy.fabric as fabric
from sempy_labs._helper_functions import (
    resolve_lakehouse_id,
    resolve_lakehouse_name,
    get_direct_lake_sql_endpoint,
    resolve_workspace_name_and_id,
    resolve_dataset_name_and_id,
)
from typing import Optional, Tuple
from uuid import UUID


def get_direct_lake_lakehouse(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    lakehouse: Optional[str] = None,
    lakehouse_workspace: Optional[str | UUID] = None,
) -> Tuple[str, UUID]:
    """
    Identifies the lakehouse used by a Direct Lake semantic model.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    lakehouse : str, default=None
        The Fabric lakehouse used by the Direct Lake semantic model.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    lakehouse_workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    Tuple[str, uuid.UUID]
        The lakehouse name and lakehouse ID.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    if lakehouse_workspace is None:
        lakehouse_workspace = workspace_name

    if lakehouse is None:
        lakehouse_id = fabric.get_lakehouse_id()
        lakehouse = resolve_lakehouse_name(lakehouse_id, lakehouse_workspace)

    # dfP = fabric.list_partitions(dataset=dataset, workspace=workspace)
    # dfP_filt = dfP[dfP["Mode"] == "DirectLake"]

    # if len(dfP_filt) == 0:
    #    raise ValueError(
    #        f"{icons.red_dot} The '{dataset}' semantic model within the '{workspace}' workspace is not in Direct Lake mode."
    #    )

    sqlEndpointId = get_direct_lake_sql_endpoint(dataset_id, workspace_id)

    dfI = fabric.list_items(workspace=lakehouse_workspace, type="SQLEndpoint")
    dfI_filt = dfI[dfI["Id"] == sqlEndpointId]
    lakehouseName = dfI_filt["Display Name"].iloc[0]

    lakehouseId = resolve_lakehouse_id(lakehouseName, lakehouse_workspace)

    return lakehouseName, lakehouseId
