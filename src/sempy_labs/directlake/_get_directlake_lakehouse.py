import sempy.fabric as fabric
from .._helper_functions import (
    resolve_lakehouse_id,
)
from typing import Optional, Tuple
from uuid import UUID
import sempy_labs._icons as icons
from sempy._utils._log import log


@log
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

    from sempy_labs.directlake._dl_helper import get_direct_lake_source

    artifact_type, artifact_name, artifact_id, workspace_id = get_direct_lake_source(
        dataset=dataset, workspace=workspace
    )

    if artifact_type in ["Lakehouse", "Warehouse"]:
        return artifact_name, artifact_id
    else:
        fabric.refresh_tom_cache(workspace=workspace)
        dfP = fabric.list_partitions(dataset=dataset, workspace=workspace)
        dfP_filt = dfP[dfP["Mode"] == "DirectLake"]
        if dfP_filt.empty:
            raise ValueError(
                f"{icons.red_dot} The '{dataset}' semantic model within the '{workspace}' workspace is not in Direct Lake mode."
            )
        lakehouse_id = resolve_lakehouse_id(
            lakehouse=lakehouse, workspace=lakehouse_workspace
        )
        return lakehouse, lakehouse_id
