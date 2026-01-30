import pandas as pd
from typing import Optional
from uuid import UUID
from sempy._utils._log import log
import sempy_labs.mirrored_warehouse as mw


@log
def list_mirrored_warehouses(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows the mirrored warehouses within a workspace.

    This is a wrapper function for the following API: `Items - List Mirrored Warehouses <https://learn.microsoft.com/rest/api/fabric/mirroredwarehouse/items/list-mirrored-warehouses>`_.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the mirrored warehouses within a workspace.
    """

    return mw.list_mirrored_warehouses(workspace=workspace)
