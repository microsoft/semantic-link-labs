from uuid import UUID
from typing import Optional
import pyarrow.dataset as ds
from sempy_labs._helper_functions import (
    _mount,
)
from sempy._utils._log import log


@log
def is_v_ordered(
    table_name: str,
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
    schema: Optional[str] = None,
) -> bool:
    """
    Checks if a delta table is v-ordered.

    Parameters
    ----------
    table_name : str
        The name of the table to check.
    lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    schema : str, optional
        The schema of the table to check. If not provided, the default schema is used.

    Returns
    -------
    bool
        True if the table is v-ordered, False otherwise.
    """

    local_path = _mount(lakehouse=lakehouse, workspace=workspace)
    table_path = (
        f"{local_path}/Tables/{schema}/{table_name}"
        if schema
        else f"{local_path}/Tables/{table_name}"
    )
    ds_schema = ds.dataset(table_path).schema.metadata

    return any(b"vorder" in key for key in ds_schema.keys())
