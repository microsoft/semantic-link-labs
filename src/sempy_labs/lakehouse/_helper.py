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

    local_path = _mount(lakehouse=lakehouse, workspace=workspace)
    table_path = (
        f"{local_path}/Tables/{schema}/{table_name}"
        if schema
        else f"{local_path}/Tables/{table_name}"
    )
    ds_schema = ds.dataset(table_path).schema.metadata

    return any(b"vorder" in key for key in ds_schema.keys())
