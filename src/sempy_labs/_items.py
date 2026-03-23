from typing import Optional, List
from uuid import UUID
import sempy.fabric as fabric
from sempy_labs._helper_functions import (
    resolve_workspace_id,
    _base_api,
)
from sempy._utils._log import log


@log
def bulk_export_items(
    item_id: Optional[UUID | List[UUID]] = None,
    item_type: Optional[str | List[str]] = None,
    workspace: Optional[str | UUID] = None,
):

    workspace_id = resolve_workspace_id(workspace)

    if item_id is None and item_type is None:
        payload = {"mode": "All"}
    else:

        df = fabric.list_items(workspace=workspace_id)
        if item_id is not None:
            if isinstance(item_id, str):
                item_id = [item_id]

            df = df[df["Id"].isin(item_id)]

        if item_type is not None:
            if isinstance(item_type, str):
                item_type = [item_type]

            df = df[df["Type"].isin(item_type)]

        ids = df["Id"].unique().tolist()

        id_payload = [{"id": i} for i in ids]

        payload = {
            "items": id_payload,
            "mode": "Selective",
        }

    result = _base_api(
        request=f"/v1/workspaces/{workspace_id}/items/bulkExportDefinitions?beta=True",
        payload=payload,
        status_codes=[200, 202],
        lro_return_json=True,
    )
    return result
