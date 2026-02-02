import requests
from uuid import UUID
from typing import Optional
from sempy_labs._helper_functions import (
    get_pbi_token_headers,
    _get_url_prefix,
    get_model_id,
    resolve_item_name_and_id,
    resolve_workspace_name_and_id,
)
from sempy.fabric.exceptions import FabricHTTPException
from sempy._utils._log import log
import sempy_labs._icons as icons


@log
def enable_query_caching(
    dataset: str | UUID, enable: bool = True, workspace: Optional[str | UUID] = None
):
    """
    Enables or disables `query caching <http://aka.ms/queryCaching>_` for a semantic model.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    enable : bool, default=True
        Set to True to enable query caching, or False to disable it.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """
    prefix = _get_url_prefix()
    headers = get_pbi_token_headers()
    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (item_name, item_id) = resolve_item_name_and_id(
        item=dataset, type="SemanticModel", workspace=workspace_id
    )
    model_id = get_model_id(item_id=item_id, headers=headers, prefix=prefix)

    caching_map = {
        True: 2,
        False: 1,
    }

    payload = {"queryCachingState": caching_map.get(enable)}

    response = requests.post(
        f"{prefix}/metadata/models/{model_id}/caching", headers=headers, json=payload
    )

    if response.status_code != 204:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} Query caching has been {'enabled' if enable else 'disabled'} for the '{item_name}' semantic model within the '{workspace_name}' workspace."
    )
