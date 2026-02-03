from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_item_name_and_id,
    _get_url_prefix,
    get_pbi_token_headers,
    get_model_id,
)
from typing import Optional
from uuid import UUID
from sempy._utils._log import log
import sempy_labs._icons as icons
import requests
from sempy.fabric.exceptions import FabricHTTPException


@log
def enable_directlake_autosync(
    dataset: str | UUID,
    enabled: bool,
    workspace: Optional[str | UUID] = None,
):
    """
    Configure Power BI to detect changes to the data in OneLake and automatically update the Direct Lake tables that are included in this semantic model.

    Parameters
    ----------
    dataset : str | uuid.UUID
        The name or UUID of the semantic model.
    enabled : bool
        Set to True to enable Direct Lake autosync, or False to disable it.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    headers = get_pbi_token_headers()
    prefix = _get_url_prefix()
    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (item_name, item_id) = resolve_item_name_and_id(
        item=dataset, type="SemanticModel", workspace=workspace_id
    )
    model_id = get_model_id(item_id=item_id, prefix=prefix, headers=headers)
    payload = {"directLakeAutoSync": enabled}

    response = requests.post(
        url=f"{prefix}/metadata/models/{model_id}/settings",
        json=payload,
        headers=headers,
    )

    if response.status_code != 204:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} Direct Lake autosync has been {'enabled' if enabled else 'disabled'} for the '{item_name}' semantic model within the '{workspace_name}' workspace."
    )
