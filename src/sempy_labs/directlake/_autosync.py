import requests
from sempy_labs._helper_functions import (
    _get_url_prefix,
    resolve_workspace_name_and_id,
    resolve_dataset_name_and_id,
)
from typing import Optional
import sempy_labs._icons as icons
from sempy._utils._log import log
from uuid import UUID
from sempy.fabric.exceptions import FabricHTTPException


@log
def set_autosync(
    dataset: str | UUID, workspace: Optional[str | UUID] = None, enable: bool = True
):
    """
    Enables or disables AutoSync for a Direct Lake semantic model.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    enable : bool, default=True
        Whether to enable (True) or disable (False) AutoSync.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    import notebookutils

    token = notebookutils.credentials.getToken("pbi")
    headers = {"Authorization": f"Bearer {token}"}

    prefix = _get_url_prefix()

    response = requests.get(
        url=f"{prefix}/metadata/models/{dataset_id}", headers=headers
    )
    id = response.json().get("model", {}).get("id")

    payload = {"directLakeAutoSync": enable}
    response = requests.post(
        url=f"{prefix}/metadata/models/{id}/settings", headers=headers, json=payload
    )

    if response.status_code != 204:
        raise FabricHTTPException(f"Failed to retrieve labels: {response.text}")

    print(
        f"{icons.green_dot} Direct Lake AutoSync has been {'enabled' if enable else 'disabled'} for the '{dataset_name}' semantic model within the '{workspace_name}' workspace."
    )
