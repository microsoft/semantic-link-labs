from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_dataset_name_and_id,
    get_model_id,
    _base_api,
)
from typing import Optional
import sempy_labs._icons as icons
from sempy._utils._log import log
from uuid import UUID


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

    id = get_model_id(item_id=dataset_id)

    payload = {"directLakeAutoSync": enable}
    _base_api(
        request=f"metadata/models/{id}/settings",
        client="internal",
        method="post",
        payload=payload,
        status_codes=204,
    )

    print(
        f"{icons.green_dot} Direct Lake AutoSync has been {'enabled' if enable else 'disabled'} for the '{dataset_name}' semantic model within the '{workspace_name}' workspace."
    )
