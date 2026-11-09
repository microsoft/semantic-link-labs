from typing import Optional
from uuid import UUID
from sempy_labs._helper_functions import (
    _base_api,
    resolve_item_name_and_id,
    resolve_workspace_name_and_id,
)
import sempy_labs._icons as icons
from sempy._utils._log import log


def _set_data_collection(
    monitoring: str | UUID, enable: bool, workspace: Optional[str | UUID] = None
):

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    item_name, item_id = resolve_item_name_and_id(
        item=monitoring, type="Monitoring", workspace=workspace_id
    )
    action = "enable" if enable else "disable"

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/monitoringItems/{item_id}/{action}DataCollection",
        method="post",
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} Data collection has been {action}d for the '{item_name}' Monitoring item within the '{workspace_name}' workspace."
    )


@log
def enable_data_collection(
    monitoring: str | UUID, workspace: Optional[str | UUID] = None
):
    """
    Enables data collection for the specified Monitoring item, using the data collection settings
    already configured in the item's definition.

    Only one Monitoring item per workspace can collect data at a time. If another Monitoring item in the
    workspace already has data collection enabled, disable it first.

    This is a wrapper function for the following API: `Data Collection - Enable <https://learn.microsoft.com/rest/api/fabric/monitoring/data-collection/enable>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    monitoring : str | uuid.UUID
        Name or ID of the Monitoring item.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    _set_data_collection(monitoring=monitoring, enable=True, workspace=workspace)


@log
def disable_data_collection(
    monitoring: str | UUID, workspace: Optional[str | UUID] = None
):
    """
    Disables data collection for the specified Monitoring item. The item's data collection settings
    definition is left unchanged.

    This is a wrapper function for the following API: `Data Collection - Disable <https://learn.microsoft.com/rest/api/fabric/monitoring/data-collection/disable>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    monitoring : str | uuid.UUID
        Name or ID of the Monitoring item.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    _set_data_collection(monitoring=monitoring, enable=False, workspace=workspace)
