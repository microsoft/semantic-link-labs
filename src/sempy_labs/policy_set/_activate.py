from sempy_labs._helper_functions import (
    resolve_workspace_id,
    resolve_item_name_and_id,
    resolve_capacity_id,
    _base_api,
)
import sempy_labs._icons as icons
from uuid import UUID
from sempy._utils._log import log
from typing import Optional


@log
def activate_policy_set(
    policy_set: str | UUID,
    capacity: str | UUID,
    allow_replace: bool = False,
    workspace: Optional[str | UUID] = None,
):
    """
    Activates a policy set on the specified capacity.

    Only a single policy set can be active on a given capacity. The caller must be an admin of the capacity.

    This is a wrapper function for the following API: `Activation - Activate <https://learn.microsoft.com/rest/api/fabric/policyset/activation/activate>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    policy_set : str | uuid.UUID
        The name or ID of the policy set.
    capacity : str | uuid.UUID
        The name or ID of the capacity on which to activate the policy set.
    allow_replace : bool, default=False
        If True, replaces the policy set which is currently active on the capacity (if any).
        If False, the activation fails if another policy set is already active on the capacity.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID in which the policy set resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    workspace_id = resolve_workspace_id(workspace)
    policy_set_name, policy_set_id = resolve_item_name_and_id(
        item=policy_set, type="PolicySet", workspace=workspace_id
    )
    capacity_id = resolve_capacity_id(capacity)

    payload = {
        "scopeType": "Capacity",
        "capacityId": str(capacity_id),
    }

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/policySets/{policy_set_id}/activate?allowReplace={str(allow_replace).lower()}",
        method="post",
        payload=payload,
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The '{policy_set_name}' policy set has been activated on the '{capacity}' capacity."
    )


@log
def deactivate_policy_set(
    policy_set: str | UUID,
    workspace: Optional[str | UUID] = None,
):
    """
    Deactivates a policy set from its capacity scope.

    The caller must be an admin of the capacity on which the policy set is active.

    This is a wrapper function for the following API: `Activation - Deactivate <https://learn.microsoft.com/rest/api/fabric/policyset/activation/deactivate>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    policy_set : str | uuid.UUID
        The name or ID of the policy set.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID in which the policy set resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    workspace_id = resolve_workspace_id(workspace)
    policy_set_name, policy_set_id = resolve_item_name_and_id(
        item=policy_set, type="PolicySet", workspace=workspace_id
    )

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/policySets/{policy_set_id}/deactivate",
        method="post",
        payload={"scopeType": "Capacity"},
        client="fabric_sp",
    )

    print(f"{icons.green_dot} The '{policy_set_name}' policy set has been deactivated.")


@log
def get_active_policy_set_for_capacity(
    capacity: Optional[str | UUID] = None,
) -> dict:
    """
    Returns the active policy set on the specified capacity.

    The caller must be an admin of the capacity.

    This is a wrapper function for the following API: `Activation - Get Active For Capacity <https://learn.microsoft.com/rest/api/fabric/policyset/activation/get-active-for-capacity>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    capacity : str | uuid.UUID, default=None
        The name or ID of the capacity.
        Defaults to None which resolves to the capacity of the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the capacity of the workspace of the notebook.

    Returns
    -------
    dict
        The active policy set, containing the 'id', 'displayName', 'description' and 'workspaceId' keys.
    """

    capacity_id = resolve_capacity_id(capacity)

    response = _base_api(
        request=f"/v1/capacities/{capacity_id}/policySets/active",
        client="fabric_sp",
    )

    return response.json()
