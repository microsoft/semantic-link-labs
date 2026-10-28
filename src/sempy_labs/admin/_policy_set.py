from typing import Optional, Tuple
from uuid import UUID
import sempy_labs._icons as icons
from sempy_labs._helper_functions import _base_api, _is_valid_uuid
from sempy_labs.admin._basic_functions import _resolve_workspace_name_and_id
from sempy_labs.admin._items import _resolve_item_name_and_id
from sempy._utils._log import log


def _resolve_policy_set(policy_set: str | UUID, workspace_id: UUID) -> Tuple[str, UUID]:
    # An ID is used as-is so it does not depend on the admin item listing.
    if _is_valid_uuid(policy_set):
        return str(policy_set), policy_set
    return _resolve_item_name_and_id(
        item=policy_set, type="PolicySet", workspace=workspace_id
    )


@log
def activate_policy_set(
    policy_set: str | UUID,
    workspace: Optional[str | UUID] = None,
    allow_replace: bool = False,
):
    """
    Activates a policy set on the tenant scope.

    Only a single policy set can be active on the tenant scope. The policy set must have a 'Tenant' scope.

    This is a wrapper function for the following API: `Policy Sets - Activate <https://learn.microsoft.com/rest/api/fabric/admin/policy-sets/activate>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    policy_set : str | uuid.UUID
        The name or ID of the policy set.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID in which the policy set resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    allow_replace : bool, default=False
        If True, replaces the policy set which is currently active on the tenant scope (if any).
        If False, the activation fails if another policy set is already active on the tenant scope.
    """

    workspace_name, workspace_id = _resolve_workspace_name_and_id(workspace)
    policy_set_name, policy_set_id = _resolve_policy_set(policy_set, workspace_id)

    _base_api(
        request=f"/v1/admin/workspaces/{workspace_id}/policySets/{policy_set_id}/activate?allowReplace={str(allow_replace).lower()}",
        method="post",
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The '{policy_set_name}' policy set within the '{workspace_name}' workspace has been activated on the tenant scope."
    )


@log
def deactivate_policy_set(
    policy_set: str | UUID,
    workspace: Optional[str | UUID] = None,
):
    """
    Deactivates a policy set from the tenant scope.

    This is a wrapper function for the following API: `Policy Sets - Deactivate <https://learn.microsoft.com/rest/api/fabric/admin/policy-sets/deactivate>`_.

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

    workspace_name, workspace_id = _resolve_workspace_name_and_id(workspace)
    policy_set_name, policy_set_id = _resolve_policy_set(policy_set, workspace_id)

    _base_api(
        request=f"/v1/admin/workspaces/{workspace_id}/policySets/{policy_set_id}/deactivate",
        method="post",
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The '{policy_set_name}' policy set within the '{workspace_name}' workspace has been deactivated from the tenant scope."
    )


@log
def get_active_policy_set() -> dict:
    """
    Returns the active policy set on the tenant scope, including its policy rules.

    This is a wrapper function for the following API: `Policy Sets - Get Active (beta) <https://learn.microsoft.com/rest/api/fabric/admin/policy-sets/get-active(beta)>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Returns
    -------
    dict
        The active policy set, containing the 'id', 'displayName', 'description', 'workspaceId' and 'policyRules' keys.
    """

    response = _base_api(
        request="/v1/admin/policySets/active?beta=true",
        client="fabric_sp",
    )

    return response.json()
