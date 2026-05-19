from typing import Optional
from uuid import UUID
from sempy._utils._log import log
from sempy_labs._helper_functions import (
    resolve_workspace_id,
    resolve_workspace_name_and_id,
    _base_api,
)
import sempy_labs._icons as icons


@log
def export_onelake_lifecycle_policy(
    workspace: Optional[str | UUID] = None,
) -> dict:
    """
    Exports the OneLake lifecycle management policy for a workspace.

    This is a wrapper function for the following API: `OneLake Lifecycle Policy - Export Policy <https://learn.microsoft.com/rest/api/fabric/core/onelake-lifecycle-policy/export-policy>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    dict
        The OneLake workspace lifecycle policy, containing the policy ``properties``
        (including ``lastModifiedTime`` and ``policy`` rules) and the ARM resource ``type``.
    """

    workspace_id = resolve_workspace_id(workspace)

    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/onelake/lifecycle/exportPolicy",
        method="post",
    ).json()

    return response


@log
def import_onelake_lifecycle_policy(
    policy: dict,
    workspace: Optional[str | UUID] = None,
) -> dict:
    """
    Imports (creates or replaces) the OneLake lifecycle management policy for a workspace.

    The policy must be sent in full as a complete replacement. To delete the existing policy,
    send a request with an empty ``rules`` array.

    This is a wrapper function for the following API: `OneLake Lifecycle Policy - Import Policy <https://learn.microsoft.com/rest/api/fabric/core/onelake-lifecycle-policy/import-policy>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    policy : dict
        The lifecycle policy request body. May be specified either as the full request body
        (``{"properties": {"policy": {"rules": [...]}}}``) or as just the policy contents
        (``{"policy": {"rules": [...]}}`` or ``{"rules": [...]}``). The structure follows
        `Azure Storage lifecycle management <https://learn.microsoft.com/azure/storage/blobs/lifecycle-management-overview>`_.
        Note that the ``tierToArchive`` action, ``delete`` action and ``blobIndexMatch`` filter are not supported.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    dict
        The OneLake workspace lifecycle policy response, containing the policy ``properties``
        and the ARM resource ``type``.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    if not isinstance(policy, dict):
        raise ValueError(
            f"{icons.red_dot} The 'policy' parameter must be a dictionary."
        )

    # Normalize the payload to the expected request body shape:
    # {"properties": {"policy": {"rules": [...]}}}
    if "properties" in policy:
        payload = policy
    elif "policy" in policy:
        payload = {"properties": policy}
    elif "rules" in policy:
        payload = {"properties": {"policy": policy}}
    else:
        raise ValueError(
            f"{icons.red_dot} The 'policy' parameter must contain a 'properties', 'policy', or 'rules' key."
        )

    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/onelake/lifecycle/importPolicy",
        method="post",
        payload=payload,
    ).json()

    print(
        f"{icons.green_dot} The OneLake lifecycle policy has been imported for the '{workspace}' workspace."
    )

    return response


@log
def delete_onelake_lifecycle_policy(
    workspace: Optional[str | UUID] = None,
):
    """
    Deletes the OneLake lifecycle management policy for a workspace.

    This sends an empty ``rules`` array to the import policy API, which removes the existing policy.

    This is a wrapper function for the following API: `OneLake Lifecycle Policy - Import Policy <https://learn.microsoft.com/rest/api/fabric/core/onelake-lifecycle-policy/import-policy>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    workspace_id = resolve_workspace_id(workspace)

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/onelake/lifecycle/importPolicy",
        method="post",
        payload={"properties": {"policy": {"rules": []}}},
    )

    print(
        f"{icons.green_dot} The OneLake lifecycle policy has been deleted for the '{workspace}' workspace."
    )
