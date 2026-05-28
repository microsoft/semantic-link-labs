from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_id,
    _base_api,
    resolve_workspace_name_and_id,
)
from uuid import UUID
from sempy._utils._log import log
import sempy_labs._icons as icons


@log
def get_airflow_workspace_settings(workspace: Optional[str | UUID] = None) -> str:
    """
    Get Apache Airflow workspace settings.

    This is a wrapper function for the following API: `Workspace Settings - GetAirflowWorkspaceSettings <https://learn.microsoft.com/rest/api/fabric/apacheairflowjob/workspace-settings/get-airflow-workspace-settings(beta)>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    str
        The default pool template ID.
    """

    workspace_id = resolve_workspace_id(workspace)

    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/apacheAirflowJobs/settings?beta=True",
        client="fabric_sp",
    )

    return response.json().get("defaultPoolTemplateId", None)


@log
def update_airflow_workspace_settings(
    default_pool_template_id: str | UUID, workspace: Optional[str | UUID] = None
):
    """
    Update Apache Airflow workspace settings.

    This is a wrapper function for the following API: `Workspace Settings - UpdateAirflowWorkspaceSettings <https://learn.microsoft.com/rest/api/fabric/apacheairflowjob/workspace-settings/update-airflow-workspace-settings(beta)>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    default_pool_template_id : str | uuid.UUID
        The default pool template ID to set.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    payload = {"defaultPoolTemplateId": str(default_pool_template_id)}

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/apacheAirflowJobs/settings?beta=True",
        client="fabric_sp",
        method="patch",
        payload=payload,
    )

    print(
        f"{icons.green_dot} The Apache Airflow workspace settings in the '{workspace_name}' workspace have been updated."
    )
