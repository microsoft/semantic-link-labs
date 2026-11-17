from typing import Optional
from sempy_labs._helper_functions import (
    resolve_item_name_and_id,
    resolve_workspace_id,
    resolve_workspace_name_and_id,
    resolve_item_id,
    _base_api,
)
from uuid import UUID
from sempy._utils._log import log
import sempy_labs._icons as icons


@log
def get_apache_airflow_job_environment(
    apache_airflow_job: str | UUID,
    workspace: Optional[str | UUID] = None,
) -> str:
    """
    Returns the status of the specified Apache Airflow job environment.

    This is a wrapper function for the following API: `Environment - Get Apache Airflow Job Environment <https://learn.microsoft.com/rest/api/fabric/apacheairflowjob/environment/get-apache-airflow-job-environment(beta)>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    apache_airflow_job : str | uuid.UUID
        The Apache Airflow job name or ID.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    str
        The status of the Apache Airflow job environment (i.e. 'Initial', 'Starting', 'Started', 'Stopping', 'Stopped', 'Upgrading', 'Failed').
    """

    workspace_id = resolve_workspace_id(workspace)
    item_id = resolve_item_id(
        item=apache_airflow_job,
        type="ApacheAirflowJob",
        workspace=workspace_id,
    )

    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/apacheAirflowJobs/{item_id}/environment?beta=True",
        client="fabric_sp",
    ).json()

    return response.get("status")


def _apache_airflow_job_environment_action(
    apache_airflow_job: str | UUID,
    action: str,
    workspace: Optional[str | UUID] = None,
):

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    item_name, item_id = resolve_item_name_and_id(
        item=apache_airflow_job,
        type="ApacheAirflowJob",
        workspace=workspace_id,
    )

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/apacheAirflowJobs/{item_id}/environment/{action}?beta=True",
        client="fabric_sp",
        method="post",
        status_codes=[200, 202],
        lro_return_status_code=True,
    )

    past_tense = "started" if action == "start" else "stopped"
    print(
        f"{icons.green_dot} The environment of the '{item_name}' Apache Airflow Job within the '{workspace_name}' workspace has been {past_tense}."
    )


@log
def start_apache_airflow_job_environment(
    apache_airflow_job: str | UUID,
    workspace: Optional[str | UUID] = None,
):
    """
    Starts an Apache Airflow job environment.

    This is a wrapper function for the following API: `Environment - Start Apache Airflow Job Environment <https://learn.microsoft.com/rest/api/fabric/apacheairflowjob/environment/start-apache-airflow-job-environment(beta)>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    apache_airflow_job : str | uuid.UUID
        The Apache Airflow job name or ID.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    _apache_airflow_job_environment_action(
        apache_airflow_job=apache_airflow_job, action="start", workspace=workspace
    )


@log
def stop_apache_airflow_job_environment(
    apache_airflow_job: str | UUID,
    workspace: Optional[str | UUID] = None,
):
    """
    Stops an Apache Airflow job environment.

    This is a wrapper function for the following API: `Environment - Stop Apache Airflow Job Environment <https://learn.microsoft.com/rest/api/fabric/apacheairflowjob/environment/stop-apache-airflow-job-environment(beta)>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    apache_airflow_job : str | uuid.UUID
        The Apache Airflow job name or ID.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    _apache_airflow_job_environment_action(
        apache_airflow_job=apache_airflow_job, action="stop", workspace=workspace
    )
