import pandas as pd
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_item_name_and_id,
    resolve_workspace_id,
    _base_api,
    _create_dataframe,
    delete_item,
    resolve_item_id,
    resolve_workspace_name_and_id,
)
from uuid import UUID
from sempy._utils._log import log
import sempy_labs._icons as icons


@log
def list_apache_airflow_job_files(
    apache_airflow_job: str | UUID,
    workspace: Optional[str | UUID] = None,
    root_path: Optional[str] = None,
) -> pd.DataFrame:
    """
    Shows a list of Apache Airflow job files from the specified Apache Airflow job.

    This is a wrapper function for the following API: `Files - List Apache Airflow Job Files <https://learn.microsoft.com/rest/api/fabric/apacheairflowjob/files/list-apache-airflow-job-files>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    apache_airflow_job : str | uuid.UUID
        The Apache Airflow job name or ID.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    root_path : str, default=None
        The folder path to list. If not provided, the root directory is used.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of Apache Airflow job files from the specified Apache Airflow job.
    """

    columns = {
        "File Path": "string",
        "Size In Bytes": "int",
    }

    df = _create_dataframe(columns=columns)

    workspace_id = resolve_workspace_id(workspace)

    apache_airflow_job_id = resolve_item_id(
        item=apache_airflow_job,
        type="ApacheAirflowJob",
        workspace=workspace_id,
    )

    url = f"/v1/workspaces/{workspace_id}/ApacheAirflowJobs/{apache_airflow_job_id}/files?beta=True"
    if root_path:
        url += f"&rootPath={root_path}"
    responses = _base_api(
        request=url,
        client="fabric_sp",
        uses_pagination=True,
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            rows.append(
                {
                    "File Path": v.get("filePath"),
                    "Size In Bytes": v.get("sizeInBytes"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def delete_apache_airflow_job_file(
    apache_airflow_job: str | UUID,
    file_path: str,
    workspace: Optional[str | UUID] = None,
):
    """
    Deletes a file from a Fabric Apache Airflow Job.

    This is a wrapper function for the following API: `Files - Delete Apache Airflow Job File <https://learn.microsoft.com/rest/api/fabric/apacheairflowjob/files/delete-apache-airflow-job-file>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    apache_airflow_job : str | uuid.UUID
        The Apache Airflow job name or ID.
    file_path : str
        The file path relative to the Apache Airflow job root. It must begin with either 'dags/' or 'plugins/' (for example, dags/example_dag.py).
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (item_name, item_id) = resolve_item_name_and_id(
        item=apache_airflow_job,
        type="ApacheAirflowJob",
        workspace=workspace_id,
    )

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/ApacheAirflowJobs/{item_id}/files/{file_path}?beta=True",
        client="fabric_sp",
        method="delete",
    )

    print(
        f"{icons.green_dot} The '{file_path}' file has been deleted from the '{item_name}' Apache Airflow Job within the '{workspace_name}' workspace."
    )


@log
def create_or_update_apache_airflow_job_file(
    apache_airflow_job: str | UUID,
    file_path: str,
    file_content: bytes,
    workspace: Optional[str | UUID] = None,
):
    """
    Creates or updates an Apache Airflow job file.

    This is a wrapper function for the following API: `Files - Create Or Update Apache Airflow Job File <https://learn.microsoft.com/rest/api/fabric/apacheairflowjob/files/create-or-update-apache-airflow-job-file>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    apache_airflow_job : str | uuid.UUID
        The Apache Airflow job name or ID.
    file_path : str
        The file path relative to the Apache Airflow job root. It must begin with either 'dags/' or 'plugins/' (for example, dags/example_dag.py).
    file_content : bytes
        The file content. Text files must be UTF-8 encoded.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (item_name, item_id) = resolve_item_name_and_id(
        item=apache_airflow_job,
        type="ApacheAirflowJob",
        workspace=workspace_id,
    )

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/ApacheAirflowJobs/{item_id}/files/{file_path}?beta=True",
        client="fabric_sp",
        method="put",
        payload=file_content,
    )

    print(
        f"{icons.green_dot} The '{file_path}' file has been created/updated in the '{item_name}' Apache Airflow Job within the '{workspace_name}' workspace."
    )
