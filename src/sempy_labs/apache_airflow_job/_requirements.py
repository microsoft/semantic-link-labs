import pandas as pd
from typing import List, Optional
from urllib.parse import quote
from sempy_labs._helper_functions import (
    resolve_item_name_and_id,
    resolve_workspace_id,
    resolve_workspace_name_and_id,
    resolve_item_id,
    _base_api,
    _create_dataframe,
)
from uuid import UUID
from sempy._utils._log import log
import sempy_labs._icons as icons


@log
def list_apache_airflow_job_libraries(
    apache_airflow_job: str | UUID,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Shows a list of the libraries installed in the specified Apache Airflow job environment.

    This is a wrapper function for the following API: `Requirements - List Apache Airflow Job Libraries <https://learn.microsoft.com/rest/api/fabric/apacheairflowjob/requirements/list-apache-airflow-job-libraries(beta)>`_.

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
    pandas.DataFrame
        A pandas dataframe showing a list of the libraries installed in the specified Apache Airflow job environment.
    """

    columns = {
        "Library Name": "string",
        "Library Type": "string",
        "Source": "string",
        "Version": "string",
    }

    df = _create_dataframe(columns=columns)

    workspace_id = resolve_workspace_id(workspace)
    item_id = resolve_item_id(
        item=apache_airflow_job,
        type="ApacheAirflowJob",
        workspace=workspace_id,
    )

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/apacheAirflowJobs/{item_id}/environment/libraries?beta=True",
        client="fabric_sp",
        uses_pagination=True,
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            rows.append(
                {
                    "Library Name": v.get("name"),
                    "Library Type": v.get("libraryType"),
                    "Source": v.get("source"),
                    "Version": v.get("version"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def deploy_apache_airflow_job_requirements(
    apache_airflow_job: str | UUID,
    requirements: Optional[str | List[str]] = None,
    file_path: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Deploys requirements to an Apache Airflow job environment, either from an existing requirements file within the Apache Airflow job or from the provided requirements content.

    This is a wrapper function for the following API: `Requirements - Deploy Apache Airflow Job Requirements <https://learn.microsoft.com/rest/api/fabric/apacheairflowjob/requirements/deploy-apache-airflow-job-requirements(beta)>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    apache_airflow_job : str | uuid.UUID
        The Apache Airflow job name or ID.
    requirements : str | List[str], default=None
        The requirements file content (i.e. "pandas==2.0.0\\nrequests==2.31.0") or a list of requirements (i.e. ["pandas==2.0.0", "requests==2.31.0"]).
        Must be provided if 'file_path' is not provided.
    file_path : str, default=None
        The path to an existing requirements file within the Apache Airflow job (i.e. "dags/requirements.txt").
        Must be provided if 'requirements' is not provided.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    if (requirements is None) == (file_path is None):
        raise ValueError(
            f"{icons.red_dot} Exactly one of the 'requirements' or 'file_path' parameters must be provided."
        )

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    item_name, item_id = resolve_item_name_and_id(
        item=apache_airflow_job,
        type="ApacheAirflowJob",
        workspace=workspace_id,
    )

    url = f"/v1/workspaces/{workspace_id}/apacheAirflowJobs/{item_id}/environment/deployRequirements?beta=True"
    if file_path:
        url += f"&filePath={quote(file_path, safe='/')}"
        payload = None
        headers = None
        source = f"the '{file_path}' file"
    else:
        if isinstance(requirements, list):
            requirements = "\n".join(requirements)
        payload = requirements.encode("utf-8")
        headers = {"Content-Type": "text/plain; charset=utf-8"}
        source = "the provided requirements"

    _base_api(
        request=url,
        client="fabric_sp",
        method="post",
        payload=payload,
        headers=headers,
        status_codes=[200, 202],
        lro_return_status_code=True,
    )

    print(
        f"{icons.green_dot} The requirements from {source} have been deployed to the '{item_name}' Apache Airflow Job within the '{workspace_name}' workspace."
    )
