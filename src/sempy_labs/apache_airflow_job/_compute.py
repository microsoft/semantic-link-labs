import pandas as pd
from typing import Optional
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
from sempy_labs.apache_airflow_job._pool_management import (
    _resolve_airflow_pool_template_name_and_id,
)


@log
def get_apache_airflow_job_compute(
    apache_airflow_job: str | UUID,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Returns the compute configuration for the specified Apache Airflow job environment.

    This is a wrapper function for the following API: `Compute - Get Apache Airflow Job Compute <https://learn.microsoft.com/rest/api/fabric/apacheairflowjob/compute/get-apache-airflow-job-compute(beta)>`_.

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
        A pandas dataframe showing the compute configuration for the specified Apache Airflow job environment.
    """

    columns = {
        "Pool Template Id": "string",
        "Pool Template Name": "string",
        "Node Size": "string",
        "Min Node Count": "int",
        "Max Node Count": "int",
        "Apache Airflow Job Version": "string",
        "Apache Airflow Version": "string",
        "Python Version": "string",
        "Availability Zones": "string",
        "Shutdown Policy": "string",
    }

    df = _create_dataframe(columns=columns)

    workspace_id = resolve_workspace_id(workspace)
    item_id = resolve_item_id(
        item=apache_airflow_job,
        type="ApacheAirflowJob",
        workspace=workspace_id,
    )

    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/apacheAirflowJobs/{item_id}/environment/compute?beta=True",
        client="fabric_sp",
    ).json()

    if response:
        scalability = response.get("computeScalability") or {}
        version_details = response.get("apacheAirflowJobVersionDetails") or {}
        row = {
            "Pool Template Id": response.get("poolTemplateId"),
            "Pool Template Name": response.get("poolTemplateName"),
            "Node Size": response.get("nodeSize"),
            "Min Node Count": scalability.get("minNodeCount"),
            "Max Node Count": scalability.get("maxNodeCount"),
            "Apache Airflow Job Version": response.get("apacheAirflowJobVersion"),
            "Apache Airflow Version": version_details.get("apacheAirflowVersion"),
            "Python Version": version_details.get("pythonVersion"),
            "Availability Zones": response.get("availabilityZones"),
            "Shutdown Policy": response.get("shutdownPolicy"),
        }
        df = pd.DataFrame([row], columns=list(columns.keys()))
        # Nullable integer type so a missing node count doesn't raise on conversion.
        df = df.astype({"Min Node Count": "Int64", "Max Node Count": "Int64"})

    return df


@log
def update_apache_airflow_job_compute(
    apache_airflow_job: str | UUID,
    pool_template: str | UUID,
    workspace: Optional[str | UUID] = None,
):
    """
    Updates the compute configuration for an Apache Airflow job environment.

    This is a wrapper function for the following API: `Compute - Update Apache Airflow Job Compute <https://learn.microsoft.com/rest/api/fabric/apacheairflowjob/compute/update-apache-airflow-job-compute(beta)>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    apache_airflow_job : str | uuid.UUID
        The Apache Airflow job name or ID.
    pool_template : str | uuid.UUID
        The Apache Airflow pool template name or ID.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    item_name, item_id = resolve_item_name_and_id(
        item=apache_airflow_job,
        type="ApacheAirflowJob",
        workspace=workspace_id,
    )

    pool_template_name, pool_template_id = _resolve_airflow_pool_template_name_and_id(
        pool_template=pool_template, workspace_id=workspace_id
    )

    payload = {"poolTemplateId": pool_template_id}

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/apacheAirflowJobs/{item_id}/environment/updateCompute?beta=True",
        client="fabric_sp",
        method="post",
        payload=payload,
        status_codes=[200, 202],
        lro_return_status_code=True,
    )

    print(
        f"{icons.green_dot} The compute configuration of the '{item_name}' Apache Airflow Job within the '{workspace_name}' workspace has been updated to use the '{pool_template_name}' pool template."
    )
