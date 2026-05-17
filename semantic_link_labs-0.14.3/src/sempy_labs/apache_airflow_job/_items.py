import pandas as pd
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_id,
    _base_api,
    _create_dataframe,
    delete_item,
)
from uuid import UUID
from sempy._utils._log import log


@log
def list_apache_airflow_jobs(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows the Apache Airflow Jobs within a workspace.

    This is a wrapper function for the following API: `Items - List Apache Airflow Jobs <https://learn.microsoft.com/rest/api/fabric/apacheairflowjob/items/list-apache-airflow-jobs>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the Apache Airflow Jobs within a workspace.
    """

    columns = {
        "Apache Airflow Job Name": "string",
        "Apache Airflow Job Id": "string",
        "Description": "string",
    }

    df = _create_dataframe(columns=columns)

    workspace_id = resolve_workspace_id(workspace)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/ApacheAirflowJobs",
        client="fabric_sp",
        uses_pagination=True,
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            rows.append(
                {
                    "Apache Airflow Job Name": v.get("displayName"),
                    "Apache Airflow Job Id": v.get("id"),
                    "Description": v.get("description"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def delete_apache_airflow_job(
    apache_airflow_job: str | UUID, workspace: Optional[str | UUID] = None
):
    """
    Deletes a Fabric Apache Airflow Job.

    Parameters
    ----------
    apache_airflow_job : str | UUID
        The name or ID of the Apache Airflow Job to delete.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    delete_item(
        item_id=apache_airflow_job,
        type="ApacheAirflowJob",
        workspace=workspace,
    )
