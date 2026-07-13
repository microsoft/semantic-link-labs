import pandas as pd
from typing import Optional, Literal
from sempy_labs._helper_functions import (
    _is_valid_uuid,
    resolve_workspace_id,
    _base_api,
    _create_dataframe,
    resolve_workspace_name_and_id,
)
from uuid import UUID
from sempy._utils._log import log
import sempy_labs._icons as icons


@log
def list_airflow_pool_templates(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    List Apache Airflow pool templates.

    This is a wrapper function for the following API: `Pool Management - ListAirflowPoolTemplates <https://learn.microsoft.com/rest/api/fabric/apacheairflowjob/pool-management/list-airflow-pool-templates(beta)>`_.

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
        A pandas dataframe containing the Apache Airflow pool templates.
    """

    workspace_id = resolve_workspace_id(workspace)

    columns = {
        "Pool Template Name": "string",
        "Pool Template Id": "string",
        "Node Size": "string",
        "Shutdown Policy": "string",
        "Compute Scalibility Min Node Count": "int",
        "Compute Scalibility Max Node Count": "int",
        "Apache Airflow Job Version": "string",
    }

    df = _create_dataframe(columns=columns)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/apacheAirflowJobs/poolTemplates?beta=True",
        client="fabric_sp",
        uses_pagination=True,
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            compute_scalability = v.get("computeScalability", {})
            rows.append(
                {
                    "Pool Template Name": v.get("name"),
                    "Pool Template Id": v.get("id"),
                    "Node Size": v.get("nodeSize"),
                    "Shutdown Policy": v.get("shutdownPolicy"),
                    "Compute Scalibility Min Node Count": compute_scalability.get(
                        "minNodeCount"
                    ),
                    "Compute Scalibility Max Node Count": compute_scalability.get(
                        "maxNodeCount"
                    ),
                    "Apache Airflow Job Version": v.get("apacheAirflowJobVersion"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=columns.keys())

    return df


def resolve_pool_template_id(
    airflow_pool_template: str | UUID, workspace_id: str
) -> str:

    if _is_valid_uuid(airflow_pool_template):
        airflow_pool_template_id = str(airflow_pool_template)
    else:
        df = list_airflow_pool_templates(workspace=workspace_id)
        df_filt = df[df["Pool Template Name"] == airflow_pool_template]
        if df_filt.empty:
            raise ValueError(
                f"Airflow Pool Template with name '{airflow_pool_template}' not found in workspace '{workspace_id}'."
            )
        airflow_pool_template_id = df_filt["Pool Template Id"].iloc[0]

    return airflow_pool_template_id


@log
def delete_airflow_pool_template(
    airflow_pool_template: str | UUID, workspace: Optional[str | UUID] = None
):
    """
    Delete an Apache Airflow pool template.

    This is a wrapper function for the following API: `Pool Management - DeleteAirflowPoolTemplate <https://learn.microsoft.com/rest/api/fabric/apacheairflowjob/pool-management/delete-airflow-pool-template(beta)>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    airflow_pool_template : str | uuid.UUID
        The Airflow Pool Template name or ID to delete.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    airflow_pool_template_id = resolve_pool_template_id(
        airflow_pool_template, workspace_id
    )

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/apacheAirflowJobs/poolTemplates/{airflow_pool_template_id}?beta=True",
        client="fabric_sp",
        method="delete",
    )

    print(
        f"{icons.green_dot} The Airflow Pool Template '{airflow_pool_template}' has been deleted from workspace '{workspace_name}'."
    )


@log
def create_airflow_pool_template(
    name: str,
    node_size: Literal["Small", "Large"],
    min_node_count: int,
    max_node_count: int,
    shutdown_policy: Literal["AlwaysOn", "OneHourInactivity"] = "OneHourInactivity",
    apache_airflow_job_version: str = "1.0.0",
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Create an Apache Airflow pool template.

    This is a wrapper function for the following API: `Pool Management - CreateAirflowPoolTemplate <https://learn.microsoft.com/rest/api/fabric/apacheairflowjob/pool-management/create-airflow-pool-template(beta)>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    name : str
        The pool template name.
    node_size : typing.Literal["Small", "Large"]
        The node size for the pool template.
    min_node_count : int
        The minimum node count. The minimum value is always 5.
    max_node_count : int
        The maximum node count.
    shutdown_policy : typing.Literal["AlwaysOn", "OneHourInactivity"], default="OneHourInactivity"
        The shutdown policy for the pool template.
    apache_airflow_job_version : str, default="1.0.0"
        The Apache Airflow job version.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    if min_node_count < 5:
        raise ValueError(f"{icons.red_dot} The minimum node count must be at least 5.")

    if max_node_count < min_node_count:
        raise ValueError(
            f"{icons.red_dot} The maximum node count must be greater than or equal to the minimum node count."
        )

    payload = {
        "name": name,
        "nodeSize": node_size,
        "computeScalability": {
            "minNodeCount": min_node_count,
            "maxNodeCount": max_node_count,
        },
        "apacheAirflowJobVersion": apache_airflow_job_version,
    }

    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/apacheAirflowJobs/poolTemplates?beta=True",
        client="fabric_sp",
        method="post",
        payload=payload,
    )
    result = response.json()

    print(
        f"{icons.green_dot} The Airflow Pool Template '{name}' has been created within the '{workspace_name}' workspace."
    )

    row = {
        "Pool Template Name": result.get("name"),
        "Pool Template Id": result.get("id"),
        "Node Size": result.get("nodeSize"),
        "Shutdown Policy": result.get("shutdownPolicy"),
        "Compute Scalability Min Node Count": result.get("computeScalability", {}).get(
            "minNodeCount"
        ),
        "Compute Scalability Max Node Count": result.get("computeScalability", {}).get(
            "maxNodeCount"
        ),
        "Apache Airflow Job Version": result.get("apacheAirflowJobVersion"),
    }

    return pd.DataFrame([row])
