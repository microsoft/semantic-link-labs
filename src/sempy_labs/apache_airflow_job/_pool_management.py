import pandas as pd
from typing import Optional, Tuple
from sempy_labs._helper_functions import (
    resolve_workspace_id,
    resolve_workspace_name_and_id,
    _base_api,
    _create_dataframe,
    _is_valid_uuid,
)
from uuid import UUID
from sempy._utils._log import log
import sempy_labs._icons as icons


def _resolve_airflow_pool_template_name_and_id(
    pool_template: str | UUID,
    workspace_id: str,
) -> Tuple[str, str]:

    df = list_airflow_pool_templates(workspace=workspace_id)
    if _is_valid_uuid(str(pool_template)):
        df_filt = df[df["Pool Template Id"] == str(pool_template)]
    else:
        df_filt = df[df["Pool Template Name"] == pool_template]

    if df_filt.empty:
        raise ValueError(
            f"{icons.red_dot} The '{pool_template}' Apache Airflow pool template does not exist within the '{workspace_id}' workspace."
        )

    return (
        df_filt["Pool Template Name"].iloc[0],
        df_filt["Pool Template Id"].iloc[0],
    )


def _pool_template_to_row(template: dict) -> dict:

    scalability = template.get("computeScalability") or {}
    return {
        "Pool Template Id": template.get("id"),
        "Pool Template Name": template.get("name"),
        "Node Size": template.get("nodeSize"),
        "Shutdown Policy": template.get("shutdownPolicy"),
        "Min Node Count": scalability.get("minNodeCount"),
        "Max Node Count": scalability.get("maxNodeCount"),
        "Apache Airflow Job Version": template.get("apacheAirflowJobVersion"),
    }


_pool_template_columns = {
    "Pool Template Id": "string",
    "Pool Template Name": "string",
    "Node Size": "string",
    "Shutdown Policy": "string",
    "Min Node Count": "int",
    "Max Node Count": "int",
    "Apache Airflow Job Version": "string",
}


def _rows_to_dataframe(rows: list) -> pd.DataFrame:

    if not rows:
        return _create_dataframe(columns=_pool_template_columns)

    df = pd.DataFrame(rows, columns=list(_pool_template_columns.keys()))
    # Nullable integer type so a missing node count doesn't raise on conversion.
    return df.astype({"Min Node Count": "Int64", "Max Node Count": "Int64"})


@log
def list_airflow_pool_templates(
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Shows a list of the Apache Airflow pool templates within the specified workspace.

    This is a wrapper function for the following API: `Pool Management - List Airflow Pool Templates <https://learn.microsoft.com/rest/api/fabric/apacheairflowjob/pool-management/list-airflow-pool-templates(beta)>`_.

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
        A pandas dataframe showing a list of the Apache Airflow pool templates within the specified workspace.
    """

    workspace_id = resolve_workspace_id(workspace)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/apacheAirflowJobs/poolTemplates?beta=True",
        client="fabric_sp",
        uses_pagination=True,
    )

    rows = [_pool_template_to_row(v) for r in responses for v in r.get("value", [])]

    return _rows_to_dataframe(rows)


@log
def get_airflow_pool_template(
    pool_template: str | UUID,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Shows the details of the specified Apache Airflow pool template.

    This is a wrapper function for the following API: `Pool Management - Get Airflow Pool Template <https://learn.microsoft.com/rest/api/fabric/apacheairflowjob/pool-management/get-airflow-pool-template(beta)>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    pool_template : str | uuid.UUID
        The Apache Airflow pool template name or ID.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the details of the specified Apache Airflow pool template.
    """

    workspace_id = resolve_workspace_id(workspace)
    if _is_valid_uuid(str(pool_template)):
        pool_template_id = str(pool_template)
    else:
        _, pool_template_id = _resolve_airflow_pool_template_name_and_id(
            pool_template=pool_template, workspace_id=workspace_id
        )

    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/apacheAirflowJobs/poolTemplates/{pool_template_id}?beta=True",
        client="fabric_sp",
    ).json()

    return _rows_to_dataframe([_pool_template_to_row(response)] if response else [])


@log
def create_airflow_pool_template(
    name: str,
    node_size: str = "Small",
    min_node_count: int = 5,
    max_node_count: int = 5,
    apache_airflow_job_version: str = "1.0.0",
    workspace: Optional[str | UUID] = None,
):
    """
    Creates an Apache Airflow pool template.

    This is a wrapper function for the following API: `Pool Management - Create Airflow Pool Template <https://learn.microsoft.com/rest/api/fabric/apacheairflowjob/pool-management/create-airflow-pool-template(beta)>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    name : str
        The pool template name.
    node_size : str, default="Small"
        The node size. Valid values: 'Small', 'Large'.
    min_node_count : int, default=5
        The minimum node count. Must be at least 5.
    max_node_count : int, default=5
        The maximum node count. Must be greater than or equal to the minimum node count.
    apache_airflow_job_version : str, default="1.0.0"
        The Apache Airflow job version.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    if min_node_count < 5:
        raise ValueError(f"{icons.red_dot} The 'min_node_count' must be at least 5.")
    if max_node_count < min_node_count:
        raise ValueError(
            f"{icons.red_dot} The 'max_node_count' must be greater than or equal to the 'min_node_count'."
        )

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)

    payload = {
        "name": name,
        "nodeSize": node_size,
        "computeScalability": {
            "minNodeCount": min_node_count,
            "maxNodeCount": max_node_count,
        },
        "apacheAirflowJobVersion": apache_airflow_job_version,
    }

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/apacheAirflowJobs/poolTemplates?beta=True",
        client="fabric_sp",
        method="post",
        payload=payload,
        status_codes=201,
    )

    print(
        f"{icons.green_dot} The '{name}' Apache Airflow pool template has been created within the '{workspace_name}' workspace."
    )


@log
def delete_airflow_pool_template(
    pool_template: str | UUID,
    workspace: Optional[str | UUID] = None,
):
    """
    Deletes an Apache Airflow pool template.

    This is a wrapper function for the following API: `Pool Management - Delete Airflow Pool Template <https://learn.microsoft.com/rest/api/fabric/apacheairflowjob/pool-management/delete-airflow-pool-template(beta)>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    pool_template : str | uuid.UUID
        The Apache Airflow pool template name or ID.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    pool_template_name, pool_template_id = _resolve_airflow_pool_template_name_and_id(
        pool_template=pool_template, workspace_id=workspace_id
    )

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/apacheAirflowJobs/poolTemplates/{pool_template_id}?beta=True",
        client="fabric_sp",
        method="delete",
    )

    print(
        f"{icons.green_dot} The '{pool_template_name}' Apache Airflow pool template has been deleted from the '{workspace_name}' workspace."
    )
