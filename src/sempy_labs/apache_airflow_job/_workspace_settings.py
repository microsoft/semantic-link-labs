import pandas as pd
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_id,
    resolve_workspace_name_and_id,
    _base_api,
    _create_dataframe,
)
from sempy_labs.apache_airflow_job._pool_management import (
    _resolve_airflow_pool_template_name_and_id,
)
from uuid import UUID
from sempy._utils._log import log
import sempy_labs._icons as icons


@log
def get_airflow_workspace_settings(
    workspace: Optional[str | UUID] = None,
    return_dataframe: bool = True,
) -> pd.DataFrame | dict:
    """
    Returns the Apache Airflow settings of the specified workspace.

    This is a wrapper function for the following API: `Workspace Settings - Get Airflow Workspace Settings <https://learn.microsoft.com/rest/api/fabric/apacheairflowjob/workspace-settings/get-airflow-workspace-settings(beta)>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    return_dataframe : bool, default=True
        If True, returns a pandas dataframe. If False, returns the API response as a dictionary.

    Returns
    -------
    pandas.DataFrame | dict
        A pandas dataframe showing the Apache Airflow settings of the specified workspace,
        or a dictionary if return_dataframe is set to False.
    """

    workspace_id = resolve_workspace_id(workspace)

    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/apacheAirflowJobs/settings?beta=True",
        client="fabric_sp",
    ).json()

    if not return_dataframe:
        return response

    columns = {
        "Default Pool Template Id": "string",
    }
    df = _create_dataframe(columns=columns)

    if response:
        df = pd.DataFrame(
            [{"Default Pool Template Id": response.get("defaultPoolTemplateId")}],
            columns=list(columns.keys()),
        )

    return df


@log
def update_airflow_workspace_settings(
    default_pool_template: str | UUID,
    workspace: Optional[str | UUID] = None,
):
    """
    Updates the Apache Airflow settings of the specified workspace.

    This is a wrapper function for the following API: `Workspace Settings - Update Airflow Workspace Settings <https://learn.microsoft.com/rest/api/fabric/apacheairflowjob/workspace-settings/update-airflow-workspace-settings(beta)>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    default_pool_template : str | uuid.UUID
        The name or ID of the Apache Airflow pool template to set as the default for the workspace.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    pool_template_name, pool_template_id = _resolve_airflow_pool_template_name_and_id(
        pool_template=default_pool_template, workspace_id=workspace_id
    )

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/apacheAirflowJobs/settings?beta=True",
        client="fabric_sp",
        method="patch",
        payload={"defaultPoolTemplateId": pool_template_id},
    )

    print(
        f"{icons.green_dot} The default Apache Airflow pool template of the '{workspace_name}' workspace has been set to '{pool_template_name}'."
    )
