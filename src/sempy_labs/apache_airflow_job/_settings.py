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


def _get_apache_airflow_job_settings(workspace_id: str, item_id: str) -> dict:

    return _base_api(
        request=f"/v1/workspaces/{workspace_id}/apacheAirflowJobs/{item_id}/environment/settings?beta=True",
        client="fabric_sp",
    ).json()


def _to_name_value_pairs(values: dict) -> list:

    return [{"name": k, "value": str(v)} for k, v in values.items()]


@log
def get_apache_airflow_job_settings(
    apache_airflow_job: str | UUID,
    workspace: Optional[str | UUID] = None,
    return_dataframe: bool = True,
) -> pd.DataFrame | dict:
    """
    Returns the environment settings (environment variables, Airflow configuration overrides and triggerers status) of the specified Apache Airflow job.

    This is a wrapper function for the following API: `Settings - Get Apache Airflow Job Settings <https://learn.microsoft.com/rest/api/fabric/apacheairflowjob/settings/get-apache-airflow-job-settings(beta)>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    apache_airflow_job : str | uuid.UUID
        The Apache Airflow job name or ID.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    return_dataframe : bool, default=True
        If True, returns a pandas dataframe. If False, returns the API response as a dictionary.

    Returns
    -------
    pandas.DataFrame | dict
        A pandas dataframe showing the environment settings of the specified Apache Airflow job,
        or a dictionary if return_dataframe is set to False.
        The 'Setting Type' column is one of 'Environment Variable', 'Airflow Configuration Override' or 'Triggerers'.
    """

    workspace_id = resolve_workspace_id(workspace)
    item_id = resolve_item_id(
        item=apache_airflow_job,
        type="ApacheAirflowJob",
        workspace=workspace_id,
    )

    response = _get_apache_airflow_job_settings(
        workspace_id=workspace_id, item_id=item_id
    )

    if not return_dataframe:
        return response

    columns = {
        "Setting Type": "string",
        "Name": "string",
        "Value": "string",
    }
    df = _create_dataframe(columns=columns)

    rows = []
    for setting_type, key in [
        ("Environment Variable", "environmentVariables"),
        ("Airflow Configuration Override", "airflowConfigurationOverrides"),
    ]:
        for v in response.get(key) or []:
            rows.append(
                {
                    "Setting Type": setting_type,
                    "Name": v.get("name"),
                    "Value": v.get("value"),
                }
            )
    if response.get("triggerers") is not None:
        rows.append(
            {
                "Setting Type": "Triggerers",
                "Name": "triggerers",
                "Value": response.get("triggerers"),
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def update_apache_airflow_job_settings(
    apache_airflow_job: str | UUID,
    environment_variables: Optional[dict] = None,
    airflow_configuration_overrides: Optional[dict] = None,
    triggerers: Optional[bool | str] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Updates the environment settings (environment variables, Airflow configuration overrides and triggerers status) of the specified Apache Airflow job.

    This is a wrapper function for the following API: `Settings - Update Apache Airflow Job Settings <https://learn.microsoft.com/rest/api/fabric/apacheairflowjob/settings/update-apache-airflow-job-settings(beta)>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    apache_airflow_job : str | uuid.UUID
        The Apache Airflow job name or ID.
    environment_variables : dict, default=None
        The environment variables as a dictionary of names and values (i.e. {"MY_ENV_VAR": "my_value"}).
        This is the complete set of desired values; existing environment variables are replaced. Pass an empty dictionary to remove all environment variables.
        Defaults to None which keeps the existing environment variables.
    airflow_configuration_overrides : dict, default=None
        The Airflow configuration overrides as a dictionary of names and values (i.e. {"core.parallelism": "32"}).
        This is the complete set of desired values; existing overrides are replaced. Pass an empty dictionary to remove all overrides.
        Defaults to None which keeps the existing Airflow configuration overrides.
    triggerers : bool | str, default=None
        The triggerers status. True or 'Enabled' enables triggerers; False or 'Disabled' disables them.
        Disabling triggerers does not delete any environment settings.
        Defaults to None which keeps the existing triggerers status.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    if (
        environment_variables is None
        and airflow_configuration_overrides is None
        and triggerers is None
    ):
        raise ValueError(
            f"{icons.red_dot} At least one of the 'environment_variables', 'airflow_configuration_overrides' or 'triggerers' parameters must be provided."
        )

    if isinstance(triggerers, bool):
        triggerers = "Enabled" if triggerers else "Disabled"

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    item_name, item_id = resolve_item_name_and_id(
        item=apache_airflow_job,
        type="ApacheAirflowJob",
        workspace=workspace_id,
    )

    # The API replaces each list it receives, so settings that were not specified are re-sent with their current values.
    current = _get_apache_airflow_job_settings(
        workspace_id=workspace_id, item_id=item_id
    )

    payload = {
        "environmentVariables": (
            _to_name_value_pairs(environment_variables)
            if environment_variables is not None
            else current.get("environmentVariables") or []
        ),
        "airflowConfigurationOverrides": (
            _to_name_value_pairs(airflow_configuration_overrides)
            if airflow_configuration_overrides is not None
            else current.get("airflowConfigurationOverrides") or []
        ),
    }
    if triggerers is None:
        triggerers = current.get("triggerers")
    if triggerers is not None:
        payload["triggerers"] = triggerers

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/apacheAirflowJobs/{item_id}/environment/updateSettings?beta=True",
        client="fabric_sp",
        method="post",
        payload=payload,
        status_codes=[200, 202],
        lro_return_status_code=True,
    )

    print(
        f"{icons.green_dot} The settings of the '{item_name}' Apache Airflow Job within the '{workspace_name}' workspace have been updated."
    )
