import pandas as pd
from typing import Optional
import sempy_labs._icons as icons
from sempy_labs._helper_functions import (
    _update_dataframe_datatypes,
    _base_api,
    _create_dataframe,
)


def list_workloads(capacity_name: str) -> pd.DataFrame:
    """
    Returns the current state of the specified capacity workloads.
    If a workload is enabled, the percentage of maximum memory that the workload can consume is also returned.

    This is a wrapper function for the following API: `Capacities - Get Workloads <https://learn.microsoft.com/rest/api/power-bi/capacities/get-workloads>`_.

    Parameters
    ----------
    capacity_name : str
        The capacity name.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the current state of the specified capacity workloads.
    """

    from sempy_labs._helper_functions import resolve_capacity_id

    columns = {
        "Workload Name": "string",
        "State": "string",
        "Max Memory Percentage Set By User": "int",
    }
    df = _create_dataframe(columns=columns)

    capacity_id = resolve_capacity_id(capacity_name=capacity_name)

    response = _base_api(request=f"/v1.0/myorg/capacities/{capacity_id}/Workloads")

    for v in response.json().get("value", []):
        new_data = {
            "Workload Name": v.get("name"),
            "State": v.get("state"),
            "Max Memory Percentage Set By User": v.get("maxMemoryPercentageSetByUser"),
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


def patch_workload(
    capacity_name: str,
    workload_name: str,
    state: Optional[str] = None,
    max_memory_percentage: Optional[int] = None,
):
    """
    Changes the state of a specific workload to Enabled or Disabled.
    When enabling a workload, specify the percentage of maximum memory that the workload can consume.

    This is a wrapper function for the following API: `Capacities - Patch Workload <https://learn.microsoft.com/rest/api/power-bi/capacities/patch-workload>`_.

    Parameters
    ----------
    capacity_name : str
        The capacity name.
    workload_name : str
        The workload name.
    state : str, default=None
        The capacity workload state.
    max_memory_percentage : int, default=None
        The percentage of the maximum memory that a workload can consume (set by the user).
    """

    from sempy_labs._helper_functions import resolve_capacity_id

    capacity_id = resolve_capacity_id(capacity_name=capacity_name)

    states = ["Disabled", "Enabled", "Unsupported"]
    state = state.capitalize()
    if state is not None and state not in states:
        raise ValueError(
            f"{icons.red_dot} Invalid 'state' parameter. Please choose from these options: {states}."
        )
    if max_memory_percentage is not None and (
        max_memory_percentage < 0 or max_memory_percentage > 100
    ):
        raise ValueError(
            f"{icons.red_dot} Invalid max memory percentage. Must be a value between 0-100."
        )

    url = f"/v1.0/myorg/capacities/{capacity_id}/Workloads/{workload_name}"

    get_response = _base_api(request=url)
    get_json = get_response.json().get("value")
    current_state = get_json.get("state")
    current_max_memory = get_json.get("maxMemoryPercentageSetByUser")

    if current_state == state and str(current_max_memory) == str(max_memory_percentage):
        print(
            f"{icons.info} The current workload settings are the same as those specified in the parameters of this function. The workload has not been updated."
        )
        return

    payload = {}
    if state is not None:
        payload["state"] = state
    else:
        payload["state"] = current_state
    if max_memory_percentage is not None:
        payload["maxMemoryPercentageSetByUser"] = max_memory_percentage
    else:
        payload["maxMemoryPercentageSetByUser"] = current_max_memory

    _base_api(request=url, method="patch", payload=payload)

    print(
        f"The '{workload_name}' workload within the '{capacity_name}' capacity has been updated accordingly."
    )
