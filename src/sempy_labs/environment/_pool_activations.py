import pandas as pd
from typing import Optional
from urllib.parse import quote
from uuid import UUID
from sempy._utils._log import log
from sempy_labs._helper_functions import (
    resolve_workspace_id,
    resolve_item_id,
    _base_api,
    _create_dataframe,
    _update_dataframe_datatypes,
)

_pool_activation_states = [
    "NotStarted",
    "InProgress",
    "Succeeded",
    "Failed",
    "Cancelled",
    "Unknown",
]

_pool_activation_columns = {
    "Pool Activation Id": "string",
    "Pool Activation Name": "string",
    "Capacity Id": "string",
    "State": "string",
    "Scheduled Start Time": "datetime_coerce",
    "Start Time": "datetime_coerce",
    "Scheduled End Time": "datetime_coerce",
    "End Time": "datetime_coerce",
    "Max Clusters To Hydrate": "int_fillna",
    "Cluster Idle Timeout": "string",
    "Custom Live Pool Lifespan": "string",
    "Is Recurring": "bool",
    "Job Type": "string",
    "Runtime Version": "string",
    "Cancellation Reason": "string",
}


def _parse_pool_activation(v: dict) -> dict:
    return {
        "Pool Activation Id": v.get("jobInstanceId"),
        "Pool Activation Name": v.get("name"),
        "Capacity Id": v.get("capacityId"),
        "State": v.get("state"),
        "Scheduled Start Time": v.get("scheduledStartDateTime"),
        "Start Time": v.get("startDateTime"),
        "Scheduled End Time": v.get("scheduledEndDateTime"),
        "End Time": v.get("endDateTime"),
        "Max Clusters To Hydrate": v.get("maxClustersToHydrate"),
        "Cluster Idle Timeout": v.get("clusterIdleTimeout"),
        "Custom Live Pool Lifespan": v.get("customLivePoolLifespan"),
        "Is Recurring": v.get("isRecurring"),
        "Job Type": v.get("jobType"),
        "Runtime Version": v.get("runtimeVersion"),
        "Cancellation Reason": v.get("cancellationReason"),
    }


@log
def list_pool_activations(
    environment: str | UUID,
    workspace: Optional[str | UUID] = None,
    state: Optional[str] = None,
) -> pd.DataFrame:
    """
    Lists the pool activations of a Fabric environment.

    This is a wrapper function for the following API: `Pool Activations - List Pool Activations <https://learn.microsoft.com/rest/api/fabric/environment/pool-activations/list-pool-activations>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    environment : str | uuid.UUID
        Name or ID of the environment.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    state : str, default=None
        Filters the pool activations by state. Valid values: 'NotStarted', 'InProgress', 'Succeeded', 'Failed', 'Cancelled', 'Unknown'.
        Defaults to None which returns pool activations in all states.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the pool activations of the environment.
    """

    if state is not None:
        state_map = {s.lower(): s for s in _pool_activation_states}
        state = state_map.get(state.lower())
        if state is None:
            raise ValueError(
                f"Invalid 'state' value. Valid values: {_pool_activation_states}."
            )

    df = _create_dataframe(columns=_pool_activation_columns)

    workspace_id = resolve_workspace_id(workspace)
    item_id = resolve_item_id(
        item=environment, type="Environment", workspace=workspace_id
    )

    url = f"/v1/workspaces/{workspace_id}/environments/{item_id}/poolActivations"
    if state is not None:
        filter_expr = quote(f"state eq '{state}'")
        url += f"?filter={filter_expr}"

    responses = _base_api(
        request=url,
        uses_pagination=True,
        client="fabric_sp",
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            rows.append(_parse_pool_activation(v))

    if rows:
        df = pd.DataFrame(rows, columns=list(_pool_activation_columns.keys()))
        _update_dataframe_datatypes(df, _pool_activation_columns)

    return df


@log
def get_pool_activation(
    environment: str | UUID,
    pool_activation_id: UUID,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Gets the details of a pool activation of a Fabric environment, including its Spark configuration and resource status summary.

    This is a wrapper function for the following API: `Pool Activations - Get Pool Activation <https://learn.microsoft.com/rest/api/fabric/environment/pool-activations/get-pool-activation>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    environment : str | uuid.UUID
        Name or ID of the environment.
    pool_activation_id : uuid.UUID
        The pool activation ID (the 'Pool Activation Id' column returned by :func:`list_pool_activations`).
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the details of the pool activation.
    """

    columns = {
        **_pool_activation_columns,
        "Pool Name": "string",
        "Driver Cores": "int_fillna",
        "Driver Memory": "string",
        "Executor Cores": "int_fillna",
        "Executor Memory": "string",
        "Num Executors": "int_fillna",
        "Dynamic Allocation Enabled": "bool",
        "Resources Starting": "int_fillna",
        "Resources Ready": "int_fillna",
        "Resources Acquired": "int_fillna",
        "Resources Stopping": "int_fillna",
        "Resources Deactivated": "int_fillna",
        "Resources Expired": "int_fillna",
        "Resources Cancelled": "int_fillna",
        "Resources Failed": "int_fillna",
    }

    workspace_id = resolve_workspace_id(workspace)
    item_id = resolve_item_id(
        item=environment, type="Environment", workspace=workspace_id
    )

    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/environments/{item_id}/poolActivations/{pool_activation_id}",
        client="fabric_sp",
    ).json()

    spark = response.get("sparkConfiguration") or {}
    status = response.get("resourceStatusSummary") or {}

    row = {
        **_parse_pool_activation(response),
        "Pool Name": response.get("poolName"),
        "Driver Cores": spark.get("driverCores"),
        "Driver Memory": spark.get("driverMemory"),
        "Executor Cores": spark.get("executorCores"),
        "Executor Memory": spark.get("executorMemory"),
        "Num Executors": spark.get("numExecutors"),
        "Dynamic Allocation Enabled": spark.get("isDynamicAllocationEnabled"),
        "Resources Starting": status.get("starting"),
        "Resources Ready": status.get("ready"),
        "Resources Acquired": status.get("acquired"),
        "Resources Stopping": status.get("stopping"),
        "Resources Deactivated": status.get("deactivated"),
        "Resources Expired": status.get("expired"),
        "Resources Cancelled": status.get("cancelled"),
        "Resources Failed": status.get("failed"),
    }

    df = pd.DataFrame([row], columns=list(columns.keys()))
    _update_dataframe_datatypes(df, columns)

    return df
