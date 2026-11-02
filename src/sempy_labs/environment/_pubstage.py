import pandas as pd
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_id,
    _base_api,
    _create_dataframe,
    resolve_item_id,
    _update_dataframe_datatypes,
)
from uuid import UUID
from sempy._utils._log import log


def _get_spark_compute(
    environment: str | UUID,
    workspace: Optional[str | UUID] = None,
    staging: bool = False,
) -> pd.DataFrame:

    columns = {
        "Instance Pool Name": "string",
        "Instance Pool Type": "string",
        "Instance Pool Id": "string",
        "Driver Cores": "int",
        "Driver Memory": "string",
        "Executor Cores": "int",
        "Executor Memory": "string",
        "Dynamic Executor Allocation Enabled": "bool",
        "Dynamic Executor Allocation Min Executors": "int",
        "Dynamic Executor Allocation Max Executors": "int",
        "Spark Properties": "string",
        "Runtime Version": "string",
    }

    df = _create_dataframe(columns=columns)

    workspace_id = resolve_workspace_id(workspace)
    item_id = resolve_item_id(
        item=environment, type="Environment", workspace=workspace_id
    )

    url = f"/v1/workspaces/{workspace_id}/environments/{item_id}/sparkCompute"
    if staging:
        url = (
            f"/v1/workspaces/{workspace_id}/environments/{item_id}/staging/sparkCompute"
        )

    response = _base_api(
        request=url,
        client="fabric_sp",
    )

    rows = []
    rows.append(
        {
            "Instance Pool Name": response.get("instancePool", {}).get("name"),
            "Instance Pool Type": response.get("instancePool", {}).get("type"),
            "Instance Pool Id": response.get("instancePool", {}).get("id"),
            "Driver Cores": response.get("driverCores"),
            "Driver Memory": response.get("driverMemory"),
            "Executor Cores": response.get("executorCores"),
            "Executor Memory": response.get("executorMemory"),
            "Dynamic Executor Allocation Enabled": response.get(
                "dynamicExecutorAllocation", {}
            ).get("enabled"),
            "Dynamic Executor Allocation Min Executors": response.get(
                "dynamicExecutorAllocation", {}
            ).get("minExecutors"),
            "Dynamic Executor Allocation Max Executors": response.get(
                "dynamicExecutorAllocation", {}
            ).get("maxExecutors"),
            "Spark Properties": response.get("sparkProperties"),
            "Runtime Version": response.get("runtimeVersion"),
        }
    )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(df, columns)

    return df


@log
def get_published_spark_compute(
    environment: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Gets the Spark compute of a published Fabric environment.

    This is a wrapper function for the following API: `Published - Get Spark Compute <https://learn.microsoft.com/rest/api/fabric/environment/published/get-spark-compute>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    environment: str | uuid.UUID
        Name or ID of the environment.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    return _get_spark_compute(environment, workspace, staging=False)


@log
def get_staging_spark_compute(
    environment: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Gets the Spark compute of a staging Fabric environment.

    This is a wrapper function for the following API: `Staging - Get Spark Compute <https://learn.microsoft.com/rest/api/fabric/environment/staging/get-spark-compute>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    environment: str | uuid.UUID
        Name or ID of the environment.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    return _get_spark_compute(environment, workspace, staging=True)


def _list_libraries(
    environment: str | UUID,
    workspace: Optional[str | UUID] = None,
    staging: bool = False,
) -> pd.DataFrame:

    columns = {
        "Library Name": "string",
        "Library Type": "string",
        "Library Version": "string",
    }

    df = _create_dataframe(columns=columns)

    workspace_id = resolve_workspace_id(workspace)
    item_id = resolve_item_id(
        item=environment, type="Environment", workspace=workspace_id
    )

    url = f"/v1/workspaces/{workspace_id}/environments/{item_id}/libraries"
    if staging:
        url = f"/v1/workspaces/{workspace_id}/environments/{item_id}/staging/libraries"

    responses = _base_api(
        request=url,
        client="fabric_sp",
        uses_pagination=True,
    )

    rows = []
    for r in responses:
        for lib in r.get("libraries", []):
            rows.append(
                {
                    "Library Name": lib.get("name"),
                    "Library Type": lib.get("libraryType"),
                    "Library Version": lib.get("version"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def list_published_libraries(
    environment: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Gets the published libraries of a Fabric environment.

    This is a wrapper function for the following API: `Published - List Libraries <https://learn.microsoft.com/rest/api/fabric/environment/published/list-libraries>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    environment: str | uuid.UUID
        Name or ID of the environment.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    return _list_libraries(environment, workspace, staging=False)


@log
def list_staging_libraries(
    environment: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Gets the staging libraries of a Fabric environment.

    This is a wrapper function for the following API: `Staging - List Libraries <https://learn.microsoft.com/rest/api/fabric/environment/staging/list-libraries>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    environment: str | uuid.UUID
        Name or ID of the environment.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    return _list_libraries(environment, workspace, staging=True)
