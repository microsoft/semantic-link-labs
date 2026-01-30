import pandas as pd
import sempy_labs._icons as icons
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_id,
    resolve_workspace_name_and_id,
    _update_dataframe_datatypes,
    _base_api,
    _create_dataframe,
)
from uuid import UUID
from sempy._utils._log import log


@log
def list_custom_pools(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Lists all `custom pools <https://learn.microsoft.com/fabric/data-engineering/create-custom-spark-pools>`_ within a workspace.

    This is a wrapper function for the following API: `Custom Pools - List Workspace Custom Pools <https://learn.microsoft.com/rest/api/fabric/spark/custom-pools/list-workspace-custom-pools>`_.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The name of the Fabric workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing all the custom pools within the Fabric workspace.
    """

    workspace_id = resolve_workspace_id(workspace)

    columns = {
        "Custom Pool ID": "string",
        "Custom Pool Name": "string",
        "Type": "string",
        "Node Family": "string",
        "Node Size": "string",
        "Auto Scale Enabled": "bool",
        "Auto Scale Min Node Count": "int",
        "Auto Scale Max Node Count": "int",
        "Dynamic Executor Allocation Enabled": "bool",
        "Dynamic Executor Allocation Min Executors": "int",
        "Dynamic Executor Allocation Max Executors": "int",
    }
    df = _create_dataframe(columns=columns)

    response = _base_api(request=f"/v1/workspaces/{workspace_id}/spark/pools")

    rows = []
    for i in response.json()["value"]:

        aScale = i.get("autoScale", {})
        d = i.get("dynamicExecutorAllocation", {})

        rows.append(
            {
                "Custom Pool ID": i.get("id"),
                "Custom Pool Name": i.get("name"),
                "Type": i.get("type"),
                "Node Family": i.get("nodeFamily"),
                "Node Size": i.get("nodeSize"),
                "Auto Scale Enabled": aScale.get("enabled"),
                "Auto Scale Min Node Count": aScale.get("minNodeCount"),
                "Auto Scale Max Node Count": aScale.get("maxNodeCount"),
                "Dynamic Executor Allocation Enabled": d.get("enabled"),
                "Dynamic Executor Allocation Min Executors": d.get("minExecutors"),
                "Dynamic Executor Allocation Max Executors": d.get("maxExecutors"),
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
def create_custom_pool(
    pool_name: str,
    node_size: str,
    min_node_count: int,
    max_node_count: int,
    min_executors: int,
    max_executors: int,
    node_family: str = "MemoryOptimized",
    auto_scale_enabled: bool = True,
    dynamic_executor_allocation_enabled: bool = True,
    workspace: Optional[str | UUID] = None,
):
    """
    Creates a `custom pool <https://learn.microsoft.com/fabric/data-engineering/create-custom-spark-pools>`_ within a workspace.

    This is a wrapper function for the following API: `Custom Pools - Create Workspace Custom Pool <https://learn.microsoft.com/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool>`_.

    Parameters
    ----------
    pool_name : str
        The custom pool name.
    node_size : str
        The `node size <https://learn.microsoft.com/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#nodesize>`_.
    min_node_count : int
        The `minimum node count <https://learn.microsoft.com/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#autoscaleproperties>`_.
    max_node_count : int
        The maximum node count.
    min_executors : int
        The `minimum executors <https://learn.microsoft.com/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#dynamicexecutorallocationproperties>`_.
    max_executors : int
        The maximum executors.
    node_family : str, default='MemoryOptimized'
        The `node family <https://learn.microsoft.com/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#nodefamily>`_.
    auto_scale_enabled : bool, default=True
        The status of `auto scale <https://learn.microsoft.com/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#autoscaleproperties>`_.
    dynamic_executor_allocation_enabled : bool, default=True
        The status of the `dynamic executor allocation <https://learn.microsoft.com/en-us/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#dynamicexecutorallocationproperties>`_.
    workspace : str | uuid.UUID, default=None
        The name or ID of the Fabric workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    payload = {
        "name": pool_name,
        "nodeFamily": node_family,
        "nodeSize": node_size,
        "autoScale": {
            "enabled": auto_scale_enabled,
            "minNodeCount": min_node_count,
            "maxNodeCount": max_node_count,
        },
        "dynamicExecutorAllocation": {
            "enabled": dynamic_executor_allocation_enabled,
            "minExecutors": min_executors,
            "maxExecutors": max_executors,
        },
    }

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/spark/pools",
        payload=payload,
        method="post",
        status_codes=201,
    )
    print(
        f"{icons.green_dot} The '{pool_name}' spark pool has been created within the '{workspace_name}' workspace."
    )


@log
def update_custom_pool(
    pool_name: str,
    node_size: Optional[str] = None,
    min_node_count: Optional[int] = None,
    max_node_count: Optional[int] = None,
    min_executors: Optional[int] = None,
    max_executors: Optional[int] = None,
    node_family: Optional[str] = None,
    auto_scale_enabled: Optional[bool] = None,
    dynamic_executor_allocation_enabled: Optional[bool] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Updates the properties of a `custom pool <https://learn.microsoft.com/fabric/data-engineering/create-custom-spark-pools>`_ within a workspace.

    This is a wrapper function for the following API: `Custom Pools - Update Workspace Custom Pool <https://learn.microsoft.com/rest/api/fabric/spark/custom-pools/update-workspace-custom-pool>`_.

    Parameters
    ----------
    pool_name : str
        The custom pool name.
    node_size : str, default=None
        The `node size <https://learn.microsoft.com/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#nodesize>`_.
        Defaults to None which keeps the existing property setting.
    min_node_count : int, default=None
        The `minimum node count <https://learn.microsoft.com/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#autoscaleproperties>`_.
        Defaults to None which keeps the existing property setting.
    max_node_count : int, default=None
        The maximum node count.
        Defaults to None which keeps the existing property setting.
    min_executors : int, default=None
        The `minimum executors <https://learn.microsoft.com/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#dynamicexecutorallocationproperties>`_.
        Defaults to None which keeps the existing property setting.
    max_executors : int, default=None
        The maximum executors.
        Defaults to None which keeps the existing property setting.
    node_family : str, default=None
        The `node family <https://learn.microsoft.com/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#nodefamily>`_.
        Defaults to None which keeps the existing property setting.
    auto_scale_enabled : bool, default=None
        The status of `auto scale <https://learn.microsoft.com/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#autoscaleproperties>`_.
        Defaults to None which keeps the existing property setting.
    dynamic_executor_allocation_enabled : bool, default=None
        The status of the `dynamic executor allocation <https://learn.microsoft.com/en-us/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#dynamicexecutorallocationproperties>`_.
        Defaults to None which keeps the existing property setting.
    workspace : str | uuid.UUID, default=None
        The name or ID of the Fabric workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    df = list_custom_pools(workspace=workspace)
    df_pool = df[df["Custom Pool Name"] == pool_name]

    if len(df_pool) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{pool_name}' custom pool does not exist within the '{workspace_name}'. Please choose a valid custom pool."
        )

    if node_family is None:
        node_family = df_pool["Node Family"].iloc[0]
    if node_size is None:
        node_size = df_pool["Node Size"].iloc[0]
    if auto_scale_enabled is None:
        auto_scale_enabled = bool(df_pool["Auto Scale Enabled"].iloc[0])
    if min_node_count is None:
        min_node_count = int(df_pool["Min Node Count"].iloc[0])
    if max_node_count is None:
        max_node_count = int(df_pool["Max Node Count"].iloc[0])
    if dynamic_executor_allocation_enabled is None:
        dynamic_executor_allocation_enabled = bool(
            df_pool["Dynami Executor Allocation Enabled"].iloc[0]
        )
    if min_executors is None:
        min_executors = int(df_pool["Min Executors"].iloc[0])
    if max_executors is None:
        max_executors = int(df_pool["Max Executors"].iloc[0])

    payload = {
        "name": pool_name,
        "nodeFamily": node_family,
        "nodeSize": node_size,
        "autoScale": {
            "enabled": auto_scale_enabled,
            "minNodeCount": min_node_count,
            "maxNodeCount": max_node_count,
        },
        "dynamicExecutorAllocation": {
            "enabled": dynamic_executor_allocation_enabled,
            "minExecutors": min_executors,
            "maxExecutors": max_executors,
        },
    }

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/spark/pools",
        payload=payload,
        method="post",
    )
    print(
        f"{icons.green_dot} The '{pool_name}' spark pool within the '{workspace_name}' workspace has been updated."
    )


@log
def delete_custom_pool(pool_name: str, workspace: Optional[str | UUID] = None):
    """
    Deletes a `custom pool <https://learn.microsoft.com/fabric/data-engineering/create-custom-spark-pools>`_ within a workspace.

    This is a wrapper function for the following API: `Custom Pools - Delete Workspace Custom Pool <https://learn.microsoft.com/rest/api/fabric/spark/custom-pools/delete-workspace-custom-pool>`_.

    Parameters
    ----------
    pool_name : str
        The custom pool name.
    workspace : str | uuid.UUID, default=None
        The name or ID of the Fabric workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    dfL = list_custom_pools(workspace=workspace_id)
    dfL_filt = dfL[dfL["Custom Pool Name"] == pool_name]

    if dfL_filt.empty:
        raise ValueError(
            f"{icons.red_dot} The '{pool_name}' custom pool does not exist within the '{workspace_name}' workspace."
        )
    pool_id = dfL_filt["Custom Pool ID"].iloc[0]

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/spark/pools/{pool_id}", method="delete"
    )
    print(
        f"{icons.green_dot} The '{pool_name}' spark pool has been deleted from the '{workspace_name}' workspace."
    )


@log
def get_spark_settings(
    workspace: Optional[str | UUID] = None, return_dataframe: bool = True
) -> pd.DataFrame | dict:
    """
    Shows the spark settings for a workspace.

    This is a wrapper function for the following API: `Workspace Settings - Get Spark Settings <https://learn.microsoft.com/rest/api/fabric/spark/workspace-settings/get-spark-settings>`_.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The name or ID of the Fabric workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    return_dataframe : bool, default=True
        If True, returns a pandas dataframe. If False, returns a json dictionary.

    Returns
    -------
    pandas.DataFrame | dict
        A pandas dataframe showing the spark settings for a workspace.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    df = pd.DataFrame(
        columns=[
            "Automatic Log Enabled",
            "High Concurrency Enabled",
            "Customize Compute Enabled",
            "Default Pool Name",
            "Default Pool Type",
            "Max Node Count",
            "Max Executors",
            "Environment Name",
            "Runtime Version",
        ]
    )

    response = _base_api(request=f"/v1/workspaces/{workspace_id}/spark/settings")

    i = response.json()
    p = i.get("pool")
    dp = i.get("pool", {}).get("defaultPool", {})
    sp = i.get("pool", {}).get("starterPool", {})
    e = i.get("environment", {})

    new_data = {
        "Automatic Log Enabled": i.get("automaticLog").get("enabled"),
        "High Concurrency Enabled": i.get("highConcurrency").get(
            "notebookInteractiveRunEnabled"
        ),
        "Customize Compute Enabled": p.get("customizeComputeEnabled"),
        "Default Pool Name": dp.get("name"),
        "Default Pool Type": dp.get("type"),
        "Max Node Count": sp.get("maxNodeCount"),
        "Max Node Executors": sp.get("maxExecutors"),
        "Environment Name": e.get("name"),
        "Runtime Version": e.get("runtimeVersion"),
    }
    df = pd.DataFrame([new_data])

    column_map = {
        "Automatic Log Enabled": "bool",
        "High Concurrency Enabled": "bool",
        "Customize Compute Enabled": "bool",
    }

    _update_dataframe_datatypes(dataframe=df, column_map=column_map)

    if return_dataframe:
        return df
    else:
        return response.json()


@log
def update_spark_settings(
    automatic_log_enabled: Optional[bool] = None,
    high_concurrency_enabled: Optional[bool] = None,
    customize_compute_enabled: Optional[bool] = None,
    default_pool_name: Optional[str] = None,
    max_node_count: Optional[int] = None,
    max_executors: Optional[int] = None,
    environment_name: Optional[str] = None,
    runtime_version: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Updates the spark settings for a workspace.

    This is a wrapper function for the following API: `Workspace Settings - Update Spark Settings <https://learn.microsoft.com/rest/api/fabric/spark/workspace-settings/update-spark-settings>`_.

    Parameters
    ----------
    automatic_log_enabled : bool, default=None
        The status of the `automatic log <https://learn.microsoft.com/rest/api/fabric/spark/workspace-settings/update-spark-settings?tabs=HTTP#automaticlogproperties>`_.
        Defaults to None which keeps the existing property setting.
    high_concurrency_enabled : bool, default=None
        The status of the `high concurrency <https://learn.microsoft.com/rest/api/fabric/spark/workspace-settings/update-spark-settings?tabs=HTTP#highconcurrencyproperties>`_ for notebook interactive run.
        Defaults to None which keeps the existing property setting.
    customize_compute_enabled : bool, default=None
        `Customize compute <https://learn.microsoft.com/rest/api/fabric/spark/workspace-settings/update-spark-settings?tabs=HTTP#poolproperties>`_ configurations for items.
        Defaults to None which keeps the existing property setting.
    default_pool_name : str, default=None
        `Default pool <https://learn.microsoft.com/rest/api/fabric/spark/workspace-settings/update-spark-settings?tabs=HTTP#poolproperties>`_ for workspace.
        Defaults to None which keeps the existing property setting.
    max_node_count : int, default=None
        The maximum node count.
        Defaults to None which keeps the existing property setting.
    max_executors : int, default=None
        The maximum executors.
        Defaults to None which keeps the existing property setting.
    environment_name : str, default=None
        The name of the `default environment <https://learn.microsoft.com/rest/api/fabric/spark/workspace-settings/update-spark-settings?tabs=HTTP#environmentproperties>`_. Empty string indicated there is no workspace default environment
        Defaults to None which keeps the existing property setting.
    runtime_version : str, default=None
        The `runtime version <https://learn.microsoft.com/rest/api/fabric/spark/workspace-settings/update-spark-settings?tabs=HTTP#environmentproperties>`_.
        Defaults to None which keeps the existing property setting.
    workspace : str | uuid.UUID, default=None
        The name or ID of the Fabric workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    payload = get_spark_settings(workspace=workspace, return_dataframe=False)

    if automatic_log_enabled is not None:
        payload["automaticLog"]["enabled"] = automatic_log_enabled
    if high_concurrency_enabled is not None:
        payload["highConcurrency"][
            "notebookInteractiveRunEnabled"
        ] = high_concurrency_enabled
    if customize_compute_enabled is not None:
        payload["pool"]["customizeComputeEnabled"] = customize_compute_enabled
    if default_pool_name is not None:
        payload["pool"]["defaultPool"]["name"] = default_pool_name
    if max_node_count is not None:
        payload["pool"]["starterPool"]["maxNodeCount"] = max_node_count
    if max_executors is not None:
        payload["pool"]["starterPool"]["maxExecutors"] = max_executors
    if environment_name is not None:
        payload["environment"]["name"] = environment_name
    if runtime_version is not None:
        payload["environment"]["runtimeVersion"] = runtime_version

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/spark/settings",
        payload=payload,
        method="patch",
    )
    print(
        f"{icons.green_dot} The spark settings within the '{workspace_name}' workspace have been updated accordingly."
    )
