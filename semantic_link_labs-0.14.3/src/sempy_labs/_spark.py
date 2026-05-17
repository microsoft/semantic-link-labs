import pandas as pd
from typing import Optional
from uuid import UUID
from sempy._utils._log import log
import sempy_labs.spark as sp


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

    return sp.list_custom_pools(workspace=workspace)


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

    sp.create_custom_pool(
        pool_name=pool_name,
        node_size=node_size,
        min_node_count=min_node_count,
        max_node_count=max_node_count,
        min_executors=min_executors,
        max_executors=max_executors,
        node_family=node_family,
        auto_scale_enabled=auto_scale_enabled,
        dynamic_executor_allocation_enabled=dynamic_executor_allocation_enabled,
        workspace=workspace,
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

    sp.update_custom_pool(
        pool_name=pool_name,
        node_size=node_size,
        min_node_count=min_node_count,
        max_node_count=max_node_count,
        min_executors=min_executors,
        max_executors=max_executors,
        node_family=node_family,
        auto_scale_enabled=auto_scale_enabled,
        dynamic_executor_allocation_enabled=dynamic_executor_allocation_enabled,
        workspace=workspace,
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

    sp.delete_custom_pool(pool_name=pool_name, workspace=workspace)


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

    return sp.get_spark_settings(workspace=workspace, return_dataframe=return_dataframe)


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

    sp.update_spark_settings(
        automatic_log_enabled=automatic_log_enabled,
        high_concurrency_enabled=high_concurrency_enabled,
        customize_compute_enabled=customize_compute_enabled,
        default_pool_name=default_pool_name,
        max_node_count=max_node_count,
        max_executors=max_executors,
        environment_name=environment_name,
        runtime_version=runtime_version,
        workspace=workspace,
    )
