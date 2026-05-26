from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_workspace_id,
    _base_api,
    _create_dataframe,
    _update_dataframe_datatypes,
)
import pandas as pd
from typing import Optional, List, Dict, Any
from uuid import UUID
from sempy._utils._log import log
import sempy_labs._icons as icons


@log
def get_sql_pools_configuration(
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Gets the SQL Pools configuration in the specified workspace.

    This is a wrapper function for the following API: `SQL Pools - Get Sql Pools Configuration <https://learn.microsoft.com/rest/api/fabric/warehouse/sql-pools/get-sql-pools-configuration>`_.

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
        A pandas dataframe showing the SQL pools configuration for the specified workspace.
    """

    workspace_id = resolve_workspace_id(workspace)

    columns = {
        "Custom SQL Pools Enabled": "bool",
        "Name": "string",
        "Is Default": "bool",
        "Max Resource Percentage": "int",
        "Optimize For Reads": "bool",
        "Classifier Type": "string",
        "Classifier Value": "list",
    }
    df = _create_dataframe(columns=columns)

    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/warehouses/sqlPoolsConfiguration?beta=True",
    ).json()

    enabled = response.get("customSQLPoolsEnabled")
    rows = []
    for pool in response.get("customSQLPools", []):
        classifier = pool.get("classifier", {}) or {}
        rows.append(
            {
                "Custom SQL Pools Enabled": enabled,
                "Name": pool.get("name"),
                "Is Default": pool.get("isDefault"),
                "Max Resource Percentage": pool.get("maxResourcePercentage"),
                "Optimize For Reads": pool.get("optimizeForReads"),
                "Classifier Type": classifier.get("type"),
                "Classifier Value": classifier.get("value"),
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
def disable_sql_pools_configuration(
    workspace: Optional[str | UUID] = None,
):
    """
    Disables the SQL Pools configuration in the specified workspace.

    When disabled, the existing configuration is preserved and can be restored by re-enabling.

    This is a wrapper function for the following API: `SQL Pools - Update Sql Pools Configuration <https://learn.microsoft.com/rest/api/fabric/warehouse/sql-pools/update-sql-pools-configuration>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    payload = {
        "customSQLPoolsEnabled": False,
        "customSQLPools": [],
    }

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/warehouses/sqlPoolsConfiguration?beta=True",
        method="patch",
        payload=payload,
    )

    print(
        f"{icons.green_dot} The SQL Pools configuration has been disabled for the '{workspace_name}' workspace."
    )


@log
def enable_sql_pools_configuration(
    custom_sql_pools: Optional[List[Dict[str, Any]]] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Enables the SQL Pools configuration in the specified workspace.

    If `custom_sql_pools` is not provided, the previously saved configuration will be restored.
    If provided, the supplied pools will replace the existing configuration. Any existing SQL pool
    not included in the request will be deleted.

    This is a wrapper function for the following API: `SQL Pools - Update Sql Pools Configuration <https://learn.microsoft.com/rest/api/fabric/warehouse/sql-pools/update-sql-pools-configuration>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    custom_sql_pools : typing.List[dict], default=None
        A list of SQL pool definitions to apply. Each dict may contain the keys: ``name`` (str),
        ``isDefault`` (bool), ``maxResourcePercentage`` (int, 1-100), ``optimizeForReads`` (bool),
        and ``classifier`` (dict with ``type`` and ``value`` keys). If None, the previously
        saved configuration will be restored.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    payload: Dict[str, Any] = {"customSQLPoolsEnabled": True}
    if custom_sql_pools is not None:
        payload["customSQLPools"] = custom_sql_pools

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/warehouses/sqlPoolsConfiguration?beta=True",
        method="patch",
        payload=payload,
    )

    print(
        f"{icons.green_dot} The SQL Pools configuration has been enabled for the '{workspace_name}' workspace."
    )
