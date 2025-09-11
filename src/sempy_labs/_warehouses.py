from sempy_labs._helper_functions import (
    resolve_item_id,
    resolve_workspace_name_and_id,
    _base_api,
    _create_dataframe,
    _update_dataframe_datatypes,
    delete_item,
    resolve_workspace_id,
    resolve_item_name_and_id,
)
import pandas as pd
from typing import Optional, List
import sempy_labs._icons as icons
from uuid import UUID
from sempy._utils._log import log


@log
def create_warehouse(
    warehouse: str,
    description: Optional[str] = None,
    case_insensitive_collation: bool = False,
    workspace: Optional[str | UUID] = None,
) -> UUID:
    """
    Creates a Fabric warehouse.

    This is a wrapper function for the following API: `Items - Create Warehouse <https://learn.microsoft.com/rest/api/fabric/warehouse/items/create-warehouse>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    warehouse: str
        Name of the warehouse.
    description : str, default=None
        A description of the warehouse.
    case_insensitive_collation: bool, default=False
        If True, creates the warehouse with case-insensitive collation.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    uuid.UUID
        The ID of the created warehouse.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    payload = {"displayName": warehouse}

    if description:
        payload["description"] = description
    if case_insensitive_collation:
        payload.setdefault("creationPayload", {})
        payload["creationPayload"][
            "defaultCollation"
        ] = "Latin1_General_100_CI_AS_KS_WS_SC_UTF8"

    result = _base_api(
        request=f"/v1/workspaces/{workspace_id}/warehouses",
        payload=payload,
        method="post",
        lro_return_json=True,
        status_codes=[201, 202],
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The '{warehouse}' warehouse has been created within the '{workspace_name}' workspace."
    )

    return result.get("id")


@log
def list_warehouses(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows the warehouses within a workspace.

    This is a wrapper function for the following API: `Items - List Warehouses <https://learn.microsoft.com/rest/api/fabric/warehouse/items/list-warehouses>`_.

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
        A pandas dataframe showing the warehouses within a workspace.
    """

    columns = {
        "Warehouse Name": "string",
        "Warehouse Id": "string",
        "Description": "string",
        "Connection Info": "string",
        "Created Date": "datetime",
        "Last Updated Time": "datetime",
    }
    df = _create_dataframe(columns=columns)

    workspace_id = resolve_workspace_id(workspace)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/warehouses",
        uses_pagination=True,
        client="fabric_sp",
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            prop = v.get("properties", {})

            rows.append(
                {
                    "Warehouse Name": v.get("displayName"),
                    "Warehouse Id": v.get("id"),
                    "Description": v.get("description"),
                    "Connection Info": prop.get("connectionInfo"),
                    "Created Date": prop.get("createdDate"),
                    "Last Updated Time": prop.get("lastUpdatedTime"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
def delete_warehouse(name: str | UUID, workspace: Optional[str | UUID] = None):
    """
    Deletes a Fabric warehouse.

    This is a wrapper function for the following API: `Items - Delete Warehouse <https://learn.microsoft.com/rest/api/fabric/warehouse/items/delete-warehouse>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    name: str | uuid.UUID
        Name or ID of the warehouse.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    delete_item(item=name, type="Warehouse", workspace=workspace)


@log
def get_warehouse_tables(
    warehouse: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Shows a list of the tables in the Fabric warehouse. This function is based on INFORMATION_SCHEMA.TABLES.

    Parameters
    ----------
    warehouse : str | uuid.UUID
        Name or ID of the Fabric warehouse.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of the tables in the Fabric warehouse.
    """

    from sempy_labs._sql import ConnectWarehouse

    with ConnectWarehouse(warehouse=warehouse, workspace=workspace) as sql:
        df = sql.query(
            """
        SELECT TABLE_SCHEMA AS [Schema], TABLE_NAME AS [Table Name], TABLE_TYPE AS [Table Type]
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        """
        )

    return df


@log
def get_warehouse_columns(
    warehouse: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Shows a list of the columns in each table within the Fabric warehouse. This function is based on INFORMATION_SCHEMA.COLUMNS.

    Parameters
    ----------
    warehouse : str | uuid.UUID
        Name or ID of the Fabric warehouse.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of the columns in each table within the Fabric warehouse.
    """

    from sempy_labs._sql import ConnectWarehouse

    with ConnectWarehouse(warehouse=warehouse, workspace=workspace) as sql:
        df = sql.query(
            """
        SELECT t.TABLE_SCHEMA AS [Schema], t.TABLE_NAME AS [Table Name], c.COLUMN_NAME AS [Column Name], c.DATA_TYPE AS [Data Type], c.IS_NULLABLE AS [Is Nullable], c.CHARACTER_MAXIMUM_LENGTH AS [Character Max Length]
        FROM INFORMATION_SCHEMA.TABLES AS t
        LEFT JOIN INFORMATION_SCHEMA.COLUMNS AS c
        ON t.TABLE_NAME = c.TABLE_NAME
        AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
        WHERE t.TABLE_TYPE = 'BASE TABLE'
        """
        )

    return df


@log
def get_warehouse_connection_string(
    warehouse: str | UUID,
    workspace: Optional[str | UUID] = None,
    guest_tenant_id: Optional[UUID] = None,
    private_link_type: Optional[str] = None,
) -> str:
    """
    Returns the SQL connection string of the specified warehouse.

    Parameters
    ----------
    warehouse : str | uuid.UUID
        Name or ID of the Fabric warehouse.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    guest_tenant_id : uuid.UUID, default=None
        The guest tenant ID if the end user's tenant is different from the warehouse tenant.
    private_link_type : str, default=None
        Indicates the type of private link this connection string uses. Must be either 'Workspace' or 'None' or left as None.

    Returns
    -------
    str
        Returns the SQL connection string of the specified warehouse.
    """
    workspace_id = resolve_workspace_id(workspace)
    warehouse_id = resolve_item_id(
        item=warehouse, type="Warehouse", workspace=workspace
    )

    url = f"/v1/workspaces/{workspace_id}/warehouses/{warehouse_id}/connectionString"

    if private_link_type is not None and private_link_type not in ["Workspace", "None"]:
        raise ValueError(
            f"{icons.red_dot} private_link_type must be 'Workspace' or 'None' or left as None."
        )

    if guest_tenant_id or private_link_type:
        params = []
        if guest_tenant_id:
            params.append(f"guestTenantId={guest_tenant_id}")
        if private_link_type:
            params.append(f"privateLinkType={private_link_type}")
        param_str = "?" + "&".join(params)
        url += param_str

    response = _base_api(request=url, client="fabric_sp")

    return response.json().get("connectionString")


@log
def get_warehouse_sql_audit_settings(
    warehouse: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Shows the SQL audit settings of a Fabric warehouse.

    This is a wrapper function for the following API: `SQL Audit Settings - Get SQL Audit Settings <https://learn.microsoft.com/rest/api/fabric/warehouse/sql-audit-settings/get-sql-audit-settings>`_.

    Service Principal Authentication is supported (see `here <https://learn.microsoft.com/rest/api/fabric/warehouse/sql-audit-settings/get-sql-audit-settings#service-principal-authentication>`_).

    Parameters
    ----------
    warehouse : str | uuid.UUID
        Name or ID of the Fabric warehouse.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe containing the SQL audit settings of the specified warehouse.
    """
    workspace_id = resolve_workspace_id(workspace)
    warehouse_id = resolve_item_id(
        item=warehouse, type="Warehouse", workspace=workspace
    )

    columns = {
        "State": "string",
        "Retention Days": "int",
        "Audit Actions And Group": "list",
    }

    df = _create_dataframe(columns=columns)

    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/warehouses/{warehouse_id}/settings/sqlAudit",
        client="fabric_sp",
    ).json()

    rows = []
    rows.append(
        {
            "State": response.get("state"),
            "Retention Days": response.get("retentionDays"),
            "Audit Actions And Group": response.get("auditActionsAndGroups"),
        }
    )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
def update_warehouse_sql_audit_settings(
    warehouse: str | UUID,
    workspace: Optional[str | UUID] = None,
    retention_days: Optional[int] = None,
    state: Optional[str] = None,
):
    """
    Update settings associated with the warehouse.

    This is a wrapper function for the following API: SQL Audit Settings - Update SQL Audit Settings <https://learn.microsoft.com/rest/api/fabric/warehouse/sql-audit-settings/update-sql-audit-settings>`_.

    Service Principal Authentication is supported (see `here <https://learn.microsoft.com/rest/api/fabric/warehouse/sql-audit-settings/get-sql-audit-settings#service-principal-authentication>`_).

    Parameters
    ----------
    warehouse : str | uuid.UUID
        Name or ID of the Fabric warehouse.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (warehouse_name, warehouse_id) = resolve_item_name_and_id(
        item=warehouse, type="Warehouse", workspace=workspace
    )

    payload = {}
    if retention_days is not None:
        if not isinstance(retention_days, int) or retention_days < 0:
            raise ValueError(
                f"{icons.red_dot} retention_days must be a non-negative integer."
            )
        payload["retentionDays"] = retention_days
    if state is not None:
        state = state.capitalize()
        if state not in ["Enabled", "Disabled"]:
            raise ValueError(
                f"{icons.red_dot} state must be either 'Enabled' or 'Disabled'."
            )
        payload["state"] = state

    if not payload:
        print(
            f"{icons.info} No updates were made as neither retention_days nor state were provided."
        )
        return

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/warehouses/{warehouse_id}/settings/sqlAudit",
        client="fabric_sp",
        method="patch",
        payload=payload,
    )

    print(
        f"{icons.green_dot} The SQL audit settings for the '{warehouse_name}' warehouse within the '{workspace_name}' workspace have been updated accordingly."
    )


@log
def set_warehouse_audit_actions_and_group(
    warehouse: str | UUID,
    sql_audit_groups: List[str],
    workspace: Optional[str | UUID] = None,
):
    """
    Update the audit actions and groups for this warehouse.

    This is a wrapper function for the following API: SQL Audit Settings - Set Audit Actions And Groups <https://learn.microsoft.com/rest/api/fabric/warehouse/sql-audit-settings/set-audit-actions-and-groups>`_.

    Service Principal Authentication is supported (see `here <https://learn.microsoft.com/rest/api/fabric/warehouse/sql-audit-settings/get-sql-audit-settings#service-principal-authentication>`_).

    Parameters
    ----------
    warehouse : str | uuid.UUID
        Name or ID of the Fabric warehouse.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (warehouse_name, warehouse_id) = resolve_item_name_and_id(
        item=warehouse, type="Warehouse", workspace=workspace
    )
    if (
        not sql_audit_groups
        or not isinstance(sql_audit_groups, list)
        or not all(isinstance(item, str) for item in sql_audit_groups)
    ):
        raise ValueError(
            f"{icons.red_dot} sql_audit_groups must be a non-empty list of strings."
        )

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/warehouses/{warehouse_id}/settings/sqlAudit/setAuditActionsAndGroups",
        client="fabric_sp",
        method="post",
        payload=sql_audit_groups,
    )

    print(
        f"{icons.green_dot} The SQL audit actions and groups for the '{warehouse_name}' warehouse within the '{workspace_name}' workspace have been updated accordingly."
    )
