from sempy_labs._helper_functions import (
    resolve_item_id,
    resolve_workspace_name_and_id,
    _base_api,
    _create_dataframe,
    _update_dataframe_datatypes,
    resolve_workspace_id,
)
import pandas as pd
from typing import Optional, List, Literal
import sempy_labs._icons as icons
from uuid import UUID
from sempy._utils._log import log


def _get_base_url(item, type, workspace):

    workspace_id = resolve_workspace_id(workspace)
    item_id = resolve_item_id(item=item, type=type, workspace=workspace)

    type_dict = {
        "Warehouse": "warehouses",
        "SQLEndpoint": "sqlEndpoints",
    }
    type_for_url = type_dict.get(type)

    if type in ["SQLEndpoint", "Warehouse"]:
        url = f"/v1/workspaces/{workspace_id}/{type_for_url}/{item_id}"
    else:
        raise ValueError(
            f"{icons.red_dot} The type must be 'Warehouse' or 'SQLEndpoint'."
        )

    return url


@log
def get_sql_audit_settings(
    item: str | UUID,
    type: Literal["Warehouse", "SQLEndpoint"],
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Shows the SQL audit settings of a Fabric item.

    This is a wrapper function for the following API: `SQL Audit Settings - Get SQL Audit Settings <https://learn.microsoft.com/rest/api/fabric/warehouse/sql-audit-settings/get-sql-audit-settings>`_.

    Service Principal Authentication is supported (see `here <https://learn.microsoft.com/rest/api/fabric/warehouse/sql-audit-settings/get-sql-audit-settings#service-principal-authentication>`_).

    Parameters
    ----------
    item : str | uuid.UUID
        The name or ID of the item (Warehouse or SQLEndpoint).
    type : Literal['Warehouse', 'SQLEndpoint']
        The type of the item. Must be 'Warehouse' or 'SQLEndpoint'.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe containing the SQL audit settings of the specified warehouse.
    """

    columns = {
        "State": "string",
        "Retention Days": "int",
        "Audit Actions And Group": "list",
    }

    df = _create_dataframe(columns=columns)

    url = _get_base_url(item=item, type=type, workspace=workspace)
    response = _base_api(
        request=f"{url}/settings/sqlAudit",
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
def update_sql_audit_settings(
    item: str | UUID,
    type: Literal["Warehouse", "SQLEndpoint"],
    workspace: Optional[str | UUID] = None,
    retention_days: Optional[int] = None,
    state: Optional[str] = None,
):
    """
    Update settings associated with the item.

    This is a wrapper function for the following API: SQL Audit Settings - Update SQL Audit Settings <https://learn.microsoft.com/rest/api/fabric/warehouse/sql-audit-settings/update-sql-audit-settings>`_.

    Service Principal Authentication is supported (see `here <https://learn.microsoft.com/rest/api/fabric/warehouse/sql-audit-settings/get-sql-audit-settings#service-principal-authentication>`_).

    Parameters
    ----------
    item : str | uuid.UUID
        The name or ID of the item (Warehouse or SQLEndpoint).
    type : Literal['Warehouse', 'SQLEndpoint']
        The type of the item. Must be 'Warehouse' or 'SQLEndpoint'.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

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

    url = _get_base_url(item=item, type=type, workspace=workspace)
    _base_api(
        request=f"{url}/settings/sqlAudit",
        client="fabric_sp",
        method="patch",
        payload=payload,
    )

    print(
        f"{icons.green_dot} The SQL audit settings for the '{item}' {type.lower()} within the '{workspace_name}' workspace have been updated accordingly."
    )


@log
def set_audit_actions_and_group(
    item: str | UUID,
    type: Literal["Warehouse", "SQLEndpoint"],
    sql_audit_groups: List[str],
    workspace: Optional[str | UUID] = None,
):
    """
    Update the audit actions and groups for this item.

    This is a wrapper function for the following API: SQL Audit Settings - Set Audit Actions And Groups <https://learn.microsoft.com/rest/api/fabric/warehouse/sql-audit-settings/set-audit-actions-and-groups>`_.

    Service Principal Authentication is supported (see `here <https://learn.microsoft.com/rest/api/fabric/warehouse/sql-audit-settings/get-sql-audit-settings#service-principal-authentication>`_).

    Parameters
    ----------
    item : str | uuid.UUID
        The name or ID of the item (Warehouse or SQLEndpoint).
    type : Literal['Warehouse', 'SQLEndpoint']
        The type of the item. Must be 'Warehouse' or 'SQLEndpoint'.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    if (
        not sql_audit_groups
        or not isinstance(sql_audit_groups, list)
        or not all(isinstance(item, str) for item in sql_audit_groups)
    ):
        raise ValueError(
            f"{icons.red_dot} sql_audit_groups must be a non-empty list of strings."
        )

    url = _get_base_url(item=item, type=type, workspace=workspace)
    _base_api(
        request=f"{url}/settings/sqlAudit/setAuditActionsAndGroups",
        client="fabric_sp",
        method="post",
        payload=sql_audit_groups,
    )

    print(
        f"{icons.green_dot} The SQL audit actions and groups for the '{item}' {type.lower()} within the '{workspace_name}' workspace have been updated accordingly."
    )
