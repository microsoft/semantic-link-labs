from typing import Optional, Literal
from uuid import UUID
import pandas as pd
from sempy_labs._helper_functions import (
    _create_dataframe,
    _base_api,
    resolve_workspace_id,
    resolve_item_id,
)
from sempy._utils._log import log
import sempy_labs._icons as icons


@log
def list_data_access_roles(
    item: str | UUID,
    type: str,
    workspace: Optional[str | UUID] = None,
    view: Literal[
        "Roles", "Security", "MicrosoftEntraMembers", "FabricItemMembers"
    ] = "Roles",
    resolve_users: bool = False,
) -> pd.DataFrame:
    """
    Returns a list of OneLake roles.

    This is a wrapper function for the following API: `OneLake Data Access Security - List Data Access Roles <https://learn.microsoft.com/rest/api/fabric/core/onelake-data-access-security/list-data-access-roles>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    item : str | uuid.UUID
        The name or ID of the item.
    type : str
        The type of the item.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    view : typing.Literal["Roles", "Security", "MicrosoftEntraMembers", "FabricItemMembers"], default="Roles"
        The view returned by the API. "Roles" returns the tables/files covered by each role, "Security" returns the column/row level security, "MicrosoftEntraMembers" returns the Microsoft Entra members for each role, and "FabricItemMembers" returns the Fabric item members for each role.
    resolve_users : bool, default=False
        If True, resovles the 'Object Id' of a user to their Display Name and User Principal Name. This requires using a SPN connection as it authenticates to MS Graph. This is only valid for the 'MicrosoftEntraMembers' view.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of OneLake roles.
    """
    from sempy_labs.graph._users import resolve_user_name_and_id

    supported_views = [
        "Roles",
        "Security",
        "MicrosoftEntraMembers",
        "FabricItemMembers",
    ]
    if "entra" in view.lower():
        view = "MicrosoftEntraMembers"
    elif "fabric" in view.lower():
        view = "FabricItemMembers"
    elif "roles" in view.lower():
        view = "Roles"
    elif "security" in view.lower():
        view = "Security"
    if view not in supported_views:
        raise ValueError(
            f"{icons.red_dot} Only the following views are supported: {supported_views}. You entered '{view}'."
        )

    columns = {
        "Role Name": "string",
        "Role Id": "string",
        "Etag": "string",
        "Kind": "string",
    }

    if view == "Roles":
        columns.update(
            {
                "Effect": "string",
                "File Path": "string",
                "Permissions": "list",
            }
        )
    elif view == "MicrosoftEntraMembers":
        columns.update(
            {
                "Tenant Id": "string",
                "Object Id": "string",
            }
        )
    elif view == "Security":
        columns.update(
            {
                "Effect": "str",
                "File Path": "str",
                "Row Level Security": "str",
                "Column Level Security": "list",
                "Column Permissions": "list",
            }
        )
    else:
        columns.update(
            {
                "Source Path": "string",
                "Item Access": "list",
            }
        )

    df = _create_dataframe(columns=columns)

    workspace_id = resolve_workspace_id(workspace)
    item_id = resolve_item_id(item=item, type=type, workspace=workspace_id)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/items/{item_id}/dataAccessRoles",
        uses_pagination=True,
    )

    rows = []
    for r in responses:
        for role in r.get("value", []):
            name = role.get("name")
            role_id = role.get("id")
            etag = role.get("etag")
            kind = role.get("kind")
            if view == "Roles":
                for rules in role.get("decisionRules", []):
                    effect = rules.get("effect")
                    permissions = rules.get("permission", [])
                    permission = next(
                        (
                            perm.get("attributeValueIncludedIn", [])
                            for perm in permissions
                            if perm.get("attributeName") == "Action"
                        ),
                        [],
                    )
                    paths = next(
                        (
                            perm.get("attributeValueIncludedIn", [])
                            for perm in permissions
                            if perm.get("attributeName") == "Path"
                        ),
                        [],
                    )

                    for path in paths:
                        rows.append(
                            {
                                "Role Name": name,
                                "Role Id": role_id,
                                "Etag": etag,
                                "Kind": kind,
                                "Effect": effect,
                                "File Path": path,
                                "Permissions": permission,
                            }
                        )
            elif view == "Security":
                for rules in role.get("decisionRules", []):
                    effect = rules.get("effect")
                    constraints = rules.get("constraints", {})
                    cls = constraints.get("columns", [])
                    cls_tables = [col.get("tablePath") for col in cls]
                    rls = constraints.get("rows", [])
                    rls_tables = [row.get("tablePath") for row in rls]

                    all_tables = list(set(cls_tables + rls_tables))

                    secured_columns = []
                    column_security = None
                    row_security = None

                    for path in all_tables:
                        col_sec = next(
                            (col for col in cls if col.get("tablePath") == path), None
                        )
                        if col_sec:
                            secured_columns = col_sec.get("columnNames", [])
                            column_security = col_sec.get("columnAction", [])
                        row_sec = next(
                            (row for row in rls if row.get("tablePath") == path), None
                        )
                        if row_sec:
                            row_security = row_sec.get("value", [])
                        rows.append(
                            {
                                "Role Name": name,
                                "Role Id": role_id,
                                "Etag": etag,
                                "Kind": kind,
                                "Effect": effect,
                                "File Path": path,
                                "Column Level Security": secured_columns,
                                "Column Permissions": column_security,
                                "Row Level Security": row_security,
                            }
                        )

            elif view == "MicrosoftEntraMembers":
                members = role.get("members", {}).get("microsoftEntraMembers", [])
                for member in members:
                    object_id = member.get("objectId")
                    row = {
                        "Role Name": name,
                        "Role Id": role_id,
                        "Etag": etag,
                        "Kind": kind,
                        "Tenant Id": member.get("tenantId"),
                        "Object Id": object_id,
                    }

                    if resolve_users:
                        display_name, upn, _ = resolve_user_name_and_id(object_id)
                        row.update(
                            {
                                "User Name": display_name,
                                "User Principal Name": upn,
                            }
                        )

                    rows.append(row)
            else:
                members = role.get("members", {}).get("fabricItemMembers", [])
                for member in members:
                    rows.append(
                        {
                            "Role Name": name,
                            "Role Id": role_id,
                            "Etag": etag,
                            "Kind": kind,
                            "Source Path": member.get("sourcePath"),
                            "Item Access": member.get("itemAccess", []),
                        }
                    )
    if rows:
        df = pd.DataFrame(rows)

    return df
