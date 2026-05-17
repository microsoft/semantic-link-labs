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
    view: Literal["Rules", "MicrosoftEntraMembers", "FabricItemMembers"] = "Rules",
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
    view : typing.Literal["Rules", "MicrosoftEntraMembers", "FabricItemMembers"], default="Rules"
        The view returned by the API. "Rules" returns the data access rules for each role, "MicrosoftEntraMembers" returns the Microsoft Entra members for each role, and "FabricItemMembers" returns the Fabric item members for each role.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of OneLake roles.
    """

    supported_views = ["Rules", "MicrosoftEntraMembers", "FabricItemMembers"]
    if "entra" in view.lower():
        view = "MicrosoftEntraMembers"
    elif "fabric" in view.lower():
        view = "FabricItemMembers"
    elif "rules" in view.lower():
        view = "Rules"
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

    if view == "Rules":
        columns.update(
            {
                "Effect": "string",
                "File Path": "string",
                "Permissions": "list",
                "Row Level Security": "string",
                "Column Level Security": "list",
                "Column Permission": "list",
            }
        )
    elif view == "MicrosoftEntraMembers":
        columns.update(
            {
                "Tenant Id": "string",
                "Object Id": "string",
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
            if view == "Rules":
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

                    cls = rules.get("constraints", {}).get("columns", [])
                    rls = rules.get("constraints", {}).get("rows", [])
                    for path in paths:
                        row_level_security = next(
                            (r.get("value") for r in rls if r.get("tablePath") == path),
                            None,
                        )
                        column_level_security, column_action = next(
                            (
                                (r.get("columnNames", []), r.get("columnAction", []))
                                for r in cls
                                if r.get("tablePath") == path
                            ),
                            (None, None),
                        )
                        rows.append(
                            {
                                "Role Name": name,
                                "Role Id": role_id,
                                "Etag": etag,
                                "Kind": kind,
                                "Effect": effect,
                                "File Path": path,
                                "Permissions": permission,
                                "Row Level Security": row_level_security,
                                "Column Level Security": column_level_security,
                                "Column Permission": column_action,
                            }
                        )
            elif view == "MicrosoftEntraMembers":
                members = role.get("members", {}).get("microsoftEntraMembers", [])
                for member in members:
                    rows.append(
                        {
                            "Role Name": name,
                            "Role Id": role_id,
                            "Etag": etag,
                            "Kind": kind,
                            "Tenant Id": member.get("tenantId"),
                            "Object Id": member.get("objectId"),
                        }
                    )
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
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df
