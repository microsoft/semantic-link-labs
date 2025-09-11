from typing import Optional
from uuid import UUID
import pandas as pd
from sempy_labs._helper_functions import (
    _create_dataframe,
    _base_api,
    resolve_workspace_id,
    resolve_item_id,
)
from sempy._utils._log import log


@log
def list_data_access_roles(
    item: str | UUID, type: str, workspace: Optional[str | UUID] = None
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

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of OneLake roles.
    """

    columns = {
        "Role Name": "string",
        "Effect": "string",
        "Attribute Name": "string",
        "Attribute Values": "string",
        "Item Access": "string",
        "Source Path": "string",
    }

    df = _create_dataframe(columns=columns)

    workspace_id = resolve_workspace_id(workspace)
    item_id = resolve_item_id(item=item, type=type, workspace=workspace_id)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/items/{item_id}/dataAccessRoles",
        uses_pagination=True,
        client="fabric_sp",
    )

    rows = []
    for r in responses:
        for role in r.get("value", []):
            name = role.get("name")

            # Loop through members first (since they are crucial)
            members = role.get("members", {}).get("fabricItemMembers", [])
            if not members:
                members = [{}]  # if no members exist, still create at least one row

            for member in members:
                item_access = member.get("itemAccess", [])
                source_path = member.get("sourcePath")

                # Loop through decision rules
                for rule in role.get("decisionRules", []):
                    effect = rule.get("effect")

                    # Loop through permissions
                    for perm in rule.get("permission", []):
                        attr_name = perm.get("attributeName")
                        attr_values = perm.get("attributeValueIncludedIn", [])

                        rows.append(
                            {
                                "Role Name": name,
                                "Effect": effect,
                                "Attribute Name": attr_name,
                                "Attribute Values": ", ".join(attr_values),
                                "Item Access": ", ".join(item_access),
                                "Source Path": source_path,
                            }
                        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df
