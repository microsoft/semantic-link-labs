from uuid import UUID
import sempy_labs._icons as icons
import pandas as pd
from sempy_labs.admin._basic_functions import _resolve_workspace_name_and_id
from sempy_labs._helper_functions import (
    _base_api,
    _create_dataframe,
    _update_dataframe_datatypes,
)
from sempy._utils._log import log


@log
def list_external_data_shares() -> pd.DataFrame:
    """
    Lists external data shares in the tenant. This function is for admins.

    This is a wrapper function for the following API: `External Data Shares - List External Data Shares <https://learn.microsoft.com/rest/api/fabric/admin/external-data-shares/list-external-data-shares>`_.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of external data shares in the tenant.
    """

    columns = {
        "External Data Share Id": "string",
        "Paths": "string",
        "Creater Principal Id": "string",
        "Creater Principal Name": "string",
        "Creater Principal Type": "string",
        "Creater Principal UPN": "string",
        "Recipient UPN": "string",
        "Status": "string",
        "Expiration Time UTC": "datetime",
        "Workspace Id": "string",
        "Item Id": "string",
        "Invitation URL": "string",
    }
    df = _create_dataframe(columns=columns)

    response = _base_api(request="/v1/admin/items/externalDataShares")

    rows = []
    for i in response.json().get("value", []):
        cp = i.get("creatorPrincipal", {})
        rows.append(
            {
                "External Data Share Id": i.get("id"),
                "Paths": [i.get("paths", [])],
                "Creater Principal Id": cp.get("id"),
                "Creater Principal Name": cp.get("displayName"),
                "Creater Principal Type": cp.get("type"),
                "Creater Principal UPN": cp.get("userDetails", {}).get(
                    "userPrincipalName"
                ),
                "Recipient UPN": i.get("recipient", {}).get("userPrincipalName"),
                "Status": i.get("status"),
                "Expiration Time UTC": i.get("expirationTimeUtc"),
                "Workspace Id": i.get("workspaceId"),
                "Item Id": i.get("itemId"),
                "Invitation URL": i.get("invitationUrl"),
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
def revoke_external_data_share(
    external_data_share_id: UUID, item_id: UUID, workspace: str | UUID
):
    """
    Revokes the specified external data share. Note: This action cannot be undone.

    This is a wrapper function for the following API: `External Data Shares - Revoke External Data Share <https://learn.microsoft.com/rest/api/fabric/admin/external-data-shares/revoke-external-data-share>`_.

    Parameters
    ----------
    external_data_share_id : uuid.UUID
        The external data share ID.
    item_id : uuid.UUID, default=None
        The Item ID
    workspace : str | uuid.UUID
        The Fabric workspace name or id.
    """
    (workspace, workspace_id) = _resolve_workspace_name_and_id(workspace)

    _base_api(
        request=f"/v1/admin/workspaces/{workspace_id}/items/{item_id}/externalDataShares/{external_data_share_id}/revoke",
        method="post",
    )

    print(
        f"{icons.green_dot} The '{external_data_share_id}' external data share for the '{item_id}' item within the '{workspace}' workspace has been revoked."
    )
