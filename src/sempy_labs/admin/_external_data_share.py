import sempy.fabric as fabric
from uuid import UUID
import sempy_labs._icons as icons
from sempy.fabric.exceptions import FabricHTTPException
import pandas as pd
from sempy_labs.admin._basic_functions import _resolve_workspace_name_and_id


def list_external_data_shares() -> pd.DataFrame:
    """
    Lists external data shares in the tenant. This function is for admins.

    This is a wrapper function for the following API: `External Data Shares - List External Data Shares <https://learn.microsoft.com/rest/api/fabric/admin/external-data-shares/list-external-data-shares>`_.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of external data shares in the tenant.
    """
    df = pd.DataFrame(
        columns=[
            "External Data Share Id",
            "Paths",
            "Creater Principal Id",
            "Creater Principal Name",
            "Creater Principal Type",
            "Creater Principal UPN",
            "Recipient UPN",
            "Status",
            "Expiration Time UTC",
            "Workspace Id",
            "Item Id",
            "Invitation URL",
        ]
    )

    client = fabric.FabricRestClient()
    response = client.get("/v1/admin/items/externalDataShares")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    for i in response.json().get("value", []):
        cp = i.get("creatorPrincipal", {})
        new_data = {
            "External Data Share Id": i.get("id"),
            "Paths": [i.get("paths", [])],
            "Creater Principal Id": cp.get("id"),
            "Creater Principal Name": cp.get("displayName"),
            "Creater Principal Type": cp.get("type"),
            "Creater Principal UPN": cp.get("userDetails", {}).get("userPrincipalName"),
            "Recipient UPN": i.get("recipient", {}).get("userPrincipalName"),
            "Status": i.get("status"),
            "Expiration Time UTC": i.get("expirationTimeUtc"),
            "Workspace Id": i.get("workspaceId"),
            "Item Id": i.get("itemId"),
            "Invitation URL": i.get("invitationUrl"),
        }

        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    date_time_columns = ["Expiration Time UTC"]
    df[date_time_columns] = pd.to_datetime(df[date_time_columns])

    return df


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

    client = fabric.FabricRestClient()
    response = client.post(
        f"/v1/admin/workspaces/{workspace_id}/items/{item_id}/externalDataShares/{external_data_share_id}/revoke"
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The '{external_data_share_id}' external data share for the '{item_id}' item within the '{workspace}' workspace has been revoked."
    )
