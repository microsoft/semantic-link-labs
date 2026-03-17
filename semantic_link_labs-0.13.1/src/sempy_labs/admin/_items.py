import pandas as pd
from typing import Optional, Tuple
from uuid import UUID
import sempy_labs._icons as icons
from sempy_labs.admin._basic_functions import (
    _resolve_workspace_name_and_id,
)
from sempy_labs.admin._capacities import (
    _resolve_capacity_name_and_id,
)
from sempy_labs._helper_functions import (
    _is_valid_uuid,
    _build_url,
    _base_api,
    _create_dataframe,
)
from sempy._utils._log import log


@log
def _resolve_item_id(
    item: str,
    type: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
) -> UUID:
    if _is_valid_uuid(item):
        item_id = item

    else:
        workspace_id = _resolve_workspace_name_and_id(workspace)[1]
        dfI = list_items(workspace=workspace_id, type=type)
        dfI_filt = dfI[dfI["Item Name"] == item]

        if len(dfI_filt) == 0:
            raise ValueError(
                f"The '{item}' {type} does not exist within the '{workspace}' workspace or is not of type '{type}'."
            )

        item_id = dfI_filt["Item Id"].iloc[0]

    return item_id


@log
def _resolve_item_name_and_id(
    item: str,
    type: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
    **kwargs,
) -> Tuple[str, UUID]:
    if "item_name" in kwargs:
        print(
            "The 'item_name' parameter has been deprecated. Please replace this parameter with 'item' from the function going forward."
        )
        item = item_name
        del kwargs["item_name"]

    dfI = list_items(workspace=workspace, type=type, item=item)

    if len(dfI) > 1:
        raise ValueError(
            f"There are more than 1 item with the name '{item}'. Please specify the 'type' and/or 'workspace' to be more precise."
        )

    try:
        item_name = dfI["Item Name"].iloc[0]
        item_id = dfI["Item Id"].iloc[0]
    except Exception:
        raise ValueError(
            f"The '{item}' {type} does not exist within the '{workspace}' workspace or is not of type '{type}'."
        )

    return item_name, item_id


@log
def list_items(
    capacity: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
    state: Optional[str] = None,
    type: Optional[str] = None,
    item: Optional[str | UUID] = None,
    **kwargs,
) -> pd.DataFrame:
    """
    Shows a list of active Fabric and Power BI items.

    This is a wrapper function for the following API: `Items - List Items <https://learn.microsoft.com/rest/api/fabric/admin/items/list-items>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    capacity : str | uuid.UUID, default=None
        The capacity name or id.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or id.
        Defaults to None which looks into all the workspaces.
    state : str, default=None
        The item state.
    type : str, default=None
        The item type.
    item : str | UUID, default=None
        Item id or name to filter the list.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of active Fabric and Power BI items.
    """
    if "capacity_name" in kwargs:
        print(
            "The 'capacity_name' parameter has been deprecated. Please replace this parameter with 'capacity' from the function going forward."
        )
        capacity = kwargs["capacity_name"]
        del kwargs["capacity_name"]

    columns = {
        "Item Id": "string",
        "Item Name": "string",
        "Type": "string",
        "Description": "string",
        "State": "string",
        "Last Updated Date": "string",
        "Creator Principal Id": "string",
        "Creator Principal Display Name": "string",
        "Creator Principal Type": "string",
        "Creator User Principal Name": "string",
        "Workspace Id": "string",
        "Capacity Id": "string",
    }
    df = _create_dataframe(columns=columns)

    params = {}
    url = "/v1/admin/items"

    if capacity is not None:
        params["capacityId"] = _resolve_capacity_name_and_id(capacity)[1]

    if workspace is not None:
        params["workspaceId"] = _resolve_workspace_name_and_id(workspace)[1]

    if state is not None:
        params["state"] = state

    if type is not None:
        params["type"] = type

    url = _build_url(url, params)

    responses = _base_api(request=url, client="fabric_sp", uses_pagination=True)

    rows = []
    for r in responses:
        for v in r.get("itemEntities", []):
            rows.append(
                {
                    "Item Id": v.get("id"),
                    "Type": v.get("type"),
                    "Item Name": v.get("name"),
                    "Description": v.get("description"),
                    "State": v.get("state"),
                    "Last Updated Date": v.get("lastUpdatedDate"),
                    "Creator Principal Id": v.get("creatorPrincipal", {}).get("id"),
                    "Creator Principal Display Name": v.get("creatorPrincipal", {}).get(
                        "displayName"
                    ),
                    "Creator Principal Type": v.get("creatorPrincipal", {}).get("type"),
                    "Creator User Principal Name": v.get("creatorPrincipal", {})
                    .get("userDetails", {})
                    .get("userPrincipalName"),
                    "Workspace Id": v.get("workspaceId"),
                    "Capacity Id": v.get("capacityId"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    if item is not None:
        if _is_valid_uuid(item):
            df = df[df["Item Id"] == item]
        else:
            df = df[df["Item Name"] == item]

    return df


@log
def list_item_access_details(
    item: str | UUID,
    type: str,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Returns a list of users (including groups and service principals) and lists their workspace roles.

    This is a wrapper function for the following API: `Items - List Item Access Details <https://learn.microsoft.com/rest/api/fabric/admin/items/list-item-access-details>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    item : str
        Name or id of the Fabric item.
    type : str
        Type of Fabric item.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or id.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of users (including groups and service principals) and lists their workspace roles.
    """

    (workspace_name, workspace_id) = _resolve_workspace_name_and_id(workspace)
    (item_name, item_id) = _resolve_item_name_and_id(
        item=item, type=type, workspace=workspace_name
    )

    columns = {
        "User Id": "string",
        "User Name": "string",
        "User Type": "string",
        "User Principal Name": "string",
        "Item Name": "string",
        "Item Type": "string",
        "Item Id": "string",
        "Permissions": "string",
        "Additional Permissions": "string",
    }
    df = _create_dataframe(columns=columns)

    response = _base_api(
        request=f"/v1/admin/workspaces/{workspace_id}/items/{item_id}/users?type={type}",
        client="fabric_sp",
    )

    rows = []
    for v in response.json().get("accessDetails", []):
        rows.append(
            {
                "User Id": v.get("principal", {}).get("id"),
                "User Name": v.get("principal", {}).get("displayName"),
                "User Type": v.get("principal", {}).get("type"),
                "User Principal Name": v.get("principal", {})
                .get("userDetails", {})
                .get("userPrincipalName"),
                "Item Type": v.get("itemAccessDetails", {}).get("type"),
                "Permissions": v.get("itemAccessDetails", {}).get("permissions"),
                "Additional Permissions": v.get("itemAccessDetails", {}).get(
                    "additionalPermissions"
                ),
                "Item Name": item_name,
                "Item Id": item_id,
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df
