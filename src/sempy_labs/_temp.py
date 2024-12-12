import sempy.fabric as fabric
import pandas as pd
from typing import Optional, Tuple
import sempy_labs._icons as icons
from uuid import UUID
from sempy.fabric.exceptions import FabricHTTPException
from sempy_labs._helper_functions import (
    pagination,
    _is_valid_uuid,
)


def _list_items(
    type: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
    token_provider: Optional[str] = None,
) -> pd.DataFrame:

    (workspace_name, workspace_id) = _resolve_workspace_name_and_id(
        workspace=workspace, token_provider=token_provider
    )
    client = fabric.FabricRestClient(token_provider=token_provider)
    url = f"/v1/workspaces/{workspace_id}/items"
    if type is not None:
        url += f"?type={type}"
    response = client.get(url)

    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    df = pd.DataFrame(columns=[])

    for r in responses:
        for v in r.get("value", []):
            new_data = {
                "Id": v.get("id"),
                "Display Name": v.get("displayName"),
                "Description": v.get("description"),
                "Type": v.get("type"),
                "Workspace Id": v.get("workspaceId"),
            }

            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def _list_workspaces(token_provider: Optional[str] = None) -> pd.DataFrame:

    client = fabric.FabricRestClient(token_provider=token_provider)
    response = client.get("/v1/workspaces")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    df = pd.DataFrame(columns=["Id", "Name", "Type", "Description"])

    responses = pagination(client, response)
    for r in responses:
        for v in r.get("value", []):
            new_data = {
                "Id": v.get("id"),
                "Name": v.get("displayName"),
                "Type": v.get("type"),
                "Description": v.get("description"),
            }

            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def _get_item(
    item_id: UUID,
    workspace: Optional[str | UUID] = None,
    token_provider: Optional[str] = None,
):

    (workspace_name, workspace_id) = _resolve_workspace_name_and_id(
        workspace=workspace, token_provider=token_provider
    )
    client = fabric.FabricRestClient(token_provider=token_provider)
    response = client.get(f"/v1/workspaces/{workspace_id}/items/{item_id}")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    df = pd.json_normalize(response.json()).rename(
        columns={
            "id": "Item Id",
            "type": "Type",
            "displayName": "Item Name",
            "description": "Description",
            "workspaceId": "Workspace Id",
        }
    )

    return df


def _get_workspace(workspace_id: UUID, token_provider: Optional[str] = None) -> str:

    client = fabric.FabricRestClient(token_provider=token_provider)
    response = client.get(f"/v1/workspaces/{workspace_id}")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    return response.json().get("displayName")


def _resolve_item_name_and_id(
    item: str | UUID,
    type: str,
    workspace: Optional[str | UUID] = None,
    token_provider: Optional[str] = None,
) -> Tuple[str, UUID]:

    (workspace_name, workspace_id) = _resolve_workspace_name_and_id(
        workspace=workspace, token_provider=token_provider
    )
    if _is_valid_uuid(item):
        item_id = item
    else:
        dfI = _list_items(
            type=type, workspace=workspace_id, token_provider=token_provider
        )
        dfI_filt = dfI[dfI["Name"] == item]
        if dfI_filt.empty:
            raise ValueError(
                f"{icons.red_dot} The '{item}' {type.lower} does not exist within the '{workspace}' workspace"
            )
        item_id = dfI_filt["Id"].iloc[0]

    i = _get_item(
        item_id=item_id, workspace=workspace_id, token_provider=token_provider
    )
    item_name = i["Item Name"].iloc[0]

    return item_name, item_id


def _resolve_workspace_name_and_id(
    workspace: Optional[str | UUID] = None, token_provider: Optional[str] = None
) -> Tuple[str, UUID]:

    if workspace is None:
        workspace_id = fabric.get_workspace_id()
    elif _is_valid_uuid(workspace):
        workspace_id = workspace
    else:
        dfW = _list_workspaces(token_provider=token_provider)
        dfW_filt = dfW[dfW["Name"] == workspace]
        if dfW_filt.empty:
            raise ValueError(
                f"{icons.red_dot} The '{workspace}' workspace does not exist."
            )
        workspace_id = dfW_filt["Id"].iloc[0]

    workspace_name = _get_workspace(workspace_id, token_provider=token_provider)

    return workspace_name, workspace_id


def _list_datasets(
    workspace: Optional[str | UUID] = None, token_provider: Optional[str] = None
) -> pd.DataFrame:

    (workspace_name, workspace_id) = _resolve_workspace_name_and_id(
        workspace=workspace, token_provider=token_provider
    )

    client = fabric.FabricRestClient(token_provider=token_provider)
    response = client.get(f"/v1.0/myorg/groups/{workspace_id}/datasets")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    df = pd.DataFrame(
        columns=["Dataset Id", "Dataset Name", "Web Url", "Configured By"]
    )

    for v in response.json().get("value", []):
        new_data = {
            "Dataset Id": v.get("id"),
            "Dataset Name": v.get("name"),
            "Web Url": v.get("webUrl"),
            "Configured By": v.get("configuredBy"),
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def _list_reports(
    workspace: Optional[str | UUID] = None, token_provider: Optional[str] = None
) -> pd.DataFrame:

    (workspace_name, workspace_id) = _resolve_workspace_name_and_id(
        workspace=workspace, token_provider=token_provider
    )

    client = fabric.FabricRestClient(token_provider=token_provider)
    response = client.get(f"/v1.0/myorg/groups/{workspace_id}/reports")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    df = pd.DataFrame(
        columns=[
            "Id",
            "Report Type",
            "Name",
            "Web Url",
            "Embed Url",
            "Is From Pbix",
            "Is Owned By Me",
            "Dataset Id",
            "Dataset Workspace Id",
            "Users",
            "Subscriptions",
        ]
    )

    for v in response.json().get("value", []):
        new_data = {
            "Id": v.get("id"),
            "Report Type": v.get("reportType"),
            "Name": v.get("name"),
            "Web Url": v.get("webUrl"),
            "Embed Url": v.get("embedUrl"),
            "Is From Pbix": v.get("isFromPbix"),
            "Is Owned By Me": v.get("isOwnedByMe"),
            "Dataset Id": v.get("datasetId"),
            "Dataset Workspace Id": v.get("datasetWorkspaceId"),
            "Users": str(v.get("users", [])),
            "Subscriptions": str(v.get("subscriptions", [])),
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    bool_cols = ["Is From Pbix", "Is Owned By Me"]
    df[bool_cols] = df[bool_cols].astype(bool)

    return df
