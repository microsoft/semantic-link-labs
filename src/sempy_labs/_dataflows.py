import sempy.fabric as fabric
import pandas as pd
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
)
from typing import Optional
import sempy_labs._icons as icons
from sempy.fabric.exceptions import FabricHTTPException


def list_dataflows(workspace: Optional[str] = None):
    """
    Shows a list of all dataflows which exist within a workspace.

    Parameters
    ----------
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the dataflows which exist within a workspace.
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)
    client = fabric.PowerBIRestClient()
    response = client.get(f"/v1.0/myorg/groups/{workspace_id}/dataflows")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    df = pd.DataFrame(
        columns=["Dataflow Id", "Dataflow Name", "Configured By", "Users", "Generation"]
    )

    for v in response.json().get("value", []):
        new_data = {
            "Dataflow Id": v.get("objectId"),
            "Dataflow Name": v.get("name"),
            "Configured By": v.get("configuredBy"),
            "Users": [v.get("users")],
            "Generation": v.get("generation"),
        }
        df = pd.concat(
            [df, pd.DataFrame(new_data, index=[0])],
            ignore_index=True,
        )

    df["Generation"] = df["Generation"].astype(int)

    return df


def assign_workspace_to_dataflow_storage(
    dataflow_storage_account: str, workspace: Optional[str] = None
):
    """
    Assigns a dataflow storage account to a workspace.

    This is a wrapper function for the following API: `Dataflow Storage Accounts - Groups AssignToDataflowStorage <https://learn.microsoft.com/rest/api/power-bi/dataflow-storage-accounts/groups-assign-to-dataflow-storage`_.

    Parameters
    ----------
    dataflow_storage_account : str
        The name of the dataflow storage account.
    workspace : str, default=None
        The name of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    df = list_dataflow_storage_accounts()
    df_filt = df[df["Dataflow Storage Account Name"] == dataflow_storage_account]

    if len(df_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{dataflow_storage_account}' does not exist."
        )

    dataflow_storage_id = df_filt["Dataflow Storage Account ID"].iloc[0]
    client = fabric.PowerBIRestClient()

    request_body = {"dataflowStorageId": dataflow_storage_id}

    response = client.post(
        f"/v1.0/myorg/groups/{workspace_id}/AssignToDataflowStorage", json=request_body
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)
    print(
        f"{icons.green_dot} The '{dataflow_storage_account}' dataflow storage account has been assigned to the '{workspace}' workspacce."
    )


def list_dataflow_storage_accounts() -> pd.DataFrame:
    """
    Shows the accessible dataflow storage accounts.

    This is a wrapper function for the following API: `Dataflow Storage Accounts - Get Dataflow Storage Accounts <https://learn.microsoft.com/rest/api/power-bi/dataflow-storage-accounts/get-dataflow-storage-accounts`_.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the accessible dataflow storage accounts.
    """

    df = pd.DataFrame(
        columns=[
            "Dataflow Storage Account ID",
            "Dataflow Storage Account Name",
            "Enabled",
        ]
    )
    client = fabric.PowerBIRestClient()
    response = client.get("/v1.0/myorg/dataflowStorageAccounts")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    for v in response.json().get("value", []):
        new_data = {
            "Dataflow Storage Account ID": v.get("id"),
            "Dataflow Storage Account Name": v.get("name"),
            "Enabled": v.get("isEnabled"),
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    df["Enabled"] = df["Enabled"].astype(bool)

    return df


def list_upstream_dataflows(dataflow: str, workspace: Optional[str] = None, df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    Shows a list of upstream dataflows for the specified dataflow.

    Parameters
    ----------
    dataflow : str
        The dataflow name.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    df : pandas.DataFrame, default=None
        The DataFrame to append the results to. Defaults to None which creates a new DataFrame.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the upstream dataflows which exist.
    """

    workspace, workspace_id = resolve_workspace_name_and_id(workspace)
    dataflow, dataflow_id = resolve_dataflow_name_and_id(dataflow, workspace)

    client = fabric.PowerBIRestClient()
    response = client.get(f"/v1.0/myorg/groups/{workspace_id}/dataflows/{dataflow_id}/upstreamDataflows")
    response.raise_for_status()

    if df is None:
        df = pd.DataFrame(columns=["Dataflow Name", "Dataflow Id", "Workspace Name", "Workspace Id", "Upstream Dataflow Name", "Upstream Dataflow Id", "Upstream Workspace Name", "Upstream Workspace Id"])

    for v in response.json().get("value", []):
    
        target_dataflow, target_dataflow_id = resolve_dataflow_name_and_id(v.get("targetDataflowId"), v.get("groupId"))
        target_workspace, target_workspace_id = resolve_workspace_name_and_id(v.get("groupId"))

        new_data = {
            "Dataflow Name": dataflow,
            "Dataflow Id": dataflow_id,
            "Workspace Name": workspace,
            "Workspace Id": workspace_id,
            "Upstream Dataflow Name": target_dataflow,
            "Upstream Dataflow Id": target_dataflow_id,
            "Upstream Workspace Name": target_workspace,
            "Upstream Workspace Id": target_workspace_id,
        }

        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

        # Recursively call the function for each target dataflow
        df = list_upstream_dataflows(v.get("targetDataflowId"), v.get("groupId"), df)

    return df
