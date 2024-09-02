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
