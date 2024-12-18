import sempy.fabric as fabric
import pandas as pd
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    _is_valid_uuid,
)
from typing import Optional, Tuple
import sempy_labs._icons as icons
from sempy.fabric.exceptions import FabricHTTPException
from uuid import UUID


def list_dataflows(workspace: Optional[str | UUID] = None):
    """
    Shows a list of all dataflows which exist within a workspace.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the dataflows which exist within a workspace.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    client = fabric.PowerBIRestClient()
    response = client.get(f"/v1.0/myorg/groups/{workspace_id}/dataflows")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    df = pd.DataFrame(
        columns=["Dataflow Id", "Dataflow Name", "Configured By", "Users", "Generation"]
    )

    data = []  # Collect rows here

    for v in response.json().get("value", []):
        new_data = {
            "Dataflow Id": v.get("objectId"),
            "Dataflow Name": v.get("name"),
            "Configured By": v.get("configuredBy"),
            "Users": v.get("users", []),
            "Generation": v.get("generation"),
        }
        data.append(new_data)

    if data:
        df = pd.DataFrame(data)
        df["Generation"] = df["Generation"].astype(int)

    return df


def assign_workspace_to_dataflow_storage(
    dataflow_storage_account: str, workspace: Optional[str | UUID] = None
):
    """
    Assigns a dataflow storage account to a workspace.

    This is a wrapper function for the following API: `Dataflow Storage Accounts - Groups AssignToDataflowStorage <https://learn.microsoft.com/rest/api/power-bi/dataflow-storage-accounts/groups-assign-to-dataflow-storage>`_.

    Parameters
    ----------
    dataflow_storage_account : str
        The name of the dataflow storage account.
    workspace : str | uuid.UUID, default=None
        The name or ID of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

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
        f"{icons.green_dot} The '{dataflow_storage_account}' dataflow storage account has been assigned to the '{workspace_name}' workspacce."
    )


def list_dataflow_storage_accounts() -> pd.DataFrame:
    """
    Shows the accessible dataflow storage accounts.

    This is a wrapper function for the following API: `Dataflow Storage Accounts - Get Dataflow Storage Accounts <https://learn.microsoft.com/rest/api/power-bi/dataflow-storage-accounts/get-dataflow-storage-accounts>`_.

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


def list_upstream_dataflows(
    dataflow: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Shows a list of upstream dataflows for the specified dataflow.

    This is a wrapper function for the following API: `Dataflows - Get Upstream Dataflows In Group <https://learn.microsoft.com/rest/api/power-bi/dataflows/get-upstream-dataflows-in-group>`_.

    Parameters
    ----------
    dataflow : str | uuid.UUID
        Name or UUID of the dataflow.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of upstream dataflows for the specified dataflow.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataflow_name, dataflow_id) = _resolve_dataflow_name_and_id(
        dataflow=dataflow, workspace=workspace_id
    )
    client = fabric.PowerBIRestClient()

    df = pd.DataFrame(
        columns=[
            "Dataflow Name",
            "Dataflow Id",
            "Workspace Name",
            "Workspace Id",
            "Upstream Dataflow Name",
            "Upstream Dataflow Id",
            "Upstream Workspace Name",
            "Upstream Workspace Id",
        ]
    )

    def collect_upstreams(
        client, dataflow_id, dataflow_name, workspace_id, workspace_name
    ):
        response = client.get(
            f"/v1.0/myorg/groups/{workspace_id}/dataflows/{dataflow_id}/upstreamDataflows"
        )
        if response.status_code != 200:
            raise FabricHTTPException(response)

        values = response.json().get("value", [])
        for v in values:
            tgt_dataflow_id = v.get("targetDataflowId")
            tgt_workspace_id = v.get("groupId")
            tgt_workspace_name = fabric.resolve_workspace_name(tgt_workspace_id)
            (tgt_dataflow_name, _) = _resolve_dataflow_name_and_id(
                dataflow=tgt_dataflow_id, workspace=tgt_workspace_id
            )

            df.loc[len(df)] = {
                "Dataflow Name": dataflow_name,
                "Dataflow Id": dataflow_id,
                "Workspace Name": workspace_name,
                "Workspace Id": workspace_id,
                "Upstream Dataflow Name": tgt_dataflow_name,
                "Upstream Dataflow Id": tgt_dataflow_id,
                "Upstream Workspace Name": tgt_workspace_name,
                "Upstream Workspace Id": tgt_workspace_id,
            }

            collect_upstreams(
                client,
                tgt_dataflow_id,
                tgt_dataflow_name,
                tgt_workspace_id,
                tgt_workspace_name,
            )

    collect_upstreams(client, dataflow_id, dataflow_name, workspace_id, workspace_name)

    return df


def _resolve_dataflow_name_and_id(
    dataflow: str | UUID, workspace: Optional[str | UUID] = None
) -> Tuple[str, UUID]:

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    dfD = list_dataflows(workspace=workspace_id)

    if _is_valid_uuid(dataflow):
        dfD_filt = dfD[dfD["Dataflow Id"] == dataflow]
    else:
        dfD_filt = dfD[dfD["Dataflow Name"] == dataflow]

    if len(dfD_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{dataflow}' dataflow does not exist within the '{workspace_name}' workspace."
        )

    dataflow_id = dfD_filt["Dataflow Id"].iloc[0]
    dataflow_name = dfD_filt["Dataflow Name"].iloc[0]

    return dataflow_name, dataflow_id
