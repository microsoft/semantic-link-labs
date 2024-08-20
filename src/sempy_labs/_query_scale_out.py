import sempy.fabric as fabric
import pandas as pd
from sempy_labs._helper_functions import (
    resolve_dataset_id,
    resolve_workspace_name_and_id,
)
from typing import Optional, Tuple
import sempy_labs._icons as icons
from sempy.fabric.exceptions import FabricHTTPException


def qso_sync(dataset: str, workspace: Optional[str] = None):
    """
    Triggers a query scale-out sync of read-only replicas for the specified dataset from the specified workspace.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------

    """

    # https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/trigger-query-scale-out-sync-in-group

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)
    dataset_id = resolve_dataset_id(dataset, workspace)

    client = fabric.PowerBIRestClient()
    response = client.post(
        f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/queryScaleOut/sync"
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)
    print(
        f"{icons.green_dot} QSO sync initiated for the '{dataset}' semantic model within the '{workspace}' workspace."
    )


def qso_sync_status(
    dataset: str, workspace: Optional[str] = None
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns the query scale-out sync status for the specified dataset from the specified workspace.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    Tuple[pandas.DataFrame, pandas.DataFrame]
        2 pandas dataframes showing the query scale-out sync status.

    """

    # https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/get-query-scale-out-sync-status-in-group

    df = pd.DataFrame(
        columns=[
            "Scale Out Status",
            "Sync Start Time",
            "Sync End Time",
            "Commit Version",
            "Commit Timestamp",
            "Target Sync Version",
            "Target Sync Timestamp",
            "Trigger Reason",
            "Min Active Read Version",
            "Min Active Read Timestamp",
        ]
    )
    dfRep = pd.DataFrame(
        columns=["Replica ID", "Replica Type", "Replica Version", "Replica Timestamp"]
    )

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)
    dataset_id = resolve_dataset_id(dataset, workspace)

    client = fabric.PowerBIRestClient()
    response = client.get(
        f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/queryScaleOut/syncStatus"
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)

    o = response.json()
    sos = o.get("scaleOutStatus")

    if sos == "Enabled":
        new_data = {
            "Scale Out Status": o.get("scaleOutStatus"),
            "Sync Start Time": o.get("syncStartTime"),
            "Sync End Time": o.get("syncEndTime"),
            "Commit Version": o.get("commitVersion"),
            "Commit Timestamp": o.get("commitTimestamp"),
            "Target Sync Version": o.get("targetSyncVersion"),
            "Target Sync Timestamp": o.get("targetSyncTimestamp"),
            "Trigger Reason": o.get("triggerReason"),
            "Min Active Read Version": o.get("minActiveReadVersion"),
            "Min Active Read Timestamp": o.get("minActiveReadTimestamp"),
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

        for r in o.get("scaleOutReplicas", []):
            new_data = {
                "Replica ID": r.get("replicaId"),
                "Replica Type": r.get("replicaType"),
                "Replica Version": str(r.get("replicaVersion")),
                "Replica Timestamp": r.get("replicaTimestamp"),
            }
            dfRep = pd.concat(
                [dfRep, pd.DataFrame(new_data, index=[0])], ignore_index=True
            )

        df["Sync Start Time"] = pd.to_datetime(df["Sync Start Time"])
        df["Sync End Time"] = pd.to_datetime(df["Sync End Time"])
        df["Commit Timestamp"] = pd.to_datetime(df["Commit Timestamp"])
        df["Target Sync Timestamp"] = pd.to_datetime(df["Target Sync Timestamp"])
        df["Min Active Read Timestamp"] = pd.to_datetime(
            df["Min Active Read Timestamp"]
        )
        dfRep["Replica Timestamp"] = pd.to_datetime(dfRep["Replica Timestamp"])
        df["Commit Version"] = df["Commit Version"].astype("int")
        df["Target Sync Version"] = df["Target Sync Version"].astype("int")
        df["Min Active Read Version"] = df["Min Active Read Version"].astype("int")

        return df, dfRep
    else:
        print(f"{sos}\n\n")
        return df, dfRep


def disable_qso(dataset: str, workspace: Optional[str] = None) -> pd.DataFrame:
    """
    Sets the max read-only replicas to 0, disabling query scale out.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the current query scale out settings.

    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)
    dataset_id = resolve_dataset_id(dataset, workspace)

    request_body = {"queryScaleOutSettings": {"maxReadOnlyReplicas": "0"}}

    client = fabric.PowerBIRestClient()
    response = client.patch(
        f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}", json=request_body
    )
    if response.status_code != 200:
        raise FabricHTTPException(response)

    df = list_qso_settings(dataset=dataset, workspace=workspace)
    print(
        f"{icons.green_dot} Query scale out has been disabled for the '{dataset}' semantic model within the '{workspace}' workspace."
    )

    return df


def set_qso(
    dataset: str,
    auto_sync: Optional[bool] = True,
    max_read_only_replicas: Optional[int] = -1,
    workspace: Optional[str] = None,
) -> pd.DataFrame:
    """
    Sets the query scale out settings for a semantic model.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    auto_sync : bool, default=True
        Whether the semantic model automatically syncs read-only replicas.
    max_read_only_replicas : int, default=-1
        To enable semantic model scale-out, set max_read_only_replicas to -1, or any non-0 value. A value of -1 allows Power BI to create as many read-only replicas as your Power BI capacity supports. You can also explicitly set the replica count to a value lower than that of the capacity maximum. Setting max_read_only_replicas to -1 is recommended.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the current query scale-out settings.

    """

    # https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/update-dataset-in-group

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)
    dataset_id = resolve_dataset_id(dataset, workspace)

    if max_read_only_replicas == 0:
        disable_qso(dataset=dataset, workspace=workspace)
        return

    request_body = {
        "queryScaleOutSettings": {
            "autoSyncReadOnlyReplicas": auto_sync,
            "maxReadOnlyReplicas": str(max_read_only_replicas),
        }
    }

    ssm = set_semantic_model_storage_format(
        dataset=dataset, storage_format="Large", workspace=workspace
    )
    if ssm == 200:
        client = fabric.PowerBIRestClient()
        response = client.patch(
            f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}",
            json=request_body,
        )
        if response.status_code != 200:
            raise FabricHTTPException(response)

        df = list_qso_settings(dataset=dataset, workspace=workspace)
        print(
            f"{icons.green_dot} Query scale out has been set on the '{dataset}' semantic model within the '{workspace}' workspace."
        )
        return df
    else:
        raise ValueError(
            f"{icons.red_dot} Failed to set the '{dataset}' semantic model within the '{workspace}' workspace to large semantic model storage format. This is a prerequisite for enabling Query Scale Out.\n\"https://learn.microsoft.com/power-bi/enterprise/service-premium-scale-out#prerequisites\""
        )


def set_semantic_model_storage_format(
    dataset: str, storage_format: str, workspace: Optional[str] = None
):
    """
    Sets the semantic model storage format.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    storage_format : str
        The storage format for the semantic model. Valid options: 'Large', 'Small'.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------

    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)
    dataset_id = resolve_dataset_id(dataset, workspace)

    storage_format = storage_format.capitalize()

    if storage_format == "Abf":
        storage_format = "Small"
    elif storage_format.startswith("Premium"):
        storage_format = "Large"

    storageFormats = ["Small", "Large"]

    if storage_format == "Large":
        request_body = {"targetStorageMode": "PremiumFiles"}
    elif storage_format == "Small":
        request_body = {"targetStorageMode": "Abf"}
    else:
        raise ValueError(
            f"{icons.red_dot} Invalid storage format value. Valid options: {storageFormats}."
        )

    client = fabric.PowerBIRestClient()
    response = client.patch(
        f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}", json=request_body
    )
    if response.status_code != 200:
        raise FabricHTTPException(response)
    print(f"{icons.green_dot} Semantic model storage format set to '{storage_format}'.")


def list_qso_settings(
    dataset: Optional[str] = None, workspace: Optional[str] = None
) -> pd.DataFrame:
    """
    Shows the query scale out settings for a semantic model (or all semantic models within a workspace).

    Parameters
    ----------
    dataset : str, default=None
        Name of the semantic model.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the query scale out settings.
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    if dataset is not None:
        dataset_id = resolve_dataset_id(dataset, workspace)

    workspace_id = fabric.get_workspace_id()
    df = pd.DataFrame(
        columns=[
            "Dataset Id",
            "Dataset Name",
            "Storage Mode",
            "QSO Auto Sync Enabled",
            "QSO Max Read Only Replicas",
        ]
    )
    client = fabric.PowerBIRestClient()
    response = client.get(f"/v1.0/myorg/groups/{workspace_id}/datasets")

    for v in response.json().get("value", []):
        tsm = v.get("targetStorageMode")
        if tsm == "Abf":
            sm = "Small"
        else:
            sm = "Large"
        new_data = {
            "Dataset Id": v.get("id"),
            "Dataset Name": v.get("name"),
            "Storage Mode": sm,
            "QSO Auto Sync Enabled": v.get("queryScaleOutSettings", {}).get(
                "autoSyncReadOnlyReplicas"
            ),
            "QSO Max Read Only Replicas": v.get("queryScaleOutSettings", {}).get(
                "maxReadOnlyReplicas"
            ),
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    df["QSO Auto Sync Enabled"] = df["QSO Auto Sync Enabled"].astype("bool")
    df["QSO Max Read Only Replicas"] = df["QSO Max Read Only Replicas"].astype("int")

    if dataset is not None:
        df = df[df["Dataset Id"] == dataset_id]

    return df


def set_workspace_default_storage_format(
    storage_format: str, workspace: Optional[str] = None
):
    """
    Sets the default storage format for semantic models within a workspace.

    Parameters
    ----------
    storage_format : str
        The storage format for the semantic model. Valid options: 'Large', 'Small'.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------

    """

    # https://learn.microsoft.com/en-us/rest/api/power-bi/groups/update-group#defaultdatasetstorageformat

    storageFormats = ["Small", "Large"]

    storage_format = storage_format.capitalize()

    if storage_format not in storageFormats:
        print(
            f"{icons.red_dot} Invalid storage format. Please choose from these options: {storageFormats}."
        )

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    request_body = {"name": workspace, "defaultDatasetStorageFormat": storage_format}

    client = fabric.PowerBIRestClient()
    response = client.patch(f"/v1.0/myorg/groups/{workspace_id}", json=request_body)

    if response.status_code != 200:
        raise FabricHTTPException(response)
    print(
        f"{icons.green_dot} The default storage format for the '{workspace}' workspace has been updated to '{storage_format}."
    )
