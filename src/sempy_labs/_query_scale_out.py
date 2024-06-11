import sempy
import sempy.fabric as fabric
import pandas as pd
from sempy_labs._helper_functions import resolve_dataset_id
from typing import List, Optional, Union
import sempy_labs._icons as icons


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

    if workspace is None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

    dataset_id = resolve_dataset_id(dataset, workspace)

    client = fabric.PowerBIRestClient()
    response = client.post(
        f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/queryScaleOut/sync"
    )

    if response.status_code == 200:
        print(
            f"{icons.green_dot} QSO sync initiated for the '{dataset}' semantic model within the '{workspace}' workspace."
        )
    else:
        print(
            f"{icons.red_dot} QSO sync failed for the '{dataset}' semantic model within the '{workspace}' workspace."
        )


def qso_sync_status(dataset: str, workspace: Optional[str] = None):
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

    if workspace is None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

    dataset_id = resolve_dataset_id(dataset, workspace)

    client = fabric.PowerBIRestClient()
    response = client.get(
        f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/queryScaleOut/syncStatus"
    )

    if response.status_code == 200:
        o = response.json()
        sos = o["scaleOutStatus"]

        if sos == "Enabled":
            new_data = {
                "Scale Out Status": o["scaleOutStatus"],
                "Sync Start Time": o["syncStartTime"],
                "Sync End Time": o["syncEndTime"],
                "Commit Version": o["commitVersion"],
                "Commit Timestamp": o["commitTimestamp"],
                "Target Sync Version": o["targetSyncVersion"],
                "Target Sync Timestamp": o["targetSyncTimestamp"],
                "Trigger Reason": o["triggerReason"],
                "Min Active Read Version": o["minActiveReadVersion"],
                "Min Active Read Timestamp": o["minActiveReadTimestamp"],
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

            for r in o["scaleOutReplicas"]:
                new_data = {
                    "Replica ID": r["replicaId"],
                    "Replica Type": r["replicaType"],
                    "Replica Version": str(r["replicaVersion"]),
                    "Replica Timestamp": r["replicaTimestamp"],
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
    else:
        return response.status_code


def disable_qso(dataset: str, workspace: Optional[str] = None):
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

    """

    if workspace is None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

    dataset_id = resolve_dataset_id(dataset, workspace)

    request_body = {"queryScaleOutSettings": {"maxReadOnlyReplicas": "0"}}

    client = fabric.PowerBIRestClient()
    response = client.patch(
        f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}", json=request_body
    )
    if response.status_code == 200:
        df = list_qso_settings(dataset=dataset, workspace=workspace)
        print(
            f"{icons.green_dot} Query scale out has been disabled for the '{dataset}' semantic model within the '{workspace}' workspace."
        )
        return df
    else:
        return f"{icons.red_dot} {response.status_code}"


def set_qso(
    dataset: str,
    auto_sync: Optional[bool] = True,
    max_read_only_replicas: Optional[int] = -1,
    workspace: Optional[str] = None,
):
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

    """

    # https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/update-dataset-in-group

    if workspace is None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

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
        if response.status_code == 200:
            df = list_qso_settings(dataset=dataset, workspace=workspace)
            print(
                f"{icons.green_dot} Query scale out has been set on the '{dataset}' semantic model within the '{workspace}' workspace."
            )
            return df
        else:
            return f"{icons.red_dot} {response.status_code}"
    else:
        print(
            f"{icons.red_dot} Failed to set the '{dataset}' semantic model within the '{workspace}' workspace to large semantic model storage format. This is a prerequisite for enabling Query Scale Out."
        )
        print(
            "https://learn.microsoft.com/power-bi/enterprise/service-premium-scale-out#prerequisites"
        )
        return


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

    if workspace is None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

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
        print(
            f"{icons.red_dot} Invalid storage format value. Valid options: {storageFormats}."
        )
        return

    client = fabric.PowerBIRestClient()
    response = client.patch(
        f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}", json=request_body
    )

    if response.status_code == 200:
        return print(
            f"{icons.green_dot} Semantic model storage format set to '{storage_format}'."
        )
    else:
        return f"{icons.red_dot} {response.status_code}"


def list_qso_settings(dataset: Optional[str] = None, workspace: Optional[str] = None):
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

    if workspace is None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

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
    for v in response.json()["value"]:
        tsm = v["targetStorageMode"]
        if tsm == "Abf":
            sm = "Small"
        else:
            sm = "Large"
        new_data = {
            "Dataset Id": v["id"],
            "Dataset Name": v["name"],
            "Storage Mode": sm,
            "QSO Auto Sync Enabled": v["queryScaleOutSettings"][
                "autoSyncReadOnlyReplicas"
            ],
            "QSO Max Read Only Replicas": v["queryScaleOutSettings"][
                "maxReadOnlyReplicas"
            ],
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
            f"Invalid storage format. Please choose from these options: {storageFormats}."
        )

    if workspace is None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

    request_body = {"name": workspace, "defaultDatasetStorageFormat": storage_format}

    client = fabric.PowerBIRestClient()
    response = client.patch(f"/v1.0/myorg/groups/{workspace_id}", json=request_body)

    if response.status_code == 200:
        print(
            f"{icons.green_dot} The default storage format for the '{workspace}' workspace has been updated to '{storage_format}."
        )
    else:
        print(f"{icons.red_dot} {response.status_code}")
