import sempy.fabric as fabric
import pandas as pd
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_dataset_name_and_id,
)
from typing import Optional, Tuple
import sempy_labs._icons as icons
from sempy.fabric.exceptions import FabricHTTPException
from uuid import UUID


def qso_sync(dataset: str | UUID, workspace: Optional[str | UUID] = None):
    """
    Triggers a query scale-out sync of read-only replicas for the specified dataset from the specified workspace.

    This is a wrapper function for the following API: `Datasets - Trigger Query Scale Out Sync In Group <https://learn.microsoft.com/rest/api/power-bi/datasets/trigger-query-scale-out-sync-in-group>`_.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    client = fabric.PowerBIRestClient()
    response = client.post(
        f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/queryScaleOut/sync"
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)
    print(
        f"{icons.green_dot} QSO sync initiated for the '{dataset_name}' semantic model within the '{workspace_name}' workspace."
    )


def qso_sync_status(
    dataset: str | UUID, workspace: Optional[str | UUID] = None
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns the query scale-out sync status for the specified dataset from the specified workspace.

    This is a wrapper function for the following API: `Datasets - Get Query Scale Out Sync Status In Group <https://learn.microsoft.com/rest/api/power-bi/datasets/get-query-scale-out-sync-status-in-group>`_.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    Tuple[pandas.DataFrame, pandas.DataFrame]
        2 pandas dataframes showing the query scale-out sync status.
    """

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

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

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


def disable_qso(
    dataset: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Sets the max read-only replicas to 0, disabling query scale out.

    This is a wrapper function for the following API: `Datasets - Update Dataset In Group <https://learn.microsoft.com/rest/api/power-bi/datasets/update-dataset-in-group>`_.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the current query scale out settings.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    request_body = {"queryScaleOutSettings": {"maxReadOnlyReplicas": "0"}}

    client = fabric.PowerBIRestClient()
    response = client.patch(
        f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}", json=request_body
    )
    if response.status_code != 200:
        raise FabricHTTPException(response)

    df = list_qso_settings(dataset=dataset_id, workspace=workspace_id)

    print(
        f"{icons.green_dot} Query scale out has been disabled for the '{dataset_name}' semantic model within the '{workspace_name}' workspace."
    )

    return df


def set_qso(
    dataset: str | UUID,
    auto_sync: bool = True,
    max_read_only_replicas: int = -1,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Sets the query scale out settings for a semantic model.

    This is a wrapper function for the following API: `Datasets - Update Dataset In Group <https://learn.microsoft.com/rest/api/power-bi/datasets/update-dataset-in-group>`_.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    auto_sync : bool, default=True
        Whether the semantic model automatically syncs read-only replicas.
    max_read_only_replicas : int, default=-1
        To enable semantic model scale-out, set max_read_only_replicas to -1, or any non-0 value. A value of -1 allows Power BI to create as many read-only replicas as your Power BI capacity supports. You can also explicitly set the replica count to a value lower than that of the capacity maximum. Setting max_read_only_replicas to -1 is recommended.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the current query scale-out settings.
    """

    from sempy_labs._helper_functions import is_default_semantic_model

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    if is_default_semantic_model(dataset=dataset_id, workspace=workspace_id):
        raise ValueError(
            f"{icons.red_dot} The 'set_qso' function does not run against default semantic models."
        )

    if max_read_only_replicas == 0:
        disable_qso(dataset=dataset_id, workspace=workspace_id)
        return

    request_body = {
        "queryScaleOutSettings": {
            "autoSyncReadOnlyReplicas": auto_sync,
            "maxReadOnlyReplicas": max_read_only_replicas,
        }
    }

    dfL = list_qso_settings(dataset=dataset_id, workspace=workspace_id)
    storage_mode = dfL["Storage Mode"].iloc[0]

    if storage_mode == "Small":
        set_semantic_model_storage_format(
            dataset=dataset_id, storage_format="Large", workspace=workspace_id
        )

    client = fabric.PowerBIRestClient()
    response = client.patch(
        f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}",
        json=request_body,
    )
    if response.status_code != 200:
        raise FabricHTTPException(response)

    df = list_qso_settings(dataset=dataset_id, workspace=workspace_id)
    print(
        f"{icons.green_dot} Query scale out has been set on the '{dataset_name}' semantic model within the '{workspace_name}' workspace."
    )

    return df


def set_semantic_model_storage_format(
    dataset: str | UUID, storage_format: str, workspace: Optional[str | UUID] = None
):
    """
    Sets the semantic model storage format.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    storage_format : str
        The storage format for the semantic model. Valid options: 'Large', 'Small'.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

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

    dfL = list_qso_settings(dataset=dataset_id, workspace=workspace_id)
    current_storage_format = dfL["Storage Mode"].iloc[0]

    if current_storage_format == storage_format:
        print(
            f"{icons.info} The '{dataset_name}' semantic model within the '{workspace_name}' workspace is already set to '{storage_format.lower()}' storage format."
        )
        return

    client = fabric.PowerBIRestClient()
    response = client.patch(
        f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}", json=request_body
    )
    if response.status_code != 200:
        raise FabricHTTPException(response)
    print(
        f"{icons.green_dot} The semantic model storage format for the '{dataset_name}' semantic model within the '{workspace_name}' workspace has been set to '{storage_format}'."
    )


def list_qso_settings(
    dataset: Optional[str | UUID] = None, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Shows the query scale out settings for a semantic model (or all semantic models within a workspace).

    Parameters
    ----------
    dataset : str | uuid.UUID, default=None
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the query scale out settings.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    if dataset is not None:
        (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

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
    storage_format: str, workspace: Optional[str | UUID] = None
):
    """
    Sets the default storage format for semantic models within a workspace.

    Parameters
    ----------
    storage_format : str
        The storage format for the semantic model. Valid options: 'Large', 'Small'.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    # https://learn.microsoft.com/en-us/rest/api/power-bi/groups/update-group#defaultdatasetstorageformat

    storageFormats = ["Small", "Large"]

    storage_format = storage_format.capitalize()

    if storage_format not in storageFormats:
        raise ValueError(
            f"{icons.red_dot} Invalid storage format. Please choose from these options: {storageFormats}."
        )

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    # Check current storage format
    dfW = fabric.list_workspaces(filter=f"name eq '{workspace_name}'")
    if len(dfW) == 0:
        raise ValueError()
    current_storage_format = dfW["Default Dataset Storage Format"].iloc[0]

    if current_storage_format == storage_format:
        print(
            f"{icons.info} The '{workspace_name}' is already set to a default storage format of '{current_storage_format}'."
        )
        return

    request_body = {
        "name": workspace_name,
        "defaultDatasetStorageFormat": storage_format,
    }

    client = fabric.PowerBIRestClient()
    response = client.patch(f"/v1.0/myorg/groups/{workspace_id}", json=request_body)

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The default storage format for the '{workspace_name}' workspace has been updated to '{storage_format}."
    )
