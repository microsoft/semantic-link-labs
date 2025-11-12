import sempy.fabric as fabric
import pandas as pd
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_dataset_name_and_id,
    _update_dataframe_datatypes,
    _base_api,
    _create_dataframe,
    resolve_workspace_id,
)
from sempy._utils._log import log
from typing import Optional, Tuple
import sempy_labs._icons as icons
from uuid import UUID


@log
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

    _base_api(
        request=f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/queryScaleOut/sync",
        method="post",
    )
    print(
        f"{icons.green_dot} QSO sync initiated for the '{dataset_name}' semantic model within the '{workspace_name}' workspace."
    )


@log
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

    columns = {
        "Scale Out Status": "string",
        "Sync Start Time": "datetime",
        "Sync End Time": "datetime",
        "Commit Version": "int",
        "Commit Timestamp": "datetime",
        "Target Sync Version": "int",
        "Target Sync Timestamp": "datetime",
        "Trigger Reason": "string",
        "Min Active Read Version": "int",
        "Min Active Read Timestamp": "datetime",
    }
    df = _create_dataframe(columns=columns)

    columns_rep = {
        "Replica ID": "string",
        "Replica Type": "string",
        "Replica Version": "string",
        "Replica Timestamp": "datetime",
    }

    dfRep = _create_dataframe(columns=columns_rep)

    workspace_id = resolve_workspace_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    response = _base_api(
        request=f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/queryScaleOut/syncStatus"
    )

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
        df = pd.DataFrame([new_data])
        for r in o.get("scaleOutReplicas", []):
            new_data = {
                "Replica ID": r.get("replicaId"),
                "Replica Type": r.get("replicaType"),
                "Replica Version": str(r.get("replicaVersion")),
                "Replica Timestamp": r.get("replicaTimestamp"),
            }
            dfRep = pd.DataFrame([new_data])

        _update_dataframe_datatypes(dataframe=df, column_map=columns)
        _update_dataframe_datatypes(dataframe=dfRep, column_map=columns_rep)

        return df, dfRep
    else:
        print(f"{sos}\n\n")
        return df, dfRep


@log
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

    payload = {"queryScaleOutSettings": {"maxReadOnlyReplicas": "0"}}

    _base_api(
        request=f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}",
        method="patch",
        payload=payload,
    )

    df = list_qso_settings(dataset=dataset_id, workspace=workspace_id)

    print(
        f"{icons.green_dot} Query scale out has been disabled for the '{dataset_name}' semantic model within the '{workspace_name}' workspace."
    )

    return df


@log
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

    payload = {
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

    _base_api(
        request=f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}",
        method="patch",
        payload=payload,
    )

    df = list_qso_settings(dataset=dataset_id, workspace=workspace_id)
    print(
        f"{icons.green_dot} Query scale out has been set on the '{dataset_name}' semantic model within the '{workspace_name}' workspace."
    )

    return df


@log
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
        payload = {"targetStorageMode": "PremiumFiles"}
    elif storage_format == "Small":
        payload = {"targetStorageMode": "Abf"}
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

    _base_api(
        request=f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}",
        method="patch",
        payload=payload,
    )
    print(
        f"{icons.green_dot} The semantic model storage format for the '{dataset_name}' semantic model within the '{workspace_name}' workspace has been set to '{storage_format}'."
    )


@log
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

    workspace_id = resolve_workspace_id(workspace)

    if dataset is not None:
        (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    columns = {
        "Dataset Id": "string",
        "Dataset Name": "string",
        "Storage Mode": "string",
        "QSO Auto Sync Enabled": "bool",
        "QSO Max Read Only Replicas": "int",
    }
    df = _create_dataframe(columns=columns)

    response = _base_api(request=f"/v1.0/myorg/groups/{workspace_id}/datasets")

    rows = []
    for v in response.json().get("value", []):
        tsm = v.get("targetStorageMode")
        if tsm == "Abf":
            sm = "Small"
        else:
            sm = "Large"
        rows.append(
            {
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
        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    if dataset is not None:
        df = df[df["Dataset Id"] == dataset_id]

    return df


@log
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
    dfW = fabric.list_workspaces(filter=f"id eq '{workspace_id}'")
    if len(dfW) == 0:
        raise ValueError()
    current_storage_format = dfW["Default Dataset Storage Format"].iloc[0]

    if current_storage_format == storage_format:
        print(
            f"{icons.info} The '{workspace_name}' is already set to a default storage format of '{current_storage_format}'."
        )
        return

    payload = {
        "name": workspace_name,
        "defaultDatasetStorageFormat": storage_format,
    }

    _base_api(
        request=f"/v1.0/myorg/groups/{workspace_id}", method="patch", payload=payload
    )

    print(
        f"{icons.green_dot} The default storage format for the '{workspace_name}' workspace has been updated to '{storage_format}."
    )
