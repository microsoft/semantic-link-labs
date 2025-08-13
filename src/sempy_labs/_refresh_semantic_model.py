import sempy.fabric as fabric
import time
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    _get_partition_map,
    _process_and_display_chart,
    resolve_dataset_name_and_id,
    _update_dataframe_datatypes,
    _base_api,
)
from typing import Any, List, Optional, Union
from sempy._utils._log import log
import sempy_labs._icons as icons
import pandas as pd
import warnings
import ipywidgets as widgets
import json
from uuid import UUID


@log
def refresh_semantic_model(
    dataset: str | UUID,
    tables: Optional[Union[str, List[str]]] = None,
    partitions: Optional[Union[str, List[str]]] = None,
    refresh_type: str = "full",
    retry_count: int = 0,
    apply_refresh_policy: bool = True,
    max_parallelism: int = 10,
    workspace: Optional[str | UUID] = None,
    visualize: bool = False,
    commit_mode: str = "transactional",
) -> pd.DataFrame | None:
    """
    Refreshes a semantic model.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    tables : str, List[str], default=None
        A string or a list of tables to refresh.
    partitions: str, List[str], default=None
        A string or a list of partitions to refresh. Partitions must be formatted as such: 'Table Name'[Partition Name].
    refresh_type : str, default="full"
        The type of processing to perform. Types align with the TMSL refresh command types: full, clearValues, calculate, dataOnly, automatic, and defragment. The add type isn't supported. Defaults to "full".
    retry_count : int, default=0
        Number of times the operation retries before failing.
    apply_refresh_policy : bool, default=True
        If an incremental refresh policy is defined, determines whether to apply the policy. Modes are true or false. If the policy isn't applied, the full process leaves partition definitions unchanged, and fully refreshes all partitions in the table. If commitMode is transactional, applyRefreshPolicy can be true or false. If commitMode is partialBatch, applyRefreshPolicy of true isn't supported, and applyRefreshPolicy must be set to false.
    max_parallelism : int, default=10
        Determines the maximum number of threads that can run the processing commands in parallel.
        This value aligns with the MaxParallelism property that can be set in the TMSL Sequence command or by using other methods.
        Defaults to 10.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    visualize : bool, default=False
        If True, displays a Gantt chart showing the refresh statistics for each table/partition.
    commit_mode : str, default="transactional"
        Determines whether to commit objects in batches or only when complete. Modes are "transactional" and "partialBatch". Defaults to "transactional".

    Returns
    -------
    pandas.DataFrame | None
        If 'visualize' is set to True, returns a pandas dataframe showing the SSAS trace output used to generate the visualization.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    if isinstance(tables, str):
        tables = [tables]
    if isinstance(partitions, str):
        partitions = [partitions]

    objects: List[Any] = []

    if tables is not None:
        objects = objects + [{"table": table} for table in tables]
    if partitions is not None:

        def extract_names(partition):
            parts = partition.split("[")
            table_name = parts[0].strip("'")
            partition_name = parts[1].strip("]")
            return {"table": table_name, "partition": partition_name}

        objects = objects + [extract_names(partition) for partition in partitions]

    refresh_type = refresh_type.lower()
    for prefix, mapped_value in icons.refresh_type_mapping.items():
        if refresh_type.startswith(prefix):
            refresh_type = mapped_value
            break

    valid_refresh_types = list(icons.refresh_type_mapping.values())
    if refresh_type not in valid_refresh_types:
        raise ValueError(
            f"{icons.red_dot} Invalid refresh type. Refresh type must be one of these values: {valid_refresh_types}."
        )

    def refresh_and_trace_dataset(
        dataset,
        workspace,
        refresh_type,
        retry_count,
        apply_refresh_policy,
        max_parallelism,
        objects,
        visualize,
        commit_mode,
    ):
        # Ignore specific warnings
        warnings.filterwarnings(
            "ignore",
            message="No trace logs have been recorded. Try starting the trace with a larger 'delay'",
        )

        def extract_failure_error():
            error_messages = []
            combined_messages = ""
            final_message = f"{icons.red_dot} The refresh of the '{dataset_name}' semantic model within the '{workspace_name}' workspace has failed."
            for _, r in fabric.get_refresh_execution_details(
                refresh_request_id=request_id,
                dataset=dataset_id,
                workspace=workspace_id,
            ).messages.iterrows():
                error_messages.append(f"{r['Type']}: {r['Message']}")

            if error_messages:
                combined_messages = "\n".join(error_messages)
            final_message += f"'\n' {combined_messages}"

            return final_message

        # Function to perform dataset refresh
        def refresh_dataset():
            return fabric.refresh_dataset(
                dataset=dataset_id,
                workspace=workspace_id,
                refresh_type=refresh_type,
                retry_count=retry_count,
                apply_refresh_policy=apply_refresh_policy,
                max_parallelism=max_parallelism,
                commit_mode=commit_mode,
                objects=objects if objects else None,
            )

        def check_refresh_status(request_id):
            request_details = fabric.get_refresh_execution_details(
                dataset=dataset_id,
                refresh_request_id=request_id,
                workspace=workspace_id,
            )
            return request_details.status

        def display_trace_logs(trace, partition_map, widget, title, stop=False):
            if stop:
                df = trace.stop()
            else:
                df = trace.get_trace_logs()
            if not df.empty:
                df = df[
                    df["Event Subclass"].isin(["ExecuteSql", "Process"])
                ].reset_index(drop=True)
                df = pd.merge(
                    df,
                    partition_map[
                        ["PartitionID", "Object Name", "TableName", "PartitionName"]
                    ],
                    left_on="Object ID",
                    right_on="PartitionID",
                    how="left",
                )
                if not df.empty:
                    _process_and_display_chart(df, title=title, widget=widget)
                if stop:
                    df.drop(["Object Name", "PartitionID"], axis=1, inplace=True)
                    df.rename(columns={"TableName": "Table Name"}, inplace=True)
                    df.rename(columns={"PartitionName": "Partition Name"}, inplace=True)
                    return df

        # Start the refresh process
        if not visualize:
            request_id = refresh_dataset()
        print(
            f"{icons.in_progress} Refresh of the '{dataset_name}' semantic model within the '{workspace_name}' workspace is in progress..."
        )

        # Monitor refresh progress and handle tracing if visualize is enabled
        if visualize:
            partition_map = _get_partition_map(dataset, workspace)
            widget = widgets.Output()

            with fabric.create_trace_connection(
                dataset=dataset_id, workspace=workspace_id
            ) as trace_connection:
                with trace_connection.create_trace(icons.refresh_event_schema) as trace:
                    trace.start()
                    request_id = refresh_dataset()

                    while True:
                        status = check_refresh_status(request_id)
                        # Check if the refresh has completed
                        if status == "Completed":
                            break
                        elif status == "Failed":
                            raise ValueError(extract_failure_error())
                        elif status == "Cancelled":
                            print(
                                f"{icons.yellow_dot} The refresh of the '{dataset_name}' semantic model within the '{workspace_name}' workspace has been cancelled."
                            )
                            return

                        # Capture and display logs in real-time
                        display_trace_logs(
                            trace,
                            partition_map,
                            widget,
                            title="Refresh in progress...",
                        )

                        time.sleep(3)  # Wait before the next check

                    # Final log display after completion
                    time.sleep(5)

                    # Stop trace and display final chart
                    final_df = display_trace_logs(
                        trace,
                        partition_map,
                        widget,
                        title="Refresh Completed",
                        stop=True,
                    )

                    print(
                        f"{icons.green_dot} Refresh '{refresh_type}' of the '{dataset_name}' semantic model within the '{workspace_name}' workspace is complete."
                    )
                    return final_df

        # For non-visualize case, only check refresh status
        else:
            while True:
                status = check_refresh_status(request_id)
                if status == "Completed":
                    break
                elif status == "Failed":
                    raise ValueError(extract_failure_error())
                elif status == "Cancelled":
                    print(
                        f"{icons.yellow_dot} The refresh of the '{dataset_name}' semantic model within the '{workspace_name}' workspace has been cancelled."
                    )
                    return

                time.sleep(3)

            print(
                f"{icons.green_dot} Refresh '{refresh_type}' of the '{dataset_name}' semantic model within the '{workspace_name}' workspace is complete."
            )

    final_output = refresh_and_trace_dataset(
        dataset=dataset,
        workspace=workspace,
        refresh_type=refresh_type,
        retry_count=retry_count,
        apply_refresh_policy=apply_refresh_policy,
        max_parallelism=max_parallelism,
        objects=objects,
        visualize=visualize,
        commit_mode=commit_mode,
    )

    return final_output


@log
def cancel_dataset_refresh(
    dataset: str | UUID,
    request_id: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Cancels the refresh of a semantic model which was executed via the `Enhanced Refresh API <https://learn.microsoft.com/power-bi/connect-data/asynchronous-refresh>`_

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    request_id : str, default=None
        The request id of a semantic model refresh.
        Defaults to finding the latest active refresh of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    rr = fabric.list_refresh_requests(dataset=dataset_id, workspace=workspace_id)
    rr_filt = rr[rr["Status"] == "Unknown"]

    if request_id is None:
        if len(rr_filt) == 0:
            raise ValueError(
                f"{icons.red_dot} There are no active Enhanced API refreshes of the '{dataset_name}' semantic model within the '{workspace_name}' workspace."
            )

        request_id = rr_filt["Request Id"].iloc[0]

    _base_api(
        request=f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes/{request_id}",
        method="delete",
    )
    print(
        f"{icons.green_dot} The '{request_id}' refresh request for the '{dataset_name}' semantic model within the '{workspace_name}' workspace has been cancelled."
    )


@log
def get_semantic_model_refresh_history(
    dataset: str | UUID,
    request_id: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Obtains the semantic model refresh history (refreshes executed via the Enhanced Refresh API).

    This is a wrapper function for the following API: `Datasets - Get Refresh History In Group <https://learn.microsoft.com/rest/api/power-bi/datasets/get-refresh-history-in-group>`_.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    request_id : str, default=None
        The request id of a semantic model refresh.
        Defaults to None which resolves to showing all refresh requests for the given semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the semantic model refresh history.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)
    df = pd.DataFrame(
        columns=[
            "Request Id",
            "Refresh Type",
            "Start Time",
            "End Time",
            "Status",
            "Extended Status",
        ]
    )

    response = _base_api(
        request=f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes"
    )
    data = []

    for i in response.json().get("value", []):
        error = i.get("serviceExceptionJson")
        if error:
            error_json = json.loads(error)
        if request_id is None:
            new_data = {
                "Request Id": i.get("requestId"),
                "Refresh Type": i.get("refreshType"),
                "Start Time": i.get("startTime"),
                "End Time": i.get("endTime"),
                "Error Code": error_json.get("errorCode") if error else None,
                "Error Description": (
                    error_json.get("errorDescription") if error else None
                ),
                "Status": i.get("status"),
                "Extended Status": i.get("extendedStatus"),
                "Attempts": i.get("refreshAttempts"),
            }
            data.append(new_data)

        elif request_id == i.get("requestId"):
            for attempt in i.get("refreshAttempts", []):
                attempt_error = attempt.get("serviceExceptionJson")
                if attempt_error:
                    attempt_error_json = json.loads(attempt_error)
                new_data = {
                    "Request Id": i.get("requestId"),
                    "Refresh Type": i.get("refreshType"),
                    "Start Time": i.get("startTime"),
                    "End Time": i.get("endTime"),
                    "Error Code": error_json.get("errorCode") if error else None,
                    "Error Description": (
                        error_json.get("errorDescription") if error else None
                    ),
                    "Status": i.get("status"),
                    "Extended Status": i.get("extendedStatus"),
                    "Attempt Id": attempt.get("attemptId"),
                    "Attempt Start Time": attempt.get("startTime"),
                    "Attempt End Time": attempt.get("endTime"),
                    "Attempt Error Code": (
                        attempt_error_json.get("errorCode") if attempt_error else None
                    ),
                    "Attempt Error Description": (
                        attempt_error_json.get("errorDescription")
                        if attempt_error
                        else None
                    ),
                    "Type": attempt.get("type"),
                }
                data.append(new_data)

    if data:
        df = pd.DataFrame(data)

    # date_cols = ["Start Time", "End Time"]
    # df[date_cols] = df[date_cols].apply(pd.to_datetime)

    if "Attempt Id" in df.columns:
        column_map = {
            "Attempt Id": "int",
        }

        _update_dataframe_datatypes(dataframe=df, column_map=column_map)

    return df
