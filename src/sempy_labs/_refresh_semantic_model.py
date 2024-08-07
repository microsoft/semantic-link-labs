import sempy.fabric as fabric
import time
from sempy_labs._helper_functions import resolve_dataset_id
from typing import Any, List, Optional, Union
from sempy._utils._log import log
import sempy_labs._icons as icons
from sempy_labs._helper_functions import resolve_workspace_name_and_id
from sempy.fabric.exceptions import FabricHTTPException


@log
def refresh_semantic_model(
    dataset: str,
    tables: Optional[Union[str, List[str]]] = None,
    partitions: Optional[Union[str, List[str]]] = None,
    refresh_type: Optional[str] = None,
    retry_count: Optional[int] = 0,
    apply_refresh_policy: Optional[bool] = True,
    max_parallelism: Optional[int] = 10,
    workspace: Optional[str] = None,
):
    """
    Refreshes a semantic model.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    tables : str, List[str], default=None
        A string or a list of tables to refresh.
    partitions: str, List[str], default=None
        A string or a list of partitions to refresh. Partitions must be formatted as such: 'Table Name'[Partition Name].
    refresh_type : str, default='full'
        The type of processing to perform. Types align with the TMSL refresh command types: full, clearValues, calculate, dataOnly, automatic, and defragment. The add type isn't supported. Defaults to "full".
    retry_count : int, default=0
        Number of times the operation retries before failing.
    apply_refresh_policy : bool, default=True
        If an incremental refresh policy is defined, determines whether to apply the policy. Modes are true or false. If the policy isn't applied, the full process leaves partition definitions unchanged, and fully refreshes all partitions in the table. If commitMode is transactional, applyRefreshPolicy can be true or false. If commitMode is partialBatch, applyRefreshPolicy of true isn't supported, and applyRefreshPolicy must be set to false.
    max_parallelism : int, default=10
        Determines the maximum number of threads that can run the processing commands in parallel.
        This value aligns with the MaxParallelism property that can be set in the TMSL Sequence command or by using other methods.
        Defaults to 10.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    workspace = fabric.resolve_workspace_name(workspace)

    if refresh_type is None:
        refresh_type = "full"

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

    refresh_type = (
        refresh_type.lower().replace("only", "Only").replace("values", "Values")
    )

    refreshTypes = [
        "full",
        "automatic",
        "dataOnly",
        "calculate",
        "clearValues",
        "defragment",
    ]

    if refresh_type not in refreshTypes:
        raise ValueError(
            f"{icons.red_dot} Invalid refresh type. Refresh type must be one of these values: {refreshTypes}."
        )

    if len(objects) == 0:
        requestID = fabric.refresh_dataset(
            dataset=dataset,
            workspace=workspace,
            refresh_type=refresh_type,
            retry_count=retry_count,
            apply_refresh_policy=apply_refresh_policy,
            max_parallelism=max_parallelism,
        )
    else:
        requestID = fabric.refresh_dataset(
            dataset=dataset,
            workspace=workspace,
            refresh_type=refresh_type,
            retry_count=retry_count,
            apply_refresh_policy=apply_refresh_policy,
            max_parallelism=max_parallelism,
            objects=objects,
        )
    print(
        f"{icons.in_progress} Refresh of the '{dataset}' semantic model within the '{workspace}' workspace is in progress..."
    )
    if len(objects) != 0:
        print(objects)

    while True:
        requestDetails = fabric.get_refresh_execution_details(
            dataset=dataset, refresh_request_id=requestID, workspace=workspace
        )
        status = requestDetails.status

        # Check if the refresh has completed
        if status == "Completed":
            break
        elif status == "Failed":
            raise ValueError(
                f"{icons.red_dot} The refresh of the '{dataset}' semantic model within the '{workspace}' workspace has failed."
            )
        elif status == "Cancelled":
            print(
                f"{icons.yellow_dot} The refresh of the '{dataset}' semantic model within the '{workspace}' workspace has been cancelled."
            )
            return

        time.sleep(3)

    print(
        f"{icons.green_dot} Refresh of the '{dataset}' semantic model within the '{workspace}' workspace is complete."
    )


@log
def cancel_dataset_refresh(
    dataset: str, request_id: Optional[str] = None, workspace: Optional[str] = None
):
    """
    Cancels the refresh of a semantic model which was executed via the `Enhanced Refresh API <https://learn.microsoft.com/power-bi/connect-data/asynchronous-refresh>`_

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    request_id : str, default=None
        The request id of a semantic model refresh.
        Defaults to finding the latest active refresh of the semantic model.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    rr = fabric.list_refresh_requests(dataset=dataset, workspace=workspace)
    rr_filt = rr[rr["Status"] == "Unknown"]

    if request_id is None:
        if len(rr_filt) == 0:
            raise ValueError(
                f"{icons.red_dot} There are no active Enhanced API refreshes of the '{dataset}' semantic model within the '{workspace}' workspace."
            )

        request_id = rr_filt["Request Id"].iloc[0]

    dataset_id = resolve_dataset_id(dataset=dataset, workspace=workspace)

    client = fabric.PowerBIRestClient()

    response = client.delete(
        f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes/{request_id}"
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)
    print(
        f"{icons.green_dot} The '{request_id}' refresh request for the '{dataset}' semantic model within the '{workspace}' workspace has been cancelled."
    )
