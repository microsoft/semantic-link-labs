from sempy_labs._helper_functions import (
    _base_api,
    resolve_workspace_id,
    resolve_item_id,
)
from typing import Optional
import pandas as pd
from uuid import UUID
from sempy._utils._log import log
import pyarrow as pa


@log
def execute_query(
    dataflow: str | UUID,
    query_name: str,
    custom_mashup_document: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Executes a query against a dataflow and returns the result.

    This is a wrapper function for the following API: `Query Execution - Execute Query <https://learn.microsoft.com/rest/api/fabric/dataflow/query-execution/execute-query>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    dataflow : str | uuid.UUID
        Name or ID of the dataflow.
    query_name : str
        The name of the query to execute from the dataflow (or from the custom mashup document if provided).
    custom_mashup_document : str, default=None
        Optional custom mashup document to override the dataflow's default mashup.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the results of the query execution.
    """

    workspace_id = resolve_workspace_id(workspace)
    item_id = resolve_item_id(item=dataflow, type="Dataflow", workspace=workspace_id)

    payload = {
        "queryName": query_name,
    }

    if custom_mashup_document:
        payload["customMashupDocument"] = custom_mashup_document

    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/dataflows/{item_id}/executeQuery",
        method="post",
        payload=payload,
        lro_return_status_code=True,
        status_codes=[200, 202],
    )

    # with pa.ipc.open_stream(response.content) as reader:
    #    df = reader.read_pandas()

    with pa.ipc.open_stream(response.content) as reader:
        batches = [batch for batch in reader]

    table = pa.Table.from_batches(batches)

    df = table.to_pandas(
        split_blocks=True,
        self_destruct=True,
    )

    return df
