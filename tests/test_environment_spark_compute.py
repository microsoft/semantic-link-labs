import uuid
from unittest.mock import MagicMock, patch

import pytest

from sempy_labs.environment import _pubstage

SPARK_COMPUTE = {
    "instancePool": {
        "name": "Starter Pool",
        "type": "Workspace",
        "id": "00000000-0000-0000-0000-000000000000",
    },
    "driverCores": 4,
    "driverMemory": "28g",
    "executorCores": 4,
    "executorMemory": "28g",
    "dynamicExecutorAllocation": {"enabled": True, "minExecutors": 1, "maxExecutors": 2},
    "sparkProperties": [],
    "runtimeVersion": "1.3",
}


@pytest.mark.parametrize(
    "func, path",
    [
        (_pubstage.get_published_spark_compute, "sparkCompute"),
        (_pubstage.get_staging_spark_compute, "staging/sparkCompute"),
    ],
)
def test_spark_compute_reads_the_response_body(func, path):
    """The API helper returns an HTTP response, so the function has to read
    its JSON body rather than call .get() on the response itself."""

    workspace_id = str(uuid.uuid4())
    item_id = str(uuid.uuid4())
    response = MagicMock()
    response.json.return_value = SPARK_COMPUTE
    # A real requests.Response has no .get(); make the mock behave the same.
    del response.get

    with patch.object(_pubstage, "resolve_workspace_id", return_value=workspace_id), patch.object(
        _pubstage, "resolve_item_id", return_value=item_id
    ), patch.object(_pubstage, "_base_api", return_value=response) as base_api:
        df = func(environment="env", workspace="ws")

    assert base_api.call_args.kwargs["request"] == f"/v1/workspaces/{workspace_id}/environments/{item_id}/{path}"
    assert len(df) == 1
    row = df.iloc[0]
    assert row["Instance Pool Name"] == "Starter Pool"
    assert row["Driver Cores"] == 4
    assert row["Executor Memory"] == "28g"
    assert bool(row["Dynamic Executor Allocation Enabled"]) is True
    assert row["Dynamic Executor Allocation Max Executors"] == 2
    assert row["Runtime Version"] == "1.3"
