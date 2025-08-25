from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_lakehouse_name_and_id,
    _base_api,
)
from uuid import UUID
from sempy._utils._log import log
import sempy_labs._icons as icons
import time
from sempy_labs._job_scheduler import (
    _get_item_job_instance,
)
import pandas as pd


@log
def refresh_materialized_lake_views(
    lakehouse: Optional[str | UUID] = None, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Run on-demand Refresh MaterializedLakeViews job instance.

    This is a wrapper function for the following API: `Background Jobs - Run On Demand Refresh Materialized Lake Views <https://learn.microsoft.com/rest/api/fabric/lakehouse/background-jobs/run-on-demand-refresh-materialized-lake-views>`_.

    Parameters
    ----------
    lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the job instance details of the refresh materialized lake views operation.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (lakehouse_name, lakehouse_id) = resolve_lakehouse_name_and_id(
        lakehouse=lakehouse, workspace=workspace_id
    )

    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/jobs/instances?jobType=RefreshMaterializedLakeViews",
        method="post",
        status_codes=[202],
    )

    print(
        f"{icons.in_progress} The refresh materialized lake views job for the '{lakehouse_name}' lakehouse within the '{workspace_name}' workspace has been initiated."
    )

    status_url = response.headers.get("Location").split("fabric.microsoft.com")[1]
    status = None
    while status not in ["Completed", "Failed"]:
        response = _base_api(request=status_url)
        status = response.json().get("status")
        time.sleep(3)

    df = _get_item_job_instance(url=status_url)

    if status == "Completed":
        print(
            f"{icons.green_dot} The refresh materialized lake views job for the '{lakehouse_name}' lakehouse within the '{workspace_name}' workspace has succeeded."
        )
    else:
        print(status)
        print(
            f"{icons.red_dot} The refresh materialized lake views job for the '{lakehouse_name}' lakehouse within the '{workspace_name}' workspace has failed."
        )

    return df
