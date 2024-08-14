import sempy.fabric as fabric
from typing import Optional, List
from sempy._utils._log import log
import sempy_labs._icons as icons
from sempy.fabric.exceptions import FabricHTTPException
import datetime
import numpy as np
import pandas as pd


@log
def assign_workspaces_to_capacity(
    source_capacity: str,
    target_capacity: str,
    workspace: Optional[str | List[str]] = None,
):
    """
    Assigns a workspace to a capacity.

    Parameters
    ----------
    source_capacity : str
        The name of the source capacity.
    target_capacity : str
        The name of the target capacity.
    workspace : str | List[str], default=None
        The name of the workspace(s).
        Defaults to None which resolves to migrating all workspaces within the source capacity to the target capacity.
    """

    if isinstance(workspace, str):
        workspace = [workspace]

    dfC = fabric.list_capacities()
    dfC_filt = dfC[dfC["Display Name"] == source_capacity]
    source_capacity_id = dfC_filt["Id"].iloc[0]

    dfC_filt = dfC[dfC["Display Name"] == target_capacity]
    target_capacity_id = dfC_filt["Id"].iloc[0]

    if workspace is None:
        workspaces = fabric.list_workspaces(
            filter=f"capacityId eq '{source_capacity_id.upper()}'"
        )["Id"].values
    else:
        dfW = fabric.list_workspaces()
        workspaces = dfW[dfW["Name"].isin(workspace)]["Id"].values

    workspaces = np.array(workspaces)
    batch_size = 999
    for i in range(0, len(workspaces), batch_size):
        batch = workspaces[i : i + batch_size].tolist()
        batch_length = len(batch)
        start_time = datetime.datetime.now()
        request_body = {
            "capacityMigrationAssignments": [
                {
                    "targetCapacityObjectId": target_capacity_id.upper(),
                    "workspacesToAssign": batch,
                }
            ]
        }

        client = fabric.PowerBIRestClient()
        response = client.post(
            "/v1.0/myorg/admin/capacities/AssignWorkspaces",
            json=request_body,
        )

        if response.status_code != 200:
            raise FabricHTTPException(response)
        end_time = datetime.datetime.now()
        print(
            f"Total time for assigning {str(batch_length)} workspaces is {str((end_time - start_time).total_seconds())}"
        )
    print(
        f"{icons.green_dot} The workspaces have been assigned to the '{target_capacity}' capacity."
    )


def list_capacities() -> pd.DataFrame:
    """
    Shows the a list of capacities and their properties.

    Parameters
    ----------

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the capacities and their properties
    """

    df = pd.DataFrame(
        columns=["Capacity Id", "Capacity Name", "Sku", "Region", "State", "Admins"]
    )

    client = fabric.PowerBIRestClient()
    response = client.get("/v1.0/myorg/admin/capacities")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    response = client.get("/v1.0/myorg/capacities")

    for i in response.json().get("value", []):
        new_data = {
            "Capacity Id": i.get("id").lower(),
            "Capacity Name": i.get("displayName"),
            "Sku": i.get("sku"),
            "Region": i.get("region"),
            "State": i.get("state"),
            "Admins": [i.get("admins", [])],
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df
