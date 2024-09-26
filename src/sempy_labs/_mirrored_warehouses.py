import sempy.fabric as fabric
import pandas as pd
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    pagination,
)
from sempy.fabric.exceptions import FabricHTTPException


def list_mirrored_warehouses(workspace: Optional[str] = None) -> pd.DataFrame:
    """
    Shows the mirrored warehouses within a workspace.

    Parameters
    ----------
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the mirrored warehouses within a workspace.
    """

    df = pd.DataFrame(
        columns=["Mirrored Warehouse Name", "Mirrored Warehouse Id", "Description"]
    )

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/mirroredWarehouses")
    if response.status_code != 200:
        raise FabricHTTPException(response)
    responses = pagination(client, response)

    for r in responses:
        for v in r.get("value", []):

            new_data = {
                "Mirrored Warehouse Name": v.get("displayName"),
                "Mirrored Warehouse Id": v.get("id"),
                "Description": v.get("description"),
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df
