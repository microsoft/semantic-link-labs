import pandas as pd
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    _base_api,
    _create_dataframe,
)
from uuid import UUID


def list_mirrored_warehouses(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows the mirrored warehouses within a workspace.

    This is a wrapper function for the following API: `Items - List Mirrored Warehouses <https://learn.microsoft.com/rest/api/fabric/mirroredwarehouse/items/list-mirrored-warehouses>`_.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the mirrored warehouses within a workspace.
    """

    columns = {
        "Mirrored Warehouse Name": "string",
        "Mirrored Warehouse Id": "string",
        "Description": "string",
    }
    df = _create_dataframe(columns=columns)

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/mirroredWarehouses",
        status_codes=200,
        uses_pagination=True,
    )

    for r in responses:
        for v in r.get("value", []):
            new_data = {
                "Mirrored Warehouse Name": v.get("displayName"),
                "Mirrored Warehouse Id": v.get("id"),
                "Description": v.get("description"),
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df
