from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_workspace_id,
    _base_api,
    _create_dataframe,
    _update_dataframe_datatypes,
    delete_item,
)
import pandas as pd
from typing import Optional
from uuid import UUID


def list_variable_libraries(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows the variable libraries within a workspace.

    This is a wrapper function for the following API: `Items - List Variable Libraries <https://learn.microsoft.com/rest/api/fabric/variablelibrary/items/list-variable-libraries>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the variable libraries within a workspace.
    """

    columns = {
        "Variable Library Name": "string",
        "Variable Library Id": "string",
        "Description": "string",
        "Active Value Set Name": "string",
    }
    df = _create_dataframe(columns=columns)

    workspace_id = resolve_workspace_id(workspace)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/VariableLibraries",
        uses_pagination=True,
        client="fabric_sp",
    )

    dfs = []
    for r in responses:
        for v in r.get("value", []):
            prop = v.get("properties", {})

            new_data = {
                "Variable Library Name": v.get("displayName"),
                "Variable Library Id": v.get("id"),
                "Description": v.get("description"),
                "Active Value Set Name": prop.get("activeValueSetName"),
            }
            dfs.append(pd.DataFrame(new_data, index=[0]))

    if dfs:
        df = pd.concat(dfs, ignore_index=True)
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


def delete_variable_library(
    variable_library: str | UUID, workspace: Optional[str | UUID] = None
):
    """
    Deletes a variable library.

    This is a wrapper function for the following API: `Items - Delete Variable Library <https://learn.microsoft.com/rest/api/fabric/warehouse/items/delete-variable-library>`_.

    Parameters
    ----------
    navariable_libraryme: str | uuid.UUID
        Name or ID of the variable library.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    delete_item(item=variable_library, type="VariableLibrary", workspace=workspace)
