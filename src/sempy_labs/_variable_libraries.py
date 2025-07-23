from ._helper_functions import (
    resolve_item_id,
    resolve_workspace_id,
    _base_api,
    _create_dataframe,
    _update_dataframe_datatypes,
    delete_item,
    _decode_b64,
)
import pandas as pd
from typing import Any, Optional
from uuid import UUID
from sempy._utils._log import log
import json
import sempy_labs._icons as icons


@log
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

    rows = []
    for r in responses:
        for v in r.get("value", []):
            prop = v.get("properties", {})

            rows.append(
                {
                    "Variable Library Name": v.get("displayName"),
                    "Variable Library Id": v.get("id"),
                    "Description": v.get("description"),
                    "Active Value Set Name": prop.get("activeValueSetName"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
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


@log
def get_variable_library_definition(
    variable_library: str | UUID,
    workspace: Optional[str | UUID] = None,
    decode: bool = True,
    return_dataframe: bool = False,
) -> dict:

    workspace_id = resolve_workspace_id(workspace)
    variable_library_id = resolve_item_id(
        item=variable_library, type="VariableLibrary", workspace=workspace
    )

    result = _base_api(
        request=f"/v1/workspaces/{workspace_id}/variableLibraries/{variable_library_id}/getDefinition",
        method="post",
        client="fabric_sp",
        status_codes=None,
        lro_return_json=True,
    )

    if decode:
        definition = {"definition": {"parts": []}}

        for part in result.get("definition", {}).get("parts", []):
            path = part.get("path")
            payload = _decode_b64(part.get("payload"))
            definition["definition"]["parts"].append({"path": path, "payload": payload})
    else:
        definition = result.copy()

    if return_dataframe:
        df = pd.DataFrame(definition["definition"]["parts"])
        df.columns = ["Path", "Payload", "Payload Type"]
        return df
    else:
        return definition


@log
def list_variables(
    variable_library: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Lists the variables in a variable library.

    """

    result = get_variable_library_definition(
        variable_library=variable_library,
        workspace=workspace,
        decode=True,
        return_dataframe=False,
    )

    columns = {
        "Variable Name": "string",
        "Note": "string",
        "Type": "string",
        "Value": "string",
    }

    df = _create_dataframe(columns=columns)

    rows = []
    for part in result.get("definition").get("parts"):
        path = part.get("path")
        payload = json.loads(part.get("payload"))
        if path == "variables.json":

            for variable in payload.get("variables", []):
                rows.append(
                    {
                        "Variable Name": variable.get("name"),
                        "Note": variable.get("note"),
                        "Type": variable.get("type"),
                        "Value": variable.get("value"),
                    }
                )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


def get_variable_value(
    variable_name: str,
    variable_library: str | UUID,
    workspace: Optional[str | UUID] = None,
) -> Any:
    """
    Gets the value of a variable in a variable library.

    Parameters
    ----------
    variable_name : str
        Name of the variable.
    variable_library : str | uuid.UUID
        Name or ID of the variable library.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    Any
        The value of the variable.
    """

    df = list_variables(variable_library=variable_library, workspace=workspace)
    variable_row = df[df["Variable Name"] == variable_name]
    if variable_row.empty:
        raise ValueError(
            f"{icons.red_dot} the '{variable_name}' variable does not exist in the '{variable_library}'  variable library within the '{workspace}' workspace."
        )
    value = variable_row["Value"].iloc[0]
    # type = variable_row['Type'].iloc[0]
    return value
