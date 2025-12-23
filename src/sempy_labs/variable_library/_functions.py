from sempy_labs._helper_functions import (
    resolve_item_id,
    resolve_workspace_id,
    _base_api,
    _create_dataframe,
    _update_dataframe_datatypes,
    delete_item,
    _decode_b64,
    create_item,
)
import pandas as pd
from typing import Any, Optional, List, Union
from uuid import UUID
from sempy._utils._log import log
import json
import sempy_labs._icons as icons
from os import PathLike


@log
def get_variable_library(
    variable_library: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Returns properties of the specified variable library.

    This is a wrapper function for the following API: `Items - Get Variable Library <https://learn.microsoft.com/rest/api/fabric/variablelibrary/items/get-variable-library>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    variable_library : str | uuid.UUID
        Name or ID of the variable library.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the properties of the variable library.
    """

    columns = {
        "Variable Library Name": "string",
        "Variable Library Id": "string",
        "Description": "string",
        "Active Value Set Name": "string",
    }
    df = _create_dataframe(columns=columns)

    workspace_id = resolve_workspace_id(workspace)
    variable_library_id = resolve_item_id(
        item=variable_library, type="VariableLibrary", workspace=workspace
    )

    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/variableLibraries/{variable_library_id}",
        client="fabric_sp",
    )

    result = response.json()
    prop = result.get("properties", {})

    if prop:
        df = pd.DataFrame(
            [
                {
                    "Variable Library Name": result.get("displayName"),
                    "Variable Library Id": result.get("id"),
                    "Description": result.get("description"),
                    "Active Value Set Name": prop.get("activeValueSetName"),
                }
            ],
            columns=list(columns.keys()),
        )

        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


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

    This is a wrapper function for the following API: `Items - Delete Variable Library https://learn.microsoft.com/rest/api/fabric/variablelibrary/items/delete-variable-library>`_.

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
) -> dict | pd.DataFrame:
    """
    Gets the definition of a variable library.

    This is a wrapper function for the following API: `Items - Get Variable Library Definition <https://learn.microsoft.com/rest/api/fabric/variablelibrary/items/delete-variable-library>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    dict | pandas.DataFrame
        A dictionary showing the definition or a pandas dataframe showing the definition.
    """

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

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    variable_library : str | uuid.UUID
        Name or ID of the variable library.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the variables within a variable library.
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

    for part in result.get("definition", {}).get("parts", []):
        path = part.get("path")
        if path.startswith("valueSets") and path.endswith(".json"):
            payload = json.loads(part.get("payload"))
            value_set_name = payload.get("name")

            # Initialize the new column with None (or pd.NA)
            df[value_set_name] = None

            for override in payload.get("variableOverrides", []):
                variable_name = override.get("name")
                variable_value = override.get("value")

                # Set the value in the appropriate row and column
                df.loc[df["Variable Name"] == variable_name, value_set_name] = (
                    variable_value
                )

    return df


@log
def get_variable_values(
    variable_names: List[str],
    variable_library: Union[str, UUID],
    workspace: Optional[Union[str, UUID]] = None,
    value_set: Optional[str] = None,
) -> dict:
    """
    Gets the values of multiple variables from a variable library with a single call to list_variables.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    variable_names : List[str]
        A list of variable names to retrieve.
    variable_library : str | uuid.UUID
        Name or ID of the variable library.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    value_set : str, default=None
        The name of the value set to use for variable overrides.
        If None, the active value set of the variable library will be used.

    Returns
    -------
    dict
        Dictionary mapping variable names to their corresponding values.
    """

    if isinstance(variable_names, str):
        variable_names = [variable_names]

    if value_set is None:
        vl_df = get_variable_library(
            variable_library=variable_library, workspace=workspace
        )
        if vl_df.empty:
            raise ValueError(
                f"{icons.red_dot} The variable library '{variable_library}' does not exist within the '{workspace}' workspace."
            )
        value_set = vl_df["Active Value Set Name"].iloc[0]

    df = list_variables(variable_library=variable_library, workspace=workspace)
    found_variables = df[df["Variable Name"].isin(variable_names)]

    missing = set(variable_names) - set(found_variables["Variable Name"])
    if missing:
        raise ValueError(
            f"{icons.red_dot} The following variables do not exist in the '{variable_library}' variable library: {', '.join(missing)}"
        )

    if value_set == "Default value set":
        value_set = "Value"
    if value_set not in df.columns:
        raise ValueError(
            f"{icons.red_dot} The value set '{value_set}' does not exist in the variable library '{variable_library}' within the '{workspace}' workspace."
        )

    return dict(zip(found_variables["Variable Name"], found_variables[value_set]))


@log
def get_variable_value(
    variable_name: str,
    variable_library: str | UUID,
    workspace: Optional[str | UUID] = None,
    value_set: Optional[str] = None,
) -> Any:
    """
    Gets the value of a single variable in a variable library.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

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
    value_set : str, default=None
        The name of the value set to use for variable overrides.
        If None, the active value set of the variable library will be used.

    Returns
    -------
    Any
        The value of the variable.
    """

    return get_variable_values(
        variable_names=[variable_name],
        variable_library=variable_library,
        workspace=workspace,
        value_set=value_set,
    )[variable_name]


def _encode_b64(content: Any) -> str:
    import base64

    json_content = json.dumps(content, separators=(",", ":"))

    return base64.b64encode(json_content.encode("utf-8")).decode("utf-8")

    # if isinstance(content, (dict, list)):
    #    content = json.dumps(content, ensure_ascii=False)

    # if isinstance(content, str):
    #    content = content.encode("utf-8")

    # return base64.b64encode(content).decode("utf-8")


@log
def create_variable_library(
    name: str,
    variables: List[dict],
    value_sets: List[dict],
    value_sets_order: Optional[List[str]] = None,
    description: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
    folder: Optional[str | PathLike] = None,
):
    """
    Creates a variable library.

    This is a wrapper function for the following API: `Items - Create Variable Library <https://learn.microsoft.com/rest/api/fabric/variablelibrary/items/create-variable-library>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    name : str
        Name of the variable library.
    variables : List[dict]
        A list of variable definitions. Each variable definition should be a dictionary with keys such as 'name', 'type', 'value', and 'note'.

        Example:

        variables = [
            {
            "name": "variable1",
            "note": "Some optional note",
            "type": "String",
            "value": "Some string value"
            },
            {
            "name": "variableWithoutNote",
            "type": "Boolean",
            "value": True
            }
        ]
    value_sets : List[dict]
        A list of value set definitions. Each value set definition should be a dictionary with keys such as 'name' and 'variableOverrides'.

        Example:

        value_sets = [
            {
                "name": "valueSet1",
                "variableOverrides": [
                    {
                        "name": "variable1",
                        "value": "Overridden string value"
                    },
                    {
                        "name": "variableWithoutNote",
                        "value": False
                    }
                ]
            },            {
                "name": "valueSet0",
                "variableOverrides": [
                    {
                        "name": "variable1",
                        "value": "Another overridden string value"
                    },
                    {
                        "name": "variableWithoutNote",
                        "value": True
                    }
                ]
            }
        ]

    value_sets_order : List[str], default=None
        The order of value sets. If None, a default order will be used.
    description : str, default=None
        Description of the variable library.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    folder : str | PathLike, default=None
        The folder path within the workspace to create the variable library in.
    """

    schema_start = "https://developer.microsoft.com/json-schemas/fabric/item/variableLibrary/definition"
    schema_end = "1.0.0/schema.json"

    # Variable type normalization and validation
    variable_types = {
        "boolean": "Boolean",
        "datetime": "DateTime",
        "number": "Number",
        "integer": "Integer",
        "string": "String",
    }

    for v in variables:
        raw_type = v.get("type")

        if not isinstance(raw_type, str):
            raise ValueError(
                f"{icons.red_dot} Variable '{v.get('name')}' has an invalid type: {raw_type}"
            )

        normalized_key = raw_type.strip().lower()

        if normalized_key not in variable_types:
            raise ValueError(
                f"{icons.red_dot} Invalid variable type '{raw_type}' for variable '{v.get('name')}'. "
                f"Valid types are: {', '.join(variable_types.values())}"
            )

        v["type"] = variable_types[normalized_key]

    variables_content = {
        "$schema": f"{schema_start}/variables/{schema_end}",
        "variables": variables,
    }

    definition = {
        "format": "VariableLibraryV1",
        "parts": [
            {
                "path": "variables.json",
                "payload": _encode_b64(variables_content),
                "payloadType": "InlineBase64",
            },
        ],
    }

    # Value Sets
    if value_sets:
        for vs in value_sets:
            vs_name = vs.get("name")
            vs_content = {
                "$schema": f"{schema_start}/valueSet/{schema_end}",
                "name": vs_name,
                "variableOverrides": vs.get("variableOverrides", []),
            }
            definition["parts"].append(
                {
                    "path": f"valueSets/{vs_name}.json",
                    "payload": _encode_b64(vs_content),
                    "payloadType": "InlineBase64",
                }
            )

    # Settings
    if value_sets_order:
        settings_content = {
            "$schema": f"{schema_start}/settings/{schema_end}",
            "valueSetsOrder": value_sets_order,
        }
    else:
        settings_content = {
            "$schema": f"{schema_start}/settings/{schema_end}",
            "valueSetsOrder": [vs.get("name") for vs in value_sets],
        }

    definition["parts"].append(
        {
            "path": "settings.json",
            "payload": _encode_b64(settings_content),
            "payloadType": "InlineBase64",
        }
    )
    # return definition
    create_item(
        name=name,
        type="VariableLibrary",
        description=description,
        definition=definition,
        workspace=workspace,
        folder=folder,
    )
