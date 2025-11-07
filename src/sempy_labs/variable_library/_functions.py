from sempy_labs._helper_functions import (
    resolve_item_id,
    resolve_workspace_id,
    _base_api,
    _create_dataframe,
    _update_dataframe_datatypes,
    delete_item,
    _decode_b64,
)
import pandas as pd
from typing import Any, Optional, List, Union, Dict
from uuid import UUID
from sempy._utils._log import log
import json
import base64
import sempy_labs._icons as icons


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


def _encode_b64(content: str) -> str:
    """
    Encode a string to base64.

    Parameters
    ----------
    content : str
        The string content to encode.

    Returns
    -------
    str
        The base64 encoded string.
    """
    return base64.b64encode(content.encode("utf-8")).decode("utf-8")


def _create_variable_library_definition(
    variables: Optional[List[Dict]] = None,
    value_sets: Optional[List[Dict]] = None,
    value_sets_order: Optional[List[str]] = None,
) -> Dict:
    """
    Create the definition structure for a variable library.

    Parameters
    ----------
    variables : List[Dict], optional
        List of variable dictionaries with keys: name, type, value, note (optional).
    value_sets : List[Dict], optional
        List of value set dictionaries with keys: name, variableOverrides.
    active_value_set : str, optional
        Name of the active value set. Defaults to "Default value set".

    Returns
    -------
    Dict
        The definition structure for the API.
    """
    parts = []
    json_schema_root = "https://developer.microsoft.com/json-schemas/fabric/item/variableLibrary/definition/"

    # Default variables if none provided
    if variables is None:
        variables = []

    # Create variables.json part
    variables_content = {
        "$schema": json_schema_root + "variables/1.0.0/schema.json",
        "variables": variables,
    }
    variables_json = json.dumps(variables_content, separators=(",", ":"))
    parts.append(
        {
            "path": "variables.json",
            "payload": _encode_b64(variables_json),
            "payloadType": "InlineBase64",
        }
    )

    if value_sets:
        for value_set in value_sets:
            value_set_name = value_set.get("name")
            value_set_data = value_set

            # Ensure proper structure for value set
            value_set_content = {
                "$schema": json_schema_root + "valueSet/1.0.0/schema.json",
                "name": value_set_name,
                "variableOverrides": value_set_data.get("variableOverrides", []),
            }

            value_set_json = json.dumps(value_set_content, separators=(",", ":"))
            parts.append(
                {
                    "path": f"valueSets/{value_set_name}.json",
                    "payload": _encode_b64(value_set_json),
                    "payloadType": "InlineBase64",
                }
            )

    # Create settings.json part
    if value_sets_order:
        settings_content = {
            "$schema": json_schema_root + "settings/1.0.0/schema.json",
            "valueSetsOrder": value_sets_order,
        }
        settings_json = json.dumps(settings_content, separators=(",", ":"))
        parts.append(
            {
                "path": "settings.json",
                "payload": _encode_b64(settings_json),
                "payloadType": "InlineBase64",
            }
        )

    return {"format": "VariableLibraryV1", "parts": parts}


@log
def create_variable_library(
    variable_library_name: str,
    description: Optional[str] = None,
    variables: Optional[List[Dict]] = None,
    value_sets: Optional[Dict[str, Dict]] = None,
    value_sets_order: Optional[List[str]] = None,
    workspace: Optional[str | UUID] = None,
) -> str:
    """
    Creates a new variable library with optional variables and value sets.

    This is a wrapper function for the following API: `Items - Create Variable Library <https://learn.microsoft.com/rest/api/fabric/variablelibrary/items/create-variable-library>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    variable_library_name : str
        Name of the variable library.
    description : str, optional
        Description of the variable library.
    variables : List[Dict], optional
        List of variable dictionaries. Each dict should contain:
        - name (str): Variable name
        - type (str): Variable type (e.g., "String", "Number", "Boolean")
        - value (Any): Variable value
        - note (str, optional): Optional note/description for the variable
        Example: [{"name": "var1", "type": "String", "value": "test", "note": "A test variable"}]
    value_sets : Dict[str, Dict], optional
        Dictionary of value sets where key is value set name and value contains:
        - variableOverrides (List[Dict]): List of variable overrides
        Example: [{"name": "ProductionSet", "variableOverrides": [{"name": "var1", "value": "prod_value"}]}]
    value_sets_order : List[str], optional
        Order of value sets. If not provided, defaults to [""].
        If value_sets are provided and this is None, the first value set will be used.
    workspace : str | uuid.UUID, optional
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    str
        Status message indicating the result of the creation operation.
    """

    workspace_id = resolve_workspace_id(workspace)

    # Create the definition
    definition = _create_variable_library_definition(
        variables=variables, value_sets=value_sets, value_sets_order=value_sets_order
    )

    payload = {
        "displayName": variable_library_name,
        "description": description or "",
        "definition": definition,
    }

    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/variableLibraries",
        method="post",
        client="fabric_sp",
        payload=payload,
        status_codes=[201, 202],
        lro_return_json=False,
    )

    if response.status_code == 201:
        print(
            f"{icons.green_dot} Variable library '{variable_library_name}' created successfully."
        )
        return "Created"
    elif response.status_code == 202:
        print(
            f"{icons.in_progress} Variable library '{variable_library_name}' creation is in progress."
        )
        return "In Progress"
    else:
        print(
            f"{icons.red_dot} Failed to create variable library '{variable_library_name}'. Status code: {response.status_code}"
        )
        result = response.json()
        return f"Failed with status code {result.get('errorCode')} and message: {result.get('message')}"


@log
def update_variable_library(
    variable_library: str | UUID,
    variable_library_name: Optional[str] = None,
    description: Optional[str] = None,
    active_value_set: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
) -> str:
    """
    Updates an existing variable library's properties.

    This is a wrapper function for the following API: `Items - Update Variable Library <https://learn.microsoft.com/rest/api/fabric/variablelibrary/items/update-variable-library>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    variable_library : str | uuid.UUID
        Name or ID of the variable library to update.
    variable_library_name : str, optional
        New name for the variable library.
    description : str, optional
        New description for the variable library.
    active_value_set : str, optional
        Name of the active value set to set for the variable library.
    workspace : str | uuid.UUID, optional
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    str
        Status message indicating the result of the update operation.
    """

    workspace_id = resolve_workspace_id(workspace)
    variable_library_id = resolve_item_id(
        item=variable_library, type="VariableLibrary", workspace=workspace
    )

    payload = {}

    if variable_library_name is not None:
        payload["displayName"] = variable_library_name

    if description is not None:
        payload["description"] = description

    if active_value_set is not None:
        payload["properties"] = {"activeValueSetName": active_value_set}

    if not payload:
        print(f"{icons.yellow_dot} No updates provided for variable library.")
        return "No updates"

    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/VariableLibraries/{variable_library_id}",
        method="patch",
        client="fabric_sp",
        payload=payload,
        status_codes=[200],
    )

    if response.status_code == 200:
        print(f"{icons.green_dot} Variable library updated successfully.")
        return "Updated"
    else:
        print(
            f"{icons.red_dot} Failed to update variable library. Status code: {response.status_code}"
        )
        result = response.json()
        return f"Failed with status code {result.get('errorCode')} and message: {result.get('message')}"


@log
def update_variable_library_definition(
    variable_library: str | UUID,
    variables: Optional[List[Dict]] = None,
    value_sets: Optional[Dict[str, Dict]] = None,
    value_sets_order: Optional[List[str]] = None,
    workspace: Optional[str | UUID] = None,
) -> str:
    """
    Updates the definition of an existing variable library.

    This is a wrapper function for the following API: `Items - Update Variable Library Definition <https://learn.microsoft.com/rest/api/fabric/variablelibrary/items/update-variable-library-definition>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    variable_library : str | uuid.UUID
        Name or ID of the variable library to update.
    variables : List[Dict], optional
        List of variable dictionaries. Each dict should contain:
        - name (str): Variable name
        - type (str): Variable type (e.g., "String", "Number", "Boolean")
        - value (Any): Variable value
        - note (str, optional): Optional note/description for the variable
        Example: [{"name": "var1", "type": "String", "value": "test", "note": "A test variable"}]
    value_sets : Dict[str, Dict], optional
        Dictionary of value sets where key is value set name and value contains:
        - variableOverrides (List[Dict]): List of variable overrides
        Example: {"ProductionSet": {"variableOverrides": [{"name": "var1", "value": "prod_value"}]}}
    value_sets_order : List[str], optional
        List of value set names in the order they should be applied.
        If not provided, defaults to the keys of the value_sets dictionary.
    workspace : str | uuid.UUID, optional
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    str
        Status message indicating the result of the update operation.
    """

    workspace_id = resolve_workspace_id(workspace)
    variable_library_id = resolve_item_id(
        item=variable_library, type="VariableLibrary", workspace=workspace
    )

    # Create the definition using the existing helper function
    definition = _create_variable_library_definition(
        variables=variables, value_sets=value_sets, value_sets_order=value_sets_order
    )

    payload = {"definition": definition}

    # Build the request URL with optional query parameter
    url = f"/v1/workspaces/{workspace_id}/VariableLibraries/{variable_library_id}/updateDefinition"

    response = _base_api(
        request=url,
        method="post",
        client="fabric_sp",
        payload=payload,
        status_codes=[200, 202],
        lro_return_json=False,
    )

    if response.status_code == 200:
        print(f"{icons.green_dot} Variable library definition updated successfully.")
        return "Updated"
    elif response.status_code == 202:
        print(f"{icons.in_progress} Variable library definition update is in progress.")
        return "In Progress"
    else:
        print(
            f"{icons.red_dot} Failed to update variable library definition. Status code: {response.status_code}"
        )
        try:
            result = response.json()
            return f"Failed with status code {result.get('errorCode')} and message: {result.get('message')}"
        except:
            return f"Failed with status code {response.status_code}"
