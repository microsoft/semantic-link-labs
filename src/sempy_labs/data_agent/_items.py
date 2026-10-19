import base64
import json
import pandas as pd
from os import PathLike
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_id,
    resolve_workspace_name_and_id,
    resolve_item_id,
    resolve_item_name_and_id,
    _base_api,
    _create_dataframe,
    create_item,
    get_item_definition,
)
from uuid import UUID
from sempy._utils._log import log
import sempy_labs._icons as icons

_data_agent_columns = {
    "Data Agent Name": "string",
    "Data Agent Id": "string",
    "Description": "string",
    "Published Description": "string",
    "Folder Id": "string",
}


def _data_agent_to_row(v: dict) -> dict:

    return {
        "Data Agent Name": v.get("displayName"),
        "Data Agent Id": v.get("id"),
        "Description": v.get("description"),
        "Published Description": (v.get("properties") or {}).get(
            "publishedDescription"
        ),
        "Folder Id": v.get("folderId"),
    }


def _validate_description(description: Optional[str], parameter: str):

    if description is not None and len(description) > 256:
        raise ValueError(
            f"{icons.red_dot} The '{parameter}' parameter cannot exceed 256 characters."
        )


def _encode_data_agent_definition(definition: dict) -> dict:
    """
    Converts a data agent definition into the API format (base64-encoded payloads).

    Accepts either {"definition": {"parts": [...]}} or {"parts": [...]}.
    Payloads may be dictionaries/lists (JSON), bytes, or strings which are assumed to be base64-encoded already
    (as returned by get_data_agent_definition with decode=False).
    """

    definition = definition.get("definition", definition)
    parts = definition.get("parts")
    if not parts:
        raise ValueError(
            f"{icons.red_dot} The definition must contain a non-empty 'parts' list."
        )

    encoded_parts = []
    for part in parts:
        payload = part.get("payload")
        if isinstance(payload, (dict, list)):
            payload = base64.b64encode(json.dumps(payload).encode("utf-8")).decode(
                "utf-8"
            )
        elif isinstance(payload, bytes):
            payload = base64.b64encode(payload).decode("utf-8")
        elif not isinstance(payload, str):
            raise ValueError(
                f"{icons.red_dot} The payload of the '{part.get('path')}' part must be a dictionary, bytes or a base64-encoded string."
            )
        encoded_parts.append(
            {
                "path": part.get("path"),
                "payload": payload,
                "payloadType": "InlineBase64",
            }
        )

    result = {"parts": encoded_parts}
    if definition.get("format"):
        result["format"] = definition.get("format")

    return result


@log
def list_data_agents(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows the data agents within a workspace.

    This is a wrapper function for the following API: `Items - List Data Agents <https://learn.microsoft.com/rest/api/fabric/dataagent/items/list-data-agents>`_.

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
        A pandas dataframe showing the data agents within a workspace.
    """

    df = _create_dataframe(columns=_data_agent_columns)

    workspace_id = resolve_workspace_id(workspace)
    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/dataAgents",
        uses_pagination=True,
        client="fabric_sp",
    )

    rows = [_data_agent_to_row(v) for r in responses for v in r.get("value", [])]

    if rows:
        df = pd.DataFrame(rows, columns=list(_data_agent_columns.keys()))

    return df


@log
def get_data_agent(
    data_agent: str | UUID,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Shows the properties of the specified data agent.

    This is a wrapper function for the following API: `Items - Get Data Agent <https://learn.microsoft.com/rest/api/fabric/dataagent/items/get-data-agent>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    data_agent : str | uuid.UUID
        The name or ID of the data agent.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the properties of the specified data agent.
    """

    workspace_id = resolve_workspace_id(workspace)
    item_id = resolve_item_id(item=data_agent, type="DataAgent", workspace=workspace_id)

    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/dataAgents/{item_id}",
        client="fabric_sp",
    ).json()

    return pd.DataFrame(
        [_data_agent_to_row(response)], columns=list(_data_agent_columns.keys())
    )


@log
def create_data_agent(
    name: str,
    description: Optional[str] = None,
    definition: Optional[dict] = None,
    folder: Optional[str | PathLike] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Creates a data agent.

    This is a wrapper function for the following API: `Items - Create Data Agent <https://learn.microsoft.com/rest/api/fabric/dataagent/items/create-data-agent>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    name : str
        The name of the data agent.
    description : str, default=None
        A description of the data agent. Maximum length is 256 characters.
    definition : dict, default=None
        The `data agent definition <https://learn.microsoft.com/rest/api/fabric/articles/item-management/definitions/data-agent-definition>`_,
        in the format returned by :func:`get_data_agent_definition` (decoded or not).
        Payloads may be dictionaries (JSON), bytes or base64-encoded strings.
        Defaults to None which creates an empty data agent.
    folder : str | os.PathLike, default=None
        The folder within the workspace where the data agent will be created.
        Defaults to None which places the data agent in the root of the workspace.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    _validate_description(description, "description")

    create_item(
        name=name,
        type="DataAgent",
        description=description,
        definition=(_encode_data_agent_definition(definition) if definition else None),
        folder=folder,
        workspace=workspace,
    )


@log
def update_data_agent(
    data_agent: str | UUID,
    name: Optional[str] = None,
    description: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Updates the name and/or description of a data agent.

    This is a wrapper function for the following API: `Items - Update Data Agent <https://learn.microsoft.com/rest/api/fabric/dataagent/items/update-data-agent>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    data_agent : str | uuid.UUID
        The name or ID of the data agent.
    name : str, default=None
        The new name of the data agent.
        Defaults to None which keeps the existing name.
    description : str, default=None
        The new description of the data agent. Maximum length is 256 characters.
        Defaults to None which keeps the existing description.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    if name is None and description is None:
        raise ValueError(
            f"{icons.red_dot} At least one of the 'name' or 'description' parameters must be provided."
        )
    _validate_description(description, "description")

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    item_name, item_id = resolve_item_name_and_id(
        item=data_agent, type="DataAgent", workspace=workspace_id
    )

    payload = {}
    if name is not None:
        payload["displayName"] = name
    if description is not None:
        payload["description"] = description

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/dataAgents/{item_id}",
        method="patch",
        payload=payload,
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The '{item_name}' data agent within the '{workspace_name}' workspace has been updated."
    )


@log
def delete_data_agent(
    data_agent: str | UUID,
    workspace: Optional[str | UUID] = None,
    hard_delete: bool = False,
):
    """
    Deletes a data agent.

    This is a wrapper function for the following API: `Items - Delete Data Agent <https://learn.microsoft.com/rest/api/fabric/dataagent/items/delete-data-agent>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    data_agent : str | uuid.UUID
        The name or ID of the data agent.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    hard_delete : bool, default=False
        If True, the data agent is permanently deleted and cannot be recovered.
        If False, the data agent is soft-deleted (if soft deletion is supported).
    """

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    item_name, item_id = resolve_item_name_and_id(
        item=data_agent, type="DataAgent", workspace=workspace_id
    )

    url = f"/v1/workspaces/{workspace_id}/dataAgents/{item_id}"
    if hard_delete:
        url += "?hardDelete=True"

    _base_api(request=url, method="delete", client="fabric_sp")

    print(
        f"{icons.green_dot} The '{item_name}' data agent has been {'permanently ' if hard_delete else ''}deleted from the '{workspace_name}' workspace."
    )


@log
def get_data_agent_definition(
    data_agent: str | UUID,
    workspace: Optional[str | UUID] = None,
    decode: bool = True,
    return_dataframe: bool = False,
) -> dict | pd.DataFrame:
    """
    Obtains the definition of a data agent.

    This is a wrapper function for the following API: `Items - Get Data Agent Definition <https://learn.microsoft.com/rest/api/fabric/dataagent/items/get-data-agent-definition>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    data_agent : str | uuid.UUID
        The name or ID of the data agent.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    decode : bool, default=True
        If True, decodes the base64 payload of each definition part.
        If False, returns the payloads in base64 format.
    return_dataframe : bool, default=False
        If True, returns a pandas dataframe with one row per definition part (payloads in base64 format).
        If False, returns a dictionary.

    Returns
    -------
    dict | pandas.DataFrame
        The data agent definition. The dictionary is in the format {"definition": {"parts": [{"path": ..., "payload": ...}]}}
        and can be modified and passed to :func:`update_data_agent_definition`.
    """

    return get_item_definition(
        item=data_agent,
        type="DataAgent",
        workspace=workspace,
        return_dataframe=return_dataframe,
        decode=decode,
    )


@log
def update_data_agent_definition(
    data_agent: str | UUID,
    definition: dict,
    update_metadata: bool = False,
    workspace: Optional[str | UUID] = None,
):
    """
    Overrides the definition of a data agent.

    This is a wrapper function for the following API: `Items - Update Data Agent Definition <https://learn.microsoft.com/rest/api/fabric/dataagent/items/update-data-agent-definition>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    data_agent : str | uuid.UUID
        The name or ID of the data agent.
    definition : dict
        The `data agent definition <https://learn.microsoft.com/rest/api/fabric/articles/item-management/definitions/data-agent-definition>`_,
        in the format returned by :func:`get_data_agent_definition` (decoded or not).
        Payloads may be dictionaries (JSON), bytes or base64-encoded strings.
    update_metadata : bool, default=False
        If True and the '.platform' part is provided in the definition, the data agent's metadata is updated using the metadata in the '.platform' part.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    payload = {"definition": _encode_data_agent_definition(definition)}

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    item_name, item_id = resolve_item_name_and_id(
        item=data_agent, type="DataAgent", workspace=workspace_id
    )

    url = f"/v1/workspaces/{workspace_id}/dataAgents/{item_id}/updateDefinition"
    if update_metadata:
        url += "?updateMetadata=True"

    _base_api(
        request=url,
        method="post",
        payload=payload,
        status_codes=[200, 202],
        lro_return_status_code=True,
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The definition of the '{item_name}' data agent within the '{workspace_name}' workspace has been updated."
    )


@log
def publish_data_agent(
    data_agent: str | UUID,
    published_description: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Publishes the staged (draft) version of a data agent.

    This is a wrapper function for the following API: `Items - Publish Data Agent <https://learn.microsoft.com/rest/api/fabric/dataagent/items/publish-data-agent>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    data_agent : str | uuid.UUID
        The name or ID of the data agent.
    published_description : str, default=None
        A description shown when the data agent appears in other experiences.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    item_name, item_id = resolve_item_name_and_id(
        item=data_agent, type="DataAgent", workspace=workspace_id
    )

    payload = {}
    if published_description is not None:
        payload["publishedDescription"] = published_description

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/dataAgents/{item_id}/staging/publish",
        method="post",
        payload=payload,
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The '{item_name}' data agent within the '{workspace_name}' workspace has been published."
    )
