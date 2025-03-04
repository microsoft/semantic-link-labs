import pandas as pd
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    _base_api,
    _print_success,
    resolve_item_id,
    _create_dataframe,
    _conv_b64,
    _decode_b64,
    delete_item,
)
from uuid import UUID
import sempy_labs._icons as icons


def create_eventhouse(
    name: str,
    definition: Optional[dict],
    description: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Creates a Fabric eventhouse.

    This is a wrapper function for the following API: `Items - Create Eventhouse <https://learn.microsoft.com/rest/api/fabric/environment/items/create-eventhouse>`_.

    Parameters
    ----------
    name: str
        Name of the eventhouse.
    definition : dict
        The definition (EventhouseProperties.json) of the eventhouse.
    description : str, default=None
        A description of the environment.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    payload = {"displayName": name}

    if description:
        payload["description"] = description

    if definition is not None:
        if not isinstance(definition, dict):
            raise ValueError(f"{icons.red_dot} The definition must be a dictionary.")

        payload["definition"] = {
            "parts": [
                {
                    "path": "EventhouseProperties.json",
                    "payload": _conv_b64(definition),
                    "payloadType": "InlineBase64",
                }
            ]
        }

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/eventhouses",
        method="post",
        status_codes=[201, 202],
        payload=payload,
        lro_return_status_code=True,
    )
    _print_success(
        item_name=name,
        item_type="eventhouse",
        workspace_name=workspace_name,
        action="created",
    )


def list_eventhouses(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows the eventhouses within a workspace.

    This is a wrapper function for the following API: `Items - List Eventhouses <https://learn.microsoft.com/rest/api/fabric/environment/items/list-eventhouses>`_.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the eventhouses within a workspace.
    """

    columns = {
        "Eventhouse Name": "string",
        "Eventhouse Id": "string",
        "Description": "string",
    }
    df = _create_dataframe(columns=columns)

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/eventhouses", uses_pagination=True
    )

    for r in responses:
        for v in r.get("value", []):
            new_data = {
                "Eventhouse Name": v.get("displayName"),
                "Eventhouse Id": v.get("id"),
                "Description": v.get("description"),
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df


def delete_eventhouse(name: str, workspace: Optional[str | UUID] = None):
    """
    Deletes a Fabric eventhouse.

    This is a wrapper function for the following API: `Items - Delete Eventhouse <https://learn.microsoft.com/rest/api/fabric/environment/items/delete-eventhouse>`_.

    Parameters
    ----------
    name: str
        Name of the eventhouse.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    delete_item(item=name, type="Eventhouse", workspace=workspace)


def get_eventhouse_definition(
    eventhouse: str | UUID,
    workspace: Optional[str | UUID] = None,
    return_dataframe: bool = False,
) -> dict | pd.DataFrame:
    """
    Gets the eventhouse definition.

    This is a wrapper function for the following API: `Items - Get Eventhouse Definition <https://learn.microsoft.com/rest/api/fabric/eventhouse/items/get-eventhouse-definition>`_.

    Parameters
    ----------
    eventhouse : str
        Name of the eventhouse.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID in which the eventhouse resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    return_dataframe : bool, default=False
        If True, returns a dataframe. If False, returns a json dictionary.

    Returns
    -------
    dict | pandas.DataFrame
        The eventhouse definition in .json format or as a pandas dataframe.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    item_id = resolve_item_id(item=eventhouse, type="Eventhouse", workspace=workspace)

    result = _base_api(
        request=f"/v1/workspaces/{workspace_id}/eventhouses/{item_id}/getDefinition",
        method="post",
        status_codes=None,
        lro_return_json=True,
    )

    df = pd.json_normalize(result["definition"]["parts"])

    if return_dataframe:
        return df
    else:
        df_filt = df[df["path"] == "EventhouseProperties.json"]
        payload = df_filt["payload"].iloc[0]
        return _decode_b64(payload)
