import pandas as pd
from typing import Optional
from sempy_labs._helper_functions import (
    _base_api,
    delete_item,
    _create_dataframe,
    create_item,
    resolve_workspace_id,
    resolve_item_id,
    _decode_b64,
)
from uuid import UUID
import sempy_labs._icons as icons
from sempy._utils._log import log
import json


@log
def list_eventstreams(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows the eventstreams within a workspace.

    This is a wrapper function for the following API: `Items - List Eventstreams <https://learn.microsoft.com/rest/api/fabric/environment/items/list-eventstreams>`_.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the eventstreams within a workspace.
    """

    columns = {
        "Eventstream Name": "string",
        "Eventstream Id": "string",
        "Description": "string",
    }
    df = _create_dataframe(columns=columns)

    workspace_id = resolve_workspace_id(workspace)
    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/eventstreams", uses_pagination=True
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            rows.append(
                {
                    "Eventstream Name": v.get("displayName"),
                    "Eventstream Id": v.get("id"),
                    "Description": v.get("description"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def create_eventstream(
    name: str, description: Optional[str] = None, workspace: Optional[str | UUID] = None
):
    """
    Creates a Fabric eventstream.

    This is a wrapper function for the following API: `Items - Create Eventstream <https://learn.microsoft.com/rest/api/fabric/environment/items/create-eventstream>`_.

    Parameters
    ----------
    name: str
        Name of the eventstream.
    description : str, default=None
        A description of the environment.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    create_item(
        name=name, description=description, type="Eventstream", workspace=workspace
    )


@log
def delete_eventstream(
    eventstream: str | UUID, workspace: Optional[str | UUID] = None, **kwargs
):
    """
    Deletes a Fabric eventstream.

    This is a wrapper function for the following API: `Items - Delete Eventstream <https://learn.microsoft.com/rest/api/fabric/environment/items/delete-eventstream>`_.

    Parameters
    ----------
    eventstream: str | uuid.UUID
        Name or ID of the eventstream.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    if "name" in kwargs:
        eventstream = kwargs["name"]
        print(
            f"{icons.warning} The 'name' parameter is deprecated. Please use 'eventstream' instead."
        )

    delete_item(item=eventstream, type="Eventstream", workspace=workspace)


@log
def get_eventstream_definition(
    eventstream: str | UUID,
    workspace: Optional[str | UUID] = None,
    decode: bool = True,
    return_dataframe: bool = False,
) -> dict:

    workspace_id = resolve_workspace_id(workspace)
    item_id = resolve_item_id(item=eventstream, type="Eventstream", workspace=workspace)

    result = _base_api(
        request=f"/v1/workspaces/{workspace_id}/eventstreams/{item_id}/getDefinition",
        method="post",
        client="fabric_sp",
        status_codes=None,
        lro_return_json=True,
    )

    if decode:
        definition = {"definition": {"parts": []}}

        for part in result.get("definition", {}).get("parts", []):
            path = part.get("path")
            payload = json.loads(_decode_b64(part.get("payload")))
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
def list_eventstream_destinations(
    eventstream: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Lists the destinations of the specified eventstream.

    Parameters
    ----------
    eventstream : str | uuid.UUID
        The name or ID of the eventstream.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the destinations of the eventstream.
    """

    definition = get_eventstream_definition(
        eventstream=eventstream, workspace=workspace
    )

    columns = {
        "Destination Id": "string",
        "Destination Name": "string",
        "Destination Type": "string",
    }

    df = _create_dataframe(columns=columns)

    rows = []
    for part in definition.get("definition").get("parts"):
        payload = part.get("payload")
        if part.get("path") == "eventstream.json":
            destinations = payload.get("destinations")
            for d in destinations:
                rows.append(
                    {
                        "Destination Id": d.get("id"),
                        "Destination Name": d.get("name"),
                        "Destination Type": d.get("type"),
                    }
                )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def list_eventstream_sources(
    eventstream: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Lists the destinations of the specified eventstream.

    Parameters
    ----------
    eventstream : str | uuid.UUID
        The name or ID of the eventstream.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the destinations of the eventstream.
    """

    definition = get_eventstream_definition(
        eventstream=eventstream, workspace=workspace
    )

    columns = {
        "Source Id": "string",
        "Source Name": "string",
        "Source Type": "string",
    }

    df = _create_dataframe(columns=columns)

    rows = []
    for part in definition.get("definition").get("parts"):
        payload = part.get("payload")
        if part.get("path") == "eventstream.json":
            sources = payload.get("sources")
            for s in sources:
                rows.append(
                    {
                        "Source Id": s.get("id"),
                        "Source Name": s.get("name"),
                        "Source Type": s.get("type"),
                    }
                )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df
