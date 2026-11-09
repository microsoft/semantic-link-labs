import pandas as pd
from typing import Optional
from os import PathLike
from uuid import UUID
from sempy_labs._helper_functions import (
    _base_api,
    _create_dataframe,
    _update_dataframe_datatypes,
    _conv_b64,
    _get_item_definition,
    create_item,
    delete_item,
    resolve_item_id,
    resolve_workspace_id,
)
import sempy_labs._icons as icons
from sempy._utils._log import log


@log
def list_monitoring_items(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows the Monitoring items within a workspace.

    This is a wrapper function for the following API: `Items - List Monitoring Items <https://learn.microsoft.com/rest/api/fabric/monitoring/items/list-monitoring-items>`_.

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
        A pandas dataframe showing the Monitoring items within a workspace.
    """

    columns = {
        "Monitoring Name": "string",
        "Monitoring Id": "string",
        "Description": "string",
        "Data Collection State": "string",
    }
    df = _create_dataframe(columns=columns)

    workspace_id = resolve_workspace_id(workspace)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/monitoringItems",
        uses_pagination=True,
        client="fabric_sp",
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            rows.append(
                {
                    "Monitoring Name": v.get("displayName"),
                    "Monitoring Id": v.get("id"),
                    "Description": v.get("description"),
                    "Data Collection State": v.get("properties", {}).get(
                        "dataCollectionState"
                    ),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
def get_monitoring(
    monitoring: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Returns properties of the specified Monitoring item.

    This is a wrapper function for the following API: `Items - Get Monitoring <https://learn.microsoft.com/rest/api/fabric/monitoring/items/get-monitoring>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    monitoring : str | uuid.UUID
        Name or ID of the Monitoring item.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the properties of the Monitoring item.
    """

    columns = {
        "Monitoring Name": "string",
        "Monitoring Id": "string",
        "Description": "string",
        "Folder Id": "string",
        "Sensitivity Label Id": "string",
        "Data Collection State": "string",
    }

    workspace_id = resolve_workspace_id(workspace)
    monitoring_id = resolve_item_id(
        item=monitoring, type="Monitoring", workspace=workspace_id
    )

    result = _base_api(
        request=f"/v1/workspaces/{workspace_id}/monitoringItems/{monitoring_id}",
        client="fabric_sp",
    ).json()

    df = pd.DataFrame(
        [
            {
                "Monitoring Name": result.get("displayName"),
                "Monitoring Id": result.get("id"),
                "Description": result.get("description"),
                "Folder Id": result.get("folderId"),
                "Sensitivity Label Id": (result.get("sensitivityLabel") or {}).get(
                    "id"
                ),
                "Data Collection State": result.get("properties", {}).get(
                    "dataCollectionState"
                ),
            }
        ],
        columns=list(columns.keys()),
    )

    _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
def create_monitoring(
    name: str,
    definition: Optional[dict] = None,
    description: Optional[str] = None,
    folder: Optional[str | PathLike] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Creates a Monitoring item.

    This is a wrapper function for the following API: `Items - Create Monitoring <https://learn.microsoft.com/rest/api/fabric/monitoring/items/create-monitoring>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    name : str
        Name of the Monitoring item. Must contain 2 to 128 characters using only ASCII letters, digits,
        underscores (_), periods (.), and hyphens (-), with no spaces.
    definition : dict, default=None
        The definition (MonitoringProperties.json) of the Monitoring item.
        See `here <https://learn.microsoft.com/rest/api/fabric/articles/item-management/definitions/monitoring-definition>`_ for more details.
    description : str, default=None
        A description of the Monitoring item.
    folder : str | os.PathLike, default=None
        The folder within the workspace where the Monitoring item will be created.
        Defaults to None which places the item in the root of the workspace.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    if definition is not None and not isinstance(definition, dict):
        raise ValueError(f"{icons.red_dot} The definition must be a dictionary.")

    definition_payload = (
        {
            "format": "MonitoringV1",
            "parts": [
                {
                    "path": "MonitoringProperties.json",
                    "payload": _conv_b64(definition),
                    "payloadType": "InlineBase64",
                }
            ],
        }
        if definition is not None
        else None
    )

    create_item(
        name=name,
        type="Monitoring",
        description=description,
        definition=definition_payload,
        workspace=workspace,
        folder=folder,
    )


@log
def delete_monitoring(monitoring: str | UUID, workspace: Optional[str | UUID] = None):
    """
    Deletes a Monitoring item.

    This is a wrapper function for the following API: `Items - Delete Monitoring <https://learn.microsoft.com/rest/api/fabric/monitoring/items/delete-monitoring>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    monitoring : str | uuid.UUID
        Name or ID of the Monitoring item.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    delete_item(item=monitoring, type="Monitoring", workspace=workspace)


@log
def get_monitoring_definition(
    monitoring: str | UUID,
    workspace: Optional[str | UUID] = None,
    return_dataframe: bool = False,
) -> dict | pd.DataFrame:
    """
    Gets the definition of the specified Monitoring item.

    This is a wrapper function for the following API: `Items - Get Monitoring Definition <https://learn.microsoft.com/rest/api/fabric/monitoring/items/get-monitoring-definition>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    monitoring : str | uuid.UUID
        Name or ID of the Monitoring item.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    return_dataframe : bool, default=False
        If True, returns a pandas dataframe of the definition parts.
        If False, returns the decoded contents of 'MonitoringProperties.json' as a dictionary.

    Returns
    -------
    dict | pandas.DataFrame
        The Monitoring definition in .json format or as a pandas dataframe.
    """

    return _get_item_definition(
        item=monitoring,
        type="Monitoring",
        workspace=workspace,
        return_dataframe=return_dataframe,
    )
