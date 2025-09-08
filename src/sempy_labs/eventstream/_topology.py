import pandas as pd
from typing import Optional
from sempy_labs._helper_functions import (
    _base_api,
    resolve_item_id,
    resolve_workspace_id,
    resolve_workspace_name_and_id,
    resolve_item_name_and_id,
    _create_dataframe,
)
from uuid import UUID
import sempy_labs._icons as icons
from sempy._utils._log import log


@log
def get_eventstream_destination(
    eventstream: str | UUID,
    destination_id: UUID,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Returns the specified destination of the eventstream.

    This is a wrapper function for the following API: `Topology - Get Eventstream Destination <https://learn.microsoft.com/rest/api/fabric/eventstream/topology/get-eventstream-destination>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    eventstream : str | uuid.UUID
        The name or ID of the eventstream.
    destination_id : uuid.UUID
        The ID of the destination.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the details of the destination.
    """

    workspace_id = resolve_workspace_id(workspace)
    eventstream_id = resolve_item_id(
        item=eventstream, type="Eventstream", workspace=workspace_id
    )
    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/eventstreams/{eventstream_id}/destinations/{destination_id}",
        client="fabric_sp",
    )

    columns = {
        "Eventstream Destination Id": "str",
        "Eventstream Destination Name": "str",
        "Eventstream Destination Type": "str",
        "Workspace Id": "str",
        "Item Id": "str",
        "Schema": "str",
        "Delta Table": "str",
        "Input Serialization Type": "str",
        "Input Serialization Encoding": "str",
        "Input Nodes": "str",
        "Status": "str",
        "Error": "str",
    }

    df = _create_dataframe(columns)

    result = response.json()

    rows = []
    prop = result.get("properties", {})
    rows.append(
        {
            "Eventstream Destination Id": result.get("id"),
            "Eventstream Destination Name": result.get("name"),
            "Eventstream Destination Type": result.get("type"),
            "Workspace Id": prop.get("workspaceId"),
            "Item Id": prop.get("itemId"),
            "Schema": prop.get("schema"),
            "Delta Table": prop.get("deltaTable"),
            "Input Serialization Type": prop.get("inputSerialization", {}).get("type"),
            "Input Serialization Encoding": prop.get("inputSerialization", {})
            .get("properties", {})
            .get("encoding"),
            "Input Nodes": (
                ", ".join([node.get("name") for node in result.get("inputNodes", [])])
                if result.get("inputNodes")
                else None
            ),
            "Status": result.get("status"),
            "Error": result.get("error"),
        }
    )

    if rows:
        df = pd.DataFrame(rows, columns=columns.keys())

    return df


@log
def get_eventstream_destination_connection(
    eventstream: str | UUID,
    destination_id: UUID,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Returns the connection information of a specified destination of the eventstream. Only custom endpoints destinations are supported.

    This is a wrapper function for the following API: `Topology - Get Eventstream Destination Connection <https://learn.microsoft.com/rest/api/fabric/eventstream/topology/get-eventstream-destination-connection>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    eventstream : str | uuid.UUID
        The name or ID of the eventstream.
    destination_id : uuid.UUID
        The ID of the destination.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the connection details of the destination.
    """

    workspace_id = resolve_workspace_id(workspace)
    eventstream_id = resolve_item_id(
        item=eventstream, type="Eventstream", workspace=workspace_id
    )
    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/eventstreams/{eventstream_id}/destinations/{destination_id}/connection",
        client="fabric_sp",
    )

    columns = {
        "Fully Qualified Namespace": "str",
        "EventHub Name": "str",
        "Consumer Group Name": "str",
        "Primary Key": "str",
        "Secondary Key": "str",
        "Primary Connection String": "str",
        "Secondary Connection String": "str",
    }

    df = _create_dataframe(columns=columns)

    result = response.json()

    rows = []
    rows.append(
        {
            "Fully Qualified Namespace": result.get("fullyQualifiedNamespace"),
            "EventHub Name": result.get("eventHubName"),
            "Consumer Group Name": result.get("consumerGroupName"),
            "Primary Key": result.get("accessKeys", {}).get("primaryKey"),
            "Secondary Key": result.get("accessKeys", {}).get("secondaryKey"),
            "Primary Connection String": result.get("accessKeys", {}).get(
                "primaryConnectionString"
            ),
            "Secondary Connection String": result.get("accessKeys", {}).get(
                "secondaryConnectionString"
            ),
        }
    )

    if rows:
        df = pd.DataFrame(rows, columns=columns.keys())

    return df


@log
def get_eventstream_source(
    eventstream: str | UUID, source_id: UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Returns the specified source of the eventstream.

    This is a wrapper function for the following API: `Topology - Get Eventstream Source <https://learn.microsoft.com/rest/api/fabric/eventstream/topology/get-eventstream-source>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    eventstream : str | uuid.UUID
        The name or ID of the eventstream.
    source_id : uuid.UUID
        The ID of the source.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the details of the source.
    """

    workspace_id = resolve_workspace_id(workspace)
    eventstream_id = resolve_item_id(
        item=eventstream, type="Eventstream", workspace=workspace_id
    )
    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/eventstreams/{eventstream_id}/sources/{source_id}",
        client="fabric_sp",
    )

    columns = {
        "Eventstream Source Id": "str",
        "Eventstream Source Name": "str",
        "Eventstream Source Type": "str",
        "Data Connection Id": "str",
        "Consumer Group Name": "str",
        "Input Serialization Type": "str",
        "Input Serialization Encoding": "str",
        "Status": "str",
        "Error": "str",
    }

    df = _create_dataframe(columns=columns)

    rows = []
    result = response.json()
    prop = result.get("properties", {})
    rows.append(
        {
            "Eventstream Source Id": result.get("id"),
            "Eventstream Source Name": result.get("name"),
            "Eventstream Source Type": result.get("type"),
            "Data Connection Id": prop.get("dataConnectionId"),
            "Consumer Group Name": prop.get("consumerGroupName"),
            "Input Serialization Type": prop.get("inputSerialization", {}).get("type"),
            "Input Serialization Encoding": prop.get("inputSerialization", {})
            .get("properties", {})
            .get("encoding"),
            "Status": result.get("status"),
            "Error": result.get("error"),
        }
    )

    if rows:
        df = pd.DataFrame(rows, columns=columns.keys())

    return df


@log
def get_eventstream_source_connection(
    eventstream: str | UUID, source_id: UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Returns the connection information of specified source of the eventstream. Only custom endpoints sources are supported.

    This is a wrapper function for the following API: `Topology - Get Eventstream Source Connection <https://learn.microsoft.com/rest/api/fabric/eventstream/topology/get-eventstream-source-connection>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    eventstream : str | uuid.UUID
        The name or ID of the eventstream.
    source_id : uuid.UUID
        The ID of the source.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the connection details of the source.
    """

    workspace_id = resolve_workspace_id(workspace)
    eventstream_id = resolve_item_id(
        item=eventstream, type="Eventstream", workspace=workspace_id
    )
    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/eventstreams/{eventstream_id}/sources/{source_id}/connection",
        client="fabric_sp",
    )
    result = response.json()

    columns = {
        "Fully Qualified Namespace": "str",
        "EventHub Name": "str",
        "Consumer Group Name": "str",
        "Primary Key": "str",
        "Secondary Key": "str",
        "Primary Connection String": "str",
        "Secondary Connection String": "str",
    }
    df = _create_dataframe(columns=columns)

    rows = []
    rows.append(
        {
            "Fully Qualified Namespace": result.get("fullyQualifiedNamespace"),
            "EventHub Name": result.get("eventHubName"),
            "Consumer Group Name": result.get("consumerGroupName"),
            "Primary Key": result.get("accessKeys", {}).get("primaryKey"),
            "Secondary Key": result.get("accessKeys", {}).get("secondaryKey"),
            "Primary Connection String": result.get("accessKeys", {}).get(
                "primaryConnectionString"
            ),
            "Secondary Connection String": result.get("accessKeys", {}).get(
                "secondaryConnectionString"
            ),
        }
    )

    if rows:
        df = pd.DataFrame(rows, columns=columns.keys())

    return df


@log
def get_eventstream_topology(
    eventstream: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Returns the topology of the specified eventstream.

    This is a wrapper function for the following API: `Topology - Get Eventstream Topology <https://learn.microsoft.com/rest/api/fabric/eventstream/topology/get-eventstream-topology>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

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
        A pandas dataframe showing the topology of the eventstream, including sources and destinations.
    """

    workspace_id = resolve_workspace_id(workspace)
    eventstream_id = resolve_item_id(
        item=eventstream, type="Eventstream", workspace=workspace_id
    )
    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/eventstreams/{eventstream_id}/topology",
        client="fabric_sp",
    )

    columns = {
        "Eventstream Source Id": "str",
        "Eventstream Source Name": "str",
        "Eventstream Source Type": "str",
        "Data Connection Id": "str",
        "Consumer Group Name": "str",
        "Input Serialization Type": "str",
        "Input Serialization Encoding": "str",
        "Status": "str",
        "Error": "str",
        "Type": "str",
        "Region": "str",
        "Topic": "str",
        "Auto Offset Reset": "str",
        "SASL Mechanism": "str",
        "Security Protocol": "str",
        "Container Name": "str",
        "Database Name": "str",
        "Offset Policy": "str",
        "Table Name": "str",
        "Server Id": "str",
        "Port": "str",
        "Slot Name": "str",
    }
    df = _create_dataframe(columns=columns)
    rows = []
    for r in response.json().get("sources", []):
        prop = r.get("properties", {})
        rows.append(
            {
                "Eventstream Source Id": r.get("id"),
                "Eventstream Source Name": r.get("name"),
                "Eventstream Source Type": r.get("type"),
                "Data Connection Id": prop.get("dataConnectionId"),
                "Consumer Group Name": prop.get("consumerGroupName"),
                "Input Serialization Type": prop.get("inputSerialization", {}).get(
                    "type"
                ),
                "Input Serialization Encoding": prop.get("inputSerialization", {})
                .get("properties", {})
                .get("encoding"),
                "Status": r.get("status"),
                "Error": r.get("error"),
                "Type": prop.get("type"),
                "Region": prop.get("region"),
                "Topic": prop.get("topic"),
                "Auto Offset Reset": prop.get("autoOffsetReset"),
                "SASL Mechanism": prop.get("saslMechanism"),
                "Security Protocol": prop.get("securityProtocol"),
                "Container Name": prop.get("containerName"),
                "Database Name": prop.get("databaseName"),
                "Offset Policy": prop.get("offsetPolicy"),
                "Table Name": prop.get("tableName"),
                "Server Id": prop.get("serverId"),
                "Port": prop.get("port"),
                "Slot Name": prop.get("slotName"),
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=columns.keys())

    return df


@log
def resume_eventstream(eventstream: str | UUID, workspace: Optional[str | UUID] = None):
    """
    Resume running all supported sources and destinations of the eventstream.

    This is a wrapper function for the following API: `Topology - Resume Eventstream <https://learn.microsoft.com/rest/api/fabric/eventstream/topology/resume-eventstream>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    eventstream : str | uuid.UUID
        The name or ID of the eventstream.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (eventstream_name, eventstream_id) = resolve_item_name_and_id(
        item=eventstream, type="Eventstream", workspace=workspace_id
    )
    _base_api(
        request=f"/v1/workspaces/{workspace_id}/eventstreams/{eventstream_id}/resume",
        client="fabric_sp",
        method="post",
    )

    print(
        f"{icons.green_dot} The '{eventstream_name}' eventstream within the '{workspace_name}' workspace has been resumed."
    )


@log
def resume_eventstream_destination(
    eventstream: str | UUID,
    destination_id: UUID,
    workspace: Optional[str | UUID] = None,
):
    """
    Resume running the specified destination in the eventstream.

    This is a wrapper function for the following API: `Topology - Resume Eventstream Destination <https://learn.microsoft.com/rest/api/fabric/eventstream/topology/resume-eventstream-destination>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    eventstream : str | uuid.UUID
        The name or ID of the eventstream.
    destination_id : uuid.UUID
        The ID of the destination.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (eventstream_name, eventstream_id) = resolve_item_name_and_id(
        item=eventstream, type="Eventstream", workspace=workspace_id
    )
    _base_api(
        request=f"/v1/workspaces/{workspace_id}/eventstreams/{eventstream_id}/destinations/{destination_id}/resume",
        client="fabric_sp",
        method="post",
    )

    print(
        f"{icons.green_dot} The '{destination_id}' destination in the '{eventstream_name}' eventstream within the '{workspace_name}' workspace has been resumed."
    )


@log
def resume_eventstream_source(
    eventstream: str | UUID, source_id: UUID, workspace: Optional[str | UUID] = None
):
    """
    Resume running the specified source in the eventstream.

    This is a wrapper function for the following API: `Topology - Resume Eventstream Source <https://learn.microsoft.com/rest/api/fabric/eventstream/topology/resume-eventstream-source>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    eventstream : str | uuid.UUID
        The name or ID of the eventstream.
    source_id : uuid.UUID
        The ID of the source.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (eventstream_name, eventstream_id) = resolve_item_name_and_id(
        item=eventstream, type="Eventstream", workspace=workspace_id
    )
    _base_api(
        request=f"/v1/workspaces/{workspace_id}/eventstreams/{eventstream_id}/sources/{source_id}/resume",
        client="fabric_sp",
        method="post",
    )

    print(
        f"{icons.green_dot} The '{source_id}' source in the '{eventstream_name}' eventstream within the '{workspace_name}' workspace has been resumed."
    )


@log
def pause_eventstream(
    eventstream: str | UUID, workspace: Optional[str | UUID] = None
) -> dict:
    """
    Pause running all supported sources and destinations of the eventstream.

    This is a wrapper function for the following API: `Topology - Pause Eventstream <https://learn.microsoft.com/rest/api/fabric/eventstream/topology/pause-eventstream>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    eventstream : str | uuid.UUID
        The name or ID of the eventstream.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (eventstream_name, eventstream_id) = resolve_item_name_and_id(
        item=eventstream, type="Eventstream", workspace=workspace_id
    )
    _base_api(
        request=f"/v1/workspaces/{workspace_id}/eventstreams/{eventstream_id}/pause",
        client="fabric_sp",
        method="post",
    )

    print(
        f"{icons.green_dot} The '{eventstream_name}' eventstream within the '{workspace_name}' workspace has been paused."
    )


@log
def pause_eventstream_destination(
    eventstream: str | UUID,
    destination_id: UUID,
    workspace: Optional[str | UUID] = None,
):
    """
    Pause running the specified destination in the eventstream.

    This is a wrapper function for the following API: `Topology - Pause Eventstream Destination <https://learn.microsoft.com/rest/api/fabric/eventstream/topology/pause-eventstream-destination>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    eventstream : str | uuid.UUID
        The name or ID of the eventstream.
    destination_id : uuid.UUID
        The ID of the destination.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (eventstream_name, eventstream_id) = resolve_item_name_and_id(
        item=eventstream, type="Eventstream", workspace=workspace_id
    )
    _base_api(
        request=f"/v1/workspaces/{workspace_id}/eventstreams/{eventstream_id}/destinations/{destination_id}/pause",
        client="fabric_sp",
        method="post",
    )

    print(
        f"{icons.green_dot} The '{destination_id}' destination in the '{eventstream_name}' eventstream within the '{workspace_name}' workspace has been paused."
    )


@log
def pause_eventstream_source(
    eventstream: str | UUID, source_id: UUID, workspace: Optional[str | UUID] = None
):
    """
    Pause running the specified source in the eventstream.

    This is a wrapper function for the following API: `Topology - Pause Eventstream Source <https://learn.microsoft.com/rest/api/fabric/eventstream/topology/pause-eventstream-source>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    eventstream : str | uuid.UUID
        The name or ID of the eventstream.
    source_id : uuid.UUID
        The ID of the source.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (eventstream_name, eventstream_id) = resolve_item_name_and_id(
        item=eventstream, type="Eventstream", workspace=workspace_id
    )
    _base_api(
        request=f"/v1/workspaces/{workspace_id}/eventstreams/{eventstream_id}/sources/{source_id}/pause",
        client="fabric_sp",
        method="post",
    )

    print(
        f"{icons.green_dot} The '{source_id}' source in the '{eventstream_name}' eventstream within the '{workspace_name}' workspace has been paused."
    )
