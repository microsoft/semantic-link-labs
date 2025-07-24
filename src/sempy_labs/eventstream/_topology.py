import pandas as pd
from typing import Optional
from sempy_labs._helper_functions import (
    _base_api,
    resolve_item_id,
    resolve_workspace_id,
    resolve_workspace_name_and_id,
    resolve_item_name_and_id,
)
from uuid import UUID
import sempy_labs._icons as icons
from sempy._utils._log import log


@log
def get_eventstream_destination(
    eventstream: str | UUID,
    destination_id: UUID,
    workspace: Optional[str | UUID] = None,
) -> dict:
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
    dict
        A dictionary showing the destination details.
    """

    workspace_id = resolve_workspace_id(workspace)
    eventstream_id = resolve_item_id(
        item=eventstream, type="Eventstream", workspace=workspace_id
    )
    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/eventstreams/{eventstream_id}/destinations/{destination_id}",
        client="fabric_sp",
    )
    return response.json()


@log
def get_eventstream_destination_connection(
    eventstream: str | UUID,
    destination_id: UUID,
    workspace: Optional[str | UUID] = None,
) -> dict:
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
    dict
        A dictionary showing the connection details of the destination.
    """

    workspace_id = resolve_workspace_id(workspace)
    eventstream_id = resolve_item_id(
        item=eventstream, type="Eventstream", workspace=workspace_id
    )
    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/eventstreams/{eventstream_id}/destinations/{destination_id}/connection",
        client="fabric_sp",
    )
    return response.json()


@log
def get_eventstream_source(
    eventstream: str | UUID, source_id: UUID, workspace: Optional[str | UUID] = None
) -> dict:
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
    dict
        A dictionary showing the source details.
    """

    workspace_id = resolve_workspace_id(workspace)
    eventstream_id = resolve_item_id(
        item=eventstream, type="Eventstream", workspace=workspace_id
    )
    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/eventstreams/{eventstream_id}/sources/{source_id}",
        client="fabric_sp",
    )
    return response.json()


@log
def get_eventstream_source_connection(
    eventstream: str | UUID, source_id: UUID, workspace: Optional[str | UUID] = None
) -> dict:
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
    dict
        A dictionary showing the connection details of the source.
    """

    workspace_id = resolve_workspace_id(workspace)
    eventstream_id = resolve_item_id(
        item=eventstream, type="Eventstream", workspace=workspace_id
    )
    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/eventstreams/{eventstream_id}/sources/{source_id}/connection",
        client="fabric_sp",
    )
    return response.json()


@log
def get_eventstream_topology(
    eventstream: str | UUID, workspace: Optional[str | UUID] = None
) -> dict:
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
    dict
        A dictionary showing the topology of the eventstream, including sources and destinations.
    """

    workspace_id = resolve_workspace_id(workspace)
    eventstream_id = resolve_item_id(
        item=eventstream, type="Eventstream", workspace=workspace_id
    )
    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/eventstreams/{eventstream_id}/topology",
        client="fabric_sp",
    )
    return response.json()


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
def resume_eventstream(
    eventstream: str | UUID, workspace: Optional[str | UUID] = None
) -> dict:
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
) -> dict:
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
) -> dict:
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
def pause_eventstream_destination(
    eventstream: str | UUID,
    destination_id: UUID,
    workspace: Optional[str | UUID] = None,
) -> dict:
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
) -> dict:
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
