from uuid import UUID
from typing import Optional, List
import pandas as pd
from sempy_labs._helper_functions import (
    _create_dataframe,
    _base_api,
    _update_dataframe_datatypes,
    resolve_workspace_name_and_id,
    resolve_dataset_name_and_id,
    delete_item,
    resolve_dataset_id,
    resolve_workspace_id,
)
import sempy_labs._icons as icons
import re
from sempy._utils._log import log


@log
def get_semantic_model_refresh_schedule(
    dataset: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Gets the refresh schedule for the specified dataset from the specified workspace.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        Shows the refresh schedule for the specified dataset from the specified workspace.
    """

    workspace_id = resolve_workspace_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace)

    columns = {
        "Days": "str",
        "Times": "str",
        "Enabled": "bool",
        "Local Time Zone Id": "str",
        "Notify Option": "str",
    }

    column_map = {
        "days": "Days",
        "times": "Times",
        "enabled": "Enabled",
        "localTimeZoneId": "Local Time Zone Id",
        "notifyOption": "Notify Option",
    }

    df = _create_dataframe(columns)

    result = _base_api(
        request=f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshSchedule",
        client="fabric_sp",
    ).json()

    df = (
        pd.json_normalize(result)
        .drop(columns=["@odata.context"], errors="ignore")
        .rename(columns=column_map)
    )

    _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
def enable_semantic_model_scheduled_refresh(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    enable: bool = True,
):
    """
    Enables the scheduled refresh for the specified dataset from the specified workspace.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    enable : bool, default=True
        If True, enables the scheduled refresh.
        If False, disables the scheduled refresh.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace)

    df = get_semantic_model_refresh_schedule(dataset=dataset, workspace=workspace)
    status = df["Enabled"].iloc[0]

    if enable and status:
        print(
            f"{icons.info} Scheduled refresh for the '{dataset_name}' within the '{workspace_name}' workspace is already enabled."
        )
    elif not enable and not status:
        print(
            f"{icons.info} Scheduled refresh for the '{dataset_name}' within the '{workspace_name}' workspace is already disabled."
        )
    else:
        payload = {"value": {"enabled": enable}}

        _base_api(
            request=f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshSchedule",
            method="patch",
            payload=payload,
            client="fabric_sp",
        )

        print(
            f"{icons.green_dot} Scheduled refresh for the '{dataset_name}' within the '{workspace_name}' workspace has been enabled."
        )


@log
def delete_semantic_model(dataset: str | UUID, workspace: Optional[str | UUID] = None):
    """
    Deletes a semantic model.

    This is a wrapper function for the following API: `Items - Delete Semantic Model <https://learn.microsoft.com/rest/api/fabric/semanticmodel/items/delete-semantic-model>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    dataset: str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    delete_item(item=dataset, type="SemanticModel", workspace=workspace)


@log
def update_semantic_model_refresh_schedule(
    dataset: str | UUID,
    days: Optional[str | List[str]] = None,
    times: Optional[str | List[str]] = None,
    time_zone: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Updates the refresh schedule for the specified dataset from the specified workspace.

    This is a wrapper function for the following API: `Datasets - Update Refresh Schedule In Group <https://learn.microsoft.com/rest/api/power-bi/datasets/update-refresh-schedule-in-group>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    days : str | list[str], default=None
        The days of the week to refresh the dataset.
        Valid values are: "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday".
        Defaults to None which means the refresh schedule will not be updated.
    times : str | list[str], default=None
        The times of the day to refresh the dataset.
        Valid format is "HH:MM" (24-hour format).
        Defaults to None which means the refresh schedule will not be updated.
    time_zone : str, default=None
        The time zone to use for the refresh schedule.
        Defaults to None which means the refresh schedule will not be updated.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace)

    payload = {"value": {}}

    def is_valid_time_format(time_str):
        pattern = r"^(?:[01]\d|2[0-3]):[0-5]\d$"
        return re.match(pattern, time_str) is not None

    weekdays = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Sunday",
        "Saturday",
    ]
    if days:
        if isinstance(days, str):
            days = [days]
            for i in range(len(days)):
                days[i] = days[i].capitalize()
                if days[i] not in weekdays:
                    raise ValueError(
                        f"{icons.red_dot} Invalid day '{days[i]}'. Valid days are: {weekdays}"
                    )
        payload["value"]["days"] = days
    if times:
        if isinstance(times, str):
            times = [times]
            for i in range(len(times)):
                if not is_valid_time_format(times[i]):
                    raise ValueError(
                        f"{icons.red_dot} Invalid time '{times[i]}'. Valid time format is 'HH:MM' (24-hour format)."
                    )
        payload["value"]["times"] = times
    if time_zone:
        payload["value"]["localTimeZoneId"] = time_zone

    if not payload.get("value"):
        print(
            f"{icons.info} No changes were made to the refresh schedule for the '{dataset_name}' within the '{workspace_name}' workspace."
        )
        return

    _base_api(
        request=f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshSchedule",
        method="patch",
        client="fabric_sp",
        payload=payload,
    )

    print(
        f"{icons.green_dot} Refresh schedule for the '{dataset_name}' within the '{workspace_name}' workspace has been updated."
    )


@log
def list_semantic_model_datasources(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    expand_details: bool = True,
) -> pd.DataFrame:
    """
    Lists the data sources for the specified semantic model.

    This is a wrapper function for the following API: `Datasets - Get Datasources In Group <https://learn.microsoft.com/rest/api/power-bi/datasets/get-datasources-in-group>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    expand_details : bool, default=True
        If True, expands the connection details for each data source.

    Returns
    -------
    pandas.DataFrame
        DataFrame containing the data sources for the specified semantic model.
    """

    workspace_id = resolve_workspace_id(workspace)
    dataset_id = resolve_dataset_id(dataset, workspace_id)

    if expand_details:
        columns = {
            "Datasource Type": "str",
            "Connection Server": "str",
            "Connection Database": "str",
            "Connection Path": "str",
            "Connection Account": "str",
            "Connection Domain": "str",
            "Connection Kind": "str",
            "Connection Email Address": "str",
            "Connection URL": "str",
            "Connection Class Info": "str",
            "Connection Login Server": "str",
            "Datasource Id": "str",
            "Gateway Id": "str",
        }
    else:
        columns = {
            "Datasource Type": "str",
            "Connection Details": "str",
            "Datasource Id": "str",
            "Gateway Id": "str",
        }

    df = _create_dataframe(columns)

    response = _base_api(
        request=f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/datasources",
        client="fabric_sp",
    )

    rows = []
    for item in response.json().get("value", []):
        ds_type = item.get("datasourceType")
        conn_details = item.get("connectionDetails", {})
        ds_id = item.get("datasourceId")
        gateway_id = item.get("gatewayId")
        if expand_details:
            rows.append(
                {
                    "Datasource Type": ds_type,
                    "Connection Server": conn_details.get("server"),
                    "Connection Database": conn_details.get("database"),
                    "Connection Path": conn_details.get("path"),
                    "Connection Account": conn_details.get("account"),
                    "Connection Domain": conn_details.get("domain"),
                    "Connection Kind": conn_details.get("kind"),
                    "Connection Email Address": conn_details.get("emailAddress"),
                    "Connection URL": conn_details.get("url"),
                    "Connection Class Info": conn_details.get("classInfo"),
                    "Connection Login Server": conn_details.get("loginServer"),
                    "Datasource Id": ds_id,
                    "Gateway Id": gateway_id,
                }
            )
        else:
            rows.append(
                {
                    "Datasource Type": ds_type,
                    "Connection Details": conn_details,
                    "Datasource Id": ds_id,
                    "Gateway Id": gateway_id,
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def bind_semantic_model_connection(
    dataset: str | UUID,
    connection_id: UUID,
    connectivity_type: str,
    connection_type: str,
    connection_path: str,
    workspace: Optional[str | UUID] = None,
):
    """
    Binds a semantic model data source reference to a data connection.
    This API can also be used to unbind data source references.

    This is a wrapper function for the following API: `Items - Bind Semantic Model Connection <https://learn.microsoft.com/rest/api/fabric/semanticmodel/items/bind-semantic-model-connection>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    connection_id : uuid.UUID
        The object ID of the connection.
    connectivity_type : str
        The connectivity type of the connection. Additional connectivity types may be added over time.
    connection_type : str
        The `type <https://learn.microsoft.com/rest/api/fabric/semanticmodel/items/bind-semantic-model-connection?tabs=HTTP#connectivitytype>`_ of the connection.
    connection_path : str
        The path of the connection.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(
        dataset=dataset, workspace=workspace_id
    )

    payload = {
        "connectionBinding": {
            "id": str(connection_id),
            "connectivityType": connectivity_type,
            "connectionDetails": {
                "type": connection_type,
                "path": connection_path,
            },
        }
    }

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/semanticModels/{dataset_id}/bindConnection",
        method="post",
        client="fabric_sp",
        payload=payload,
    )

    print(
        f"{icons.green_dot} Connection '{connection_id}' has been bound to the '{dataset_name}' semantic model within the '{workspace_name}' workspace."
    )


@log
def unbind_semantic_model_connection(
    dataset: str | UUID,
    connection_type: str,
    connection_path: str,
    workspace: Optional[str | UUID] = None,
):
    """
    Unbinds a semantic model data source reference to a data connection.

    This is a wrapper function for the following API: `Items - Bind Semantic Model Connection <https://learn.microsoft.com/rest/api/fabric/semanticmodel/items/bind-semantic-model-connection>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    connection_type : str
        The `type <https://learn.microsoft.com/rest/api/fabric/semanticmodel/items/bind-semantic-model-connection?tabs=HTTP#connectivitytype>`_ of the connection.
    connection_path : str
        The path of the connection.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(
        dataset=dataset, workspace=workspace_id
    )

    payload = {
        "connectionBinding": {
            "connectivityType": "None",
            "connectionDetails": {
                "type": connection_type,
                "path": connection_path,
            },
        }
    }

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/semanticModels/{dataset_id}/bindConnection",
        method="post",
        client="fabric_sp",
        payload=payload,
    )

    print(
        f"{icons.green_dot} The '{dataset_name}' semantic model within the '{workspace_name}' workspace has been unbound from its connection."
    )
