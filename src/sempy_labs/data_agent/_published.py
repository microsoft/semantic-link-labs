import pandas as pd
from typing import Optional, Tuple
from sempy_labs._helper_functions import (
    resolve_workspace_id,
    resolve_item_name_and_id,
    _base_api,
    _create_dataframe,
    _is_valid_uuid,
)
from uuid import UUID
from sempy._utils._log import log
import sempy_labs._icons as icons

_datasource_columns = {
    "Datasource Id": "string",
    "Datasource Name": "string",
    "Datasource Type": "string",
    "Fabric Item Type": "string",
    "Description": "string",
    "Instructions": "string",
    "Reference Type": "string",
    "Item Id": "string",
    "Item Workspace Id": "string",
    "Variable Reference": "string",
}

_fewshot_columns = {
    "Fewshot Id": "string",
    "Question": "string",
    "Query": "string",
    "Validation Status": "string",
    "Validation Reason": "string",
}


def _resolve_data_agent(
    data_agent: str | UUID, workspace: Optional[str | UUID]
) -> Tuple[str, str]:

    workspace_id = resolve_workspace_id(workspace)
    item_name, item_id = resolve_item_name_and_id(
        item=data_agent, type="DataAgent", workspace=workspace_id
    )

    return item_name, f"/v1/workspaces/{workspace_id}/dataAgents/{item_id}"


def _resolve_data_agent_url(
    data_agent: str | UUID, workspace: Optional[str | UUID]
) -> str:

    return _resolve_data_agent(data_agent, workspace)[1]


def _datasource_to_row(v: dict) -> dict:

    # FabricItem datasources use 'itemReference'; LakehouseTables datasources use 'lakehouseReference'.
    reference = v.get("itemReference") or v.get("lakehouseReference") or {}
    return {
        "Datasource Id": v.get("id"),
        "Datasource Name": v.get("displayName"),
        "Datasource Type": v.get("type"),
        "Fabric Item Type": v.get("fabricItemType"),
        "Description": v.get("description"),
        "Instructions": v.get("instructions"),
        "Reference Type": reference.get("referenceType"),
        "Item Id": reference.get("itemId"),
        "Item Workspace Id": reference.get("workspaceId"),
        "Variable Reference": reference.get("variableReference"),
    }


def _fewshot_to_row(v: dict) -> dict:

    status = v.get("validationStatus") or {}
    return {
        "Fewshot Id": v.get("id"),
        "Question": v.get("question"),
        "Query": v.get("query"),
        "Validation Status": status.get("value"),
        "Validation Reason": status.get("reason"),
    }


def _rows_to_dataframe(rows: list, columns: dict) -> pd.DataFrame:

    if not rows:
        return _create_dataframe(columns=columns)

    return pd.DataFrame(rows, columns=list(columns.keys()))


def _list_datasources(base_url: str) -> pd.DataFrame:

    responses = _base_api(
        request=f"{base_url}/datasources",
        uses_pagination=True,
        client="fabric_sp",
    )
    rows = [_datasource_to_row(v) for r in responses for v in r.get("value", [])]

    return _rows_to_dataframe(rows, _datasource_columns)


def _resolve_datasource_name_and_id(
    base_url: str, datasource: str | UUID
) -> Tuple[str, str]:

    df = _list_datasources(base_url)
    if _is_valid_uuid(str(datasource)):
        df_filt = df[df["Datasource Id"] == str(datasource)]
    else:
        df_filt = df[df["Datasource Name"] == datasource]

    if df_filt.empty:
        raise ValueError(
            f"{icons.red_dot} The '{datasource}' datasource does not exist within the data agent."
        )

    return (df_filt["Datasource Name"].iloc[0], df_filt["Datasource Id"].iloc[0])


def _get_datasource(base_url: str, datasource: str | UUID) -> pd.DataFrame:

    _, datasource_id = _resolve_datasource_name_and_id(base_url, datasource)
    response = _base_api(
        request=f"{base_url}/datasources/{datasource_id}",
        client="fabric_sp",
    ).json()

    return _rows_to_dataframe([_datasource_to_row(response)], _datasource_columns)


def _list_fewshots(base_url: str, datasource: str | UUID) -> pd.DataFrame:

    _, datasource_id = _resolve_datasource_name_and_id(base_url, datasource)
    responses = _base_api(
        request=f"{base_url}/datasources/{datasource_id}/fewshots",
        uses_pagination=True,
        client="fabric_sp",
    )
    rows = [_fewshot_to_row(v) for r in responses for v in r.get("value", [])]

    return _rows_to_dataframe(rows, _fewshot_columns)


def _get_fewshot(
    base_url: str, datasource: str | UUID, fewshot_id: str | UUID
) -> pd.DataFrame:

    _, datasource_id = _resolve_datasource_name_and_id(base_url, datasource)
    response = _base_api(
        request=f"{base_url}/datasources/{datasource_id}/fewshots/{fewshot_id}",
        client="fabric_sp",
    ).json()

    return _rows_to_dataframe([_fewshot_to_row(response)], _fewshot_columns)


def _settings_to_dataframe(response: dict) -> pd.DataFrame:

    return _rows_to_dataframe(
        [{"AI Instructions": response.get("aiInstructions")}] if response else [],
        {"AI Instructions": "string"},
    )


def _get_settings(base_url: str, return_dataframe: bool) -> pd.DataFrame | dict:

    response = _base_api(request=f"{base_url}/settings", client="fabric_sp").json()

    return _settings_to_dataframe(response) if return_dataframe else response


@log
def list_data_agent_published_datasources(
    data_agent: str | UUID,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Shows the published datasources of the specified data agent.

    This is a wrapper function for the following API: `Published - List Datasources <https://learn.microsoft.com/rest/api/fabric/dataagent/published/list-datasources>`_.

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
        A pandas dataframe showing the published datasources of the specified data agent.
    """

    agent_url = _resolve_data_agent_url(data_agent, workspace)

    return _list_datasources(agent_url)


@log
def get_data_agent_published_datasource(
    data_agent: str | UUID,
    datasource: str | UUID,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Shows the metadata of a published datasource of the specified data agent.

    This is a wrapper function for the following API: `Published - Get Datasource <https://learn.microsoft.com/rest/api/fabric/dataagent/published/get-datasource>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    data_agent : str | uuid.UUID
        The name or ID of the data agent.
    datasource : str | uuid.UUID
        The name or ID of the published datasource.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the metadata of the published datasource.
    """

    agent_url = _resolve_data_agent_url(data_agent, workspace)

    return _get_datasource(agent_url, datasource)


@log
def list_data_agent_published_fewshots(
    data_agent: str | UUID,
    datasource: str | UUID,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Shows the published fewshots (example questions and queries) of a datasource of the specified data agent.

    This is a wrapper function for the following API: `Published - List Datasource Fewshots <https://learn.microsoft.com/rest/api/fabric/dataagent/published/list-datasource-fewshots>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    data_agent : str | uuid.UUID
        The name or ID of the data agent.
    datasource : str | uuid.UUID
        The name or ID of the published datasource.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the published fewshots of the datasource.
    """

    agent_url = _resolve_data_agent_url(data_agent, workspace)

    return _list_fewshots(agent_url, datasource)


@log
def get_data_agent_published_fewshot(
    data_agent: str | UUID,
    datasource: str | UUID,
    fewshot_id: str | UUID,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Shows a published fewshot (example question and query) of a datasource of the specified data agent.

    This is a wrapper function for the following API: `Published - Get Datasource Fewshot <https://learn.microsoft.com/rest/api/fabric/dataagent/published/get-datasource-fewshot>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    data_agent : str | uuid.UUID
        The name or ID of the data agent.
    datasource : str | uuid.UUID
        The name or ID of the published datasource.
    fewshot_id : str | uuid.UUID
        The ID of the fewshot.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the published fewshot.
    """

    agent_url = _resolve_data_agent_url(data_agent, workspace)

    return _get_fewshot(agent_url, datasource, fewshot_id)


@log
def get_data_agent_published_settings(
    data_agent: str | UUID,
    workspace: Optional[str | UUID] = None,
    return_dataframe: bool = True,
) -> pd.DataFrame | dict:
    """
    Shows the published settings of the specified data agent.

    This is a wrapper function for the following API: `Published - Get Settings <https://learn.microsoft.com/rest/api/fabric/dataagent/published/get-settings>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    data_agent : str | uuid.UUID
        The name or ID of the data agent.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    return_dataframe : bool, default=True
        If True, returns a pandas dataframe. If False, returns the API response as a dictionary.

    Returns
    -------
    pandas.DataFrame | dict
        A pandas dataframe showing the published settings of the specified data agent,
        or a dictionary if return_dataframe is set to False.
    """

    agent_url = _resolve_data_agent_url(data_agent, workspace)

    return _get_settings(agent_url, return_dataframe)
