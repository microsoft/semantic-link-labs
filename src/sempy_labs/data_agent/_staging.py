import pandas as pd
from typing import Literal, Optional, Tuple
from urllib.parse import quote
from sempy_labs._helper_functions import (
    resolve_workspace_id,
    resolve_item_id,
    _base_api,
)
from sempy_labs.data_agent._published import (
    _resolve_data_agent,
    _resolve_datasource_name_and_id,
    _list_datasources,
    _get_datasource,
    _list_fewshots,
    _get_fewshot,
    _get_settings,
    _fewshot_to_row,
    _fewshot_columns,
    _rows_to_dataframe,
)
from uuid import UUID
from sempy._utils._log import log
import sempy_labs._icons as icons

_element_columns = {
    "Element Id": "string",
    "Element Name": "string",
    "Element Type": "string",
    "Parent Element Id": "string",
    "Is Selected": "bool",
    "Has Sub Elements": "bool",
    "State": "string",
    "Description": "string",
    "Data Type": "string",
    "Index State": "string",
    "Last Sync Date Time": "datetime",
}


def _staging(
    data_agent: str | UUID, workspace: Optional[str | UUID]
) -> Tuple[str, str]:

    agent_name, agent_url = _resolve_data_agent(data_agent, workspace)

    return agent_name, f"{agent_url}/staging"


def _staging_url(data_agent: str | UUID, workspace: Optional[str | UUID]) -> str:

    return _staging(data_agent, workspace)[1]


def _element_to_row(v: dict, parent_id: Optional[str]) -> dict:

    return {
        "Element Id": v.get("id"),
        "Element Name": v.get("displayName"),
        "Element Type": v.get("type"),
        "Parent Element Id": parent_id,
        "Is Selected": v.get("isSelected"),
        "Has Sub Elements": v.get("hasSubElements"),
        "State": v.get("state"),
        "Description": v.get("description"),
        "Data Type": v.get("dataType"),
        "Index State": v.get("indexState"),
        "Last Sync Date Time": v.get("lastSyncDateTime"),
    }


def _element_url(base_url: str, datasource_id: str, element_id: str) -> str:

    # Element IDs are base64 strings which may contain '+', '/' and '=' characters.
    return f"{base_url}/datasources/{datasource_id}/elements?id={quote(str(element_id), safe='')}"


# ---------------------------------------------------------------------------
# Datasources
# ---------------------------------------------------------------------------


@log
def list_data_agent_staged_datasources(
    data_agent: str | UUID,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Shows the staged (draft) datasources of the specified data agent.

    This is a wrapper function for the following API: `Staging - List Datasources <https://learn.microsoft.com/rest/api/fabric/dataagent/staging/list-datasources>`_.

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
        A pandas dataframe showing the staged datasources of the specified data agent.
    """

    return _list_datasources(_staging_url(data_agent, workspace))


@log
def get_data_agent_staged_datasource(
    data_agent: str | UUID,
    datasource: str | UUID,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Shows the metadata of a staged (draft) datasource of the specified data agent.

    This is a wrapper function for the following API: `Staging - Get Datasource <https://learn.microsoft.com/rest/api/fabric/dataagent/staging/get-datasource>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    data_agent : str | uuid.UUID
        The name or ID of the data agent.
    datasource : str | uuid.UUID
        The name or ID of the staged datasource.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the metadata of the staged datasource.
    """

    return _get_datasource(_staging_url(data_agent, workspace), datasource)


@log
def create_data_agent_staged_datasource(
    data_agent: str | UUID,
    item: str | UUID,
    item_type: Optional[str] = None,
    item_workspace: Optional[str | UUID] = None,
    datasource_type: Literal["FabricItem", "LakehouseTables"] = "FabricItem",
    workspace: Optional[str | UUID] = None,
):
    """
    Adds a Fabric item as a datasource to the staging (draft) environment of the specified data agent.

    This is a wrapper function for the following API: `Staging - Create Datasource <https://learn.microsoft.com/rest/api/fabric/dataagent/staging/create-datasource>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    data_agent : str | uuid.UUID
        The name or ID of the data agent.
    item : str | uuid.UUID
        The name or ID of the Fabric item to add as a datasource.
    item_type : str, default=None
        The `type <https://learn.microsoft.com/rest/api/fabric/dataagent/staging/create-datasource#dataagentsupporteditemtype>`_ of the Fabric item
        (i.e. 'Lakehouse', 'Warehouse', 'SemanticModel', 'KQLDatabase', 'MirroredDatabase', 'SQLDatabase', 'GraphModel', 'Ontology').
        Required when datasource_type is 'FabricItem'. Defaults to 'Lakehouse' when datasource_type is 'LakehouseTables'.
    item_workspace : str | uuid.UUID, default=None
        The name or ID of the workspace in which the Fabric item resides.
        Defaults to None which resolves to the workspace of the data agent.
    datasource_type : str, default="FabricItem"
        The datasource type. Valid values: 'FabricItem', 'LakehouseTables' (a lakehouse in tables-only mode).
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID of the data agent.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    if datasource_type not in ["FabricItem", "LakehouseTables"]:
        raise ValueError(
            f"{icons.red_dot} Invalid datasource_type '{datasource_type}'. Valid values: 'FabricItem', 'LakehouseTables'."
        )
    if datasource_type == "LakehouseTables":
        item_type = item_type or "Lakehouse"
        if item_type != "Lakehouse":
            raise ValueError(
                f"{icons.red_dot} The 'LakehouseTables' datasource type requires a lakehouse."
            )
    elif item_type is None:
        raise ValueError(
            f"{icons.red_dot} The 'item_type' parameter is required when the datasource_type is 'FabricItem'."
        )

    agent_name, base_url = _staging(data_agent, workspace)
    item_workspace_id = resolve_workspace_id(
        item_workspace if item_workspace is not None else workspace
    )
    item_id = resolve_item_id(item=item, type=item_type, workspace=item_workspace_id)

    reference = {
        "referenceType": "ById",
        "itemId": item_id,
        "workspaceId": item_workspace_id,
    }
    reference_key = (
        "itemReference" if datasource_type == "FabricItem" else "lakehouseReference"
    )
    payload = {"type": datasource_type, reference_key: reference}

    _base_api(
        request=f"{base_url}/datasources",
        method="post",
        payload=payload,
        status_codes=[201, 202],
        lro_return_status_code=True,
        client="fabric_sp",
    )

    df = _list_datasources(base_url)
    df_filt = df[df["Item Id"] == str(item_id)]
    if df_filt.empty:
        print(
            f"{icons.green_dot} The '{item}' {item_type} has been added as a datasource to the staging environment of the '{agent_name}' data agent."
        )
    else:
        print(
            f"{icons.green_dot} The '{df_filt['Datasource Name'].iloc[0]}' datasource (ID: '{df_filt['Datasource Id'].iloc[0]}') has been added to the staging environment of the '{agent_name}' data agent."
        )


@log
def update_data_agent_staged_datasource(
    data_agent: str | UUID,
    datasource: str | UUID,
    description: Optional[str] = None,
    instructions: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Updates the description and/or AI instructions of a staged (draft) datasource of the specified data agent.

    This is a wrapper function for the following API: `Staging - Update Datasource <https://learn.microsoft.com/rest/api/fabric/dataagent/staging/update-datasource>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    data_agent : str | uuid.UUID
        The name or ID of the data agent.
    datasource : str | uuid.UUID
        The name or ID of the staged datasource.
    description : str, default=None
        The description of the datasource.
        Defaults to None which keeps the existing description.
    instructions : str, default=None
        Custom AI instructions specific to this datasource.
        Defaults to None which keeps the existing instructions.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    if description is None and instructions is None:
        raise ValueError(
            f"{icons.red_dot} At least one of the 'description' or 'instructions' parameters must be provided."
        )

    agent_name, base_url = _staging(data_agent, workspace)
    datasource_name, datasource_id = _resolve_datasource_name_and_id(
        base_url, datasource
    )

    payload = {}
    if description is not None:
        payload["description"] = description
    if instructions is not None:
        payload["instructions"] = instructions

    _base_api(
        request=f"{base_url}/datasources/{datasource_id}",
        method="patch",
        payload=payload,
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The '{datasource_name}' staged datasource of the '{agent_name}' data agent has been updated."
    )


@log
def delete_data_agent_staged_datasource(
    data_agent: str | UUID,
    datasource: str | UUID,
    workspace: Optional[str | UUID] = None,
):
    """
    Removes a datasource from the staging (draft) environment of the specified data agent. The underlying Fabric item is not deleted.

    This is a wrapper function for the following API: `Staging - Delete Datasource <https://learn.microsoft.com/rest/api/fabric/dataagent/staging/delete-datasource>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    data_agent : str | uuid.UUID
        The name or ID of the data agent.
    datasource : str | uuid.UUID
        The name or ID of the staged datasource.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    agent_name, base_url = _staging(data_agent, workspace)
    datasource_name, datasource_id = _resolve_datasource_name_and_id(
        base_url, datasource
    )

    _base_api(
        request=f"{base_url}/datasources/{datasource_id}",
        method="delete",
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The '{datasource_name}' datasource has been removed from the staging environment of the '{agent_name}' data agent."
    )


# ---------------------------------------------------------------------------
# Datasource elements
# ---------------------------------------------------------------------------


@log
def list_data_agent_staged_elements(
    data_agent: str | UUID,
    datasource: str | UUID,
    root_id: Optional[str] = None,
    recursive: bool = False,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Shows the schema elements (i.e. schemas, tables, columns) of a staged (draft) datasource of the specified data agent.

    This is a wrapper function for the following API: `Staging - List Datasource Elements <https://learn.microsoft.com/rest/api/fabric/dataagent/staging/list-datasource-elements>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    data_agent : str | uuid.UUID
        The name or ID of the data agent.
    datasource : str | uuid.UUID
        The name or ID of the staged datasource.
    root_id : str, default=None
        The ID of a parent element whose children are listed.
        Defaults to None which lists the root level of the schema tree.
    recursive : bool, default=False
        If True, traverses the schema tree below the root level and returns all descendant elements.
        This makes one API call per element that has sub elements, so it may be slow for large schemas.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the schema elements of the staged datasource.
    """

    _, base_url = _staging(data_agent, workspace)
    _, datasource_id = _resolve_datasource_name_and_id(base_url, datasource)

    rows = []
    pending = [root_id]
    while pending:
        parent_id = pending.pop(0)
        url = f"{base_url}/datasources/{datasource_id}/elements"
        if parent_id is not None:
            url += f"?rootId={quote(str(parent_id), safe='')}"
        responses = _base_api(request=url, uses_pagination=True, client="fabric_sp")
        for r in responses:
            for v in r.get("value", []):
                rows.append(_element_to_row(v, parent_id))
                if recursive and v.get("hasSubElements"):
                    pending.append(v.get("id"))

    df = _rows_to_dataframe(rows, _element_columns)
    if rows:
        df["Last Sync Date Time"] = pd.to_datetime(
            df["Last Sync Date Time"], errors="coerce"
        )

    return df


@log
def update_data_agent_staged_element(
    data_agent: str | UUID,
    datasource: str | UUID,
    element_id: str,
    is_selected: Optional[bool] = None,
    description: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Selects/deselects a schema element (i.e. a table or column) of a staged (draft) datasource and/or sets its description.

    This is a wrapper function for the following API: `Staging - Update Datasource Element <https://learn.microsoft.com/rest/api/fabric/dataagent/staging/update-datasource-element>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    data_agent : str | uuid.UUID
        The name or ID of the data agent.
    datasource : str | uuid.UUID
        The name or ID of the staged datasource.
    element_id : str
        The ID of the element (see :func:`list_data_agent_staged_elements`).
    is_selected : bool, default=None
        If True, selects the element for use by the data agent. If False, deselects it (its configuration is preserved).
        Defaults to None which keeps the existing selection.
    description : str, default=None
        A custom description for the element.
        Defaults to None which keeps the existing description.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    if is_selected is None and description is None:
        raise ValueError(
            f"{icons.red_dot} At least one of the 'is_selected' or 'description' parameters must be provided."
        )

    _, base_url = _staging(data_agent, workspace)
    datasource_name, datasource_id = _resolve_datasource_name_and_id(
        base_url, datasource
    )

    payload = {}
    if is_selected is not None:
        payload["isSelected"] = is_selected
    if description is not None:
        payload["description"] = description

    response = _base_api(
        request=_element_url(base_url, datasource_id, element_id),
        method="patch",
        payload=payload,
        client="fabric_sp",
    ).json()

    print(
        f"{icons.green_dot} The '{response.get('displayName', element_id)}' element of the '{datasource_name}' staged datasource has been updated."
    )


@log
def delete_data_agent_staged_element(
    data_agent: str | UUID,
    datasource: str | UUID,
    element_id: str,
    workspace: Optional[str | UUID] = None,
):
    """
    Deletes a stale schema element (an element which no longer exists in the live schema of the datasource) from a staged (draft) datasource.

    This is a wrapper function for the following API: `Staging - Delete Datasource Element <https://learn.microsoft.com/rest/api/fabric/dataagent/staging/delete-datasource-element>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    data_agent : str | uuid.UUID
        The name or ID of the data agent.
    datasource : str | uuid.UUID
        The name or ID of the staged datasource.
    element_id : str
        The ID of the element (see :func:`list_data_agent_staged_elements`). Only elements whose state is 'NotAvailable' can be deleted.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    _, base_url = _staging(data_agent, workspace)
    datasource_name, datasource_id = _resolve_datasource_name_and_id(
        base_url, datasource
    )

    _base_api(
        request=_element_url(base_url, datasource_id, element_id),
        method="delete",
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The '{element_id}' element has been deleted from the '{datasource_name}' staged datasource."
    )


# ---------------------------------------------------------------------------
# Fewshots
# ---------------------------------------------------------------------------


@log
def list_data_agent_staged_fewshots(
    data_agent: str | UUID,
    datasource: str | UUID,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Shows the staged (draft) fewshots (example questions and queries) of a datasource of the specified data agent.

    This is a wrapper function for the following API: `Staging - List Datasource Fewshots <https://learn.microsoft.com/rest/api/fabric/dataagent/staging/list-datasource-fewshots>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    data_agent : str | uuid.UUID
        The name or ID of the data agent.
    datasource : str | uuid.UUID
        The name or ID of the staged datasource.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the staged fewshots of the datasource.
    """

    return _list_fewshots(_staging_url(data_agent, workspace), datasource)


@log
def get_data_agent_staged_fewshot(
    data_agent: str | UUID,
    datasource: str | UUID,
    fewshot_id: str | UUID,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Shows a staged (draft) fewshot (example question and query) of a datasource of the specified data agent.

    This is a wrapper function for the following API: `Staging - Get Datasource Fewshot <https://learn.microsoft.com/rest/api/fabric/dataagent/staging/get-datasource-fewshot>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    data_agent : str | uuid.UUID
        The name or ID of the data agent.
    datasource : str | uuid.UUID
        The name or ID of the staged datasource.
    fewshot_id : str | uuid.UUID
        The ID of the fewshot.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the staged fewshot.
    """

    return _get_fewshot(_staging_url(data_agent, workspace), datasource, fewshot_id)


@log
def create_data_agent_staged_fewshot(
    data_agent: str | UUID,
    datasource: str | UUID,
    question: str,
    query: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Creates a fewshot (example question and query) for a staged (draft) datasource of the specified data agent.

    This is a wrapper function for the following API: `Staging - Create Datasource Fewshot <https://learn.microsoft.com/rest/api/fabric/dataagent/staging/create-datasource-fewshot>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    data_agent : str | uuid.UUID
        The name or ID of the data agent.
    datasource : str | uuid.UUID
        The name or ID of the staged datasource.
    question : str
        The natural language question.
    query : str, default=None
        The SQL/KQL query which answers the question.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the created fewshot (including its ID and validation status).
    """

    _, base_url = _staging(data_agent, workspace)
    datasource_name, datasource_id = _resolve_datasource_name_and_id(
        base_url, datasource
    )

    payload = {"question": question}
    if query is not None:
        payload["query"] = query

    response = _base_api(
        request=f"{base_url}/datasources/{datasource_id}/fewshots",
        method="post",
        payload=payload,
        status_codes=201,
        client="fabric_sp",
    ).json()

    print(
        f"{icons.green_dot} The fewshot (ID: '{response.get('id')}') has been created for the '{datasource_name}' staged datasource."
    )

    return _rows_to_dataframe([_fewshot_to_row(response)], _fewshot_columns)


@log
def update_data_agent_staged_fewshot(
    data_agent: str | UUID,
    datasource: str | UUID,
    fewshot_id: str | UUID,
    question: Optional[str] = None,
    query: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Updates a staged (draft) fewshot (example question and query) of a datasource of the specified data agent.

    This is a wrapper function for the following API: `Staging - Update Datasource Fewshot <https://learn.microsoft.com/rest/api/fabric/dataagent/staging/update-datasource-fewshot>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    data_agent : str | uuid.UUID
        The name or ID of the data agent.
    datasource : str | uuid.UUID
        The name or ID of the staged datasource.
    fewshot_id : str | uuid.UUID
        The ID of the fewshot.
    question : str, default=None
        The natural language question.
        Defaults to None which keeps the existing question.
    query : str, default=None
        The SQL/KQL query which answers the question.
        Defaults to None which keeps the existing query.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    if question is None and query is None:
        raise ValueError(
            f"{icons.red_dot} At least one of the 'question' or 'query' parameters must be provided."
        )

    _, base_url = _staging(data_agent, workspace)
    datasource_name, datasource_id = _resolve_datasource_name_and_id(
        base_url, datasource
    )

    payload = {}
    if question is not None:
        payload["question"] = question
    if query is not None:
        payload["query"] = query

    _base_api(
        request=f"{base_url}/datasources/{datasource_id}/fewshots/{fewshot_id}",
        method="patch",
        payload=payload,
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The '{fewshot_id}' fewshot of the '{datasource_name}' staged datasource has been updated."
    )


@log
def delete_data_agent_staged_fewshot(
    data_agent: str | UUID,
    datasource: str | UUID,
    fewshot_id: str | UUID,
    workspace: Optional[str | UUID] = None,
):
    """
    Deletes a staged (draft) fewshot from a datasource of the specified data agent.

    This is a wrapper function for the following API: `Staging - Delete Datasource Fewshot <https://learn.microsoft.com/rest/api/fabric/dataagent/staging/delete-datasource-fewshot>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    data_agent : str | uuid.UUID
        The name or ID of the data agent.
    datasource : str | uuid.UUID
        The name or ID of the staged datasource.
    fewshot_id : str | uuid.UUID
        The ID of the fewshot.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    _, base_url = _staging(data_agent, workspace)
    datasource_name, datasource_id = _resolve_datasource_name_and_id(
        base_url, datasource
    )

    _base_api(
        request=f"{base_url}/datasources/{datasource_id}/fewshots/{fewshot_id}",
        method="delete",
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The '{fewshot_id}' fewshot has been deleted from the '{datasource_name}' staged datasource."
    )


@log
def delete_all_data_agent_staged_fewshots(
    data_agent: str | UUID,
    datasource: str | UUID,
    workspace: Optional[str | UUID] = None,
):
    """
    Deletes all staged (draft) fewshots from a datasource of the specified data agent.

    This is a wrapper function for the following API: `Staging - Delete All Datasource Fewshots <https://learn.microsoft.com/rest/api/fabric/dataagent/staging/delete-all-datasource-fewshots>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    data_agent : str | uuid.UUID
        The name or ID of the data agent.
    datasource : str | uuid.UUID
        The name or ID of the staged datasource.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    _, base_url = _staging(data_agent, workspace)
    datasource_name, datasource_id = _resolve_datasource_name_and_id(
        base_url, datasource
    )

    _base_api(
        request=f"{base_url}/datasources/{datasource_id}/fewshots/deleteAll",
        method="post",
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} All fewshots have been deleted from the '{datasource_name}' staged datasource."
    )


# ---------------------------------------------------------------------------
# Settings & reset
# ---------------------------------------------------------------------------


@log
def get_data_agent_staged_settings(
    data_agent: str | UUID,
    workspace: Optional[str | UUID] = None,
    return_dataframe: bool = True,
) -> pd.DataFrame | dict:
    """
    Shows the staged (draft) settings of the specified data agent.

    This is a wrapper function for the following API: `Staging - Get Settings <https://learn.microsoft.com/rest/api/fabric/dataagent/staging/get-settings>`_.

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
        A pandas dataframe showing the staged settings of the specified data agent,
        or a dictionary if return_dataframe is set to False.
    """

    return _get_settings(_staging_url(data_agent, workspace), return_dataframe)


@log
def update_data_agent_staged_settings(
    data_agent: str | UUID,
    ai_instructions: str,
    workspace: Optional[str | UUID] = None,
):
    """
    Updates the staged (draft) settings of the specified data agent.

    This is a wrapper function for the following API: `Staging - Update Settings <https://learn.microsoft.com/rest/api/fabric/dataagent/staging/update-settings>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    data_agent : str | uuid.UUID
        The name or ID of the data agent.
    ai_instructions : str
        The custom AI instructions (system prompt) for the data agent.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    agent_name, base_url = _staging(data_agent, workspace)

    _base_api(
        request=f"{base_url}/settings",
        method="patch",
        payload={"aiInstructions": ai_instructions},
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The staged AI instructions of the '{agent_name}' data agent have been updated."
    )


@log
def reset_data_agent_staging(
    data_agent: str | UUID,
    workspace: Optional[str | UUID] = None,
):
    """
    Reverts the staging (draft) environment of the specified data agent to its published state, discarding all unpublished changes.

    This is a wrapper function for the following API: `Staging - Reset <https://learn.microsoft.com/rest/api/fabric/dataagent/staging/reset>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    data_agent : str | uuid.UUID
        The name or ID of the data agent.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    agent_name, base_url = _staging(data_agent, workspace)

    _base_api(
        request=f"{base_url}/reset",
        method="post",
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The staging environment of the '{agent_name}' data agent has been reset to its published state."
    )
