import pandas as pd
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_id,
    _decode_b64,
    _base_api,
    resolve_item_id,
    _create_dataframe,
    delete_item,
    create_item,
)
from uuid import UUID
from sempy._utils._log import log


@log
def list_data_pipelines(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows the data pipelines within a workspace.

    This is a wrapper function for the following API: `Items - List Data Pipelines <https://learn.microsoft.com/rest/api/fabric/datapipeline/items/list-data-pipelines>`_.

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
        A pandas dataframe showing the data pipelines within a workspace.
    """

    columns = {
        "Data Pipeline Name": "string",
        "Data Pipeline ID": "string",
        "Description": "string",
    }
    df = _create_dataframe(columns=columns)

    workspace_id = resolve_workspace_id(workspace)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/dataPipelines",
        uses_pagination=True,
        client="fabric_sp",
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            rows.append(
                {
                    "Data Pipeline Name": v.get("displayName"),
                    "Data Pipeline ID": v.get("id"),
                    "Description": v.get("description"),
                }
            )
    if rows:
        df = pd.DataFrame(rows, columns=columns.keys())

    return df


@log
def create_data_pipeline(
    name: str, description: Optional[str] = None, workspace: Optional[str | UUID] = None
):
    """
    Creates a Fabric data pipeline.

    This is a wrapper function for the following API: `Items - Create Data Pipeline <https://learn.microsoft.com/rest/api/fabric/datapipeline/items/create-data-pipeline>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    name: str
        Name of the data pipeline.
    description : str, default=None
        A description of the environment.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    create_item(
        name=name, description=description, type="DataPipeline", workspace=workspace
    )


@log
def delete_data_pipeline(name: str | UUID, workspace: Optional[str | UUID] = None):
    """
    Deletes a Fabric data pipeline.

    This is a wrapper function for the following API: `Items - Delete Data Pipeline <https://learn.microsoft.com/rest/api/fabric/datapipeline/items/delete-data-pipeline>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    name: str | uuid.UUID
        Name or ID of the data pipeline.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    delete_item(item=name, type="DataPipeline", workspace=workspace)


@log
def get_data_pipeline_definition(
    name: str | UUID, workspace: Optional[str | UUID] = None, decode: bool = True
) -> dict | pd.DataFrame:
    """
    Obtains the definition of a data pipeline.

    Parameters
    ----------
    name : str or uuid.UUID
        The name or ID of the data pipeline.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    decode : bool, default=True
        decode : bool, default=True
        If True, decodes the data pipeline definition file into .json format.
        If False, obtains the data pipeline definition file a pandas DataFrame format.

    Returns
    -------
    dict | pandas.DataFrame
        A pandas dataframe showing the data pipelines within a workspace.
    """

    workspace_id = resolve_workspace_id(workspace)

    item_id = resolve_item_id(item=name, type="DataPipeline", workspace=workspace)
    result = _base_api(
        request=f"/v1/workspaces/{workspace_id}/dataPipelines/{item_id}/getDefinition",
        method="post",
        lro_return_json=True,
        status_codes=None,
        client="fabric_sp",
    )
    df = pd.json_normalize(result["definition"]["parts"])

    if not decode:
        return df
    content = df[df["path"] == "pipeline-content.json"]
    payload = content["payload"].iloc[0]

    return _decode_b64(payload)
