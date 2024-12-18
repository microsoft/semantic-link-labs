import sempy.fabric as fabric
import pandas as pd
import sempy_labs._icons as icons
from typing import Optional
import base64
import requests
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    lro,
    _decode_b64,
)
from sempy.fabric.exceptions import FabricHTTPException
import os
from uuid import UUID

_notebook_prefix = "notebook-content."


def _get_notebook_definition_base(
    notebook_name: str, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    item_id = fabric.resolve_item_id(
        item_name=notebook_name, type="Notebook", workspace=workspace_id
    )
    client = fabric.FabricRestClient()
    response = client.post(
        f"v1/workspaces/{workspace_id}/notebooks/{item_id}/getDefinition",
    )

    result = lro(client, response).json()

    return pd.json_normalize(result["definition"]["parts"])


def _get_notebook_type(
    notebook_name: str, workspace: Optional[str | UUID] = None
) -> str:

    df_items = _get_notebook_definition_base(
        notebook_name=notebook_name, workspace=workspace
    )

    file_path = df_items[df_items["path"].str.startswith(_notebook_prefix)][
        "path"
    ].iloc[0]

    _, file_extension = os.path.splitext(file_path)

    return file_extension[1:]


def get_notebook_definition(
    notebook_name: str, workspace: Optional[str | UUID] = None, decode: bool = True
) -> str:
    """
    Obtains the notebook definition.

    This is a wrapper function for the following API: `Items - Get Notebook Definition <https://learn.microsoft.com/rest/api/fabric/notebook/items/get-notebook-definition>`_.

    Parameters
    ----------
    notebook_name : str
        The name of the notebook.
    workspace : str | uuid.UUID, default=None
        The name or ID of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    decode : bool, default=True
        If True, decodes the notebook definition file into .ipynb format.
        If False, obtains the notebook definition file in base64 format.

    Returns
    -------
    str
        The notebook definition.
    """

    df_items = _get_notebook_definition_base(
        notebook_name=notebook_name, workspace=workspace
    )
    df_items_filt = df_items[df_items["path"].str.startswith(_notebook_prefix)]
    payload = df_items_filt["payload"].iloc[0]

    if decode:
        result = _decode_b64(payload)
    else:
        result = payload

    return result


def import_notebook_from_web(
    notebook_name: str,
    url: str,
    description: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
    overwrite: bool = False,
):
    """
    Creates a new notebook within a workspace based on a Jupyter notebook hosted in the web.

    Note: When specifying a notebook from GitHub, please use the raw file path. Note that if the non-raw file path is specified, the url will be
    converted to the raw URL as the raw URL is needed to obtain the notebook content.

    Parameters
    ----------
    notebook_name : str
        The name of the notebook to be created.
    url : str
        The url of the Jupyter Notebook (.ipynb)
    description : str, default=None
        The description of the notebook.
        Defaults to None which does not place a description.
    workspace : str | uuid.UUID, default=None
        The name or ID of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    overwrite : bool, default=False
        If set to True, overwrites the existing notebook in the workspace if it exists.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    # Fix links to go to the raw github file
    starting_text = "https://github.com/"
    starting_text_len = len(starting_text)
    if url.startswith(starting_text):
        url = f"https://raw.githubusercontent.com/{url[starting_text_len:]}".replace(
            "/blob/", "/"
        )

    response = requests.get(url)
    if response.status_code != 200:
        raise FabricHTTPException(response)

    dfI = fabric.list_items(workspace=workspace, type="Notebook")
    dfI_filt = dfI[dfI["Display Name"] == notebook_name]
    if len(dfI_filt) == 0:
        create_notebook(
            name=notebook_name,
            notebook_content=response.content,
            workspace=workspace_id,
            description=description,
        )
    elif len(dfI_filt) > 0 and overwrite:
        print(f"{icons.info} Overwrite of notebooks is currently not supported.")
        # update_notebook_definition(
        #    name=notebook_name, notebook_content=response.content, workspace=workspace
        # )
    else:
        raise ValueError(
            f"{icons.red_dot} The '{notebook_name}' already exists within the '{workspace_name}' workspace and 'overwrite' is set to False."
        )


def create_notebook(
    name: str,
    notebook_content: str,
    type: str = "py",
    description: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Creates a new notebook with a definition within a workspace.

    Parameters
    ----------
    name : str
        The name of the notebook to be created.
    notebook_content : str
        The Jupyter notebook content (not in Base64 format).
    type : str, default="py"
        The notebook type.
    description : str, default=None
        The description of the notebook.
        Defaults to None which does not place a description.
    workspace : str | uuid.UUID, default=None
        The name or ID of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    client = fabric.FabricRestClient()
    notebook_payload = base64.b64encode(notebook_content)

    request_body = {
        "displayName": name,
        "definition": {
            "format": "ipynb",
            "parts": [
                {
                    "path": f"{_notebook_prefix}.{type}",
                    "payload": notebook_payload,
                    "payloadType": "InlineBase64",
                }
            ],
        },
    }
    if description is not None:
        request_body["description"] = description

    response = client.post(f"v1/workspaces/{workspace_id}/notebooks", json=request_body)

    lro(client, response, status_codes=[201, 202])

    print(
        f"{icons.green_dot} The '{name}' notebook was created within the '{workspace_name}' workspace."
    )


def update_notebook_definition(
    name: str, notebook_content: str, workspace: Optional[str | UUID] = None
):
    """
    Updates an existing notebook with a new definition.

    Parameters
    ----------
    name : str
        The name of the notebook to be updated.
    notebook_content : str
        The Jupyter notebook content (not in Base64 format).
    workspace : str | uuid.UUID, default=None
        The name or ID of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    client = fabric.FabricRestClient()
    notebook_payload = base64.b64encode(notebook_content)
    notebook_id = fabric.resolve_item_id(
        item_name=name, type="Notebook", workspace=workspace_id
    )

    type = _get_notebook_type(notebook_name=name, workspace=workspace_id)

    request_body = {
        "definition": {
            "parts": [
                {
                    "path": f"{_notebook_prefix}.{type}",
                    "payload": notebook_payload,
                    "payloadType": "InlineBase64",
                }
            ],
        },
    }

    response = client.post(
        f"v1/workspaces/{workspace_id}/notebooks/{notebook_id}/updateDefinition",
        json=request_body,
    )

    lro(client, response, return_status_code=True)

    print(
        f"{icons.green_dot} The '{name}' notebook was updated within the '{workspace_name}' workspace."
    )
