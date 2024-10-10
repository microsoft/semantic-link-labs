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


def get_notebook_definition(
    notebook_name: str, workspace: Optional[str] = None, decode: bool = True
) -> str:
    """
    Obtains the notebook definition.

    Parameters
    ----------
    notebook_name : str
        The name of the notebook.
    workspace : str, default=None
        The name of the workspace.
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

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)
    item_id = fabric.resolve_item_id(
        item_name=notebook_name, type="Notebook", workspace=workspace
    )
    client = fabric.FabricRestClient()
    response = client.post(
        f"v1/workspaces/{workspace_id}/notebooks/{item_id}/getDefinition",
    )

    result = lro(client, response).json()
    df_items = pd.json_normalize(result["definition"]["parts"])
    df_items_filt = df_items[df_items["path"] == "notebook-content.py"]
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
    workspace: Optional[str] = None,
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
    workspace : str, default=None
        The name of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)
    client = fabric.FabricRestClient()
    dfI = fabric.list_items(workspace=workspace, type="Notebook")
    dfI_filt = dfI[dfI["Display Name"] == notebook_name]
    if len(dfI_filt) > 0:
        raise ValueError(
            f"{icons.red_dot} The '{notebook_name}' already exists within the '{workspace}' workspace."
        )

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
    file_content = response.content
    notebook_payload = base64.b64encode(file_content)

    request_body = {
        "displayName": notebook_name,
        "definition": {
            "format": "ipynb",
            "parts": [
                {
                    "path": "notebook-content.py",
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
        f"{icons.green_dot} The '{notebook_name}' notebook was created within the '{workspace}' workspace."
    )
