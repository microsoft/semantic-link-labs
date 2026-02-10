import pandas as pd
from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_workspace_id,
    _decode_b64,
    _base_api,
    resolve_item_id,
    resolve_item_name_and_id,
    _create_dataframe,
    delete_item,
    create_item,
)
from uuid import UUID
from sempy._utils._log import log
import sempy_labs._icons as icons


@log
def list_copy_jobs(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Returns a list of copy jobs from the specified workspace.

    This is a wrapper function for the following API: `Items - List Copy Jobs <https://learn.microsoft.com/rest/api/fabric/copyjob/items/list-copy-jobs>`_.

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
        A pandas dataframe showing the copy jobs within a workspace.
    """

    columns = {
        "Copy Job Name": "string",
        "Copy Job Id": "string",
        "Description": "string",
    }
    df = _create_dataframe(columns=columns)

    workspace_id = resolve_workspace_id(workspace)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/copyJobs",
        uses_pagination=True,
        client="fabric_sp",
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            rows.append(
                {
                    "Copy Job Name": v.get("displayName"),
                    "Copy Job Id": v.get("id"),
                    "Description": v.get("description"),
                }
            )
    if rows:
        df = pd.DataFrame(rows, columns=columns.keys())

    return df


@log
def create_copy_job(
    name: str,
    description: Optional[str] = None,
    definition: Optional[dict] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Creates a copy job in the specified workspace.

    This is a wrapper function for the following API: `Items - Create Copy Job <https://learn.microsoft.com/rest/api/fabric/copyjob/items/create-copy-job>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    name : str
        The display name of the copy job.
    description : str, default=None
        The description of the copy job. Maximum length is 256 characters.
    definition : dict, default=None
        The copy job public definition. Refer to `Copy job definition <https://learn.microsoft.com/rest/api/fabric/articles/item-management/definitions/copyjob-definition>`_ for more details.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    create_item(
        name=name,
        description=description,
        type="CopyJob",
        definition=definition,
        workspace=workspace,
    )


@log
def delete_copy_job(name: str | UUID, workspace: Optional[str | UUID] = None):
    """
    Deletes the specified copy job.

    This is a wrapper function for the following API: `Items - Delete Copy Job <https://learn.microsoft.com/rest/api/fabric/copyjob/items/delete-copy-job>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    name : str | uuid.UUID
        The name or ID of the copy job.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    delete_item(item=name, type="CopyJob", workspace=workspace)


@log
def get_copy_job(
    name: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Returns properties of the specified copy job.

    This is a wrapper function for the following API: `Items - Get Copy Job <https://learn.microsoft.com/rest/api/fabric/copyjob/items/get-copy-job>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    name : str | uuid.UUID
        The name or ID of the copy job.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the properties of the specified copy job.
    """

    columns = {
        "Copy Job Name": "string",
        "Copy Job Id": "string",
        "Description": "string",
        "Workspace Id": "string",
        "Folder Id": "string",
    }
    df = _create_dataframe(columns=columns)

    workspace_id = resolve_workspace_id(workspace)
    item_id = resolve_item_id(item=name, type="CopyJob", workspace=workspace)

    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/copyJobs/{item_id}",
        client="fabric_sp",
    ).json()

    rows = [
        {
            "Copy Job Name": response.get("displayName"),
            "Copy Job Id": response.get("id"),
            "Description": response.get("description"),
            "Workspace Id": response.get("workspaceId"),
            "Folder Id": response.get("folderId"),
        }
    ]

    df = pd.DataFrame(rows, columns=columns.keys())

    return df


@log
def update_copy_job(
    name: str | UUID,
    new_name: Optional[str] = None,
    description: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Updates the properties of the specified copy job.

    This is a wrapper function for the following API: `Items - Update Copy Job <https://learn.microsoft.com/rest/api/fabric/copyjob/items/update-copy-job>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    name : str | uuid.UUID
        The name or ID of the copy job.
    new_name : str, default=None
        The new display name of the copy job.
    description : str, default=None
        The new description of the copy job. Maximum length is 256 characters.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (item_name, item_id) = resolve_item_name_and_id(
        item=name, type="CopyJob", workspace=workspace_id
    )

    payload = {}
    if new_name:
        payload["displayName"] = new_name
    if description:
        payload["description"] = description

    if not payload:
        raise ValueError(
            f"{icons.red_dot} At least one of 'new_name' or 'description' must be provided."
        )

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/copyJobs/{item_id}",
        method="patch",
        payload=payload,
        client="fabric_sp",
    )

    display_name = new_name if new_name else item_name
    print(
        f"{icons.green_dot} The '{item_name}' copy job within the '{workspace_name}' workspace has been updated."
    )


@log
def get_copy_job_definition(
    name: str | UUID, workspace: Optional[str | UUID] = None, decode: bool = True
) -> dict | pd.DataFrame:
    """
    Returns the specified copy job public definition.

    This is a wrapper function for the following API: `Items - Get Copy Job Definition <https://learn.microsoft.com/rest/api/fabric/copyjob/items/get-copy-job-definition>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    name : str | uuid.UUID
        The name or ID of the copy job.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    decode : bool, default=True
        If True, decodes the copy job definition file into .json format.
        If False, returns the copy job definition as a pandas DataFrame.

    Returns
    -------
    dict | pandas.DataFrame
        If decode is True, returns a dictionary with the decoded copy job definition.
        If decode is False, returns a pandas DataFrame with the raw definition parts.
    """

    workspace_id = resolve_workspace_id(workspace)
    item_id = resolve_item_id(item=name, type="CopyJob", workspace=workspace)

    result = _base_api(
        request=f"/v1/workspaces/{workspace_id}/copyJobs/{item_id}/getDefinition",
        method="post",
        lro_return_json=True,
        status_codes=None,
        client="fabric_sp",
    )

    df = pd.json_normalize(result["definition"]["parts"])

    if not decode:
        return df

    content = df[df["path"] == "copyjob-content.json"]
    payload = content["payload"].iloc[0]

    return _decode_b64(payload)


@log
def update_copy_job_definition(
    name: str | UUID,
    definition: dict,
    update_metadata: bool = False,
    workspace: Optional[str | UUID] = None,
):
    """
    Overrides the definition for the specified copy job.

    This is a wrapper function for the following API: `Items - Update Copy Job Definition <https://learn.microsoft.com/rest/api/fabric/copyjob/items/update-copy-job-definition>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    name : str | uuid.UUID
        The name or ID of the copy job.
    definition : dict
        The copy job public definition. Refer to `Copy job definition <https://learn.microsoft.com/rest/api/fabric/articles/item-management/definitions/copyjob-definition>`_ for more details.
    update_metadata : bool, default=False
        When set to True and the .platform file is provided as part of the definition,
        the item's metadata is updated using the metadata in the .platform file.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (item_name, item_id) = resolve_item_name_and_id(
        item=name, type="CopyJob", workspace=workspace_id
    )

    payload = {"definition": definition}

    request_url = f"/v1/workspaces/{workspace_id}/copyJobs/{item_id}/updateDefinition"
    if update_metadata:
        request_url += "?updateMetadata=True"

    _base_api(
        request=request_url,
        method="post",
        payload=payload,
        status_codes=[200, 202],
        lro_return_status_code=True,
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The definition for the '{item_name}' copy job within the '{workspace_name}' workspace has been updated."
    )
