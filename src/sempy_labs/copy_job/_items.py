import json
import pandas as pd
from os import PathLike
from typing import Any, List, Optional
from uuid import UUID
import sempy.fabric as fabric
from sempy._utils._log import log
from sempy_labs._helper_functions import (
    _base_api,
    _build_url,
    _conv_b64,
    _create_dataframe,
    _is_valid_uuid,
    create_item,
    decode_payload,
    is_base64,
    resolve_item_id,
    resolve_item_name_and_id,
    resolve_workspace_id,
    resolve_workspace_name_and_id,
)
import sempy_labs._icons as icons

_COPY_JOB_COLUMNS = {
    "Copy Job Name": "string",
    "Copy Job Id": "string",
    "Description": "string",
    "Folder Id": "string",
    "Sensitivity Label Id": "string",
}


def _copy_job_row(v: dict) -> dict:
    return {
        "Copy Job Name": v.get("displayName"),
        "Copy Job Id": v.get("id"),
        "Description": v.get("description"),
        "Folder Id": v.get("folderId"),
        "Sensitivity Label Id": (v.get("sensitivityLabel") or {}).get("id"),
    }


def _encode_payload(payload: Any) -> str:
    if isinstance(payload, (dict, list)):
        return _conv_b64(payload)
    if isinstance(payload, bytes):
        return _conv_b64(payload.decode("utf-8"), json_dumps=False)
    if isinstance(payload, str):
        return payload if is_base64(payload) else _conv_b64(payload, json_dumps=False)

    raise ValueError(
        f"{icons.red_dot} Definition part payloads must be a dictionary, list, string or Base64-encoded string."
    )


def _encode_definition(definition: dict) -> dict:
    # Accepts either {"definition": {"parts": [...]}} or {"parts": [...]} with decoded or Base64 payloads.
    definition_object = definition.get("definition", definition)
    parts = definition_object.get("parts")

    if not parts:
        raise ValueError(
            f"{icons.red_dot} The 'definition' parameter must contain a non-empty list of definition parts."
        )

    encoded = {
        "parts": [
            {
                "path": part.get("path"),
                "payload": _encode_payload(part.get("payload")),
                "payloadType": part.get("payloadType") or "InlineBase64",
            }
            for part in parts
        ]
    }
    if definition_object.get("format"):
        encoded["format"] = definition_object.get("format")

    return encoded


@log
def list_copy_jobs(
    workspace: Optional[str | UUID] = None,
    recursive: bool = True,
    root_folder: Optional[str | PathLike | UUID] = None,
) -> pd.DataFrame:
    """
    Shows the copy jobs within a workspace.

    This is a wrapper function for the following API: `Items - List Copy Jobs <https://learn.microsoft.com/rest/api/fabric/copyjob/items/list-copy-jobs>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    recursive : bool, default=True
        If True, lists the copy jobs in the root folder and all of its nested folders.
        If False, lists only the copy jobs directly within the root folder.
    root_folder : str | os.PathLike | uuid.UUID, default=None
        The folder path or ID used as the root folder for listing copy jobs.
        Defaults to None which uses the workspace as the root folder.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the copy jobs within a workspace.
    """

    df = _create_dataframe(columns=_COPY_JOB_COLUMNS)

    workspace_id = resolve_workspace_id(workspace)

    params = {"recursive": str(recursive).lower()}
    if root_folder is not None:
        params["rootFolderId"] = (
            str(root_folder)
            if _is_valid_uuid(str(root_folder))
            else fabric.resolve_folder_id(folder=root_folder, workspace=workspace_id)
        )

    responses = _base_api(
        request=_build_url(f"/v1/workspaces/{workspace_id}/copyJobs", params),
        uses_pagination=True,
        client="fabric_sp",
    )

    rows = [_copy_job_row(v) for r in responses for v in r.get("value", [])]

    if rows:
        df = pd.DataFrame(rows, columns=list(_COPY_JOB_COLUMNS.keys()))

    return df


@log
def get_copy_job(
    copy_job: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Returns properties of the specified copy job.

    This is a wrapper function for the following API: `Items - Get Copy Job <https://learn.microsoft.com/rest/api/fabric/copyjob/items/get-copy-job>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    copy_job : str | uuid.UUID
        The copy job name or ID.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the properties of the copy job.
    """

    workspace_id = resolve_workspace_id(workspace)
    item_id = resolve_item_id(item=copy_job, type="CopyJob", workspace=workspace_id)

    result = _base_api(
        request=f"/v1/workspaces/{workspace_id}/copyJobs/{item_id}",
        client="fabric_sp",
    ).json()

    return pd.DataFrame([_copy_job_row(result)], columns=list(_COPY_JOB_COLUMNS.keys()))


@log
def create_copy_job(
    name: str,
    definition: Optional[dict] = None,
    description: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
    folder: Optional[str | PathLike] = None,
):
    """
    Creates a copy job.

    This is a wrapper function for the following API: `Items - Create Copy Job <https://learn.microsoft.com/rest/api/fabric/copyjob/items/create-copy-job>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    name : str
        The name of the copy job.
    definition : dict, default=None
        The `copy job definition <https://learn.microsoft.com/rest/api/fabric/articles/item-management/definitions/copyjob-definition>`_,
        in the format returned by :func:`sempy_labs.copy_job.get_copy_job_definition`. The dictionary may either contain a
        'definition' key or be the definition object itself (i.e. containing the 'parts' key). The payload of each
        part may be provided either as decoded content (a dictionary or a string) or as a Base64-encoded string.
        Decoded payloads are encoded to Base64 automatically.
        Defaults to None which creates an empty copy job.

        Example:

        definition = {
            "parts": [
                {
                    "path": "copyjob-content.json",
                    "payload": {"properties": {"jobMode": "Batch"}, "activities": []},
                }
            ]
        }
    description : str, default=None
        The description of the copy job. Maximum length is 256 characters.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    folder : str | os.PathLike, default=None
        The folder within the workspace where the copy job will be created.
        Defaults to None which places the copy job in the root of the workspace.
    """

    create_item(
        name=name,
        type="CopyJob",
        description=description,
        definition=_encode_definition(definition) if definition else None,
        workspace=workspace,
        folder=folder,
    )


@log
def update_copy_job(
    copy_job: str | UUID,
    name: Optional[str] = None,
    description: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Updates the properties of the specified copy job.

    This is a wrapper function for the following API: `Items - Update Copy Job <https://learn.microsoft.com/rest/api/fabric/copyjob/items/update-copy-job>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    copy_job : str | uuid.UUID
        The copy job name or ID.
    name : str, default=None
        The new name of the copy job.
    description : str, default=None
        The new description of the copy job. Maximum length is 256 characters.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    payload = {}
    if name is not None:
        payload["displayName"] = name
    if description is not None:
        payload["description"] = description

    if not payload:
        raise ValueError(
            f"{icons.red_dot} At least one of the 'name' or 'description' parameters must be specified."
        )

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    item_name, item_id = resolve_item_name_and_id(
        item=copy_job, type="CopyJob", workspace=workspace_id
    )

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/copyJobs/{item_id}",
        method="patch",
        payload=payload,
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The '{item_name}' copy job within the '{workspace_name}' workspace has been updated."
    )


@log
def delete_copy_job(
    copy_job: str | UUID,
    workspace: Optional[str | UUID] = None,
    hard_delete: bool = False,
):
    """
    Deletes the specified copy job.

    This is a wrapper function for the following API: `Items - Delete Copy Job <https://learn.microsoft.com/rest/api/fabric/copyjob/items/delete-copy-job>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    copy_job : str | uuid.UUID
        The copy job name or ID.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    hard_delete : bool, default=False
        If True, the copy job is permanently deleted and cannot be recovered.
        If False, the copy job is soft-deleted (if soft deletion is supported).
    """

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    item_name, item_id = resolve_item_name_and_id(
        item=copy_job, type="CopyJob", workspace=workspace_id
    )

    url = f"/v1/workspaces/{workspace_id}/copyJobs/{item_id}"
    if hard_delete:
        url += "?hardDelete=true"

    _base_api(request=url, method="delete", client="fabric_sp")

    print(
        f"{icons.green_dot} The '{item_name}' copy job has been successfully deleted from the '{workspace_name}' workspace."
    )


@log
def get_copy_job_definition(
    copy_job: str | UUID,
    workspace: Optional[str | UUID] = None,
    decode: bool = True,
    return_dataframe: bool = False,
    format: Optional[str] = None,
) -> dict | pd.DataFrame:
    """
    Gets the definition of the specified copy job.

    This is a wrapper function for the following API: `Items - Get Copy Job Definition <https://learn.microsoft.com/rest/api/fabric/copyjob/items/get-copy-job-definition>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    copy_job : str | uuid.UUID
        The copy job name or ID.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    decode : bool, default=True
        If True, decodes the Base64 payload of each definition part.
    return_dataframe : bool, default=False
        If True, returns a pandas dataframe. If False, returns a dictionary.
    format : str, default=None
        The format of the copy job public definition.

    Returns
    -------
    dict | pandas.DataFrame
        A dictionary or a pandas dataframe showing the copy job definition.
    """

    workspace_id = resolve_workspace_id(workspace)
    item_id = resolve_item_id(item=copy_job, type="CopyJob", workspace=workspace_id)

    url = f"/v1/workspaces/{workspace_id}/copyJobs/{item_id}/getDefinition"
    if format:
        url = _build_url(url, {"format": format})

    result = _base_api(
        request=url,
        method="post",
        status_codes=None,
        lro_return_json=True,
        client="fabric_sp",
    )

    definition = {"definition": {"parts": []}}
    for part in result.get("definition", {}).get("parts", []):
        payload = part.get("payload")
        definition["definition"]["parts"].append(
            {
                "path": part.get("path"),
                "payload": decode_payload(payload) if decode else payload,
                "payloadType": part.get("payloadType"),
            }
        )

    if return_dataframe:
        df = pd.DataFrame(
            definition["definition"]["parts"],
            columns=["path", "payload", "payloadType"],
        )
        df.columns = ["Path", "Payload", "Payload Type"]
        if decode:
            df["Payload"] = df["Payload"].apply(
                lambda p: json.dumps(p) if isinstance(p, (dict, list)) else p
            )
        return df

    return definition


@log
def update_copy_job_definition(
    copy_job: str | UUID,
    definition: dict,
    update_metadata: bool = False,
    workspace: Optional[str | UUID] = None,
):
    """
    Overrides the definition of the specified copy job.

    This is a wrapper function for the following API: `Items - Update Copy Job Definition <https://learn.microsoft.com/rest/api/fabric/copyjob/items/update-copy-job-definition>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    copy_job : str | uuid.UUID
        The copy job name or ID.
    definition : dict
        The `copy job definition <https://learn.microsoft.com/rest/api/fabric/articles/item-management/definitions/copyjob-definition>`_,
        in the format returned by :func:`sempy_labs.copy_job.get_copy_job_definition`. The dictionary may either contain a
        'definition' key or be the definition object itself (i.e. containing the 'parts' key). The payload of each
        part may be provided either as decoded content (a dictionary or a string) or as a Base64-encoded string.
        Decoded payloads are encoded to Base64 automatically.

        Example:

        definition = {
            "definition": {
                "parts": [
                    {
                        "path": "copyjob-content.json",
                        "payload": {"properties": {"jobMode": "CDC"}, "activities": []},
                        "payloadType": "InlineBase64",
                    }
                ]
            }
        }
    update_metadata : bool, default=False
        If True and the '.platform' file is provided as part of the definition, the item's metadata is updated
        using the metadata in the '.platform' file.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    payload = {"definition": _encode_definition(definition)}

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    item_name, item_id = resolve_item_name_and_id(
        item=copy_job, type="CopyJob", workspace=workspace_id
    )

    url = f"/v1/workspaces/{workspace_id}/copyJobs/{item_id}/updateDefinition"
    if update_metadata:
        url += "?updateMetadata=True"

    _base_api(
        request=url,
        method="post",
        payload=payload,
        status_codes=[200, 202],
        lro_return_status_code=True,
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The definition of the '{item_name}' copy job within the '{workspace_name}' workspace has been updated."
    )


@log
def reset_copy_job(
    copy_job: str | UUID,
    copy_job_entities: Optional[str | UUID | List[str | UUID]] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Resets the specified copy job. If no copy job entities are specified, all entities are reset.

    This is a wrapper function for the following API: `Items - Reset Copy Job <https://learn.microsoft.com/rest/api/fabric/copyjob/items/reset-copy-job>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    copy_job : str | uuid.UUID
        The copy job name or ID.
    copy_job_entities : str | uuid.UUID | List[str | uuid.UUID], default=None
        The ID(s) of the copy job entities to reset.
        Defaults to None which resets all copy job entities.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    if isinstance(copy_job_entities, (str, UUID)):
        copy_job_entities = [copy_job_entities]

    if copy_job_entities:
        payload = {
            "resetAllCopyJobEntities": False,
            "copyJobEntitiesToReset": [
                {"copyJobEntityId": str(e)} for e in copy_job_entities
            ],
        }
    else:
        payload = {"resetAllCopyJobEntities": True}

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    item_name, item_id = resolve_item_name_and_id(
        item=copy_job, type="CopyJob", workspace=workspace_id
    )

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/copyJobs/{item_id}/resetCopyJob",
        method="post",
        payload=payload,
        client="fabric_sp",
    )

    scope = "The specified" if copy_job_entities else "All"
    print(
        f"{icons.green_dot} {scope} entities of the '{item_name}' copy job within the '{workspace_name}' workspace have been reset."
    )
