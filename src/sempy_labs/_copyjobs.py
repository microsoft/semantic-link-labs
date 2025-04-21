import pandas as pd
from typing import Optional
from uuid import UUID
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    _base_api,
    _create_dataframe,
    _update_dataframe_datatypes,
    delete_item,
    get_item_definition,
    _conv_b64,
)


def list_copy_jobs(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Shows a list of CopyJobs from the specified workspace.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of CopyJobs from the specified workspace.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    columns = {
        "Copy Job Name": "string",
        "Copy Job Id": "string",
        "Description": "string",
    }

    df = _create_dataframe(columns=columns)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/copyJobs",
        client="fabric_sp",
        uses_pagination=True,
    )

    for r in responses:
        for v in r.get("value", []):
            new_data = {
                "Copy Job Name": v.get("displayName"),
                "Copy Job Id": v.get("id"),
                "Description": v.get("description"),
            }

            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


def delete_copy_job(copy_job: str | UUID, workspace: Optional[str | UUID] = None):

    delete_item(item=copy_job, type="CopyJob", workspace=workspace)


def get_copy_job_definition(
    copy_job: str | UUID,
    workspace: Optional[str | UUID] = None,
    return_dataframe: bool = False,
) -> pd.DataFrame | dict:

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    if return_dataframe:
        return get_item_definition(
            item=copy_job, type="CopyJob", workspace=workspace, return_dataframe=True
        )
    else:
        return get_item_definition(
            item=copy_job, type="CopyJob", workspace=workspace, return_dataframe=False
        )


def create_copy_job(
    name: str,
    definition: Optional[dict] = None,
    description: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
) -> dict:

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    _conv_b64(file=definition)

    payload = {
        "displayName": name,
    }
    if description:
        payload["description"] = description
    if definition:
        payload["definition"] = {
            "parts": [
                {
                    "path": "copyjob-content.json",
                    "payload": _conv_b64(file=definition),
                    "payloadType": "InlineBase64",
                }
            ]
        }

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/copyJobs",
        client="fabric_sp",
        method="post",
        payload=payload,
        status_codes=[201, 202],
    )
