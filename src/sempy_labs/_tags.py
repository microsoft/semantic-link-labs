from sempy_labs._helper_functions import (
    _base_api,
    _create_dataframe,
    _update_dataframe_datatypes,
    resolve_item_name_and_id,
    resolve_workspace_name_and_id,
    _is_valid_uuid,
)
import pandas as pd
from typing import Optional, List
from uuid import UUID
import sempy_labs._icons as icons
from sempy._utils._log import log


@log
def list_tags() -> pd.DataFrame:
    """
    Shows a list of all the tenant's tags.

    This is a wrapper function for the following API: `Tags - List Tags <https://learn.microsoft.com/rest/api/fabric/core/tags/list-tags>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of all the tenant's tags.
    """

    columns = {
        "Tag Name": "string",
        "Tag Id": "string",
    }
    df = _create_dataframe(columns=columns)

    responses = _base_api(
        request="/v1/tags",
        uses_pagination=True,
        client="fabric_sp",
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            rows.append(
                {
                    "Tag Name": v.get("displayName"),
                    "Tag Id": v.get("id"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
def resolve_tags(tags: str | List[str]) -> List[str]:
    """
    Resolves the tags to a list of strings.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    tags : str | List[str]
        The tags to resolve.

    Returns
    -------
    List[str]
        A list of resolved tags.
    """

    if isinstance(tags, str):
        tags = [tags]

    if all(_is_valid_uuid(tag) for tag in tags):
        return tags

    df = list_tags()

    tag_list = []
    for tag in tags:
        if _is_valid_uuid(tag):
            tag_list.append(tag)
        else:
            df_filt = df[df["Tag Name"] == tag]
            if df_filt.empty:
                raise ValueError(f"Tag '{tag}' not found in the tenant's tags.")
            tag_id = df_filt["Tag Id"].iloc[0]
            tag_list.append(tag_id)

    return tag_list


@log
def apply_tags(
    item: str | UUID,
    type: str,
    tags: str | UUID | List[str | UUID],
    workspace: Optional[str | UUID] = None,
):
    """
    Shows a list of all the tenant's tags.

    This is a wrapper function for the following API: `Tags - Apply Tags <https://learn.microsoft.com/rest/api/fabric/core/tags/apply-tags>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    item : str | uuid.UUID
        The name or ID of the item to apply tags to.
    type : str
        The type of the item to apply tags to. For example: "Lakehouse".
    tags : str | uuid.UUID | List[str | uuid.UUID]
        The name or ID of the tag(s) to apply to the item.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (item_name, item_id) = resolve_item_name_and_id(item, type, workspace_id)

    if isinstance(tags, str):
        tags = [tags]

    tag_list = resolve_tags(tags)

    payload = {
        "tags": tag_list,
    }

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/items/{item_id}/applyTags",
        client="fabric_sp",
        method="post",
        payload=payload,
    )

    print(
        f"{icons.green_dot} Tags {tags} applied to the '{item_name}' {type.lower()} within the '{workspace_name}' workspace"
    )


@log
def unapply_tags(
    item: str | UUID,
    type: str,
    tags: str | UUID | List[str | UUID],
    workspace: Optional[str | UUID] = None,
):
    """
    Shows a list of all the tenant's tags.

    This is a wrapper function for the following API: `Tags - Unapply Tags <https://learn.microsoft.com/rest/api/fabric/core/tags/unapply-tags>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    item : str | uuid.UUID
        The name or ID of the item to apply tags to.
    type : str
        The type of the item to apply tags to. For example: "Lakehouse".
    tags : str | uuid.UUID | List[str | uuid.UUID]
        The name or ID of the tag(s) to apply to the item.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (item_name, item_id) = resolve_item_name_and_id(item, type, workspace_id)

    if isinstance(tags, str):
        tags = [tags]

    tag_list = resolve_tags(tags)

    payload = {
        "tags": tag_list,
    }

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/items/{item_id}/unapplyTags",
        client="fabric_sp",
        method="post",
        payload=payload,
    )

    print(
        f"{icons.green_dot} Tags {tags} applied to the '{item_name}' {type.lower()} within the '{workspace_name}' workspace"
    )
