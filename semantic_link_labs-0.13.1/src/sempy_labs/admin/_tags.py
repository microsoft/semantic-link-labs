from .._helper_functions import (
    _base_api,
    _is_valid_uuid,
)
from uuid import UUID
from sempy_labs._tags import list_tags
import sempy_labs._icons as icons
from typing import List
from sempy._utils._log import log


@log
def resolve_tag_id(tag: str | UUID):

    if _is_valid_uuid(tag):
        tag_id = tag
    else:
        df = list_tags()
        df[df["Tag Name"] == tag]
        if df.empty:
            raise ValueError(f"{icons.red_dot} The '{tag}' tag does not exist.")
        tag_id = df.iloc[0]["Tag Id"]

    return tag_id


@log
def create_tags(tags: str | List[str]):
    """
    Creates a new tag or tags.

    This is a wrapper function for the following API: `Tags - Bulk Create Tags <https://learn.microsoft.com/rest/api/fabric/admin/tags/bulk-create-tags>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    tags : str | List[str]
        The name of the tag or tags to create.
    """

    if isinstance(tags, str):
        tags = [tags]

    # Check the length of the tags
    for tag in tags:
        if len(tag) > 40:
            raise ValueError(
                f"{icons.red_dot} The '{tag}' tag name is too long. It must be 40 characters or less."
            )

    # Check if the tags already exist
    df = list_tags()
    existing_names = df["Tag Name"].tolist()
    existing_ids = df["Tag Id"].tolist()

    available_tags = [
        tag for tag in tags if tag not in existing_names and tag not in existing_ids
    ]
    unavailable_tags = [
        tag for tag in tags if tag in existing_names or tag in existing_ids
    ]

    print(f"{icons.warning} The following tags already exist: {unavailable_tags}")
    if not available_tags:
        print(f"{icons.info} No new tags to create.")
        return

    payload = [{"displayName": name} for name in available_tags]

    for tag in tags:
        _base_api(
            request="/v1/admin/bulkCreateTags",
            client="fabric_sp",
            method="post",
            payload=payload,
            status_codes=201,
        )

    print(f"{icons.green_dot} The '{available_tags}' tag(s) have been created.")


@log
def delete_tag(tag: str | UUID):
    """
    Deletes a tag.

    This is a wrapper function for the following API: `Tags - Delete Tag <https://learn.microsoft.com/rest/api/fabric/admin/tags/delete-tag>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    tag : str | uuid.UUID
        The name or ID of the tag to delete.
    """

    tag_id = resolve_tag_id(tag)

    _base_api(request=f"/v1/admin/tags/{tag_id}", client="fabric_sp", method="delete")

    print(f"{icons.green_dot} The '{tag}' tag has been deleted.")


@log
def update_tag(name: str, tag: str | UUID):
    """
    Updates the name of a tag.

    This is a wrapper function for the following API: `Tags - Update Tag <https://learn.microsoft.com/rest/api/fabric/admin/tags/update-tag>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    name : str
        The new name of the tag.
    tag : str | uuid.UUID
        The name or ID of the tag to update.
    """

    tag_id = resolve_tag_id(tag)

    _base_api(
        request=f"/v1/admin/tags/{tag_id}",
        client="fabric_sp",
        method="patch",
        payload={"displayName": name},
    )

    print(f"{icons.green_dot} The '{tag}' tag has been renamed to '{name}'.")
