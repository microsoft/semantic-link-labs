from typing import Optional, List
from uuid import UUID
import sempy.fabric as fabric
from sempy_labs._helper_functions import (
    resolve_workspace_id,
    _base_api,
)
from sempy._utils._log import log
import sempy_labs._icons as icons


@log
def bulk_export_items(
    item_id: Optional[UUID | List[UUID]] = None,
    item_type: Optional[str | List[str]] = None,
    workspace: Optional[str | UUID] = None,
) -> dict:
    """
    Bulk export item definitions from the workspace.

    This is a wrapper function for the following API: `Items - Bulk Export Item Definitions (beta) <https://learn.microsoft.com/rest/api/fabric/core/items/bulk-export-item-definitions(beta)>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    item_id : uuid.UUID | typing.List[uuid.UUID], default=None
        The item ID or list of item IDs to export. If not provided, all items will be exported.
    item_type : str | typing.List[str], default=None
        The item type or list of item types to export. If not provided, all item types will be exported.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    dict
        A dictionary containing the bulk export item definitions.
    """

    workspace_id = resolve_workspace_id(workspace)

    if item_id is None and item_type is None:
        payload = {"mode": "All"}
    else:

        df = fabric.list_items(workspace=workspace_id)
        if item_id is not None:
            if isinstance(item_id, str):
                item_id = [item_id]

            df = df[df["Id"].isin(item_id)]

        if item_type is not None:
            if isinstance(item_type, str):
                item_type = [item_type]

            df = df[df["Type"].isin(item_type)]

        ids = df["Id"].unique().tolist()

        id_payload = [{"id": i} for i in ids]

        payload = {
            "items": id_payload,
            "mode": "Selective",
        }

    result = _base_api(
        request=f"/v1/workspaces/{workspace_id}/items/bulkExportDefinitions?beta=True",
        payload=payload,
        status_codes=[200, 202],
        lro_return_json=True,
        method="post",
    )
    return result


@log
def bulk_import_items(
    definition_parts: dict[List],
    workspace: Optional[str | UUID] = None,
    allow_pairing_by_name: bool = False,
) -> dict:
    """
    Bulk import item definitions into the workspace.

    This is a wrapper function for the following API: `Items - Bulk Import Item Definitions (beta) <https://learn.microsoft.com/rest/api/fabric/core/items/bulk-import-item-definitions(beta)>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    definition_parts : dict[List], required
        A dictionary containing the item definitions to import.

        Example:

        {
            "definitionParts": [
                {
                "path": "/Folder1/Folder2/MyReport.Report/.platform",
                "payload": "ewogICIkc2NoZW1hIjogImh0dHBzOi8vZGV2ZWxvcGVyLm1pY3Jvc29mdC5jb20vanNvbi1zY2hlbWFzL2ZhYnJpYy9naXRJbnRlZ3JhdGlvbi9wbGF0Zm9ybVByb3BlcnRpZXMvMi4wLjAvc2NoZW1hLmpzb24iLAogICJtZXRhZGF0YSI6IHsKICAgICJ0eXBlIjogIlJlcG9ydCIsCiAgICAiZGlzcGxheU5hbWUiOiAiTXlSZXBvcnQiCiAgfSwKICAiY29uZmlnIjogewogICAgInZlcnNpb24iOiAiMi4wIiwKICAgICJsb2dpY2FsSWQiOiAiODg0MzZlNjUtNmVkMS04MTg1LTQ5ZmYtZjYxMDc3ZmM3M2Q0IgogIH0KfQ==",
                "payloadType": "InlineBase64"
                },
                {
                "path": "/Folder1/Folder2/MyReport.Report/definition.pbir",
                "payload": "ewogICIkc2NoZW1hIjogImh0dHBzOi8vZGV2ZWxvcGVyLm1pY3Jvc29mdC5jb20vanNvbi1zY2hlbWFzL2ZhYnJpYy9pdGVtL3JlcG9ydC9kZWZpbml0aW9uUHJvcGVydGllcy8yLjAuMC9zY2hlbWEuanNvbiIsCiAgInZlcnNpb24iOiAiNC4wIiwKICAiZGF0YXNldFJlZmVyZW5jZSI6IHsKICAgICJieVBhdGgiOiB7CiAgICAgICJwYXRoIjogIi4uLy4uL015RGF0YXNldC5TZW1hbnRpY01vZGVsIgogICAgfQogIH0KfQ==",
                "payloadType": "InlineBase64"
                }
            ]
        }

    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    dict
        A dictionary containing the bulk export item definitions.
    """

    workspace = resolve_workspace_id(workspace)

    if "definitionParts" in definition_parts:
        definition_parts = definition_parts["definitionParts"]
    elif "DefinitionParts" in definition_parts:
        definition_parts = definition_parts["DefinitionParts"]

    if not isinstance(definition_parts, list):
        raise ValueError(
            f"{icons.red_dot} definition_parts must be a dictionary containing a list of item definitions."
        )

    payload = {
        "definitionParts": definition_parts,
        "options": {
            "allowPairingByName": allow_pairing_by_name,
        },
    }

    result = _base_api(
        request=f"/v1/workspaces/{workspace}/items/bulkImportDefinitions?beta=True",
        payload=payload,
        status_codes=[200, 202],
        lro_return_json=True,
        method="post",
    )
    return result
