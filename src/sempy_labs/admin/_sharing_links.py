from sempy._utils._log import log
from sempy_labs._helper_functions import (
    _base_api,
    _is_valid_uuid,
)
import sempy_labs._icons as icons
from typing import List


@log
def remove_all_sharing_links(sharing_link_type: str = "OrgLink"):
    """
    Deletes all organization sharing links for all Fabric items in the tenant. This action cannot be undone.

    This is a wrapper function for the following API: `Sharing Links - Remove All Sharing Links <https://learn.microsoft.com/rest/api/fabric/admin/sharing-links/remove-all-sharing-links>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    sharing_link_type : str, default='OrgLink'
        Specifies the type of sharing link that is required to be deleted. Additional sharing link types may be added over time.
    """

    payload = {"sharingLinkType": sharing_link_type}

    _base_api(
        request="/v1/admin/items/removeAllSharingLinks",
        client="fabric_sp",
        lro_return_status_code=True,
        status_codes=[200, 202],
        method="post",
        payload=payload,
    )

    print(
        f"{icons.green_dot} All organization sharing links for all Fbric items in the tenant have been deleted."
    )


@log
def remove_sharing_links(items: List[dict], sharing_link_type: str = "OrgLink"):
    """
    Deletes all organization sharing links for the specified Fabric items. This action cannot be undone.

    This is a wrapper function for the following API: `Sharing Links - Bulk Remove Sharing Links <https://learn.microsoft.com/rest/api/fabric/admin/sharing-links/bulk-remove-sharing-links>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    items : List[dict]
        A list of dictionaries, each representing an item. The 'item' and 'workspace' accepts either name or ID. Examples:

        [
            {
                "item": "MyReport",
                "type": "Report",
                "workspace": "Workpsace 1",
            },
            {
                "item": "MyReport2",
                "type": "Report",
                "workspace": "Workspace 2",
            },
        ]

        [
            {
                "item": "fe472f5e-636e-4c10-a1c6-7e9edc0b542a",
                "type": "Report",
                "workspace": "Workspace 1",
            },
            {
                "item": "fe472f5e-636e-4c10-a1c6-7e9edc0b542c",
                "type": "Notebook",
                "workspace": "476fcafe-b514-495d-b13f-ca9a4f0b1d8f",
            },
        ]

    sharing_link_type : str, default='OrgLink'
        Specifies the type of sharing link that is required to be deleted. Additional sharing link types may be added over time.
    """

    from sempy_labs.admin._items import _resolve_item_id

    payload = {"items": [], "sharingLinkType": sharing_link_type}

    for i in items:
        item = i.get("item")
        type = i.get("type")
        workspace = i.get("workspace")
        if _is_valid_uuid(item):
            payload["items"].append({"id": item, "type": type})
        else:
            item_id = _resolve_item_id(item=item, type=type, workspace=workspace)
            payload["items"].append({"id": item_id, "type": type})

    _base_api(
        request="/v1/admin/items/bulkRemoveSharingLinks",
        client="fabric_sp",
        method="post",
        payload=payload,
        lro_return_status_code=True,
        status_codes=[200, 202],
    )

    print(
        f"{icons.green_dot} Organizational sharing links for all specified items have been deleted."
    )
