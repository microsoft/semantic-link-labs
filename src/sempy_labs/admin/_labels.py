from typing import Literal, List
from uuid import UUID
import sempy_labs._icons as icons
from sempy_labs.admin._basic_functions import (
    _resolve_workspace_name_and_id,
)
from sempy_labs.admin._items import (
    list_items,
)
from sempy_labs._helper_functions import (
    _is_valid_uuid,
    _base_api,
)
from sempy._utils._log import log


@log
def bulk_set_labels(
    items: List[dict],
    label_id: UUID,
    assignment_method: Literal["Standard", "Priviledged"] = "Standard",
):
    """
    Sets sensitivity labels on Fabric items.

    Note: Please use the sempy_labs.graph.resolve_sensitivity_label_id function to retrieve label IDs.

    This is a wrapper function for the following API: `Labels - Bulk Set Labels <https://learn.microsoft.com/rest/api/fabric/admin/labels/bulk-set-labels>`_.

    Parameters
    ----------
    items : List[dict]
        A list of dictionaries containing the item details.

        Example 1:
        items = [
            {
            "id": "fe472f5e-636e-4c10-a1c6-7e9edc0b542a",
            "type": "Dashboard"
            },
            {
            "id": "fe472f5e-636e-4c10-a1c6-7e9edc0b542c",
            "type": "Report"
            },
            {
            "id": "fe472f5e-636e-4c10-a1c6-7e9edc0b542e",
            "type": "SemanticModel"
            },
        ]

        Example 2:
        items = [
            {
            "id": "Dashboard 1",
            "type": "Dashboard",
            "workspace": "Sales Workspace"
            },
            {
            "id": "Sales Report",
            "type": "Report",
            "workspace": "Sales Workspace"
            },
            {
            "id": "KPI Model",
            "type": "SemanticModel",
            "workspace": "Workspace 2"
            },
        ]

    label_id : uuid.UUID
        The label ID, which must be in the user's label policy.
    assignment_method : Literal["Standard", "Priviledged"], default="Standard"
        Specifies whether the assigned label was set by an automated process or manually. Additional tenant setting property types may be added over time.
    """

    if assignment_method not in ["Standard", "Priviledged"]:
        raise ValueError("assignment_method must be either 'Standard' or 'Priviledged'")

    payload = {"items": []}
    df = list_items()

    for i in items:
        item = i.get("item")
        type = i.get("type")
        workspace = i.get("workspace")
        if _is_valid_uuid(item):
            payload["items"].append(
                {
                    "id": item,
                    "type": type,
                }
            )
        else:
            workspace_id = _resolve_workspace_name_and_id(workspace)[1]
            df_filtered = df[
                (df["Item Name"] == item)
                & (df["Type"] == type)
                & (df["Workspace Id"] == workspace_id)
            ]
            if df_filtered.empty:
                raise ValueError(
                    f"The item '{item}' of type '{type}' does not exist in workspace '{workspace}'."
                )
            else:
                payload["items"].append(
                    {
                        "id": df_filtered["Item Id"].iloc[0],
                        "type": type,
                    }
                )

    payload["labelId"] = label_id
    payload["assignmentMethod"] = assignment_method

    _base_api(request="/v1/admin/items/bulkSetLabels", method="post", payload=payload)

    print(
        f"{icons.green_dot} Labels have been successfully set on the specified items."
    )


@log
def bulk_remove_labels(
    items: List[dict],
):
    """
    Removes sensitivity labels from Fabric items.

    This is a wrapper function for the following API: `Labels - Bulk Remove Labels <https://learn.microsoft.com/rest/api/fabric/admin/labels/bulk-remove-labels>`_.

    Parameters
    ----------
    items : List[dict]
        A list of dictionaries containing the item details.

        Example 1:
        items = [
            {
            "id": "fe472f5e-636e-4c10-a1c6-7e9edc0b542a",
            "type": "Dashboard"
            },
            {
            "id": "fe472f5e-636e-4c10-a1c6-7e9edc0b542c",
            "type": "Report"
            },
            {
            "id": "fe472f5e-636e-4c10-a1c6-7e9edc0b542e",
            "type": "SemanticModel"
            },
        ]

        Example 2:
        items = [
            {
            "id": "Dashboard 1",
            "type": "Dashboard",
            "workspace": "Sales Workspace"
            },
            {
            "id": "Sales Report",
            "type": "Report",
            "workspace": "Sales Workspace"
            },
            {
            "id": "KPI Model",
            "type": "SemanticModel",
            "workspace": "Workspace 2"
            },
        ]
    """

    payload = {"items": []}
    df = list_items()

    for i in items:
        item = i.get("item")
        type = i.get("type")
        workspace = i.get("workspace")
        if _is_valid_uuid(item):
            payload["items"].append(
                {
                    "id": item,
                    "type": type,
                }
            )
        else:
            workspace_id = _resolve_workspace_name_and_id(workspace)[1]
            df_filtered = df[
                (df["Item Name"] == item)
                & (df["Type"] == type)
                & (df["Workspace Id"] == workspace_id)
            ]
            if df_filtered.empty:
                raise ValueError(
                    f"The item '{item}' of type '{type}' does not exist in workspace '{workspace}'."
                )
            else:
                payload["items"].append(
                    {
                        "id": df_filtered["Item Id"].iloc[0],
                        "type": type,
                    }
                )

    _base_api(
        request="/v1/admin/items/bulkRemoveLabels", method="post", payload=payload
    )

    print(
        f"{icons.green_dot} Labels have been successfully removed from the specified items."
    )
