from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_item_name_and_id,
    _get_url_prefix,
    get_pbi_token_headers,
    get_model_id,
)
from typing import Optional, Literal
from uuid import UUID
from sempy._utils._log import log
import sempy_labs._icons as icons
import requests
from sempy.fabric.exceptions import FabricHTTPException


@log
def approved_for_copilot(
    dataset: str | UUID,
    approved_for_copilot: bool,
    workspace: Optional[str | UUID] = None,
):
    """
    Turn on this setting to approve your model for Copilot use. This improves visibility in search results and removes the warning in standalone Copilot in Power BI that your organization hasn't approved the data.

    Parameters
    ----------
    dataset : str | uuid.UUID
        The name or UUID of the semantic model.
    approved_for_copilot : bool
        Set to True to approve the model for Copilot use, or False to disapprove it.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    headers = get_pbi_token_headers()
    url_prefix = _get_url_prefix()
    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (item_name, item_id) = resolve_item_name_and_id(
        item=dataset, type="SemanticModel", workspace=workspace_id
    )
    payload = {"preppedForCopilot": approved_for_copilot, "isReadOnly": False}

    response = requests.put(
        url=f"{url_prefix}/metadata/workspace/{workspace_id}/model/{item_id}/preppedCopilot",
        json=payload,
        headers=headers,
    )

    if response.status_code != 200:
        raise FabricHTTPException(
            f"{icons.red_dot} Failed to set approved_for_copilot: {response.text}"
        )

    print(
        f"{icons.green_dot} The '{item_name}' semantic model within the '{workspace_name}' workspace has been set to to approved_for_copilot={approved_for_copilot}."
    )


@log
def set_endorsement(
    dataset: str | UUID,
    endorsement: Literal["None", "Promoted"],
    workspace: Optional[str | UUID] = None,
):
    """
    Sets the endorsement status for a semantic model.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    endorsement : Literal["None", "Promoted"]
        The endorsement status to set for the semantic model.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (item_name, item_id) = resolve_item_name_and_id(
        item=dataset, type="SemanticModel", workspace=workspace_id
    )
    headers = get_pbi_token_headers()
    prefix = _get_url_prefix()
    id = get_model_id(item_id=item_id, headers=headers, prefix=prefix)
    endorsement = endorsement.strip().lower()

    endorsement_mapping = {
        "none": 0,
        "promoted": 1,
    }

    if endorsement not in endorsement_mapping:
        raise ValueError("Endorsement must be either 'None' or 'Promoted'.")

    stage = endorsement_mapping.get(endorsement)
    payload = {"stage": stage}

    response = requests.put(
        url=f"{prefix}/metadata/gallery/models/{id}",
        headers=headers,
        json=payload,
    )

    if response.status_code != 200:
        raise FabricHTTPException(f"Failed to set endorsement: {response.text}")

    print(
        f"{icons.green_dot} The endorsement for the '{item_name}' semantic model within the '{workspace_name}' workspace has been set to '{endorsement}'."
    )
