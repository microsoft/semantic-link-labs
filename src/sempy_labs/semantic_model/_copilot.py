from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_item_name_and_id,
    get_model_id,
    _base_api,
)
from typing import Optional, Literal
from uuid import UUID
from sempy._utils._log import log
import sempy_labs._icons as icons


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

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (item_name, item_id) = resolve_item_name_and_id(
        item=dataset, type="SemanticModel", workspace=workspace_id
    )
    payload = {"preppedForCopilot": approved_for_copilot, "isReadOnly": False}

    _base_api(
        request=f"metadata/workspace/{workspace_id}/model/{item_id}/preppedCopilot",
        client="internal",
        method="put",
        payload=payload,
    )

    print(
        f"{icons.green_dot} The '{item_name}' semantic model within the '{workspace_name}' workspace has been set to to approved_for_copilot={approved_for_copilot}."
    )


@log
def set_endorsement(
    dataset: str | UUID,
    endorsement: Literal["None", "Promoted", "Certified", "Master data"],
    workspace: Optional[str | UUID] = None,
):
    """
    Sets the endorsement status for a semantic model.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    endorsement : typing.Literal["None", "Promoted", "Certified", "Master data"]
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
    id = get_model_id(item_id=item_id)
    endorsement = endorsement.strip().lower()

    endorsement_mapping = {
        "none": 0,
        "promoted": 1,
        "certified": 2,
        "master data": 3,
    }

    if endorsement not in endorsement_mapping:
        raise ValueError(
            "Endorsement must be either 'None', 'Promoted', 'Certified', or 'Master data'."
        )

    stage = endorsement_mapping.get(endorsement)
    payload = {"stage": stage}

    _base_api(
        request=f"metadata/gallery/models/{id}",
        client="internal",
        method="put",
        payload=payload,
    )

    print(
        f"{icons.green_dot} The endorsement for the '{item_name}' semantic model within the '{workspace_name}' workspace has been set to '{endorsement}'."
    )


@log
def make_discoverable(
    dataset: str | UUID, make_discoverable: bool, workspace: Optional[str | UUID] = None
):
    """
    Allow users without access to this semantic model to discover it and request permissions to access the data. The semantic model must be endorsed as 'Promoted', 'Certified', or 'Master data' to be made discoverable.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    make_discoverable : bool
        Whether to make the semantic model discoverable.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (item_name, item_id) = resolve_item_name_and_id(
        item=dataset, type="SemanticModel", workspace=workspace_id
    )
    id = get_model_id(item_id=item_id)

    if make_discoverable:
        _base_api(
            request=f"metadata/gallery/discover/models/{id}",
            client="internal",
            method="post",
        )
        print(
            f"{icons.green_dot} The '{item_name}' semantic model within the '{workspace_name}' workspace is now discoverable in the gallery."
        )
    else:
        _base_api(
            request=f"metadata/gallery/discover/models/{id}",
            client="internal",
            method="delete",
        )
        print(
            f"{icons.green_dot} The '{item_name}' semantic model within the '{workspace_name}' workspace is no longer discoverable in the gallery."
        )
