from typing import Optional, Literal
from uuid import UUID
from sempy_labs._helper_functions import (
    _base_api,
    resolve_workspace_name_and_id,
    resolve_item_name_and_id,
)
import sempy_labs._icons as icons
from sempy._utils._log import log


@log
def takeover_item_ownership(
    item: str | UUID,
    type: Literal["Report", "SemanticModel"],
    workspace: Optional[str | UUID] = None,
):
    """
    Takes over ownership of a Report or Semantic Model.

    Parameters
    ----------
    item : str | uuid.UUID
        The name or UUID of the Report or Semantic Model to take over.
    type : typing.Literal["Report", "SemanticModel"]
        The type of item to take over. Must be either 'Report' or 'SemanticModel'.
    workspace : Optional[str | uuid.UUID], default=None
        The name or ID of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    if type not in ["Report", "SemanticModel"]:
        raise ValueError(
            f"{icons.red_dot} Type must be either 'Report' or 'SemanticModel'."
        )

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (item_name, item_id) = resolve_item_name_and_id(item, type, workspace_id)

    map = {
        "Report": "reports",
        "SemanticModel": "datasets",
    }

    _base_api(
        request=f"/v1.0/myorg/groups/{workspace_id}/{map.get(type)}/{item_id}/Default.TakeOver",
        method="post",
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} Successfully took over the '{item_name}' {type} in workspace '{workspace_name}'."
    )
