import sempy.fabric as fabric
from sempy_labs.directlake._generate_shared_expression import generate_shared_expression
from sempy_labs._helper_functions import (
    resolve_lakehouse_name,
)
from sempy_labs.tom import connect_semantic_model
from typing import Optional
import sempy_labs._icons as icons


def update_direct_lake_model_lakehouse_connection(
    dataset: str,
    workspace: Optional[str] = None,
    lakehouse: Optional[str] = None,
    lakehouse_workspace: Optional[str] = None,
):
    """
    Remaps a Direct Lake semantic model's SQL Endpoint connection to a new lakehouse.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    workspace : str, default=None
        The Fabric workspace name in which the semantic model exists.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    lakehouse : str, default=None
        The Fabric lakehouse used by the Direct Lake semantic model.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    lakehouse_workspace : str, default=None
        The Fabric workspace used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    workspace = fabric.resolve_workspace_name(workspace)

    if lakehouse_workspace is None:
        lakehouse_workspace = workspace

    if lakehouse is None:
        lakehouse_id = fabric.get_lakehouse_id()
        lakehouse = resolve_lakehouse_name(lakehouse_id, lakehouse_workspace)

    # Check if lakehouse is valid
    dfI = fabric.list_items(workspace=lakehouse_workspace, type="Lakehouse")
    dfI_filt = dfI[(dfI["Display Name"] == lakehouse)]

    if len(dfI_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{lakehouse}' lakehouse does not exist within the '{lakehouse_workspace}' workspace. "
            f"Therefore it cannot be used to support the '{dataset}' semantic model within the '{workspace}' workspace."
        )

    icons.sll_tags.append("UpdateDLConnection")

    shEx = generate_shared_expression(
        item_name=lakehouse, item_type="Lakehouse", workspace=lakehouse_workspace
    )

    with connect_semantic_model(
        dataset=dataset, readonly=False, workspace=workspace
    ) as tom:

        if not tom.is_direct_lake():
            raise ValueError(
                f"{icons.red_dot} The '{dataset}' semantic model is not in Direct Lake. This function is only applicable to Direct Lake semantic models."
            )

        tom.model.Expressions["DatabaseQuery"].Expression = shEx

    print(
        f"{icons.green_dot} The expression in the '{dataset}' semantic model has been updated to point to the '{lakehouse}' lakehouse in the '{lakehouse_workspace}' workspace."
    )


def update_direct_lake_model_connection(
    dataset: str,
    workspace: Optional[str] = None,
    source: Optional[str] = None,
    source_type: Optional[str] = "Lakehouse",
    source_workspace: Optional[str] = None,
):
    """
    Remaps a Direct Lake semantic model's SQL Endpoint connection to a new lakehouse/warehouse.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    workspace : str, default=None
        The Fabric workspace name in which the semantic model exists.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    source : str, default=None
        The name of the Fabric lakehouse/warehouse used by the Direct Lake semantic model.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    source_type : str, default="Lakehouse"
        The type of source for the Direct Lake semantic model. Valid options: "Lakehouse", "Warehouse".
    source_workspace : str, default=None
        The Fabric workspace used by the lakehouse/warehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    if workspace is None:
        workspace = fabric.resolve_workspace_name(workspace)

    source_type = source_type.capitalize()

    if source_type not in ["Lakehouse", "Warehouse"]:
        raise ValueError(
            f"{icons.red_dot} The 'source_type' must be either 'Lakehouse' or 'Warehouse'."
        )

    if source_workspace is None:
        source_workspace = workspace

    if source is None:
        source_id = fabric.get_lakehouse_id()
        source = resolve_lakehouse_name(source_id, source_workspace)
    else:
        source_id = fabric.resolve_item_id(
            item_name=source, type=source_type, workspace=source_workspace
        )
        source = fabric.resolve_item_name(
            item_id=source_id, workspace=source_workspace, type=source_type
        )

    icons.sll_tags.append("UpdateDLConnection")

    shEx = generate_shared_expression(
        item_name=source, item_type=source_type, workspace=source_workspace
    )

    with connect_semantic_model(
        dataset=dataset, readonly=False, workspace=workspace
    ) as tom:

        if not tom.is_direct_lake():
            raise ValueError(
                f"{icons.red_dot} The '{dataset}' semantic model is not in Direct Lake. This function is only applicable to Direct Lake semantic models."
            )

        tom.model.Expressions["DatabaseQuery"].Expression = shEx

    print(
        f"{icons.green_dot} The expression in the '{dataset}' semantic model has been updated to point to the '{source}' {source_type.lower()} in the '{source_workspace}' workspace."
    )
