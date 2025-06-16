from typing import Optional
from uuid import UUID
from sempy._utils._log import log


@log
def get_shared_expression(
    lakehouse: Optional[str] = None, workspace: Optional[str | UUID] = None
) -> str:
    """
    Dynamically generates the M expression used by a Direct Lake model for a given lakehouse.

    Parameters
    ----------
    lakehouse : str, default=None
        The Fabric lakehouse used by the Direct Lake semantic model.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    str
        Shows the expression which can be used to connect a Direct Lake semantic model to its SQL Endpoint.
    """
    from sempy_labs.directlake._generate_shared_expression import (
        generate_shared_expression,
    )

    return generate_shared_expression(
        item_name=lakehouse, item_type="Lakehouse", workspace=workspace
    )
