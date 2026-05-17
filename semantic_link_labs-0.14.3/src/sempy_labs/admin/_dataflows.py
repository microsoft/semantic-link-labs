from typing import Optional
from sempy_labs._helper_functions import (
    _base_api,
)
from sempy_labs.admin._items import (
    _resolve_item_id,
)
from uuid import UUID
from sempy._utils._log import log


@log
def export_dataflow(
    dataflow: str | UUID,
    workspace: Optional[str | UUID] = None,
) -> dict:
    """
    Shows a list of datasets for the organization.

    This is a wrapper function for the following API: `Admin - Dataflows ExportDataflowAsAdmin <https://learn.microsoft.com/rest/api/power-bi/admin/dataflows-export-dataflow-as-admin>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    dataflow : str | UUID, default=None
        The dataflow Name or Id.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or id.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
        Only used if given a dataflow name and not an id.

    Returns
    -------
    dict
        Exported Json file.
    """
    dataflow_id = _resolve_item_id(item=dataflow, type="dataflow", workspace=workspace)

    url = f"/v1.0/myorg/admin/dataflows/{dataflow_id}/export"

    response = _base_api(request=url, client="fabric_sp")

    return response.json()
