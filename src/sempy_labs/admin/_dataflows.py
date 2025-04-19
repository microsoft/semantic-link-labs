import pandas as pd
from typing import Optional
from sempy_labs._helper_functions import (
    _base_api,
    _is_valid_uuid,
)
from sempy_labs.admin._items import (
    _resolve_item_id,
)
from uuid import UUID
from sempy._utils._log import log


@log
def export_dataflow(
    dataflow: str | UUID,
) -> dict:
    """
    Shows a list of datasets for the organization.

    This is a wrapper function for the following API: `Admin - Dataflows ExportDataflowAsAdmin <https://learn.microsoft.com/rest/api/power-bi/admin/dataflows-export-dataflow-as-admin>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    dataflow : str | UUID, default=None
        The dataflow Name or Id.

    Returns
    -------
    dict
        Exported Json file.
    """
    if not _is_valid_uuid(dataflow):
        dataflow = _resolve_item_id(item_name=dataflow, type="dataflow")

    url = f"/v1.0/myorg/admin/dataflows/{dataflow}/export"

    response = _base_api(request=url, client="fabric_sp")

    return response.json()
