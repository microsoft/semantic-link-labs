from uuid import UUID
from typing import List
from sempy_labs._helper_functions import (
    _base_api,
)
from sempy._utils._log import log


@log
def generate_embed_token(dataset_ids: List[UUID], report_ids: List[UUID]):

    if not isinstance(dataset_ids, list):
        dataset_ids = [dataset_ids]
    if not isinstance(report_ids, list):
        report_ids = [report_ids]

    payload = {
        "datasets": [{"id": dataset_id} for dataset_id in dataset_ids],
        "reports": [{"id": report_id, "allowEdit": True} for report_id in report_ids],
    }

    response = _base_api(
        request="/v1.0/myorg/GenerateToken",
        method="post",
        client="fabric_sp",
        payload=payload,
    )
    return response.json().get("token")
