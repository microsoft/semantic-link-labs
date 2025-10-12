from uuid import UUID
import sempy_labs._icons as icons
from typing import List, Optional
from sempy_labs._helper_functions import (
    _base_api,
)
from sempy._utils._log import log
from sempy_labs.graph._users import resolve_user_id


@log
def add_user_license(
    user: str | UUID, sku_id: UUID, disabled_plans: Optional[UUID | List[UUID]] = None
):

    user_id = resolve_user_id(user)

    payload = {
        "addLicenses": [
            {
                "skuId": sku_id,
            },
        ],
        "removeLicenses": [],
    }

    if disabled_plans:
        if isinstance(disabled_plans, str):
            disabled_plans = [disabled_plans]
        payload["addLicenses"][0]["disabledPlans"] = disabled_plans

    _base_api(
        request=f"users/{user_id}/assignLicense",
        client="graph",
        method="post",
        payload=payload,
    )

    print(
        f"{icons.green_dot} The '{sku_id}' license has been assigned to the user '{user}'."
    )


@log
def remove_user_license(user: str | UUID, sku_ids: UUID | List[UUID]):

    user_id = resolve_user_id(user)

    if isinstance(sku_ids, str):
        sku_ids = [sku_ids]

    payload = {
        "addLicenses": [],
        "removeLicenses": sku_ids,
    }

    _base_api(
        request=f"users/{user_id}/assignLicense",
        client="graph",
        method="post",
        payload=payload,
    )

    print(
        f"{icons.green_dot} The '{', '.join([str(s) for s in sku_ids])}' license(s) have been removed from the user '{user}'."
    )
