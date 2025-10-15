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
    """
    Assigns a license to a user.

    This is a wrapper function for the following API: `user: assignLicense <https://learn.microsoft.com/graph/api/user-assignlicense>`_.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    user : str | uuid.UUID
        The user ID or user principal name.
    sku_id : uuid.UUID
        The SKU ID of the license to assign.
    disabled_plans : Optional[uuid.UUID | List[uuid.UUID]], default=None
        A single service plan ID or a list of service plan IDs to disable within the assigned license.
    """

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
    """
    Removes a license from a user.

    This is a wrapper function for the following API: `user: assignLicense <https://learn.microsoft.com/graph/api/user-assignlicense>`_.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    user : str | uuid.UUID
        The user ID or user principal name.
    sku_id : uuid.UUID
        The SKU ID of the license to remove.
    """

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
