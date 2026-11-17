from sempy._utils._log import log
from sempy_labs._helper_functions import _base_api
from sempy_labs.gateway._items import resolve_gateway_id
from sempy_labs.gateway._members import resolve_gateway_member_id
from uuid import UUID


@log
def check_gateway_status(gateway: str | UUID) -> dict:
    """
    Checks the status of the specified gateway.

    This is a wrapper function for the following API: `Gateways - Check Gateway Status <https://learn.microsoft.com/rest/api/fabric/core/gateways/check-gateway-status>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    gateway : str | uuid.UUID
        The gateway name or ID.

    Returns
    -------
    dict
        The `gateway status <https://learn.microsoft.com/rest/api/fabric/core/gateways/check-gateway-status#gatewaystatusresponse>`_,
        containing the 'status', 'gatewayVersion', 'gatewayUpgradeState' and (if applicable) 'errorDetails' properties.
    """

    gateway_id = resolve_gateway_id(gateway)

    return _base_api(
        request=f"/v1/gateways/{gateway_id}/checkStatus",
        client="fabric_sp",
        method="post",
        lro_return_json=True,
    )


@log
def check_gateway_member_status(
    gateway: str | UUID, gateway_member: str | UUID
) -> dict:
    """
    Checks the status of the specified gateway member of an on-premises gateway.

    This is a wrapper function for the following API: `Gateways - Check Gateway Member Status <https://learn.microsoft.com/rest/api/fabric/core/gateways/check-gateway-member-status>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    gateway : str | uuid.UUID
        The gateway name or ID.
    gateway_member : str | uuid.UUID
        The gateway member name or ID.

    Returns
    -------
    dict
        The `gateway member status <https://learn.microsoft.com/rest/api/fabric/core/gateways/check-gateway-member-status#gatewaystatusresponse>`_,
        containing the 'status', 'gatewayVersion', 'gatewayUpgradeState' and (if applicable) 'errorDetails' properties.
    """

    gateway_id = resolve_gateway_id(gateway)
    member_id = resolve_gateway_member_id(
        gateway=gateway_id, gateway_member=gateway_member
    )

    return _base_api(
        request=f"/v1/gateways/{gateway_id}/members/{member_id}/checkStatus",
        client="fabric_sp",
        method="post",
        lro_return_json=True,
    )
