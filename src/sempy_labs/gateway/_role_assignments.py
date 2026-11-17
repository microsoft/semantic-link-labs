from sempy._utils._log import log
import pandas as pd
from sempy_labs._helper_functions import (
    _base_api,
    _create_dataframe,
)
from sempy_labs.gateway._items import resolve_gateway_id
from uuid import UUID
import sempy_labs._icons as icons

_gateway_roles = ["Admin", "ConnectionCreatorWithResharing", "ConnectionCreator"]
_principal_types = [
    "User",
    "ServicePrincipal",
    "Group",
    "ServicePrincipalProfile",
    "EntireTenant",
]


def _validate_option(value: str, options: list, name: str) -> str:
    match = next((o for o in options if o.lower() == str(value).lower()), None)
    if match is None:
        raise ValueError(
            f"{icons.red_dot} Invalid {name} '{value}'. Valid options: {options}."
        )
    return match


@log
def list_gateway_role_assigments(gateway: str | UUID) -> pd.DataFrame:
    """
    Returns a list of gateway role assignments.

    This is a wrapper function for the following API: `Gateways - List Gateway Role Assignments <https://learn.microsoft.com/rest/api/fabric/core/gateways/list-gateway-role-assignments>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    gateway : str | uuid.UUID
        The gateway name or ID.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of gateway role assignments.
    """

    columns = {
        "Gateway Role Assignment Id": "string",
        "Principal Id": "string",
        "Principal Type": "string",
        "Role": "string",
    }
    df = _create_dataframe(columns=columns)
    gateway_id = resolve_gateway_id(gateway)
    responses = _base_api(
        request=f"/v1/gateways/{gateway_id}/roleAssignments",
        client="fabric_sp",
        uses_pagination=True,
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            rows.append(
                {
                    "Gateway Role Assignment Id": v.get("id"),
                    "Principal Id": v.get("principal", {}).get("id"),
                    "Principal Type": v.get("principal", {}).get("type"),
                    "Role": v.get("role"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df


@log
def delete_gateway_role_assignment(gateway: str | UUID, role_assignment_id: UUID):
    """
    Delete the specified role assignment for the gateway.

    This is a wrapper function for the following API: `Gateways - Delete Gateway Role Assignment <https://learn.microsoft.com/rest/api/fabric/core/gateways/delete-gateway-role-assignment>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    gateway : str | uuid.UUID
        The gateway name or ID.
    role_assignment_id : uuid.UUID
        The role assignment ID.
    """

    gateway_id = resolve_gateway_id(gateway)
    _base_api(
        request=f"/v1/gateways/{gateway_id}/roleAssignments/{role_assignment_id}",
        client="fabric_sp",
        method="delete",
    )

    print(
        f"{icons.green_dot} The '{role_assignment_id}' role assignment for the '{gateway}' gateway has been deleted."
    )


@log
def add_gateway_role_assignment(
    gateway: str | UUID,
    principal_id: UUID,
    principal_type: str,
    role: str,
):
    """
    Adds a gateway role assignment.

    This is a wrapper function for the following API: `Gateways - Add Gateway Role Assignment <https://learn.microsoft.com/rest/api/fabric/core/gateways/add-gateway-role-assignment>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    gateway : str | uuid.UUID
        The gateway name or ID.
    principal_id : uuid.UUID
        The ID of the principal (user, service principal, group, service principal profile or tenant).
    principal_type : str
        The `principal type <https://learn.microsoft.com/rest/api/fabric/core/gateways/add-gateway-role-assignment#principaltype>`_.
        Valid options: 'User', 'ServicePrincipal', 'Group', 'ServicePrincipalProfile', 'EntireTenant'.
    role : str
        The `gateway role <https://learn.microsoft.com/rest/api/fabric/core/gateways/add-gateway-role-assignment#gatewayrole>`_ of the principal.
        Valid options: 'Admin', 'ConnectionCreatorWithResharing', 'ConnectionCreator'.
    """

    principal_type = _validate_option(
        principal_type, _principal_types, "principal type"
    )
    role = _validate_option(role, _gateway_roles, "gateway role")
    gateway_id = resolve_gateway_id(gateway)

    payload = {
        "principal": {"id": str(principal_id), "type": principal_type},
        "role": role,
    }

    _base_api(
        request=f"/v1/gateways/{gateway_id}/roleAssignments",
        client="fabric_sp",
        method="post",
        payload=payload,
        status_codes=201,
    )

    print(
        f"{icons.green_dot} The '{principal_id}' {principal_type} principal has been assigned the '{role}' role for the '{gateway}' gateway."
    )


@log
def update_gateway_role_assignment(
    gateway: str | UUID,
    role_assignment_id: UUID,
    role: str,
):
    """
    Updates the principal's role assignment for the gateway.

    This is a wrapper function for the following API: `Gateways - Update Gateway Role Assignment <https://learn.microsoft.com/rest/api/fabric/core/gateways/update-gateway-role-assignment>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    gateway : str | uuid.UUID
        The gateway name or ID.
    role_assignment_id : uuid.UUID
        The role assignment ID.
    role : str
        The `gateway role <https://learn.microsoft.com/rest/api/fabric/core/gateways/update-gateway-role-assignment#gatewayrole>`_ of the principal.
        Valid options: 'Admin', 'ConnectionCreatorWithResharing', 'ConnectionCreator'.
    """

    role = _validate_option(role, _gateway_roles, "gateway role")
    gateway_id = resolve_gateway_id(gateway)

    _base_api(
        request=f"/v1/gateways/{gateway_id}/roleAssignments/{role_assignment_id}",
        client="fabric_sp",
        method="patch",
        payload={"role": role},
    )

    print(
        f"{icons.green_dot} The '{role_assignment_id}' role assignment for the '{gateway}' gateway has been updated to the '{role}' role."
    )
