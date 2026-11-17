from sempy._utils._log import log
import pandas as pd
from typing import Optional
from sempy_labs._helper_functions import (
    _is_valid_uuid,
    _update_dataframe_datatypes,
    _base_api,
    _create_dataframe,
)
from sempy_labs.gateway._items import resolve_gateway_id
from uuid import UUID
import sempy_labs._icons as icons


@log
def list_gateway_members(gateway: str | UUID) -> pd.DataFrame:
    """
    Lists gateway members of an on-premises gateway.

    This is a wrapper function for the following API: `Gateways - List Gateway Members <https://learn.microsoft.com/rest/api/fabric/core/gateways/list-gateway-members>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    gateway : str | uuid.UUID
        The gateway name or ID.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of gateway members of an on-premises gateway.
    """

    gateway_id = resolve_gateway_id(gateway)

    columns = {
        "Member Id": "string",
        "Member Name": "string",
        "Public Key Exponent": "string",
        "Public Key Modulus": "string",
        "Version": "string",
        "Enabled": "bool",
    }
    df = _create_dataframe(columns=columns)

    response = _base_api(
        request=f"/v1/gateways/{gateway_id}/members", client="fabric_sp"
    )

    rows = []
    for v in response.json().get("value", []):
        rows.append(
            {
                "Member Id": v.get("id"),
                "Member Name": v.get("displayName"),
                "Public Key Exponent": v.get("publicKey", {}).get("exponent"),
                "Public Key Modulus": v.get("publicKey", {}).get("modulus"),
                "Version": v.get("version"),
                "Enabled": v.get("enabled"),
            }
        )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
def resolve_gateway_member_id(gateway: str | UUID, gateway_member: str | UUID) -> UUID:

    gateway_id = resolve_gateway_id(gateway)
    dfM = list_gateway_members(gateway=gateway_id)

    if _is_valid_uuid(gateway_member):
        dfM_filt = dfM[dfM["Member Id"] == gateway_member]
    else:
        dfM_filt = dfM[dfM["Member Name"] == gateway_member]
    if len(dfM_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{gateway_member}' gateway member does not exist within the '{gateway}' gateway."
        )

    return dfM_filt["Member Id"].iloc[0]


@log
def delete_gateway_member(gateway: str | UUID, gateway_member: str | UUID):
    """
    Delete gateway member of an on-premises gateway.

    This is a wrapper function for the following API: `Gateways - Delete Gateway Member <https://learn.microsoft.com/rest/api/fabric/core/gateways/delete-gateway-member>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    gateway : str | uuid.UUID
        The gateway name or ID.
    gateway_member : str | uuid.UUID
        The gateway member name or ID.
    """

    gateway_id = resolve_gateway_id(gateway)
    member_id = resolve_gateway_member_id(
        gateway=gateway_id, gateway_member=gateway_member
    )

    _base_api(
        request=f"/v1/gateways/{gateway_id}/members/{member_id}",
        client="fabric_sp",
        method="delete",
    )
    print(
        f"{icons.green_dot} The '{member_id}' member for the '{gateway}' gateway has been deleted."
    )


@log
def update_gateway_member(
    gateway: str | UUID,
    gateway_member: str | UUID,
    name: Optional[str] = None,
    enabled: Optional[bool] = None,
):
    """
    Updates a gateway member of an on-premises gateway.

    This is a wrapper function for the following API: `Gateways - Update Gateway Member <https://learn.microsoft.com/rest/api/fabric/core/gateways/update-gateway-member>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    gateway : str | uuid.UUID
        The gateway name or ID.
    gateway_member : str | uuid.UUID
        The gateway member name or ID.
    name : str, default=None
        The new display name of the gateway member. Maximum length is 200 characters.
    enabled : bool, default=None
        Whether the gateway member is enabled. True - Enabled, False - Not enabled.
    """

    payload = {}
    if name is not None:
        if len(name) > 200:
            raise ValueError(
                f"{icons.red_dot} The gateway member name cannot exceed 200 characters."
            )
        payload["displayName"] = name
    if enabled is not None:
        payload["enabled"] = enabled

    if not payload:
        raise ValueError(
            f"{icons.yellow_dot} The '{gateway_member}' gateway member has not been updated as no valid settings were provided."
        )

    gateway_id = resolve_gateway_id(gateway)
    member_id = resolve_gateway_member_id(
        gateway=gateway_id, gateway_member=gateway_member
    )

    _base_api(
        request=f"/v1/gateways/{gateway_id}/members/{member_id}",
        client="fabric_sp",
        method="patch",
        payload=payload,
    )

    print(
        f"{icons.green_dot} The '{gateway_member}' member for the '{gateway}' gateway has been updated accordingly."
    )
