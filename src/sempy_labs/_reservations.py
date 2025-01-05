from typing import Optional
import sempy_labs._icons as icons
from sempy.fabric.exceptions import FabricHTTPException
import requests
import pandas as pd
from sempy.fabric._token_provider import TokenProvider
from sempy_labs._authentication import _get_headers


def list_reservation_orders(token_provider: TokenProvider) -> pd.DataFrame:

    # https://learn.microsoft.com/rest/api/reserved-vm-instances/reservation-order/list?view=rest-reserved-vm-instances-2022-11-01

    url = "https://management.azure.com/providers/Microsoft.Capacity/reservationOrders?api-version=2022-11-01"
    headers = _get_headers(token_provider=token_provider, audience="azure")
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        raise FabricHTTPException(response)

    df = pd.DataFrame(
        columns=[
            "Order Id",
            "Order Type",
            "Order Name",
            "Etag",
            "Display Name",
            "Request Date Time",
            "Created Date Time",
            "Benefit Start Time",
            "Expiry Date",
            "Expiry Date Time",
            "Term",
            "Billing Plan",
            "Provisioning State",
            "Reservations",
            "Original Quantity",
        ]
    )

    for v in response.json().get("value"):
        p = v.get("properties", {})
        new_data = {
            "Order Id": v.get("id"),
            "Order Type": v.get("type"),
            "Order Name": v.get("name"),
            "Etag": v.get("etag"),
            "Display Name": p.get("displayName"),
            "Request Date Time": p.get("requestDateTime"),
            "Created Date Time": p.get("createdDateTime"),
            "Benefit Start Time": p.get("benefitStartTime"),
            "Expiry Date": p.get("expiryDate"),
            "Expiry Date Time": p.get("expiryDateTime"),
            "Term": p.get("term"),
            "Billing Plan": p.get("billingPlan"),
            "Provisioning State": p.get("provisioningState"),
            "Reservations": [
                reservation.get("id") for reservation in p.get("reservations", [])
            ],
            "Original Quantity": p.get("originalQuanitity"),
        }

        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    df["Request Date Time"] = pd.to_datetime(df["Request Date Time"])
    df["Created Date Time"] = pd.to_datetime(df["Created Date Time"])
    df["Benefit Start Time"] = pd.to_datetime(df["Benefit Start Time"])
    df["Expiry Date Time"] = pd.to_datetime(df["Expiry Date Time"])

    return df


def list_reservation_transactions(
    billing_account_id: str, token_provider: TokenProvider
) -> pd.DataFrame:

    # https://learn.microsoft.com/rest/api/consumption/reservation-transactions/list?view=rest-consumption-2024-08-01

    url = f"https://management.azure.com/providers/Microsoft.Billing/billingAccounts/{billing_account_id}/providers/Microsoft.Consumption/reservationTransactions?api-version=2024-08-01"

    headers = _get_headers(token_provider=token_provider, audience="azure")
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        raise FabricHTTPException(response)

    df = pd.DataFrame(columns=[])

    return df


def list_reservations(
    reservation_id: str,
    reservation_order_id: str,
    token_provider: TokenProvider,
    grain="monthly",
    filter: Optional[str] = None,
) -> pd.DataFrame:

    # https://learn.microsoft.com/rest/api/consumption/reservations-summaries/list-by-reservation-order-and-reservation?view=rest-consumption-2024-08-01

    grain_options = ["monthly", "daily"]
    if grain not in grain_options:
        raise ValueError(
            f"{icons.red_dot} Invalid grain. Valid options: {grain_options}."
        )

    if filter is None and grain == "daily":
        raise ValueError(
            f"{icons.red_dot} The 'filter' parameter is required for daily grain."
        )

    url = f"https://management.azure.com/providers/Microsoft.Capacity/reservationorders/{reservation_order_id}/reservations/{reservation_id}/providers/Microsoft.Consumption/reservationSummaries?grain={grain}&"

    if filter is not None:
        url += f"$filter={filter}&"

    url += "api-version=2024-08-01"

    df = pd.DataFrame(columns=[])

    headers = _get_headers(token_provider=token_provider, audience="azure")
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        raise FabricHTTPException(response)

    for v in response.json().get("value", []):
        p = v.get("properties", {})
        new_data = {
            "Reservation Summary Id": v.get("id"),
            "Reservation Summary Name": v.get("name"),
            "Type": v.get("type"),
            "Tags": v.get("tags"),
            "Reservation Order Id": p.get("reservationOrderId"),
            "Reservation Id": p.get("reservationId"),
            "Sku Name": p.get("skuName"),
            "Kind": p.get("kind"),
            "Reserved Hours": p.get("reservedHours"),
            "Usage Date": p.get("usageDate"),
            "Used Hours": p.get("usedHours"),
            "Min Utilization Percentage": p.get("minUtilizationPercentage"),
            "Avg Utilization Percentage": p.get("avgUtilizationPercentage"),
            "Max Utilization Percentage": p.get("maxUtilizationPercentage"),
            "Purchased Quantity": p.get("purchasedQuantity"),
            "Remaining Quantity": p.get("remainingQuantity"),
            "Total Reserved Quantity": p.get("totalReservedQuantity"),
            "Used Quantity": p.get("usedQuantity"),
            "Utilized Percentage": p.get("utilizedPercentage"),
        }

        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    int_cols = [
        "Reserved Hours",
        "Used Hours",
        "Min Utilization Percentage",
        "Avg Utilization Percentage",
        "Max Utilization Percentage",
        "Purchased Quantity",
        "Remaining Quanity",
        "Total Reserved Quantity",
        "Used Quantity",
        "Utilized Percentage",
    ]

    df[int_cols] = df[int_cols].astype(int)

    return df


def buy_reservation(
    reservation_order_id: str,
    name: str,
    sku: str,
    region: str,
    billing_scope_id: str,
    term: str,
    billing_plan: str,
    quantity: int,
    token_provider: TokenProvider,
    renew: bool = False,
    applied_scope_type: str = "Shared",
    instance_flexibility: str = "On",
):

    # https://learn.microsoft.com/rest/api/reserved-vm-instances/reservation-order/purchase?view=rest-reserved-vm-instances-2022-11-01&tabs=HTTP

    url = f"https://management.azure.com/providers/Microsoft.Capacity/reservationOrders/{reservation_order_id}?api-version=2022-11-01"

    payload = {
        "sku": {
            "name": sku,
        },
        "location": region,
        "properties": {
            "reservedResourceType": "VirtualMachines",
            "billingScopeId": billing_scope_id,
            "term": term,
            "billingPlan": billing_plan,
            "quantity": quantity,
            "displayName": name,
            "appliedScopes": None,
            "appliedScopeType": applied_scope_type,
            "reservedResourceProperties": {"instanceFlexibility": instance_flexibility},
            "renew": renew,
        },
    }

    headers = _get_headers(token_provider=token_provider, audience="azure")
    response = requests.get(url, headers=headers, json=payload)

    if response.status_code not in [200, 202]:
        raise FabricHTTPException(response)
