import requests
from uuid import UUID
from sempy_labs._helper_functions import (
    get_pbi_token_headers,
    _get_url_prefix,
    resolve_capacity_id,
)
import sempy_labs._icons as icons
from sempy._utils._log import log
from sempy.fabric.exceptions import FabricHTTPException


"""
Workspace Consumption
When CU consumption by a single workspace reaches the rejection threshold, reject new operation requests and block the workspace for the specified amount of time.


Background Operations
When total CU consumption reaches the rejection threshold, reject new background operation requests. When total CU consumption drops below the recovery threshold, accept new background operation requests.
"""


def _surge_api(
    capacity, request, payload, method="get", status_code=200, return_json=True
):

    headers = get_pbi_token_headers()
    prefix = _get_url_prefix()
    capacity_id = resolve_capacity_id(capacity)

    response = requests.request(
        method=method,
        url=f"{prefix}/capacities/{capacity_id}/{request}",
        headers=headers,
        json=payload,
    )

    if response.status_code != status_code:
        raise FabricHTTPException(response.text)
    if return_json:
        return response.json()
    else:
        return None  # response


@log
def get_workspace_consumption_rules(capacity: str | UUID = None):

    return _surge_api(capacity=capacity, request="detectionRules", payload=None)


@log
def get_background_operation_rules(capacity: str | UUID = None):

    return _surge_api(capacity=capacity, request="surgeProtectionRules", payload=None)


@log
def set_workspace_consumption_rules(
    capacity: str | UUID = None,
    rejection_threshold: int = 75,
    block_duration_hours: int = 24,
    block_indefinitely: bool = False,
):

    if not block_indefinitely and block_duration_hours < 1:
        raise ValueError(
            f"{icons.red_dot} The block_duration_hours must be at least 1 hour."
        )

    if rejection_threshold < 1 or rejection_threshold > 100:
        raise ValueError(
            f"{icons.red_dot} The rejection_threshold must be between 1 and 100."
        )

    payload = {
        "detectionRuleId": None,
        "detectionRuleType": "detectHighWorkspaceUsageAndBlockWorkspace",
        "blockWorkspaceAction": {
            "blockedDurationPolicy": (
                "indefinite" if block_indefinitely else "fixedDuration"
            ),
            "blockedDuration": (
                None if block_indefinitely else f"PT{block_duration_hours}H"
            ),
        },
        "highWorkspaceUsageCondition": {
            "usageThreshold": rejection_threshold,
        },
    }

    return _surge_api(
        capacity=capacity, request="detectionRules", payload=payload, method="post"
    )


@log
def set_background_operation_rules(
    capacity: str | UUID = None,
    rejection_threshold: int = 75,
    recovery_threshold: int = 25,
):

    if rejection_threshold < 1 or rejection_threshold > 100:
        raise ValueError(
            f"{icons.red_dot} The rejection_threshold must be between 1 and 100."
        )
    if recovery_threshold < 5 or recovery_threshold > 100:
        raise ValueError(
            f"{icons.red_dot} The recovery_threshold must be between 1 and 100."
        )
    if recovery_threshold >= rejection_threshold:
        raise ValueError(
            f"{icons.red_dot} The recovery_threshold must be less than the rejection_threshold."
        )

    payload = {
        "rules": [
            {
                "ruleTypeId": 1,
                "ruleInstanceId": 1,
                "triggeredThrottlingLevel": {
                    "utilizationType": "Background",
                    "throttlingLevel": "Extreme",
                },
                "usageThrottlingCriteria": {
                    "triggerThresholdPercentage": rejection_threshold,
                    "recoveryThresholdPercentage": recovery_threshold,
                },
            }
        ]
    }

    _surge_api(
        capacity=capacity,
        request="surgeProtectionRules",
        payload=payload,
        method="put",
        return_json=False,
    )

    return get_background_operation_rules(capacity=capacity)


@log
def delete_workspace_consumption_rules(capacity: str | UUID = None):

    rules = get_workspace_consumption_rules(capacity=capacity)
    for v in rules.get("value", []):
        rule_id = v.get("detectionRuleId")
        break
    return _surge_api(
        capacity=capacity,
        request=f"detectionRules/{rule_id}",
        payload=None,
        method="delete",
        status_code=204,
        return_json=False,
    )


@log
def delete_background_operation_rules(capacity: str | UUID = None):

    return _surge_api(
        capacity=capacity,
        request="surgeProtectionRules",
        payload=None,
        method="delete",
        status_code=200,
        return_json=False,
    )
