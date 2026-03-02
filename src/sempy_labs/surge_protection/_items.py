import pandas as pd
from uuid import UUID
from sempy_labs._helper_functions import (
    resolve_capacity_id,
    _create_dataframe,
    _update_dataframe_datatypes,
    _base_api,
)
import sempy_labs._icons as icons
from sempy._utils._log import log


def _surge_api(capacity, url, payload, method="get", status_code=200, return_json=True):

    capacity_id = resolve_capacity_id(capacity)

    response = _base_api(
        request=f"capacities/{capacity_id}/{url}",
        client="internal",
        method=method,
        payload=payload,
        status_codes=status_code,
    )

    if return_json:
        return response.json()
    else:
        return None  # response


@log
def get_workspace_consumption_rules(
    capacity: str | UUID = None, return_dataframe: bool = True
) -> pd.DataFrame | dict:
    """
    Retrieves the workspace consumption surge protection rules for the specified capacity.

    Workspace Consumption
        When CU consumption by a single workspace reaches the rejection threshold, reject new operation requests and block the workspace for the specified amount of time.

    Parameters
    ----------
    capacity : str | uuid.UUID, default=None
        The capacity name or ID.
        Defaults to None which resolves to the capacity id of the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the capacity name of the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame | dict
        A pandas dataframe showing the workspace consumption surge protection rules for the specified capacity,
        or a dictionary if return_dataframe is set to False.
    """

    response_json = _surge_api(capacity=capacity, url="detectionRules", payload=None)

    if not return_dataframe:
        return response_json
    else:
        columns = {
            "Usage Threshold": "float",
            "Blocked Duration": "str",
            "Blocked Duration Policy": "str",
            "Detection Rule Id": "str",
            "Detection Rule Type": "str",
        }
        df = _create_dataframe(columns=columns)
        rows = []
        workspace_action = response_json.get("blockWorkspaceAction", {})
        rows.append(
            {
                "Usage Threshold": response_json.get(
                    "highWorkspaceUsageCondition", {}
                ).get("usageThreshold"),
                "Blocked Duration": workspace_action.get("blockedDuration"),
                "Blocked Duration Policy": workspace_action.get(
                    "blockedDurationPolicy"
                ),
                "Detection Rule Id": response_json.get("detectionRuleId"),
                "Detection Rule Type": response_json.get("detectionRuleType"),
            }
        )
        if rows:
            df = pd.DataFrame(rows)
            _update_dataframe_datatypes(dataframe=df, column_map=columns)
        return df


@log
def get_background_operation_rules(
    capacity: str | UUID = None, return_dataframe: bool = True
) -> pd.DataFrame | dict:
    """
    Retrieves the background operation surge protection rules for the specified capacity.

    Background Operations
        When total CU consumption reaches the rejection threshold, reject new background operation requests. When total CU consumption drops below the recovery threshold, accept new background operation requests.

    Parameters
    ----------
    capacity : str | uuid.UUID, default=None
        The capacity name or ID.
        Defaults to None which resolves to the capacity id of the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the capacity name of the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame | dict
        A pandas dataframe showing the background operation surge protection rules for the specified capacity,
        or a dictionary if return_dataframe is set to False.
    """

    response_json = _surge_api(
        capacity=capacity, url="surgeProtectionRules", payload=None
    )

    if not return_dataframe:
        return response_json
    else:
        columns = {
            "Rule Instance Id": "str",
            "Rule Type Id": "int",
            "Rule Id": "str",
            "Rejection Threshold Percentage": "int",
            "Recovery Threshold Percentage": "int",
            "Utilization Type": "str",
            "Throttling Level": "str",
        }

        df = _create_dataframe(columns=columns)
        rows = []
        for rule in response_json.get("rules", []):
            trig = rule.get("triggeredThrottlingLevel", {})
            throt = rule.get("usageThrottlingCriteria", {})
            rows.append(
                {
                    "Rule Instance Id": rule.get("ruleInstanceId"),
                    "Rule Type Id": rule.get("ruleTypeId"),
                    "Rule Id": rule.get("ruleId"),
                    "Rejection Threshold Percentage": throt.get(
                        "triggerThresholdPercentage"
                    ),
                    "Recovery Threshold Percentage": throt.get(
                        "recoveryThresholdPercentage"
                    ),
                    "Utilization Type": trig.get("utilizationType"),
                    "Throttling Level": trig.get("throttlingLevel"),
                }
            )
        if rows:
            df = pd.DataFrame(rows)
            _update_dataframe_datatypes(dataframe=df, column_map=columns)
        return df


@log
def set_workspace_consumption_rules(
    capacity: str | UUID = None,
    rejection_threshold: int = 75,
    block_duration_hours: int = 24,
    block_indefinitely: bool = False,
) -> dict:
    """
    Sets the workspace consumption surge protection rules for the specified capacity.

    Workspace Consumption
        When total CU consumption by a single workspace reaches the rejection threshold, reject new operation requests and block the workspace for the specified amount of time.

    Parameters
    ----------
    capacity : str | uuid.UUID, default=None
        The capacity name or ID.
        Defaults to None which resolves to the capacity id of the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the capacity name of the workspace of the notebook.
    rejection_threshold : int, default=75
        The CU consumption percentage threshold (1-100) at which new operation requests will be rejected.
    block_duration_hours : int, default=24
        The duration in hours to block the workspace when the rejection threshold is reached. Must be at least 1 hour.
        Ignored if block_indefinitely is set to True.
    block_indefinitely : bool, default=False
        If True, the workspace will be blocked indefinitely when the rejection threshold is reached.

    Returns
    -------
    dict
        A dictionary showing the workspace consumption surge protection rules for the specified capacity.
    """

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
        capacity=capacity, url="detectionRules", payload=payload, method="post"
    )


@log
def set_background_operation_rules(
    capacity: str | UUID = None,
    rejection_threshold: int = 75,
    recovery_threshold: int = 25,
) -> dict:
    """
    Sets the background operation surge protection rules for the specified capacity.

    Background Operations
        When total CU consumption reaches the rejection threshold, reject new background operation requests. When total CU consumption drops below the recovery threshold, accept new background operation requests.

    Parameters
    ----------
    capacity : str | uuid.UUID, default=None
        The capacity name or ID.
        Defaults to None which resolves to the capacity id of the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the capacity name of the workspace of the notebook.
    rejection_threshold : int, default=75
        The CU consumption percentage threshold (1-100) at which new background operation requests will be rejected.
    recovery_threshold : int, default=25
        The CU consumption percentage threshold (5-100) at which new background operation requests will be accepted.

    Returns
    -------
    dict
        A dictionary showing the background operation surge protection rules for the specified capacity.
    """

    if rejection_threshold < 1 or rejection_threshold > 100:
        raise ValueError(
            f"{icons.red_dot} The rejection_threshold must be between 1 and 100."
        )
    if recovery_threshold < 5 or recovery_threshold > 100:
        raise ValueError(
            f"{icons.red_dot} The recovery_threshold must be between 5 and 100."
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
        url="surgeProtectionRules",
        payload=payload,
        method="put",
        return_json=False,
    )

    return get_background_operation_rules(capacity=capacity, return_dataframe=False)


@log
def delete_workspace_consumption_rules(capacity: str | UUID = None):
    """
    Deletes the workspace consumption surge protection rules for the specified capacity.

    Workspace Consumption
        When total CU consumption by a single workspace reaches the rejection threshold, reject new operation requests and block the workspace for the specified amount of time.

    Parameters
    ----------
    capacity : str | uuid.UUID, default=None
        The capacity name or ID.
        Defaults to None which resolves to the capacity id of the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the capacity name of the workspace of the notebook.
    """

    rules = get_workspace_consumption_rules(capacity=capacity, return_dataframe=False)
    if not rules:
        print(f"{icons.yellow_dot} No workspace consumption rules found to delete.")
        return

    for v in rules.get("value", []):
        rule_id = v.get("detectionRuleId")
        break

    _surge_api(
        capacity=capacity,
        url=f"detectionRules/{rule_id}",
        payload=None,
        method="delete",
        status_code=204,
        return_json=False,
    )

    print(f"{icons.green_dot} The workspace consumption rules deleted successfully.")


@log
def delete_background_operation_rules(capacity: str | UUID = None):
    """
    Deletes the background operation surge protection rules for the specified capacity.

    Background Operations
        When total CU consumption reaches the rejection threshold, reject new background operation requests. When total CU consumption drops below the recovery threshold, accept new background operation requests.

    Parameters
    ----------
    capacity : str | uuid.UUID, default=None
        The capacity name or ID.
        Defaults to None which resolves to the capacity id of the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the capacity name of the workspace of the notebook.
    """

    _surge_api(
        capacity=capacity,
        url="surgeProtectionRules",
        payload=None,
        method="delete",
        status_code=200,
        return_json=False,
    )

    print(
        f"{icons.green_dot} The background operation rules have been deleted successfully."
    )
