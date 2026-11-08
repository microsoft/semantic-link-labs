import pandas as pd
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID
from sempy._utils._log import log
from sempy_labs._helper_functions import (
    resolve_capacity_id,
    resolve_workspace_id,
    _is_valid_uuid,
    _base_api,
    _create_dataframe,
    _update_dataframe_datatypes,
)
import sempy_labs._icons as icons


def _surge_url(capacity: Optional[str | UUID], suffix: str = "") -> str:

    capacity_id = resolve_capacity_id(capacity)
    url = f"/v1/capacities/{capacity_id}/surgeProtection"
    if suffix:
        url = f"{url}/{suffix}"

    return url


def _resolve_actor_workspace_id(workspace: str | UUID) -> str:

    # A workspace ID is used as-is so that actors referencing deleted or
    # inaccessible workspaces can still be managed.
    if _is_valid_uuid(workspace):
        return str(workspace)

    return resolve_workspace_id(workspace)


def _format_expiration_date_time(expire_after: datetime | str) -> str:

    if isinstance(expire_after, str):
        try:
            expire_after = datetime.fromisoformat(expire_after.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError(
                f"{icons.red_dot} The expire_after value '{expire_after}' is not a valid ISO 8601 date/time."
            )
    elif not isinstance(expire_after, datetime):
        raise ValueError(
            f"{icons.red_dot} The expire_after parameter must be a datetime or an ISO 8601 string."
        )

    # Naive datetimes are treated as UTC.
    if expire_after.tzinfo is not None:
        expire_after = expire_after.astimezone(timezone.utc)

    return expire_after.strftime("%Y-%m-%dT%H:%M:%SZ")


def _expiration_policy(expire_after: Optional[datetime | str]) -> dict:

    if expire_after is None:
        return {"policyType": "NeverExpire"}

    return {
        "policyType": "ExpireAfterDate",
        "expireAfterDateTime": _format_expiration_date_time(expire_after),
    }


def _validate_cu_threshold(threshold: int):

    if threshold < 1 or threshold > 100:
        raise ValueError(
            f"{icons.red_dot} The capacity_units_consumption_threshold_percentage must be between 1 and 100."
        )


def _block_duration(block_duration_hours: Optional[int], block_indefinitely: bool):

    if block_indefinitely:
        return {"type": "Indefinite"}
    if block_duration_hours is None:
        return None
    if block_duration_hours < 1:
        raise ValueError(
            f"{icons.red_dot} The block_duration_hours must be at least 1 hour."
        )

    return {"type": "Hours", "hours": block_duration_hours}


# Capacity surge protection


def _capacity_surge_protection_df(response: dict) -> pd.DataFrame:

    df = pd.DataFrame(
        [
            {
                "State": response.get("state"),
                "Rejection Threshold": response.get("rejectionThreshold"),
                "Recovery Threshold": response.get("recoveryThreshold"),
            }
        ]
    )
    for col in ["Rejection Threshold", "Recovery Threshold"]:
        df[col] = df[col].astype("Int64")

    return df


@log
def get_capacity_surge_protection(
    capacity: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Returns the surge protection configuration for background operations on the specified capacity.

    This is a wrapper function for the following API: `Surge Protection - Get Capacity Surge Protection <https://learn.microsoft.com/rest/api/fabric/core/surge-protection/get-capacity-surge-protection>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    capacity : str | uuid.UUID, default=None
        The capacity name or ID.
        Defaults to None which resolves to the capacity of the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the capacity of the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the surge protection configuration of the capacity.
        The threshold columns are empty when surge protection is disabled.
    """

    response = _base_api(request=_surge_url(capacity), client="fabric_sp").json()

    return _capacity_surge_protection_df(response)


@log
def update_capacity_surge_protection(
    capacity: Optional[str | UUID] = None,
    enabled: Optional[bool] = None,
    rejection_threshold: Optional[int] = None,
    recovery_threshold: Optional[int] = None,
) -> pd.DataFrame:
    """
    Updates the surge protection configuration for background operations on the specified capacity.

    When total CU consumption reaches the rejection threshold, new background operation requests are rejected.
    When total CU consumption drops below the recovery threshold, new background operation requests are accepted again.

    This is a wrapper function for the following API: `Surge Protection - Update Capacity Surge Protection <https://learn.microsoft.com/rest/api/fabric/core/surge-protection/update-capacity-surge-protection>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    capacity : str | uuid.UUID, default=None
        The capacity name or ID.
        Defaults to None which resolves to the capacity of the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the capacity of the workspace of the notebook.
    enabled : bool, default=None
        Set to True to enable surge protection or False to disable it (which also clears the persisted threshold values).
        Defaults to None which leaves the state unchanged.
    rejection_threshold : int, default=None
        The 24-hour background utilization percentage (15-100) at which background operations are rejected.
        Defaults to None which leaves the value unchanged.
    recovery_threshold : int, default=None
        The 24-hour background utilization percentage (6-99) at which surge protection deactivates.
        Must be strictly less than the rejection threshold.
        Defaults to None which leaves the value unchanged.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the updated surge protection configuration of the capacity.
    """

    if enabled is None and rejection_threshold is None and recovery_threshold is None:
        raise ValueError(
            f"{icons.red_dot} At least one of enabled, rejection_threshold or recovery_threshold must be specified."
        )
    if rejection_threshold is not None and not 15 <= rejection_threshold <= 100:
        raise ValueError(
            f"{icons.red_dot} The rejection_threshold must be between 15 and 100."
        )
    if recovery_threshold is not None and not 6 <= recovery_threshold <= 99:
        raise ValueError(
            f"{icons.red_dot} The recovery_threshold must be between 6 and 99."
        )
    if (
        rejection_threshold is not None
        and recovery_threshold is not None
        and recovery_threshold >= rejection_threshold
    ):
        raise ValueError(
            f"{icons.red_dot} The recovery_threshold must be less than the rejection_threshold."
        )

    payload = {}
    if enabled is not None:
        payload["state"] = "Enabled" if enabled else "Disabled"
    if rejection_threshold is not None:
        payload["rejectionThreshold"] = rejection_threshold
    if recovery_threshold is not None:
        payload["recoveryThreshold"] = recovery_threshold

    response = _base_api(
        request=_surge_url(capacity),
        method="patch",
        payload=payload,
        client="fabric_sp",
    ).json()

    print(f"{icons.green_dot} The capacity surge protection has been updated.")

    return _capacity_surge_protection_df(response)


# Actor auto-detection rules

_rule_columns = {
    "Actor Auto Detection Rule Id": "string",
    "Actors Subject To Rule": "string",
    "Capacity Units Consumption Threshold Percentage": "Int64",
    "Block Duration Type": "string",
    "Block Duration Hours": "Int64",
}


def _rules_df(rules: list) -> pd.DataFrame:

    df = _create_dataframe(columns=_rule_columns)
    rows = []
    for r in rules:
        block = r.get("blockDuration", {})
        rows.append(
            {
                "Actor Auto Detection Rule Id": r.get("id"),
                "Actors Subject To Rule": r.get("actorsSubjectToRule", {}).get("type"),
                "Capacity Units Consumption Threshold Percentage": r.get(
                    "capacityUnitsConsumptionThresholdPercentage"
                ),
                "Block Duration Type": block.get("type"),
                "Block Duration Hours": block.get("hours"),
            }
        )

    if rows:
        df = pd.DataFrame(rows)
        for col in [
            "Capacity Units Consumption Threshold Percentage",
            "Block Duration Hours",
        ]:
            df[col] = df[col].astype("Int64")

    return df


@log
def list_actor_auto_detection_rules(
    capacity: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Returns a list of the actor auto-detection rules on the specified capacity.

    This is a wrapper function for the following API: `Surge Protection - List Actor Auto Detection Rules <https://learn.microsoft.com/rest/api/fabric/core/surge-protection/list-actor-auto-detection-rules>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    capacity : str | uuid.UUID, default=None
        The capacity name or ID.
        Defaults to None which resolves to the capacity of the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the capacity of the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the actor auto-detection rules on the capacity.
    """

    responses = _base_api(
        request=_surge_url(capacity, "actorAutoDetectionRules"),
        uses_pagination=True,
        client="fabric_sp",
    )

    return _rules_df([v for r in responses for v in r.get("value", [])])


@log
def get_actor_auto_detection_rule(
    actor_auto_detection_rule_id: UUID,
    capacity: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Returns the specified actor auto-detection rule on the specified capacity.

    This is a wrapper function for the following API: `Surge Protection - Get Actor Auto Detection Rule <https://learn.microsoft.com/rest/api/fabric/core/surge-protection/get-actor-auto-detection-rule>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    actor_auto_detection_rule_id : uuid.UUID
        The actor auto-detection rule ID.
    capacity : str | uuid.UUID, default=None
        The capacity name or ID.
        Defaults to None which resolves to the capacity of the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the capacity of the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the actor auto-detection rule.
    """

    response = _base_api(
        request=_surge_url(
            capacity, f"actorAutoDetectionRules/{actor_auto_detection_rule_id}"
        ),
        client="fabric_sp",
    ).json()

    return _rules_df([response])


@log
def create_actor_auto_detection_rule(
    capacity_units_consumption_threshold_percentage: int,
    block_duration_hours: Optional[int] = 24,
    block_indefinitely: bool = False,
    capacity: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Creates an actor auto-detection rule on the specified capacity. The rule applies to all workspaces on the capacity.

    When a workspace's CU consumption reaches the threshold (as a percentage of total capacity units over a rolling 24-hour window),
    the workspace is automatically blocked for the specified duration.

    This is a wrapper function for the following API: `Surge Protection - Create Actor Auto Detection Rule <https://learn.microsoft.com/rest/api/fabric/core/surge-protection/create-actor-auto-detection-rule>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    capacity_units_consumption_threshold_percentage : int
        The capacity unit consumption threshold per actor (1-100) as a percentage of total capacity units over a rolling 24-hour window.
    block_duration_hours : int, default=24
        The number of hours an automatically blocked actor remains blocked. Must be at least 1.
        Ignored if block_indefinitely is set to True.
    block_indefinitely : bool, default=False
        If True, an automatically blocked actor remains blocked until an admin manually unblocks it.
    capacity : str | uuid.UUID, default=None
        The capacity name or ID.
        Defaults to None which resolves to the capacity of the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the capacity of the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the created actor auto-detection rule.
    """

    _validate_cu_threshold(capacity_units_consumption_threshold_percentage)
    block_duration = _block_duration(block_duration_hours, block_indefinitely)
    if block_duration is None:
        raise ValueError(
            f"{icons.red_dot} Either block_duration_hours must be specified or block_indefinitely must be set to True."
        )

    payload = {
        "actorsSubjectToRule": {"type": "AllWorkspaces"},
        "capacityUnitsConsumptionThresholdPercentage": capacity_units_consumption_threshold_percentage,
        "blockDuration": block_duration,
    }

    response = _base_api(
        request=_surge_url(capacity, "actorAutoDetectionRules"),
        method="post",
        payload=payload,
        status_codes=201,
        client="fabric_sp",
    ).json()

    print(
        f"{icons.green_dot} The actor auto-detection rule '{response.get('id')}' has been created."
    )

    return _rules_df([response])


@log
def update_actor_auto_detection_rule(
    actor_auto_detection_rule_id: UUID,
    capacity_units_consumption_threshold_percentage: Optional[int] = None,
    block_duration_hours: Optional[int] = None,
    block_indefinitely: bool = False,
    capacity: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Updates the specified actor auto-detection rule on the specified capacity.

    This is a wrapper function for the following API: `Surge Protection - Update Actor Auto Detection Rule <https://learn.microsoft.com/rest/api/fabric/core/surge-protection/update-actor-auto-detection-rule>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    actor_auto_detection_rule_id : uuid.UUID
        The actor auto-detection rule ID.
    capacity_units_consumption_threshold_percentage : int, default=None
        The capacity unit consumption threshold per actor (1-100) as a percentage of total capacity units over a rolling 24-hour window.
        Defaults to None which leaves the value unchanged.
    block_duration_hours : int, default=None
        The number of hours an automatically blocked actor remains blocked. Must be at least 1.
        Defaults to None which leaves the block duration unchanged (unless block_indefinitely is True).
    block_indefinitely : bool, default=False
        If True, an automatically blocked actor remains blocked until an admin manually unblocks it.
        Cannot be combined with block_duration_hours.
    capacity : str | uuid.UUID, default=None
        The capacity name or ID.
        Defaults to None which resolves to the capacity of the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the capacity of the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the updated actor auto-detection rule.
    """

    if block_indefinitely and block_duration_hours is not None:
        raise ValueError(
            f"{icons.red_dot} The block_duration_hours and block_indefinitely parameters cannot be used together."
        )

    payload = {}
    if capacity_units_consumption_threshold_percentage is not None:
        _validate_cu_threshold(capacity_units_consumption_threshold_percentage)
        payload["capacityUnitsConsumptionThresholdPercentage"] = (
            capacity_units_consumption_threshold_percentage
        )
    block_duration = _block_duration(block_duration_hours, block_indefinitely)
    if block_duration is not None:
        payload["blockDuration"] = block_duration

    if not payload:
        raise ValueError(
            f"{icons.red_dot} At least one of capacity_units_consumption_threshold_percentage, block_duration_hours or block_indefinitely must be specified."
        )

    response = _base_api(
        request=_surge_url(
            capacity, f"actorAutoDetectionRules/{actor_auto_detection_rule_id}"
        ),
        method="patch",
        payload=payload,
        client="fabric_sp",
    ).json()

    print(
        f"{icons.green_dot} The actor auto-detection rule '{actor_auto_detection_rule_id}' has been updated."
    )

    return _rules_df([response])


@log
def delete_actor_auto_detection_rule(
    actor_auto_detection_rule_id: UUID,
    capacity: Optional[str | UUID] = None,
):
    """
    Deletes the specified actor auto-detection rule from the specified capacity.

    Deleting a rule does not clear existing blocks. Actors blocked for a specified number of hours remain blocked until the
    originally scheduled expiration time, and actors blocked indefinitely remain blocked until an admin manually unblocks them.

    This is a wrapper function for the following API: `Surge Protection - Delete Actor Auto Detection Rule <https://learn.microsoft.com/rest/api/fabric/core/surge-protection/delete-actor-auto-detection-rule>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    actor_auto_detection_rule_id : uuid.UUID
        The actor auto-detection rule ID.
    capacity : str | uuid.UUID, default=None
        The capacity name or ID.
        Defaults to None which resolves to the capacity of the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the capacity of the workspace of the notebook.
    """

    _base_api(
        request=_surge_url(
            capacity, f"actorAutoDetectionRules/{actor_auto_detection_rule_id}"
        ),
        method="delete",
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The actor auto-detection rule '{actor_auto_detection_rule_id}' has been deleted."
    )


# Blocked actors

_blocked_actor_columns = {
    "Blocked Actor Id": "string",
    "Actor Type": "string",
    "Workspace Id": "string",
    "Expiration Policy Type": "string",
    "Expire After Date Time": "datetime",
}


def _blocked_actors_df(actors: list) -> pd.DataFrame:

    df = _create_dataframe(columns=_blocked_actor_columns)
    rows = []
    for a in actors:
        details = a.get("actorDetails", {})
        policy = a.get("expirationPolicy", {})
        rows.append(
            {
                "Blocked Actor Id": a.get("id"),
                "Actor Type": details.get("actorType"),
                "Workspace Id": details.get("workspaceId"),
                "Expiration Policy Type": policy.get("policyType"),
                "Expire After Date Time": policy.get("expireAfterDateTime"),
            }
        )

    if rows:
        df = pd.DataFrame(rows)
        _update_dataframe_datatypes(
            dataframe=df, column_map={"Expire After Date Time": "datetime"}
        )

    return df


def _list_blocked_actors(capacity: Optional[str | UUID]) -> list:

    responses = _base_api(
        request=_surge_url(capacity, "blockedActors"),
        uses_pagination=True,
        client="fabric_sp",
    )

    return [v for r in responses for v in r.get("value", [])]


def _resolve_blocked_actor_id(
    workspace: str | UUID, capacity: Optional[str | UUID]
) -> str:

    workspace_id = _resolve_actor_workspace_id(workspace)
    for a in _list_blocked_actors(capacity):
        if str(a.get("actorDetails", {}).get("workspaceId")) == str(workspace_id):
            return a.get("id")

    raise ValueError(
        f"{icons.red_dot} The '{workspace}' workspace is not a blocked actor on the capacity."
    )


@log
def list_blocked_actors(capacity: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Returns a list of the blocked actors on the specified capacity.

    This is a wrapper function for the following API: `Surge Protection - List Blocked Actors <https://learn.microsoft.com/rest/api/fabric/core/surge-protection/list-blocked-actors>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    capacity : str | uuid.UUID, default=None
        The capacity name or ID.
        Defaults to None which resolves to the capacity of the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the capacity of the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the blocked actors on the capacity.
    """

    return _blocked_actors_df(_list_blocked_actors(capacity))


@log
def get_blocked_actor(
    workspace: str | UUID,
    capacity: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Returns the blocked actor for the specified workspace on the specified capacity.

    This is a wrapper function for the following API: `Surge Protection - Get Blocked Actor <https://learn.microsoft.com/rest/api/fabric/core/surge-protection/get-blocked-actor>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    workspace : str | uuid.UUID
        The name or ID of the blocked workspace.
    capacity : str | uuid.UUID, default=None
        The capacity name or ID.
        Defaults to None which resolves to the capacity of the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the capacity of the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the blocked actor.
    """

    capacity_id = resolve_capacity_id(capacity)
    blocked_actor_id = _resolve_blocked_actor_id(workspace, capacity_id)

    response = _base_api(
        request=_surge_url(capacity_id, f"blockedActors/{blocked_actor_id}"),
        client="fabric_sp",
    ).json()

    return _blocked_actors_df([response])


@log
def create_blocked_actor(
    workspace: str | UUID,
    expire_after: Optional[datetime | str] = None,
    capacity: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Blocks the specified workspace on the specified capacity. The workspace's background operations will be rejected regardless of capacity utilization.

    If the workspace is already blocked on the capacity, its expiration policy is overwritten.

    This is a wrapper function for the following API: `Surge Protection - Create Blocked Actor <https://learn.microsoft.com/rest/api/fabric/core/surge-protection/create-blocked-actor>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    workspace : str | uuid.UUID
        The name or ID of the workspace to block.
    expire_after : datetime.datetime | str, default=None
        The date and time at which the block expires (a datetime or an ISO 8601 string). Naive values are treated as UTC.
        Defaults to None which means the block never expires.
    capacity : str | uuid.UUID, default=None
        The capacity name or ID.
        Defaults to None which resolves to the capacity of the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the capacity of the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the blocked actor.
    """

    payload = {
        "actorDetails": {
            "actorType": "Workspace",
            "workspaceId": _resolve_actor_workspace_id(workspace),
        },
        "expirationPolicy": _expiration_policy(expire_after),
    }

    response = _base_api(
        request=_surge_url(capacity, "blockedActors"),
        method="post",
        payload=payload,
        status_codes=[200, 201],
        client="fabric_sp",
    ).json()

    print(f"{icons.green_dot} The '{workspace}' workspace has been blocked.")

    return _blocked_actors_df([response])


@log
def update_blocked_actor(
    workspace: str | UUID,
    expire_after: Optional[datetime | str] = None,
    capacity: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Updates the expiration policy of the blocked actor for the specified workspace on the specified capacity.

    This is a wrapper function for the following API: `Surge Protection - Update Blocked Actor <https://learn.microsoft.com/rest/api/fabric/core/surge-protection/update-blocked-actor>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    workspace : str | uuid.UUID
        The name or ID of the blocked workspace.
    expire_after : datetime.datetime | str, default=None
        The date and time at which the block expires (a datetime or an ISO 8601 string). Naive values are treated as UTC.
        Defaults to None which means the block never expires.
    capacity : str | uuid.UUID, default=None
        The capacity name or ID.
        Defaults to None which resolves to the capacity of the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the capacity of the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the updated blocked actor.
    """

    payload = {"expirationPolicy": _expiration_policy(expire_after)}
    capacity_id = resolve_capacity_id(capacity)
    blocked_actor_id = _resolve_blocked_actor_id(workspace, capacity_id)

    response = _base_api(
        request=_surge_url(capacity_id, f"blockedActors/{blocked_actor_id}"),
        method="patch",
        payload=payload,
        client="fabric_sp",
    ).json()

    print(
        f"{icons.green_dot} The blocked actor for the '{workspace}' workspace has been updated."
    )

    return _blocked_actors_df([response])


@log
def delete_blocked_actor(
    workspace: str | UUID,
    capacity: Optional[str | UUID] = None,
):
    """
    Unblocks the specified workspace on the specified capacity by deleting its blocked actor.

    This is a wrapper function for the following API: `Surge Protection - Delete Blocked Actor <https://learn.microsoft.com/rest/api/fabric/core/surge-protection/delete-blocked-actor>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    workspace : str | uuid.UUID
        The name or ID of the blocked workspace.
    capacity : str | uuid.UUID, default=None
        The capacity name or ID.
        Defaults to None which resolves to the capacity of the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the capacity of the workspace of the notebook.
    """

    capacity_id = resolve_capacity_id(capacity)
    blocked_actor_id = _resolve_blocked_actor_id(workspace, capacity_id)

    _base_api(
        request=_surge_url(capacity_id, f"blockedActors/{blocked_actor_id}"),
        method="delete",
        client="fabric_sp",
    )

    print(f"{icons.green_dot} The '{workspace}' workspace has been unblocked.")


# Mission critical actors

_mission_critical_actor_columns = {
    "Mission Critical Actor Id": "string",
    "Actor Type": "string",
    "Workspace Id": "string",
}


def _mission_critical_actors_df(actors: list) -> pd.DataFrame:

    df = _create_dataframe(columns=_mission_critical_actor_columns)
    rows = []
    for a in actors:
        details = a.get("actorDetails", {})
        rows.append(
            {
                "Mission Critical Actor Id": a.get("id"),
                "Actor Type": details.get("actorType"),
                "Workspace Id": details.get("workspaceId"),
            }
        )

    if rows:
        df = pd.DataFrame(rows)

    return df


def _list_mission_critical_actors(capacity: Optional[str | UUID]) -> list:

    responses = _base_api(
        request=_surge_url(capacity, "missionCriticalActors"),
        uses_pagination=True,
        client="fabric_sp",
    )

    return [v for r in responses for v in r.get("value", [])]


def _resolve_mission_critical_actor_id(
    workspace: str | UUID, capacity: Optional[str | UUID]
) -> str:

    workspace_id = _resolve_actor_workspace_id(workspace)
    for a in _list_mission_critical_actors(capacity):
        if str(a.get("actorDetails", {}).get("workspaceId")) == str(workspace_id):
            return a.get("id")

    raise ValueError(
        f"{icons.red_dot} The '{workspace}' workspace is not a mission critical actor on the capacity."
    )


@log
def list_mission_critical_actors(
    capacity: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Returns a list of the mission critical actors on the specified capacity.

    This is a wrapper function for the following API: `Surge Protection - List Mission Critical Actors <https://learn.microsoft.com/rest/api/fabric/core/surge-protection/list-mission-critical-actors>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    capacity : str | uuid.UUID, default=None
        The capacity name or ID.
        Defaults to None which resolves to the capacity of the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the capacity of the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the mission critical actors on the capacity.
    """

    return _mission_critical_actors_df(_list_mission_critical_actors(capacity))


@log
def get_mission_critical_actor(
    workspace: str | UUID,
    capacity: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Returns the mission critical actor for the specified workspace on the specified capacity.

    This is a wrapper function for the following API: `Surge Protection - Get Mission Critical Actor <https://learn.microsoft.com/rest/api/fabric/core/surge-protection/get-mission-critical-actor>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    workspace : str | uuid.UUID
        The name or ID of the mission critical workspace.
    capacity : str | uuid.UUID, default=None
        The capacity name or ID.
        Defaults to None which resolves to the capacity of the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the capacity of the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the mission critical actor.
    """

    capacity_id = resolve_capacity_id(capacity)
    actor_id = _resolve_mission_critical_actor_id(workspace, capacity_id)

    response = _base_api(
        request=_surge_url(capacity_id, f"missionCriticalActors/{actor_id}"),
        client="fabric_sp",
    ).json()

    return _mission_critical_actors_df([response])


@log
def create_mission_critical_actor(
    workspace: str | UUID,
    capacity: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Marks the specified workspace as a mission critical actor on the specified capacity, shielding it from surge-protection rejection.

    If the workspace is already a mission critical actor on the capacity, the existing record is returned unchanged.

    This is a wrapper function for the following API: `Surge Protection - Create Mission Critical Actor <https://learn.microsoft.com/rest/api/fabric/core/surge-protection/create-mission-critical-actor>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    workspace : str | uuid.UUID
        The name or ID of the workspace.
    capacity : str | uuid.UUID, default=None
        The capacity name or ID.
        Defaults to None which resolves to the capacity of the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the capacity of the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the mission critical actor.
    """

    payload = {
        "actorDetails": {
            "actorType": "Workspace",
            "workspaceId": _resolve_actor_workspace_id(workspace),
        }
    }

    response = _base_api(
        request=_surge_url(capacity, "missionCriticalActors"),
        method="post",
        payload=payload,
        status_codes=[200, 201],
        client="fabric_sp",
    ).json()

    print(
        f"{icons.green_dot} The '{workspace}' workspace has been set as a mission critical actor."
    )

    return _mission_critical_actors_df([response])


@log
def delete_mission_critical_actor(
    workspace: str | UUID,
    capacity: Optional[str | UUID] = None,
):
    """
    Removes the specified workspace from the mission critical actors on the specified capacity.
    The workspace will no longer be shielded from surge-protection rejection.

    This is a wrapper function for the following API: `Surge Protection - Delete Mission Critical Actor <https://learn.microsoft.com/rest/api/fabric/core/surge-protection/delete-mission-critical-actor>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    workspace : str | uuid.UUID
        The name or ID of the mission critical workspace.
    capacity : str | uuid.UUID, default=None
        The capacity name or ID.
        Defaults to None which resolves to the capacity of the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the capacity of the workspace of the notebook.
    """

    capacity_id = resolve_capacity_id(capacity)
    actor_id = _resolve_mission_critical_actor_id(workspace, capacity_id)

    _base_api(
        request=_surge_url(capacity_id, f"missionCriticalActors/{actor_id}"),
        method="delete",
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The '{workspace}' workspace has been removed from the mission critical actors."
    )
