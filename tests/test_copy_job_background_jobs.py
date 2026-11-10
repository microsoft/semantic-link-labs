from types import SimpleNamespace

import pandas as pd
import pytest

import sempy_labs.copy_job._background_jobs as bg

WORKSPACE_ID = "cfafbeb1-8037-4d0c-896e-a46fb27ff229"
ITEM_ID = "5b218778-e7a5-4d73-8187-f10824047715"
SCHEDULE_ID = "bbbbbbbb-1111-2222-3333-cccccccccccc"
JOB_ID = "aaaaaaaa-1111-2222-3333-dddddddddddd"
BASE = f"/v1/workspaces/{WORKSPACE_ID}/copyJobs/{ITEM_ID}/jobs/execute"

CRON_SCHEDULE = {
    "id": SCHEDULE_ID,
    "enabled": True,
    "createdDateTime": "2026-03-28T05:35:20.5366667",
    "configuration": {
        "startDateTime": "2026-03-28T00:00:00",
        "endDateTime": "2026-03-30T23:59:00",
        "localTimeZoneId": "Central Standard Time",
        "type": "Cron",
        "interval": 10,
    },
    "owner": {"id": "owner-id", "type": "User"},
}


def _response(json_body=None, headers=None):
    return SimpleNamespace(json=lambda: json_body, headers=headers or {})


@pytest.fixture
def api(monkeypatch):
    """Patches name resolution and routes _base_api calls to queued responses."""
    calls = []
    queue = []

    def fake_base_api(**kwargs):
        calls.append(kwargs)
        return queue.pop(0) if queue else None

    monkeypatch.setattr(bg, "_base_api", fake_base_api)
    monkeypatch.setattr(bg, "resolve_workspace_id", lambda w: WORKSPACE_ID)
    monkeypatch.setattr(
        bg, "resolve_workspace_name_and_id", lambda w: ("My Workspace", WORKSPACE_ID)
    )
    monkeypatch.setattr(bg, "resolve_item_id", lambda **kw: ITEM_ID)
    monkeypatch.setattr(
        bg, "resolve_item_name_and_id", lambda **kw: ("My Copy Job", ITEM_ID)
    )
    monkeypatch.setattr(bg.time, "sleep", lambda s: None)
    return SimpleNamespace(calls=calls, queue=queue)


def _job(status, failure=None):
    return {
        "id": JOB_ID,
        "itemId": ITEM_ID,
        "jobType": "Execute",
        "invokeType": "Manual",
        "status": status,
        "startTimeUtc": "2026-03-28T00:00:00",
        "endTimeUtc": None,
        "failureReason": failure,
    }


class TestRunCopyJob:
    def _submit(self):
        return _response(
            headers={
                "Location": f"https://api.fabric.microsoft.com{BASE}/instances/{JOB_ID}",
                "Retry-After": "1",
            }
        )

    def test_waits_until_terminal_status(self, api, capsys):
        api.queue.extend(
            [
                self._submit(),
                _response(_job("InProgress")),
                _response(_job("Completed")),
            ]
        )

        df = bg.run_copy_job("My Copy Job")

        assert api.calls[0]["request"] == f"{BASE}/instances"
        assert api.calls[0]["method"] == "post"
        assert api.calls[1]["request"] == f"{BASE}/instances/{JOB_ID}"
        assert len(api.calls) == 3
        assert df.loc[0, "Status"] == "Completed"
        assert "has completed" in capsys.readouterr().out

    @pytest.mark.parametrize("status", ["Cancelled", "Deduped", "Failed"])
    def test_stops_on_any_terminal_status(self, api, capsys, status):
        api.queue.extend([self._submit(), _response(_job(status, {"message": "boom"}))])

        df = bg.run_copy_job("My Copy Job")

        assert df.loc[0, "Status"] == status
        out = capsys.readouterr().out
        assert f"status '{status}'" in out
        assert "Error: boom" in out

    def test_no_wait_returns_after_first_poll(self, api):
        api.queue.extend([self._submit(), _response(_job("NotStarted"))])

        df = bg.run_copy_job("My Copy Job", wait=False)

        assert len(api.calls) == 2
        assert df.loc[0, "Job Instance Id"] == JOB_ID


def test_list_copy_job_instances(api):
    api.queue.append(
        [{"value": [_job("Completed"), _job("Failed", {"errorCode": "E1"})]}]
    )

    df = bg.list_copy_job_instances("My Copy Job")

    assert api.calls[0]["request"] == f"{BASE}/instances"
    assert api.calls[0]["uses_pagination"] is True
    assert list(df["Status"]) == ["Completed", "Failed"]
    assert df.loc[1, "Error Code"] == "E1"
    assert pd.api.types.is_datetime64_any_dtype(df["Start Time UTC"])


def test_list_copy_job_instances_empty(api):
    api.queue.append([{"value": []}])

    df = bg.list_copy_job_instances("My Copy Job")

    assert df.empty
    assert "Job Instance Id" in df.columns


def test_get_copy_job_instance(api):
    api.queue.append(_response(_job("InProgress")))

    df = bg.get_copy_job_instance("My Copy Job", job_instance_id=JOB_ID)

    assert api.calls[0]["request"] == f"{BASE}/instances/{JOB_ID}"
    assert df.loc[0, "Status"] == "InProgress"


class TestBuildScheduleConfiguration:
    common = dict(
        start_date_time="2026-01-01T00:00:00",
        end_date_time="2026-12-31T00:00:00",
        local_time_zone="UTC",
    )

    def test_cron(self):
        config = bg._build_schedule_configuration(
            "cron", interval_minutes=15, **self.common
        )
        assert config["type"] == "Cron"
        assert config["interval"] == 15
        assert "times" not in config

    def test_weekly_normalizes_single_values(self):
        config = bg._build_schedule_configuration(
            "Weekly", times="08:00", weekdays="Monday", **self.common
        )
        assert config["times"] == ["08:00"]
        assert config["weekdays"] == ["Monday"]

    def test_monthly_day_of_month(self):
        config = bg._build_schedule_configuration(
            "Monthly", times=["08:00"], recurrence=1, day_of_month=5, **self.common
        )
        assert config["occurrence"] == {
            "occurrenceType": "DayOfMonth",
            "dayOfMonth": 5,
        }

    def test_monthly_ordinal_weekday(self):
        config = bg._build_schedule_configuration(
            "Monthly",
            times=["08:00"],
            recurrence=2,
            week_index="First",
            weekday="Monday",
            **self.common,
        )
        assert config["occurrence"] == {
            "occurrenceType": "OrdinalWeekday",
            "weekIndex": "First",
            "weekday": "Monday",
        }

    def test_monthly_rejects_both_occurrence_types(self):
        with pytest.raises(ValueError, match="not both"):
            bg._build_schedule_configuration(
                "Monthly",
                times=["08:00"],
                recurrence=1,
                day_of_month=1,
                week_index="First",
                weekday="Monday",
                **self.common,
            )

    @pytest.mark.parametrize(
        "schedule_type,missing",
        [
            ("Cron", "interval_minutes"),
            ("Daily", "times"),
            ("Weekly", "weekdays"),
            ("Monthly", "recurrence"),
        ],
    )
    def test_missing_required_parameters(self, schedule_type, missing):
        kwargs = {"times": ["08:00"]} if schedule_type == "Weekly" else {}
        with pytest.raises(ValueError, match=missing):
            bg._build_schedule_configuration(schedule_type, **kwargs, **self.common)

    def test_invalid_type(self):
        with pytest.raises(ValueError, match="Invalid schedule type"):
            bg._build_schedule_configuration("Hourly", **self.common)


def test_create_copy_job_schedule(api):
    api.queue.append(_response(CRON_SCHEDULE))

    df = bg.create_copy_job_schedule(
        "My Copy Job",
        schedule_type="Cron",
        start_date_time="2026-03-28T00:00:00",
        end_date_time="2026-03-30T23:59:00",
        local_time_zone="Central Standard Time",
        interval_minutes=10,
    )

    call = api.calls[0]
    assert call["request"] == f"{BASE}/schedules"
    assert call["method"] == "post"
    assert call["status_codes"] == [201]
    assert call["payload"] == {
        "enabled": True,
        "configuration": CRON_SCHEDULE["configuration"],
    }
    assert df.loc[0, "Schedule Id"] == SCHEDULE_ID
    assert df.loc[0, "Interval Minutes"] == 10


def test_create_copy_job_schedule_validates_before_calling_api(api):
    with pytest.raises(ValueError):
        bg.create_copy_job_schedule(
            "My Copy Job",
            schedule_type="Daily",
            start_date_time="2026-03-28T00:00:00",
            end_date_time="2026-03-30T23:59:00",
            local_time_zone="UTC",
        )
    assert api.calls == []


def test_list_copy_job_schedules(api):
    api.queue.append([{"value": [CRON_SCHEDULE]}])

    df = bg.list_copy_job_schedules("My Copy Job")

    assert api.calls[0]["request"] == f"{BASE}/schedules"
    assert api.calls[0]["uses_pagination"] is True
    assert df.loc[0, "Type"] == "Cron"
    assert df.loc[0, "Owner Type"] == "User"
    assert bool(df.loc[0, "Enabled"]) is True


def test_list_copy_job_schedules_empty(api):
    api.queue.append([{"value": []}])

    df = bg.list_copy_job_schedules("My Copy Job")

    assert df.empty
    assert "Schedule Id" in df.columns


def test_get_copy_job_schedule(api):
    api.queue.append(_response(CRON_SCHEDULE))

    df = bg.get_copy_job_schedule("My Copy Job", schedule_id=SCHEDULE_ID)

    assert api.calls[0]["request"] == f"{BASE}/schedules/{SCHEDULE_ID}"
    assert df.loc[0, "Local Time Zone Id"] == "Central Standard Time"


def test_update_copy_job_schedule_merges_existing_values(api):
    api.queue.extend([_response(CRON_SCHEDULE), _response(CRON_SCHEDULE)])

    bg.update_copy_job_schedule(
        "My Copy Job", schedule_id=SCHEDULE_ID, enabled=False, interval_minutes=30
    )

    patch = api.calls[1]
    assert patch["method"] == "patch"
    assert patch["request"] == f"{BASE}/schedules/{SCHEDULE_ID}"
    assert patch["payload"]["enabled"] is False
    assert patch["payload"]["configuration"] == {
        **CRON_SCHEDULE["configuration"],
        "interval": 30,
    }


def test_update_copy_job_schedule_switches_monthly_occurrence(api):
    monthly = {
        **CRON_SCHEDULE,
        "configuration": {
            **CRON_SCHEDULE["configuration"],
            "type": "Monthly",
            "times": ["08:00"],
            "recurrence": 1,
            "occurrence": {"occurrenceType": "DayOfMonth", "dayOfMonth": 3},
        },
    }
    api.queue.extend([_response(monthly), _response(monthly)])

    bg.update_copy_job_schedule(
        "My Copy Job", schedule_id=SCHEDULE_ID, week_index="Second", weekday="Friday"
    )

    assert api.calls[1]["payload"]["configuration"]["occurrence"] == {
        "occurrenceType": "OrdinalWeekday",
        "weekIndex": "Second",
        "weekday": "Friday",
    }


def test_update_copy_job_schedule_change_type_requires_new_fields(api):
    api.queue.append(_response(CRON_SCHEDULE))

    with pytest.raises(ValueError, match="weekdays"):
        bg.update_copy_job_schedule(
            "My Copy Job",
            schedule_id=SCHEDULE_ID,
            schedule_type="Weekly",
            times="08:00",
        )
    assert len(api.calls) == 1


def test_delete_copy_job_schedule(api, capsys):
    bg.delete_copy_job_schedule("My Copy Job", schedule_id=SCHEDULE_ID)

    assert api.calls[0]["method"] == "delete"
    assert api.calls[0]["request"] == f"{BASE}/schedules/{SCHEDULE_ID}"
    assert "has been deleted" in capsys.readouterr().out
