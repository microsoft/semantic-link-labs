import pandas as pd

import sempy_labs._dax as dax_module


class _FakeTrace:
    def __init__(self, df):
        self._df = df

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def start(self):
        return None

    def stop(self):
        return self._df


class _FakeTraceConnection:
    def __init__(self, df):
        self._df = df

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def create_trace(self, event_schema):
        return _FakeTrace(self._df)


def test_capture_dax_query_timings_returns_summary_df(monkeypatch):
    run_df = pd.DataFrame(
        [
            {
                "EventClass": "VertiPaqSEQueryEnd",
                "CurrentTime": "2026-05-25T12:00:00.200",
                "Duration": 120,
                "SessionID": "s1",
                "ActivityID": "a1",
                "RequestID": "r1",
                "TextData": "SE event",
            },
            {
                "EventClass": "QueryEnd",
                "CurrentTime": "2026-05-25T12:00:00.300",
                "Duration": 300,
                "SessionID": "s1",
                "ActivityID": "a1",
                "RequestID": "r1",
                "TextData": "-- RUNID:abc EVALUATE ROW(\"x\", 1)",
            },
        ]
    )

    monkeypatch.setattr(dax_module.uuid, "uuid4", lambda: type("U", (), {"hex": "abc"})())
    monkeypatch.setattr(dax_module.time, "sleep", lambda _: None)
    monkeypatch.setattr(dax_module.fabric, "create_trace_connection", lambda dataset, workspace=None: _FakeTraceConnection(run_df))
    monkeypatch.setattr(dax_module.fabric, "evaluate_dax", lambda dataset, workspace, dax_string: pd.DataFrame())

    calls = {"clear": 0}

    def _fake_clear_cache(dataset, workspace=None):
        calls["clear"] += 1

    monkeypatch.setattr(dax_module, "clear_cache", _fake_clear_cache, raising=False)
    monkeypatch.setattr("sempy_labs._clear_cache.clear_cache", _fake_clear_cache)

    result = dax_module.capture_dax_query_timings(dataset="Model", dax_query='EVALUATE ROW("x", 1)', workspace="WS")

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    assert result.loc[0, "Dataset"] == "Model"
    assert result.loc[0, "Workspace"] == "WS"
    assert result.loc[0, "Total Elapsed (ms)"] == 300.0
    assert result.loc[0, "Storage Engine (ms)"] == 120.0
    assert result.loc[0, "Formula Engine (ms)"] == 180.0
    assert result.loc[0, "Trace Event Count"] == 2
    assert calls["clear"] == 1


def test_capture_dax_query_timings_empty_trace(monkeypatch):
    empty_df = pd.DataFrame()

    monkeypatch.setattr(dax_module.time, "sleep", lambda _: None)
    monkeypatch.setattr(dax_module.fabric, "create_trace_connection", lambda dataset, workspace=None: _FakeTraceConnection(empty_df))
    monkeypatch.setattr(dax_module.fabric, "evaluate_dax", lambda dataset, workspace, dax_string: pd.DataFrame())

    perf_values = iter([1.0, 1.5])
    monkeypatch.setattr(dax_module.time, "perf_counter", lambda: next(perf_values))

    result = dax_module.capture_dax_query_timings(
        dataset="Model",
        dax_query='EVALUATE ROW("x", 1)',
        clear_cache=False,
    )

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    assert result.loc[0, "Storage Engine (ms)"] == 0.0
    assert result.loc[0, "Trace Event Count"] == 0
