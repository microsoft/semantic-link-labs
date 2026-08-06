import pandas as pd

import sempy_labs.lakehouse._lakehouse as lakehouse


class _TqdmMock:
    def __init__(self, iterable):
        self._iterable = iterable

    def __iter__(self):
        return iter(self._iterable)

    def set_description(self, *_args, **_kwargs):
        return None


def test_vacuum_lakehouse_tables_zero_pads_retention_hours(monkeypatch):
    monkeypatch.setattr(
        lakehouse,
        "_collect_tables",
        lambda **_kwargs: pd.DataFrame(
            [{"Table Name": "test_table", "Schema Name": "test_schema"}]
        ),
    )
    monkeypatch.setattr(
        lakehouse,
        "tqdm",
        lambda iterable, **_kwargs: _TqdmMock(iterable),
    )

    captured = {}

    def _mock_run_table_maintenance(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(lakehouse, "run_table_maintenance", _mock_run_table_maintenance)

    lakehouse.vacuum_lakehouse_tables(retain_n_hours=49)

    assert captured["retention_period"] == "2:01:00:00"
