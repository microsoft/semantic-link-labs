import pandas as pd

import sempy_labs.lakehouse._lakehouse as lakehouse


class _TqdmMock:
    def __init__(self, iterable):
        self._iterable = iterable

    def __iter__(self):
        return iter(self._iterable)

    def set_description(self, *_args, **_kwargs):
        return None


def _patch_lakehouse(monkeypatch):
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

    return captured


def test_vacuum_lakehouse_tables_zero_pads_retention_hours(monkeypatch):
    captured = _patch_lakehouse(monkeypatch)

    lakehouse.vacuum_lakehouse_tables(retain_n_hours=49)

    assert captured["retention_period"] == "2:01:00:00"


def test_vacuum_lakehouse_tables_default_retention_hours(monkeypatch):
    captured = _patch_lakehouse(monkeypatch)

    lakehouse.vacuum_lakehouse_tables()

    assert captured["retention_period"] is None


def _list_blobs(monkeypatch, xml: str) -> pd.DataFrame:
    import xml.etree.ElementTree as ET

    import sempy_labs.lakehouse._blobs as blobs

    monkeypatch.setattr(blobs, "resolve_workspace_id", lambda *_a, **_k: "ws")
    monkeypatch.setattr(blobs, "resolve_lakehouse_id", lambda *_a, **_k: "lh")
    monkeypatch.setattr(
        blobs, "_request_blob_api", lambda **_kwargs: [ET.fromstring(xml)]
    )

    return blobs.list_blobs()


def test_list_blobs_handles_a_page_with_no_blobs(monkeypatch):
    # An empty XML element parses to None, which used to raise
    # "'NoneType' object has no attribute 'get'".
    df = _list_blobs(monkeypatch, "<EnumerationResults><Blobs /></EnumerationResults>")

    assert df.empty


def test_list_blobs_handles_a_blob_without_properties(monkeypatch):
    df = _list_blobs(
        monkeypatch,
        "<EnumerationResults><Blobs><Blob><Name>Tables/sales/part-0.parquet</Name>"
        "<Properties /></Blob></Blobs></EnumerationResults>",
    )

    assert list(df["Blob Name"]) == ["Tables/sales/part-0.parquet"]
