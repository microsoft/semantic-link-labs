import pandas as pd

import sempy_labs.lakehouse._partitioning as partitioning


def test_get_delta_table_details_returns_size_files_and_partition_state(monkeypatch):
    def _mock_get_partitions(table_name, schema_name=None, lakehouse=None, workspace=None):
        return {
            "sizeInBytes": 2147483648,
            "numFiles": 8,
            "partitionColumns": ["event_date"],
        }

    monkeypatch.setattr(partitioning, "_get_partitions", _mock_get_partitions)

    actual = partitioning.get_delta_table_details(table="sales")

    assert actual["Table Name"] == "sales"
    assert actual["Size In Bytes"] == 2147483648
    assert actual["Size In GB"] == 2.0
    assert actual["Files"] == 8
    assert actual["Partition Columns"] == ["event_date"]
    assert actual["Is Partitioned"] is True


def test_is_over_partitioned_uses_thresholds(monkeypatch):
    monkeypatch.setattr(
        partitioning,
        "get_delta_table_details",
        lambda **kwargs: {"Size In GB": 100, "Is Partitioned": True, "Files": 200},
    )

    assert partitioning.is_over_partitioned(table="sales") is True


def test_list_over_partitioned_tables_returns_matching_tables(monkeypatch):
    def _mock_list_tables(lakehouse=None, workspace=None, schema=None):
        return pd.DataFrame(
            [
                {"Table Name": "sales", "Schema Name": "dbo", "Format": "delta"},
                {"Table Name": "customers", "Schema Name": "dbo", "Format": "delta"},
                {"Table Name": "staging_sales", "Schema Name": "dbo", "Format": "csv"},
            ]
        )

    monkeypatch.setattr("sempy_labs.lakehouse._schemas.list_tables", _mock_list_tables)
    called_tables = []

    def _mock_get_delta_table_details(
        table, schema, lakehouse=None, workspace=None
    ):
        called_tables.append(table)
        return {
            "Table Name": table,
            "Schema Name": schema,
            "Size In Bytes": 2147483648,
            "Size In GB": 2,
            "Files": 8 if table == "sales" else 1,
            "Partition Columns": ["event_date"],
            "Is Partitioned": table == "sales",
        }

    monkeypatch.setattr(
        partitioning,
        "get_delta_table_details",
        _mock_get_delta_table_details,
    )

    actual = partitioning.list_over_partitioned_tables()

    assert list(actual["Table Name"]) == ["sales"]
    assert called_tables == ["sales", "customers"]
