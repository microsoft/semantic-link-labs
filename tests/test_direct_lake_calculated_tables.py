import sys
import types
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

SRC = Path(__file__).parents[1] / "src" / "sempy_labs"


class _Enum:
    """Minimal stand-in for a .NET enum type."""

    def __init__(self, *names):
        for n in names:
            setattr(self, n, n)


class _Coll(list):
    """List which can also be indexed by object name (like a TOM collection)."""

    def __getitem__(self, key):
        if isinstance(key, str):
            return next(o for o in self if o.Name == key)
        return super().__getitem__(key)


@pytest.fixture
def fake_tom_module(monkeypatch):
    modules = _fake_tom_namespace()
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)
    return modules["Microsoft.AnalysisServices.Tabular"]


def _column(name, col_type, **kwargs):
    defaults = dict(
        Name=name,
        Type=col_type,
        DataType="String",
        FormatString="",
        IsHidden=False,
        Description="",
        DisplayFolder="",
        DataCategory="",
        IsKey=False,
        SummarizeBy="Default",
        SourceColumn=f"[{name}]",
        IsNameInferred=False,
        IsDataTypeInferred=True,
        Expression=None,
    )
    defaults.update(kwargs)
    return types.SimpleNamespace(**defaults)


def _table(name, expression, columns, source_type="Calculated"):
    partition = types.SimpleNamespace(
        SourceType=source_type, Source=types.SimpleNamespace(Expression=expression)
    )
    return types.SimpleNamespace(
        Name=name,
        Partitions=_Coll([partition]),
        Columns=_Coll(columns),
        Description="",
        DataCategory="",
        IsHidden=False,
    )


class _SourceTom:
    def __init__(self, tables):
        self.model = types.SimpleNamespace(Tables=_Coll(tables))

    def is_auto_date_table(self, table_name):
        return table_name.startswith("LocalDateTable_")


_ONELAKE_EXPRESSION = [
    "11111111-1111-1111-1111-111111111111",
    "22222222-2222-2222-2222-222222222222",
    False,
]
_SQL_EXPRESSION = ["server.datawarehouse.fabric.microsoft.com", "endpoint-id", True]


class _TargetTom:
    def __init__(self, tables=(), expressions=None):
        self.model = types.SimpleNamespace(Tables=_Coll(tables))
        self.expressions = (
            {"DatabaseQuery": _ONELAKE_EXPRESSION}
            if expressions is None
            else expressions
        )
        self.calc_tables = {}
        self.table_columns = []
        self.calc_columns = []

    def _get_direct_lake_expressions(self):
        return self.expressions

    def add_calculated_table(self, name, expression, **kwargs):
        self.calc_tables[name] = expression
        self.model.Tables.append(types.SimpleNamespace(Name=name, Columns=_Coll()))

    def add_calculated_table_column(self, table_name, column_name, **kwargs):
        self.table_columns.append((table_name, column_name, kwargs))
        self.model.Tables[table_name].Columns.append(
            types.SimpleNamespace(Name=column_name)
        )

    def add_calculated_column(self, table_name, column_name, expression, **kwargs):
        self.calc_columns.append((table_name, column_name, expression, kwargs))


def _run_migrate_calc_tables(source, target, on_connect=None, **kwargs):
    from sempy_labs.migration import _migrate_calctables_to_semantic_model as mod

    @contextmanager
    def fake_connect(dataset, readonly, workspace):
        if on_connect is not None:
            on_connect()
        yield source if dataset == "src" else target

    with (
        patch.object(mod, "connect_semantic_model", fake_connect),
        patch.object(mod.fabric, "refresh_tom_cache"),
    ):
        mod.migrate_calc_tables_to_semantic_model(
            dataset="src", new_dataset="new", **kwargs
        )


def test_migrate_calc_tables_adds_calculated_tables_with_their_columns(
    fake_tom_module,
):
    source = _SourceTom(
        [
            _table(
                "Sales Summary",
                "SUMMARIZECOLUMNS('Sales'[Year], \"Total\", [Sales])",
                [
                    _column("Year", "CalculatedTableColumn", DataType="Int64"),
                    _column("Total", "CalculatedTableColumn", DataType="Decimal"),
                    _column(
                        "Total x2",
                        "Calculated",
                        DataType="Decimal",
                        Expression="[Total] * 2",
                    ),
                    _column("RowNumber-1", "RowNumber"),
                ],
            ),
            _table("Sales", "let Source = ... in Source", [], source_type="M"),
        ]
    )
    target = _TargetTom()

    _run_migrate_calc_tables(source, target)

    assert target.calc_tables == {
        "Sales Summary": "SUMMARIZECOLUMNS('Sales'[Year], \"Total\", [Sales])"
    }
    assert [(t, c) for t, c, _ in target.table_columns] == [
        ("Sales Summary", "Year"),
        ("Sales Summary", "Total"),
    ]
    assert target.table_columns[0][2]["source_column"] == "[Year]"
    assert target.table_columns[0][2]["data_type"] == "Int64"
    # Calculated columns within calculated tables are migrated as is.
    assert len(target.calc_columns) == 1
    table_name, column_name, expression, kwargs = target.calc_columns[0]
    assert (table_name, column_name, expression) == (
        "Sales Summary",
        "Total x2",
        "[Total] * 2",
    )
    assert "expression_context" not in kwargs
    # Inferred flags are copied from the source columns.
    new_col = target.model.Tables["Sales Summary"].Columns["Year"]
    assert new_col.IsNameInferred is False
    assert new_col.IsDataTypeInferred is True


def test_migrate_calc_tables_skips_field_parameters_auto_date_and_existing_tables(
    fake_tom_module,
):
    source = _SourceTom(
        [
            _table(
                "Parameter",
                "{(\"Sales\", NAMEOF('Sales'[Amount]), 0)}",
                [_column("Parameter", "CalculatedTableColumn")],
            ),
            _table(
                "LocalDateTable_123",
                "CALENDAR(DATE(2020, 1, 1), DATE(2020, 12, 31))",
                [_column("Date", "CalculatedTableColumn")],
            ),
            _table(
                "Existing",
                "CALENDARAUTO()",
                [_column("Date", "CalculatedTableColumn")],
            ),
            _table(
                "Dates",
                "CALENDARAUTO()",
                [_column("Date", "CalculatedTableColumn", DataType="DateTime")],
            ),
        ]
    )
    target = _TargetTom([types.SimpleNamespace(Name="Existing", Columns=_Coll())])

    _run_migrate_calc_tables(source, target)

    assert list(target.calc_tables) == ["Dates"]


def _fake_tom_namespace():
    tom = types.ModuleType("Microsoft.AnalysisServices.Tabular")
    tom.PartitionSourceType = _Enum("Calculated", "M", "Entity")
    tom.ColumnType = _Enum("Data", "Calculated", "CalculatedTableColumn", "RowNumber")
    microsoft = types.ModuleType("Microsoft")
    analysis_services = types.ModuleType("Microsoft.AnalysisServices")
    microsoft.AnalysisServices = analysis_services
    analysis_services.Tabular = tom
    return {
        "Microsoft": microsoft,
        "Microsoft.AnalysisServices": analysis_services,
        "Microsoft.AnalysisServices.Tabular": tom,
    }


def test_migrate_calc_tables_imports_tom_after_connecting(monkeypatch):
    # Cold start: the Analysis Services namespace is only importable once a TOM
    # connection has initialized it.
    for name in _fake_tom_namespace():
        monkeypatch.setitem(sys.modules, name, None)

    def initialize_analysis_services():
        for name, module in _fake_tom_namespace().items():
            if sys.modules.get(name) is None:
                monkeypatch.setitem(sys.modules, name, module)

    source = _SourceTom(
        [_table("Dates", "CALENDARAUTO()", [_column("Date", "CalculatedTableColumn")])]
    )
    target = _TargetTom()

    _run_migrate_calc_tables(source, target, on_connect=initialize_analysis_services)

    assert list(target.calc_tables) == ["Dates"]


def test_migrate_calc_tables_uses_lakehouse_tables_for_direct_lake_on_sql(
    fake_tom_module,
):
    from sempy_labs.migration import _migrate_calctables_to_semantic_model as mod

    source = _SourceTom(
        [_table("Dates", "CALENDARAUTO()", [_column("Date", "CalculatedTableColumn")])]
    )
    target = _TargetTom(expressions={"DatabaseQuery": _SQL_EXPRESSION})

    with patch.object(mod, "_add_lakehouse_calc_tables_to_semantic_model") as legacy:
        _run_migrate_calc_tables(
            source, target, lakehouse="lh", lakehouse_workspace="ws"
        )

    legacy.assert_called_once_with(
        dataset="src",
        new_dataset="new",
        workspace=None,
        new_dataset_workspace=None,
        lakehouse="lh",
        lakehouse_workspace="ws",
    )
    assert target.calc_tables == {}


def _run_migrate_calc_tables_to_lakehouse(target):
    import sempy_labs.tom
    from sempy_labs.migration import _migrate_calctables_to_lakehouse as mod

    @contextmanager
    def fake_connect(dataset, readonly, workspace):
        assert dataset == "new"
        yield target

    with (
        patch.object(sempy_labs.tom, "connect_semantic_model", fake_connect),
        patch.object(mod, "_materialize_calc_tables_in_lakehouse") as materialize,
        patch.object(mod.fabric, "list_partitions") as list_partitions,
    ):
        mod.migrate_calc_tables_to_lakehouse(
            dataset="src", new_dataset="new", lakehouse="lh"
        )

    list_partitions.assert_not_called()
    return materialize


@pytest.mark.parametrize(
    "expressions",
    [{"DatabaseQuery": _ONELAKE_EXPRESSION}, {}],
    ids=["onelake", "blank"],
)
def test_migrate_calc_tables_to_lakehouse_is_a_no_op_unless_direct_lake_on_sql(
    expressions, capsys
):
    materialize = _run_migrate_calc_tables_to_lakehouse(
        _TargetTom(expressions=expressions)
    )

    materialize.assert_not_called()
    assert "deprecated" in capsys.readouterr().out


def test_migrate_calc_tables_to_lakehouse_materializes_for_direct_lake_on_sql(capsys):
    materialize = _run_migrate_calc_tables_to_lakehouse(
        _TargetTom(expressions={"DatabaseQuery": _SQL_EXPRESSION})
    )

    materialize.assert_called_once_with(
        dataset="src",
        new_dataset="new",
        workspace=None,
        new_dataset_workspace=None,
        lakehouse="lh",
        lakehouse_workspace=None,
    )
    assert "Direct Lake on SQL" in capsys.readouterr().out


def _dep_df(rows):
    return pd.DataFrame(
        rows,
        columns=[
            "Table Name",
            "Object Name",
            "Object Type",
            "Referenced Table",
            "Referenced Object",
        ],
    )


def test_expand_dependents_removes_calc_tables_referencing_removed_objects():
    from sempy_labs.semantic_model._direct_lake_migration import _expand_dependents

    dep_df = _dep_df(
        [
            ["Agg Copy", "Agg Copy", "Calc Table", "Agg", ""],
            ["Agg Copy 2", "Agg Copy 2", "Calc Table", "Agg Copy", ""],
            ["Summary", "Summary", "Calc Table", "Sales", "Amount"],
            ["Parameter", "Parameter", "Calc Table", "Agg", ""],
            ["Sales", "Copy Total", "Measure", "Agg Copy 2", "Value"],
        ]
    )
    removed_tables = {"Agg"}
    removed_columns, removed_measures, unsupported = set(), set(), []

    _expand_dependents(
        dep_df,
        removed_tables,
        removed_columns,
        removed_measures,
        unsupported,
        {"Agg Copy", "Agg Copy 2", "Summary"},
    )

    # Calculated tables referencing a removed object are removed (transitively),
    # those referencing migrated objects are kept and field parameters (not in
    # the calc_tables set) are left to the DAX rewrite.
    assert removed_tables == {"Agg", "Agg Copy", "Agg Copy 2"}
    assert removed_measures == {"Sales|Copy Total"}
    assert {u["name"] for u in unsupported if u["category"] == "Calculated table"} == {
        "Agg Copy",
        "Agg Copy 2",
    }


@pytest.mark.parametrize("reverse", [False, True], ids=["a-first", "b-first"])
def test_field_parameter_removals_iterate_until_stable(reverse):
    from sempy_labs.semantic_model._direct_lake_migration import (
        _resolve_field_parameter_removals,
    )

    # Removing 'Aggregation' empties Parameter A, which removes the calculated
    # table 'Copy' (which reads Parameter A), which Parameter B references.
    params = [
        ("Parameter A", "{\n\t(\"Order\", NAMEOF('Aggregation'[Order]), 0)\n}"),
        (
            "Parameter B",
            "{\n\t(\"Order\", NAMEOF('Copy'[Order]), 0),\n"
            "\t(\"Amount\", NAMEOF('Sales'[Amount]), 1)\n}",
        ),
    ]
    field_parameters = dict(reversed(params) if reverse else params)
    dep_df = _dep_df(
        [
            ["Copy", "Copy", "Calc Table", "Parameter A", "Order"],
            ["Sales", "Copy Count", "Measure", "Copy", "Order"],
        ]
    )
    removed_tables = {"Aggregation"}
    removed_columns, removed_measures, unsupported = set(), set(), []

    rewrites = _resolve_field_parameter_removals(
        dep_df,
        field_parameters,
        removed_tables,
        removed_columns,
        removed_measures,
        unsupported,
        {"Copy"},
    )

    assert removed_tables == {"Aggregation", "Parameter A", "Copy"}
    assert removed_measures == {"Sales|Copy Count"}
    assert list(field_parameters) == ["Parameter B"]
    assert "'Copy'" not in field_parameters["Parameter B"]
    assert "NAMEOF('Sales'[Amount])" in field_parameters["Parameter B"]
    assert rewrites == {"Parameter B": (dict(params)["Parameter B"], ["'Copy'[Order]"])}
    # Each removal is reported once.
    names = [u["name"] for u in unsupported]
    assert sorted(names) == ["Copy", "Copy Count", "Parameter A"]


def test_show_unsupported_objects_allows_relationships_on_calc_columns_in_calc_tables():
    from sempy_labs.directlake import _show_unsupported_directlake_objects as mod

    dfT = pd.DataFrame(
        {
            "Name": ["Sales", "Customer", "Segments"],
            "Type": ["Table", "Table", "Calculated Table"],
        }
    )
    dfC = pd.DataFrame(
        {
            "Table Name": ["Sales", "Customer", "Customer", "Segments"],
            "Column Name": ["CustKey", "CalcKey", "Segment", "SegmentKey"],
            "Type": ["Data", "Calculated", "Data", "Calculated"],
            "Data Type": ["Int64", "Int64", "String", "String"],
            "Source": ["CustKey", "[CustKey] * 1", "Segment", "[Segment]"],
        }
    )
    dfR = pd.DataFrame(
        {
            "From Table": ["Sales", "Customer"],
            "From Column": ["CustKey", "Segment"],
            "To Table": ["Customer", "Segments"],
            "To Column": ["CalcKey", "SegmentKey"],
        }
    )

    with (
        patch.object(
            mod, "resolve_workspace_name_and_id", return_value=("ws", "ws-id")
        ),
        patch.object(mod, "resolve_dataset_name_and_id", return_value=("ds", "ds-id")),
        patch.object(mod.fabric, "refresh_tom_cache"),
        patch.object(mod.fabric, "list_tables", return_value=dfT),
        patch.object(mod.fabric, "list_columns", return_value=dfC),
        patch.object(mod.fabric, "list_relationships", return_value=dfR),
    ):
        t, c, r = mod.show_unsupported_direct_lake_objects(dataset="ds")

    assert t["Table Name"].tolist() == ["Segments"]
    # Only the relationship on the Direct Lake table's calculated column is flagged.
    assert r["To Column"].tolist() == ["CalcKey"]


def test_migration_wizard_keeps_calculated_tables():
    source = (SRC / "semantic_model" / "_direct_lake_migration.py").read_text(
        encoding="utf-8"
    )

    assert "Calculated (DAX) tables are not supported in Direct Lake." not in source
    assert '"kind": "Calculated table",' in source
    assert '"calcTables": sorted(calc_tables),' in source
    assert '"kind": "calculatedTable",' in source
    assert 't.kind === "calculatedTable" ? IC.calculated_table' in source
    assert (
        '.replace("__IC_CALCULATED_TABLE__", _UI_ICONS["calculated_table"])' in source
    )
    assert "migrate_calc_tables_to_semantic_model," in source
    assert '"Migrate calculated tables",' in source


def test_migration_wizard_done_screen_links_to_model_and_flags_calc_tables():
    source = (SRC / "semantic_model" / "_direct_lake_migration.py").read_text(
        encoding="utf-8"
    )

    # The result payload carries the new model's ID and its calculated tables.
    assert '"datasetId": new_dataset_id,' in source
    assert '"calcTables": created_calc_tables,' in source
    assert '"datasetId": dataset_id,' in source
    assert '"calcTables": calc_tables,' in source
    # Link to the semantic model in the Power BI service (new tab).
    assert (
        "https://app.powerbi.com/groups/${encodeURIComponent(r.targetWorkspaceId)}"
        "/modeling/${encodeURIComponent(r.datasetId)}/modelView" in source
    )
    assert 'href="${esc(modelUrl)}" target="_blank" rel="noopener noreferrer"' in source
    # Calculated tables require a cloud connection before refreshing.
    assert "const calcTables = r.calcTables || [];" in source
    assert (
        "<b>Data access</b> &rarr; <b>Gateway and cloud connections</b> "
        "&rarr; <b>Cloud connections</b>" in source
    )
    assert "<b>Before refreshing the model</b>" in source
