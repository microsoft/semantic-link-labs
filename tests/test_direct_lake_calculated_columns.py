import sys
import types
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


def _fake_tom(supports_expression_context: bool):
    tom = types.ModuleType("Microsoft.AnalysisServices.Tabular")
    tom.ModeType = _Enum("Import", "DirectQuery", "DirectLake")

    class CalculatedColumn:
        pass

    if supports_expression_context:
        tom.ExpressionContext = _Enum("Standard", "UserContext")
        CalculatedColumn.ExpressionContext = None
    tom.CalculatedColumn = CalculatedColumn
    return tom


@pytest.fixture
def fake_dotnet(monkeypatch):
    """Installs fake 'Microsoft.AnalysisServices.Tabular' and 'System' modules."""

    def install(supports_expression_context=True):
        tom = _fake_tom(supports_expression_context)
        microsoft = types.ModuleType("Microsoft")
        analysis_services = types.ModuleType("Microsoft.AnalysisServices")
        microsoft.AnalysisServices = analysis_services
        analysis_services.Tabular = tom
        system = types.ModuleType("System")
        system.Enum = types.SimpleNamespace(
            Parse=lambda enum, value: getattr(enum, value)
        )
        monkeypatch.setitem(sys.modules, "Microsoft", microsoft)
        monkeypatch.setitem(
            sys.modules, "Microsoft.AnalysisServices", analysis_services
        )
        monkeypatch.setitem(sys.modules, "Microsoft.AnalysisServices.Tabular", tom)
        monkeypatch.setitem(sys.modules, "System", system)
        return tom

    return install


def _model(compat_level):
    return types.SimpleNamespace(
        Database=types.SimpleNamespace(CompatibilityLevel=compat_level)
    )


def test_supports_expression_context_detects_tom_support(fake_dotnet):
    from sempy_labs.tom._model import _supports_expression_context

    fake_dotnet(supports_expression_context=True)
    assert _supports_expression_context() is True

    fake_dotnet(supports_expression_context=False)
    assert _supports_expression_context() is False


def test_set_expression_context_sets_user_context_and_raises_compat_level(fake_dotnet):
    from sempy_labs.tom._model import _set_expression_context

    tom = fake_dotnet()
    model = _model(1604)
    column = tom.CalculatedColumn()

    _set_expression_context(model, column, "UserContext")

    assert column.ExpressionContext == tom.ExpressionContext.UserContext
    assert model.Database.CompatibilityLevel == 1705


def test_set_expression_context_keeps_higher_compat_level(fake_dotnet):
    from sempy_labs.tom._model import _set_expression_context

    tom = fake_dotnet()
    model = _model(1710)

    _set_expression_context(model, tom.CalculatedColumn(), "usercontext")

    assert model.Database.CompatibilityLevel == 1710


def test_set_expression_context_rejects_invalid_value(fake_dotnet):
    from sempy_labs.tom._model import _set_expression_context

    tom = fake_dotnet()
    model = _model(1604)

    with pytest.raises(ValueError, match="Invalid expression_context"):
        _set_expression_context(model, tom.CalculatedColumn(), "Materialized")
    assert model.Database.CompatibilityLevel == 1604


def test_set_expression_context_raises_when_tom_is_too_old(fake_dotnet):
    from sempy_labs.tom._model import _set_expression_context

    tom = fake_dotnet(supports_expression_context=False)

    with pytest.raises(ValueError, match="v19.117.0"):
        _set_expression_context(_model(1604), tom.CalculatedColumn(), "UserContext")


def test_is_direct_lake_table(fake_dotnet):
    from sempy_labs.tom._model import _is_direct_lake_table

    tom = fake_dotnet()
    dl = types.SimpleNamespace(
        Partitions=[types.SimpleNamespace(Mode=tom.ModeType.DirectLake)]
    )
    imp = types.SimpleNamespace(
        Partitions=[types.SimpleNamespace(Mode=tom.ModeType.Import)]
    )

    assert _is_direct_lake_table(dl) is True
    assert _is_direct_lake_table(imp) is False


def test_show_unsupported_objects_flags_relationships_on_calculated_columns():
    from sempy_labs.directlake import _show_unsupported_directlake_objects as mod

    dfT = pd.DataFrame({"Name": ["Sales", "Customer"], "Type": ["Table", "Table"]})
    dfC = pd.DataFrame(
        {
            "Table Name": ["Sales", "Sales", "Customer", "Customer"],
            "Column Name": ["CustKey", "Photo", "CustKey", "CalcKey"],
            "Type": ["Data", "Data", "Data", "Calculated"],
            "Data Type": ["Int64", "Binary", "Int64", "Int64"],
            "Source": ["CustKey", "Photo", "CustKey", "[CustKey] * 1"],
        }
    )
    dfR = pd.DataFrame(
        {
            "From Table": ["Sales", "Sales"],
            "From Column": ["CustKey", "CustKey"],
            "To Table": ["Customer", "Customer"],
            "To Column": ["CustKey", "CalcKey"],
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

    # Calculated columns are supported (User Context); only binary columns remain.
    assert c["Column Name"].tolist() == ["Photo"]
    assert len(r) == 1
    assert r.iloc[0]["To Column"] == "CalcKey"
    assert "calculated column" in r.iloc[0]["Reason"]


def test_migration_wizard_keeps_calculated_columns_with_user_context():
    source = (SRC / "semantic_model" / "_direct_lake_migration.py").read_text(
        encoding="utf-8"
    )

    assert "supports_expression_context = _supports_expression_context()" in source
    assert '"kind": "Calculated column",' in source
    assert '"category": "Relationship",' in source
    assert '_set_expression_context(tom.model, c, "UserContext")' in source
    assert (
        'const keptCc = changes.filter((c) => c.category === "kept" && c.kind === "Calculated column");'
        in source
    )
    assert "support the <b>User Context</b>" in source
    # Calculated columns are no longer reported as unsupported by default.
    assert "Calculated (DAX) columns are not supported in Direct Lake." not in source


def test_calculated_columns_within_calculated_tables_are_not_migrated():
    migration = SRC / "migration"

    # Only calculated columns within regular (data) tables are migrated.
    tables_columns = (
        migration / "_migrate_tables_columns_to_semantic_model.py"
    ).read_text(encoding="utf-8")
    assert (
        'dfC_calc = dfC[(dfC["Table Type"] == "Table") & (dfC["Type"] == "Calculated")]'
        in tables_columns
    )

    # Calculated tables only bring over their own (calculated table) columns.
    calc_tables = (migration / "_migrate_calctables_to_semantic_model.py").read_text(
        encoding="utf-8"
    )
    assert '& (dfC["Type"] == "CalculatedTableColumn")' in calc_tables

    # Calculated columns are not materialized into the calculated table's delta table.
    to_lakehouse = (migration / "_migrate_calctables_to_lakehouse.py").read_text(
        encoding="utf-8"
    )
    assert "ADDCOLUMNS(" not in to_lakehouse
    assert 'daxQuery = f"EVALUATE\\n{query}"' in to_lakehouse

    # The wizard does not keep calculated columns of calculated tables.
    wizard = (SRC / "semantic_model" / "_direct_lake_migration.py").read_text(
        encoding="utf-8"
    )
    assert (
        "# Only calculated columns within regular (data) tables are kept; those"
        in wizard
    )
