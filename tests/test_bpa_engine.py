import pandas as pd

from sempy_labs.semantic_model import _bpa_engine as engine


class _Obj:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class _Tables(list):
    @property
    def Count(self):
        return len(self)


class _FakeTom:
    """A minimal stand-in for the TOM wrapper covering the scopes the engine uses."""

    def __init__(self):
        self.sales = _Obj(Name="Sales")
        self.date = _Obj(Name="date")
        self.columns = [
            _Obj(Name="Amount", Parent=self.sales),
            _Obj(Name="Qty", Parent=self.sales),
            _Obj(Name="Year", Parent=self.date),
        ]
        self.measures = [
            _Obj(
                Name="Total",
                Parent=self.sales,
                Expression="SUM([Amount]) + 'Sales'[Total Qty]",
            ),
            _Obj(Name="Total Qty", Parent=self.sales, Expression="SUM('Sales'[Qty])"),
        ]
        self.model = _Obj(
            Tables=_Tables([self.sales, self.date]), Relationships=[], Roles=[]
        )

    def all_columns(self):
        return iter(self.columns)

    def all_measures(self):
        return iter(self.measures)

    def all_calculated_columns(self):
        return iter([])

    def all_calculation_items(self):
        return iter([])

    def all_calculated_tables(self):
        return iter([])

    def all_hierarchies(self):
        return iter([])

    def all_partitions(self):
        return iter([])

    def all_rls(self):
        return iter([])

    def all_functions(self):
        return iter([])


def _rules():
    return pd.DataFrame(
        [
            (
                "Formatting",
                ["Table", "Column"],
                "Info",
                "First letter of objects must be capitalized",
                lambda obj, tom: obj.Name[0] != obj.Name[0].upper(),
                "The first letter of object names should be capitalized.",
                None,
            ),
            (
                "Maintenance",
                "Measure",
                "Warning",
                "Rule that raises",
                lambda obj, tom: obj.PropertyThatDoesNotExist,
                "Always raises when evaluated.",
                None,
            ),
        ],
        columns=[
            "Category",
            "Scope",
            "Severity",
            "Rule Name",
            "Expression",
            "Description",
            "URL",
        ],
    )


def test_rule_id_slugifies_the_rule_name():
    assert (
        engine._rule_id("Column references should be fully qualified")
        == "column_references_should_be_fully_qualified"
    )
    assert (
        engine._rule_id("Set 'Data Coverage Definition' property")
        == "set_data_coverage_definition_property"
    )


def test_rules_payload_flags_fixable_rules():
    payload = engine.rules_payload(_rules())

    by_name = {r["name"]: r for r in payload}
    assert by_name["First letter of objects must be capitalized"]["fixable"] is True
    assert by_name["First letter of objects must be capitalized"]["scopes"] == [
        "Table",
        "Column",
    ]
    assert by_name["Rule that raises"]["fixable"] is False


def test_scan_model_returns_violations_and_ignores_failing_rules():
    violations = engine.scan_model(_FakeTom(), _rules())

    assert [(v["objectType"], v["objectName"]) for v in violations] == [
        ("Table", "date")
    ]
    assert violations[0]["ruleName"] == "First letter of objects must be capitalized"


def test_scan_model_skips_disabled_rules():
    violations = engine.scan_model(
        _FakeTom(),
        _rules(),
        disabled_rule_ids=["first_letter_of_objects_must_be_capitalized"],
    )

    assert violations == []


def test_qualify_column_references_only_qualifies_unambiguous_columns():
    fixes = engine._qualify_column_references(_FakeTom())

    assert [(f["objectName"], f["after"]) for f in fixes] == [
        ("Total", "SUM('Sales'[Amount]) + 'Sales'[Total Qty]")
    ]


def test_unqualify_measure_references_strips_the_table_name():
    fixes = engine._unqualify_measure_references(_FakeTom())

    assert [(f["objectName"], f["after"]) for f in fixes] == [
        ("Total", "SUM([Amount]) + [Total Qty]")
    ]


def test_apply_fixes_renames_the_flagged_object():
    tom = _FakeTom()
    rules = _rules()

    applied = engine.apply_fixes(
        tom, rules, "First letter of objects must be capitalized"
    )

    assert applied == 1
    assert tom.date.Name == "Date"


def test_apply_fixes_honors_the_object_name_filter():
    tom = _FakeTom()
    rules = _rules()

    applied = engine.apply_fixes(
        tom,
        rules,
        "First letter of objects must be capitalized",
        object_names=["other"],
    )

    assert applied == 0
    assert tom.date.Name == "date"


def test_collect_fixes_returns_nothing_for_a_rule_without_a_fix():
    assert engine.collect_fixes(_FakeTom(), _rules(), "Rule that raises") == []
