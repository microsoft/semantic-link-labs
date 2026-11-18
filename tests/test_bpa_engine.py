import pandas as pd
import pytest

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


def test_scan_model_stops_when_cancelled():
    calls = []

    def should_cancel():
        calls.append(1)
        # Stops before the first rule is evaluated.
        return True

    violations = engine.scan_model(_FakeTom(), _rules(), should_cancel=should_cancel)

    assert violations == []
    assert len(calls) == 1


def test_scan_model_runs_to_completion_when_not_cancelled():
    violations = engine.scan_model(_FakeTom(), _rules(), should_cancel=lambda: False)

    assert [v["objectName"] for v in violations] == ["date"]


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


def test_rules_to_json_round_trips_through_parse_rules_json():
    defaults = _rules()

    exported = engine.rules_to_json(defaults, disabled_rule_ids=["rule_that_raises"])
    parsed, disabled = engine.parse_rules_json(exported, defaults)

    assert exported[0]["ID"] == "FIRST_LETTER_OF_OBJECTS_MUST_BE_CAPITALIZED"
    assert exported[0]["Severity"] == 1
    assert exported[0]["Scope"] == "Table, Column"
    assert list(parsed["Rule Name"]) == list(defaults["Rule Name"])
    assert disabled == ["rule_that_raises"]


def test_rules_to_json_includes_the_expression_and_fix_expression():
    exported = engine.rules_to_json(_rules())

    capitalize = exported[0]
    # The expression is the source of the rule's predicate, read from the rules
    # module rather than from the (stubbed) test predicate.
    assert capitalize["Expression"] == (
        "lambda obj, tom: obj.Name[0] != obj.Name[0].upper()"
    )
    assert capitalize["FixExpression"] == (
        "obj.Name = obj.Name[0].upper() + obj.Name[1:]"
    )
    # A rule without an automatic fix omits FixExpression entirely.
    assert "FixExpression" not in exported[1]


def test_rule_expression_falls_back_to_the_predicate_source():
    def predicate(obj, tom):
        return True

    # A rule that is not one of the built-ins still reports its own source.
    assert engine.rule_expression("Not a built-in rule", predicate) == ""
    assert engine.rule_expression(
        "Not a built-in rule", lambda obj, tom: obj.IsHidden
    ).startswith("lambda obj, tom:")


def test_rules_payload_includes_the_expression_and_fix_expression():
    payload = {r["name"]: r for r in engine.rules_payload(_rules())}

    capitalize = payload["First letter of objects must be capitalized"]
    assert capitalize["expression"].startswith("lambda obj, tom:")
    assert capitalize["fixExpression"]
    assert payload["Rule that raises"]["fixExpression"] == ""


def test_every_built_in_rule_has_an_expression():
    expressions = engine._builtin_rule_expressions()

    # Guards against a rule being added whose logic cannot be recovered.
    assert len(expressions) > 50
    assert all(e.startswith("lambda obj, tom:") for e in expressions.values())
    assert set(engine.RULE_FIX_EXPRESSIONS) == engine.FIXABLE_RULES
    assert engine.FIXABLE_RULES.issubset(expressions)


def test_every_fixable_rule_name_matches_a_real_rule():
    # A renamed rule would otherwise silently lose its "Apply fix" button and its
    # FixExpression, because both are keyed by the rule name.
    assert engine.FIXABLE_RULES.issubset(set(engine._builtin_rule_expressions()))


def test_parse_rules_json_matches_names_with_a_category_prefix():
    parsed, _ = engine.parse_rules_json(
        [
            {
                "ID": "SOME_OTHER_ID",
                "Name": "[Formatting] First letter of objects must be capitalized",
                "Severity": 3,
                "Description": "Overridden.",
            }
        ],
        _rules(),
    )

    assert list(parsed["Rule Name"]) == ["First letter of objects must be capitalized"]
    assert parsed.iloc[0]["Severity"] == "Error"
    assert parsed.iloc[0]["Description"] == "Overridden."


def test_parse_rules_json_ignores_unknown_rules_and_scopes():
    parsed, _ = engine.parse_rules_json(
        [
            {"ID": "NOT_A_REAL_RULE", "Name": "[X] Nope"},
            {
                "Name": "Rule that raises",
                # A Tabular Editor scope this library cannot evaluate.
                "Scope": "DataColumn, CalculatedColumn",
            },
        ],
        _rules(),
    )

    assert list(parsed["Rule Name"]) == ["Rule that raises"]
    # Falls back to the built-in scope rather than an unusable one.
    assert parsed.iloc[0]["Scope"] == "Measure"


def test_normalize_rules_accepts_none_dataframe_list_and_dict():
    defaults = _rules()
    entries = engine.rules_to_json(defaults)

    assert engine.normalize_rules(None, defaults) is defaults
    assert engine.normalize_rules(defaults, defaults) is defaults
    assert len(engine.normalize_rules(entries, defaults)) == len(defaults)
    assert len(engine.normalize_rules({"rules": entries}, defaults)) == len(defaults)


def test_normalize_rules_raises_when_nothing_matches():
    with pytest.raises(ValueError):
        engine.normalize_rules([{"ID": "NOT_A_REAL_RULE"}], _rules())


def test_validate_rules_json_accepts_a_valid_ruleset():
    defaults = _rules()

    errors, warnings = engine.validate_rules_json(
        engine.rules_to_json(defaults), defaults
    )

    assert errors == []
    assert warnings == []


def test_validate_rules_json_reports_an_unusable_file():
    defaults = _rules()

    assert engine.validate_rules_json("not a ruleset", defaults)[0]
    assert engine.validate_rules_json([], defaults)[0]
    # Nothing matched a built-in rule, so there is nothing to import.
    errors, warnings = engine.validate_rules_json(
        [{"ID": "NOT_A_REAL_RULE", "Name": "Nope"}], defaults
    )
    assert len(errors) == 1
    assert len(warnings) == 1


def test_validate_rules_json_reports_every_problem_it_finds():
    defaults = _rules()

    errors, warnings = engine.validate_rules_json(
        [
            {"Name": "First letter of objects must be capitalized"},
            # A duplicate of the entry above.
            {"ID": "FIRST_LETTER_OF_OBJECTS_MUST_BE_CAPITALIZED"},
            {
                "Name": "Rule that raises",
                "Severity": "Critical",
                "Scope": "DataColumn",
                "Enabled": "yes",
            },
            "not a rule object",
            {"Category": "Formatting"},
        ],
        defaults,
    )

    assert errors == []
    assert len(warnings) == 6
    assert any("duplicate" in w for w in warnings)
    assert any("'Severity'" in w for w in warnings)
    assert any("'Scope'" in w for w in warnings)
    assert any("'Enabled'" in w for w in warnings)
    assert any("no 'ID' or 'Name'" in w for w in warnings)
    assert any("not a rule object" in w for w in warnings)


def _fake_translations(monkeypatch):
    po = {
        ("Category", "First letter of objects must be capitalized"): "Formattazione",
        (
            "Rule Name",
            "First letter of objects must be capitalized",
        ): "Iniziale maiuscola",
        (
            "Description",
            "First letter of objects must be capitalized",
        ): "Descrizione tradotta.",
    }
    machine_calls = []

    def fake_spark(texts, language):
        machine_calls.append(list(texts))
        return {t: f"[{language}] {t}" for t in texts}

    monkeypatch.setattr(engine, "_po_translations", lambda language: po)
    monkeypatch.setattr(engine, "_spark_translations", fake_spark)
    return machine_calls


def test_translator_localizes_violations_and_maps_fixes_back(monkeypatch):
    machine_calls = _fake_translations(monkeypatch)
    rules = _rules().iloc[[0]].reset_index(drop=True)
    translator = engine.RuleTranslator("it-IT")

    violations = translator.localize_violations(
        engine.scan_model(_FakeTom(), rules), rules
    )

    assert machine_calls == []
    assert [v["objectName"] for v in violations] == ["date"]
    for v in violations:
        assert v["ruleName"] == "Iniziale maiuscola"
        assert v["category"] == "Formattazione"
        assert v["categoryKey"] == "Formatting"
        assert v["description"] == "Descrizione tradotta."
        assert v["ruleId"] == "first_letter_of_objects_must_be_capitalized"
        assert v["fixable"] is True

    source = translator.source_rule_name("Iniziale maiuscola")
    assert source == "First letter of objects must be capitalized"
    assert engine.preview_fixes(_FakeTom(), rules, source)


def test_translator_machine_translates_text_changed_from_the_built_in(monkeypatch):
    machine_calls = _fake_translations(monkeypatch)
    defaults = _rules()
    custom = defaults.copy()
    custom.loc[0, "Description"] = "My own description."
    translator = engine.RuleTranslator("it-IT")

    payload = translator.localize_payload(engine.rules_payload(custom), defaults)

    by_id = {p["id"]: p for p in payload}
    first = by_id["first_letter_of_objects_must_be_capitalized"]
    assert first["name"] == "Iniziale maiuscola"
    assert first["description"] == "[it-IT] My own description."
    assert first["source"] == {
        "name": "First letter of objects must be capitalized",
        "category": "Formatting",
        "description": "My own description.",
    }
    # Built-in text without a bundled translation stays as-is.
    assert by_id["rule_that_raises"]["name"] == "Rule that raises"
    assert machine_calls == [["My own description."]]


def test_translator_switches_language_and_back_to_english(monkeypatch):
    machine_calls = _fake_translations(monkeypatch)
    rules = _rules().iloc[[0]].reset_index(drop=True)
    english = engine.scan_model(_FakeTom(), rules)
    monkeypatch.setattr(
        engine,
        "_po_translations",
        lambda language: {
            ("Rule Name", "First letter of objects must be capitalized"): language
        },
    )
    translator = engine.RuleTranslator("it-IT")
    italian = translator.localize_violations(english, rules)
    assert italian[0]["ruleName"] == "it-IT"

    translator.language = "de-DE"
    german = translator.localize_violations(italian, rules)
    assert german[0]["ruleName"] == "de-DE"

    # An unsupported language machine-translates the English text, not the
    # German text on screen.
    translator.language = "xx-XX"
    other = translator.localize_violations(german, rules)
    assert other[0]["ruleName"] == (
        "[xx-XX] First letter of objects must be capitalized"
    )
    assert other[0]["categoryKey"] == "Formatting"
    assert "First letter of objects must be capitalized" in machine_calls[0]

    translator.language = None
    assert translator.localize_violations(other, rules) == english


def test_language_options_list_english_and_bundled_languages():
    from sempy_labs.semantic_model._bpa import _language_options

    options = _language_options()
    values = [o["value"] for o in options]

    assert options[0] == {"value": "", "label": "English"}
    assert {"value": "it-IT", "label": "Italian (it-IT)"} in options
    assert set(values[1:]) == set(engine.supported_languages())
    assert _language_options("xx-XX")[-1] == {"value": "xx-XX", "label": "xx-XX"}
    assert _language_options("it-IT") == options


def test_translator_gives_every_rule_of_a_category_the_same_translation(monkeypatch):
    rules = _rules()
    rules.loc[1, "Category"] = "Formatting"
    # Only one of the two Formatting rules has a bundled category translation.
    monkeypatch.setattr(
        engine,
        "_po_translations",
        lambda language: {
            ("Category", "First letter of objects must be capitalized"): "Formato"
        },
    )
    translator = engine.RuleTranslator("it-IT")

    payload = translator.localize_payload(engine.rules_payload(rules), rules)

    assert {p["category"] for p in payload} == {"Formato"}


def test_ui_strings_cover_every_supported_language():
    from sempy_labs.semantic_model._bpa_ui_strings import _KEYS, ui_strings

    for language in engine.supported_languages():
        strings = ui_strings(language)
        assert set(strings) == set(_KEYS), language
        assert all(strings.values()), language
    assert ui_strings(None) == {}
    assert ui_strings("xx-XX") == {}


def test_every_built_in_rule_has_bundled_translations():
    import ast

    # The rules module needs the Analysis Services client to run, so the rule
    # names and descriptions are read from its source.
    with open(engine._RULES_MODULE, "r", encoding="utf-8") as f:
        tree = ast.parse(f.read())
    rules = {}
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Tuple)
            and len(node.elts) >= 5
            and isinstance(node.elts[2], ast.Constant)
            and node.elts[2].value in ("Error", "Warning", "Info")
            and isinstance(node.elts[3], ast.Constant)
        ):
            has_description = len(node.elts) > 5 and isinstance(
                node.elts[5], ast.Constant
            )
            rules[node.elts[3].value] = has_description

    for language in engine.supported_languages():
        po = engine._po_translations(language)
        for name, has_description in rules.items():
            assert ("Rule Name", name) in po, (language, name)
            assert ("Category", name) in po, (language, name)
            if has_description:
                description = po.get(("Description", name), "")
                assert description and not description.startswith("http"), (
                    language,
                    name,
                )
