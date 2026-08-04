import ast
from pathlib import Path


ROOT = Path(__file__).parents[1]
BPA_PATH = ROOT / "src/sempy_labs/semantic_model/_bpa.py"
UI_COMPONENTS_PATH = ROOT / "src/sempy_labs/_ui_components.py"
BPA_SOURCE = BPA_PATH.read_text(encoding="utf-8")
UI_COMPONENTS_SOURCE = UI_COMPONENTS_PATH.read_text(encoding="utf-8")


def test_rule_editor_sources_are_valid_python():
    ast.parse(BPA_SOURCE)
    ast.parse(UI_COMPONENTS_SOURCE)


def test_rule_editor_rules_are_warmed_after_initial_display():
    assert "_catalog_lock = threading.Lock()" in BPA_SOURCE
    assert "with _catalog_lock:" in BPA_SOURCE
    assert "def _warm_rule_editor_rules():" in BPA_SOURCE
    display_index = BPA_SOURCE.index("    display(widget)")
    warm_index = BPA_SOURCE.index(
        "threading.Thread(target=_warm_rule_editor_rules, daemon=True).start()"
    )
    assert display_index < warm_index


def test_rule_editor_confirms_default_reset():
    assert "function openResetRulesConfirm()" in BPA_SOURCE
    assert 'heading.textContent = "Restore default rules?"' in BPA_SOURCE
    assert (
        'makeButton("Restore defaults", "slls-bpa-btn-sm '
        'slls-bpa-btn-danger", ICON.reset)' in BPA_SOURCE
    )
    assert 'resetBtn.addEventListener("click", openResetRulesConfirm)' in BPA_SOURCE
    assert 'confirmBtn.addEventListener("click", () =>' in BPA_SOURCE


def test_rule_editor_uses_top_right_close_button():
    assert ".slls-bpa-rule-editor-close" in BPA_SOURCE
    assert 'modal.className = "slls-bpa-modal slls-bpa-rule-editor-modal"' in BPA_SOURCE
    assert 'slls-bpa-rule-editor-close", ICON.close)' in BPA_SOURCE
    assert 'closeBtn.title = "Close rule editor"' in BPA_SOURCE
    rule_editor = BPA_SOURCE.split("function openRulesPanel()", 1)[1].split(
        "// ------------------------------------------------------------------\n    // Screen switching", 1
    )[0]
    assert 'makeButton("Close", "")' not in rule_editor


def test_rule_editor_deletes_rules_through_active_ruleset():
    assert 'trash: `__SLLS_ICON_TRASH__`' in BPA_SOURCE
    assert '.replace("__SLLS_ICON_TRASH__", _UI_ICONS["trash"])' in BPA_SOURCE
    assert '"trash": (' in UI_COMPONENTS_SOURCE
    assert 'remaining = (model.get("rules") || []).filter' in BPA_SOURCE
    assert 'deleteBtn.disabled = (model.get("rules") || []).length <= 1' in BPA_SOURCE
    assert '"At least one rule must remain"' in BPA_SOURCE
    assert 'ruleset: { source: "custom", rules: rulesetEntries(remaining) }' in BPA_SOURCE
    assert 'history: { label: `Deleted "${rule.name}"`' in BPA_SOURCE
    assert 'runAction("set_ruleset", {' in BPA_SOURCE
    assert "silent: true" in BPA_SOURCE
    assert 'message: `Deleted "${rule.name}".`' not in BPA_SOURCE
    assert 'if not payload.get("silent"):' in BPA_SOURCE


def test_rule_editor_prompts_download_after_rule_changes():
    assert ".slls-bpa-rule-download-needed" in BPA_SOURCE
    assert ".slls-bpa-rule-download-hint" not in BPA_SOURCE
    assert ".slls-bpa-rule-download-cue" in BPA_SOURCE
    assert "Rules have changed. Use the highlighted Download rules button" in BPA_SOURCE
    assert 'const downloadCue = document.createElement("div")' in BPA_SOURCE
    assert "modal.appendChild(downloadCue)" in BPA_SOURCE
    assert "bar.appendChild(downloadCue)" not in BPA_SOURCE
    assert "let rulesNeedExport = false" in BPA_SOURCE
    assert "function markRulesChanged()" in BPA_SOURCE
    assert BPA_SOURCE.count("markRulesChanged();") >= 7
    assert 'exportBtn.setAttribute("data-rules-export", "")' in BPA_SOURCE
    assert 'exportBtn.classList.toggle("slls-bpa-rule-download-needed", rulesNeedExport)' in BPA_SOURCE
    assert 'button.classList.add("slls-bpa-rule-download-needed")' in BPA_SOURCE
    assert 'button.title = "Download your changed rules for future use"' in BPA_SOURCE
    assert BPA_SOURCE.count('button.setAttribute("aria-label", button.title)') == 2
    assert "function markRulesExported()" in BPA_SOURCE
    assert 'button.classList.remove("slls-bpa-rule-download-needed")' in BPA_SOURCE
    assert 'if (cue) cue.classList.add("show")' in BPA_SOURCE
    assert 'if (cue) cue.classList.remove("show")' in BPA_SOURCE


def test_rule_editor_shows_transient_status_inside_editor():
    assert ".slls-bpa-rule-status" in BPA_SOURCE
    assert 'ruleStatus.setAttribute("role", "status")' in BPA_SOURCE
    assert "activeRuleStatus = ruleStatus" in BPA_SOURCE
    assert 'activeRuleStatus && overlay.classList.contains("show")' in BPA_SOURCE
    assert 'setStatus("", "")' in BPA_SOURCE
    assert "activeRuleStatus.textContent = s.message" in BPA_SOURCE
    assert "}, 3500)" in BPA_SOURCE
