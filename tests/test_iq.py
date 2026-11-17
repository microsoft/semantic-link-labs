from types import SimpleNamespace

import pytest

import sempy_labs.iq._items as iq_items


def _mock_base_api(monkeypatch, response_json):
    calls = []

    def fake_base_api(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(json=lambda: response_json)

    monkeypatch.setattr(iq_items, "_base_api", fake_base_api)
    return calls


def test_retrieve_context_sends_payload_and_flattens_results(monkeypatch):
    calls = _mock_base_api(
        monkeypatch,
        {
            "results": [
                {
                    "identifier": "abc",
                    "type": "application/agent-instructions",
                    "score": 0.87,
                    "tokenCount": 214,
                    "rank": 0,
                    "lexicalMatch": True,
                    "data": {
                        "summary": "Onboarding guidance",
                        "uri": {
                            "$type": "MicrosoftIQSkillReference",
                            "url": "https://example/doc.md",
                        },
                    },
                },
                {
                    "identifier": "def",
                    "type": "application/agent-instructions",
                    "score": 0.5,
                    "tokenCount": 10,
                    "rank": 1,
                    "lexicalMatch": False,
                    "data": {"summary": "No pointer", "uri": None},
                },
            ]
        },
    )

    df = iq_items.retrieve_context("How do I onboard?", token_budget=100)

    assert calls[0]["request"] == "/v1/microsoftIQ/retrieve"
    assert calls[0]["method"] == "post"
    assert calls[0]["payload"] == {
        "query": {"text": "How do I onboard?"},
        "tokenBudget": 100,
    }
    assert df["Identifier"].tolist() == ["abc", "def"]
    assert df.loc[0, "Reference Type"] == "MicrosoftIQSkillReference"
    assert df.loc[0, "Reference"] == {"url": "https://example/doc.md"}
    assert df.loc[1, "Reference Type"] is None
    assert df.loc[1, "Reference"] is None


def test_retrieve_context_empty_results_returns_empty_dataframe(monkeypatch):
    _mock_base_api(monkeypatch, {"results": []})

    df = iq_items.retrieve_context("anything")

    assert df.empty
    assert "Summary" in df.columns


@pytest.mark.parametrize(
    "query, token_budget", [("", 10), ("   ", 10), ("q", 0), ("q", True)]
)
def test_retrieve_context_rejects_invalid_input(monkeypatch, query, token_budget):
    calls = _mock_base_api(monkeypatch, {})

    with pytest.raises(ValueError):
        iq_items.retrieve_context(query, token_budget=token_budget)

    assert calls == []


@pytest.mark.parametrize("in_scope", [True, False])
def test_am_i_in_scope(monkeypatch, in_scope):
    calls = _mock_base_api(monkeypatch, {"inScope": in_scope})

    assert iq_items.am_i_in_scope() is in_scope
    assert calls[0]["request"] == "/v1/microsoftIQ/amIInScope"
    assert calls[0]["method"] == "post"
