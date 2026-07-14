import sempy_labs._dax as dax


def test_evaluate_dax_impersonation_passes_dax_string(monkeypatch):
    dax_query = "EVALUATE ROW(\"x\", 1)"
    captured_kwargs = {}

    def _mock_evaluate_dax(**kwargs):
        captured_kwargs.update(kwargs)
        return "ok"

    monkeypatch.setattr(dax.fabric, "evaluate_dax", _mock_evaluate_dax)

    result = dax.evaluate_dax_impersonation(
        dataset="dataset-id",
        dax_query=dax_query,
        user_name="user@contoso.com",
        workspace="workspace-id",
    )

    assert result == "ok"
    assert captured_kwargs == {
        "dataset": "dataset-id",
        "dax_string": dax_query,
        "effective_user_name": "user@contoso.com",
        "workspace": "workspace-id",
    }
