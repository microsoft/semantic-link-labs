import pandas as pd

import sempy_labs._generate_semantic_model as generate_semantic_model
from sempy_labs import get_semantic_model_size


def _mock_evaluate_dax_factory(dictionary_size_values, used_size_values):
    def _mock_evaluate_dax(dataset, workspace, dax_string):
        if "DICTIONARY_SIZE" in dax_string:
            return pd.DataFrame({"[DICTIONARY_SIZE]": dictionary_size_values})
        return pd.DataFrame({"[USED_SIZE]": used_size_values})

    return _mock_evaluate_dax


def test_get_semantic_model_size_returns_raw_byte_sum(monkeypatch):
    """The result should be the plain sum of dictionary and used sizes,
    with no unit rescaling, matching the documented "size in bytes"."""
    monkeypatch.setattr(
        generate_semantic_model.fabric,
        "evaluate_dax",
        _mock_evaluate_dax_factory([500], [500]),
    )

    result = get_semantic_model_size(dataset="test-dataset")

    assert result == 1000
    assert isinstance(result, int)


def test_get_semantic_model_size_is_monotonic(monkeypatch):
    """A larger contributing size should never produce a smaller result -
    the previous KiB/KB rescaling broke this for values crossing 1024."""
    monkeypatch.setattr(
        generate_semantic_model.fabric,
        "evaluate_dax",
        _mock_evaluate_dax_factory([499], [500]),
    )
    smaller = get_semantic_model_size(dataset="test-dataset")

    monkeypatch.setattr(
        generate_semantic_model.fabric,
        "evaluate_dax",
        _mock_evaluate_dax_factory([500], [500]),
    )
    larger = get_semantic_model_size(dataset="test-dataset")

    assert smaller == 999
    assert larger == 1000
    assert larger > smaller
