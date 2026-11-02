---
name: write-tests
description: Guide for writing unit tests. Use this when creating tests to verify Python logic.
---

# Writing Tests

This skill covers how to write tests for the Semantic Link Labs project.

## When to Use This Skill

Use this skill when you need to:
- Write tests for new functions
- Add test coverage for existing code
- Test error handling and edge cases
- Validate input parsing and data transformations

---

## Test Framework

| Component | Details |
|-----------|---------|
| **Framework** | pytest |
| **Location** | `tests/` directory |
| **Assertions** | Standard pytest assertions |

---

## Test File Structure

```
tests/
├── __init__.py
├── test_helper_functions.py
├── test_workspaces.py
├── test_admin.py
└── ...
```

### Naming Conventions

- Test files: `test_*.py` or `*_test.py`
- Test functions: `test_<function_name>_<scenario>`
- Test classes: `Test<ClassName>`

---

## Writing Basic Tests

### Simple Function Test

```python
import pytest
import pandas as pd


def test_my_function_returns_dataframe():
    """Test that my_function returns a DataFrame."""
    from sempy_labs import my_function

    result = my_function()

    assert isinstance(result, pd.DataFrame)


def test_my_function_has_expected_columns():
    """Test that result has expected columns."""
    from sempy_labs import my_function

    result = my_function()

    expected_columns = ["Id", "Name", "Type"]
    for col in expected_columns:
        assert col in result.columns
```

### Test with Parameters

```python
def test_my_function_filters_by_type():
    """Test that my_function filters by item_type."""
    from sempy_labs import my_function

    result = my_function(item_type="Report")

    assert all(result["Type"] == "Report")
```

---

## Testing Error Handling

### Expected Exceptions

```python
def test_my_function_raises_on_invalid_workspace():
    """Test that invalid workspace raises ValueError."""
    from sempy_labs import my_function

    with pytest.raises(ValueError, match="Invalid workspace"):
        my_function(workspace="NonExistent")


def test_my_function_raises_on_missing_parameter():
    """Test that missing required parameter raises error."""
    from sempy_labs import my_function

    with pytest.raises(TypeError):
        my_function()  # Missing required parameter
```

### Exception Message Matching

```python
def test_error_message_is_descriptive():
    """Test that error message contains helpful information."""
    from sempy_labs import my_function

    with pytest.raises(ValueError) as exc_info:
        my_function(invalid_param="bad")

    assert "invalid_param" in str(exc_info.value)
    assert "bad" in str(exc_info.value)
```

---

## Using Fixtures

### Basic Fixture

```python
import pytest
import pandas as pd


@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame({
        "Id": ["1", "2", "3"],
        "Name": ["Item A", "Item B", "Item C"],
        "Type": ["Report", "Dataset", "Report"],
    })


def test_filter_function(sample_dataframe):
    """Test filtering function with sample data."""
    from sempy_labs._helper_functions import filter_items

    result = filter_items(sample_dataframe, type="Report")

    assert len(result) == 2
    assert all(result["Type"] == "Report")
```

### Fixture with Parameters

```python
@pytest.fixture(params=["Report", "Dataset", "Lakehouse"])
def item_type(request):
    """Parameterized fixture for item types."""
    return request.param


def test_with_different_item_types(item_type):
    """Test function with different item types."""
    from sempy_labs import my_function

    result = my_function(item_type=item_type)

    assert isinstance(result, pd.DataFrame)
```

---

## Mocking External Dependencies

### Mocking API Calls

```python
from unittest.mock import patch, MagicMock


def test_function_with_mocked_api():
    """Test with mocked API response."""
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "value": [
            {"id": "123", "name": "Test Item"}
        ]
    }
    mock_response.status_code = 200

    with patch('sempy_labs._helper_functions._base_api') as mock_api:
        mock_api.return_value = mock_response

        from sempy_labs import list_items
        result = list_items()

        assert len(result) == 1
        assert result.iloc[0]["Name"] == "Test Item"
```

### Mocking Fabric Client

```python
from unittest.mock import patch


def test_function_with_mocked_fabric():
    """Test with mocked sempy.fabric calls."""
    with patch('sempy.fabric.resolve_workspace_id') as mock_resolve:
        mock_resolve.return_value = "12345678-1234-1234-1234-123456789012"

        from sempy_labs._helper_functions import resolve_workspace_id
        result = resolve_workspace_id("My Workspace")

        mock_resolve.assert_called_once()
```

---

## Parameterized Tests

### Multiple Input Values

```python
import pytest


@pytest.mark.parametrize("input_value,expected", [
    ("value1", "result1"),
    ("value2", "result2"),
    ("value3", "result3"),
])
def test_function_with_multiple_inputs(input_value, expected):
    """Test function with multiple input values."""
    from sempy_labs import my_function

    result = my_function(input_value)

    assert result == expected
```

### Testing Edge Cases

```python
@pytest.mark.parametrize("workspace", [
    None,           # Default workspace
    "My Workspace", # By name
    "12345678-1234-1234-1234-123456789012",  # By UUID string
])
def test_accepts_various_workspace_formats(workspace):
    """Test that function accepts various workspace formats."""
    from sempy_labs import my_function

    # Should not raise
    result = my_function(workspace=workspace)
    assert result is not None
```

---

## Testing DataFrame Results

### Column Validation

```python
def test_result_has_required_columns():
    """Test that result DataFrame has required columns."""
    from sempy_labs import list_items

    result = list_items()

    required_columns = ["Id", "Name", "Type"]
    for col in required_columns:
        assert col in result.columns, f"Missing column: {col}"
```

### Data Type Validation

```python
def test_result_column_types():
    """Test that result columns have correct types."""
    from sempy_labs import list_items

    result = list_items()

    assert result["Id"].dtype == "object"  # string
    assert result["Name"].dtype == "object"  # string
```

### Empty Result Handling

```python
def test_handles_empty_result():
    """Test that function handles empty results gracefully."""
    from sempy_labs import list_items

    result = list_items(item_type="NonExistentType")

    assert isinstance(result, pd.DataFrame)
    assert result.empty
    # Columns should still exist even if empty
    assert "Id" in result.columns
```

---

## Test Organization

### Grouping Related Tests

```python
class TestWorkspaceFunctions:
    """Tests for workspace-related functions."""

    def test_list_workspaces(self):
        """Test listing workspaces."""
        pass

    def test_resolve_workspace_id(self):
        """Test resolving workspace ID."""
        pass

    def test_resolve_workspace_name(self):
        """Test resolving workspace name."""
        pass


class TestHelperFunctions:
    """Tests for helper utility functions."""

    def test_is_valid_uuid(self):
        """Test UUID validation."""
        pass

    def test_create_dataframe(self):
        """Test DataFrame creation helper."""
        pass
```

---

## Best Practices

### Do's

- ✅ Use descriptive test names
- ✅ Test one thing per test function
- ✅ Include docstrings explaining what's being tested
- ✅ Use fixtures for reusable setup
- ✅ Test both success and failure cases
- ✅ Mock external dependencies when needed

### Don'ts

- ❌ Don't test multiple behaviors in one test
- ❌ Don't rely on external services in unit tests
- ❌ Don't use hardcoded secrets or credentials
- ❌ Don't write tests that depend on test execution order
- ❌ Don't ignore flaky tests - fix them

---

## Pre-Commit Test Checklist

Before committing new tests:

1. Run the tests locally:
   ```bash
   pytest -sv tests/ -k my_new_test
   ```

2. Verify tests pass consistently (run multiple times)

3. Check test coverage for the new code

4. Ensure tests are independent and don't rely on each other
