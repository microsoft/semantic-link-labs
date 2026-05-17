---
name: run-tests
description: Guide for running pytest tests locally. Use this when running tests to verify code changes.
---

# Running Tests

This skill covers running pytest tests for the Semantic Link Labs project.

## When to Use This Skill

Use this skill when you need to:
- Run unit tests to verify code changes
- Run specific tests by name
- Debug failing tests
- Validate changes before committing

---

## Test Framework

| Component | Details |
|-----------|---------|
| **Framework** | pytest |
| **Location** | `tests/` directory |
| **Configuration** | `pyproject.toml` |

---

## Prerequisites

### Install Development Dependencies

```bash
# Install package in editable mode with test dependencies
pip install -e ".[test]"
```

### Environment Setup

Create conda environment from `environment.yml`:

```bash
conda env create -f environment.yml
conda activate fabric
pip install -e .
```

---

## Running Tests

### Basic Commands

```bash
# Run all tests
pytest -s tests/

# Run with verbose output
pytest -sv tests/

# Run specific test file
pytest -s tests/test_example.py

# Run specific test by name pattern
pytest -s tests/ -k test_my_function

# Run multiple tests by pattern
pytest -s tests/ -k "test_list or test_create"

# Stop on first failure
pytest -sx tests/
```

### Test Discovery

pytest automatically discovers tests in:
- Files named `test_*.py` or `*_test.py`
- Functions named `test_*`
- Classes named `Test*`

---

## Test Output Options

### Verbose Output

```bash
# Show test names and results
pytest -v tests/

# Show test names with print statements
pytest -sv tests/
```

### Show Print Statements

The `-s` flag captures stdout:

```bash
# Show print output during tests
pytest -s tests/
```

### Show Test Durations

```bash
# Show slowest 10 tests
pytest --durations=10 tests/
```

---

## Filtering Tests

### By Test Name

```bash
# Run tests matching pattern
pytest -k "workspace" tests/

# Run tests NOT matching pattern
pytest -k "not slow" tests/

# Combine patterns
pytest -k "workspace and not admin" tests/
```

### By File

```bash
# Run specific test file
pytest tests/test_workspaces.py

# Run tests in directory
pytest tests/admin/
```

---

## Debugging Failed Tests

### Stop on First Failure

```bash
pytest -x tests/
```

### Enter Debugger on Failure

```bash
pytest --pdb tests/
```

### Show Local Variables on Failure

```bash
pytest -l tests/
```

### Increase Verbosity

```bash
pytest -vvv tests/
```

---

## Test Structure

### Basic Test Example

```python
import pytest
import pandas as pd


def test_my_function_returns_dataframe():
    """Test that my_function returns a DataFrame."""
    from sempy_labs import my_function

    result = my_function()

    assert isinstance(result, pd.DataFrame)


def test_my_function_with_parameter():
    """Test my_function with specific parameter."""
    from sempy_labs import my_function

    result = my_function(workspace="Test Workspace")

    assert not result.empty
    assert "Name" in result.columns
```

### Test with Expected Exception

```python
def test_my_function_raises_on_invalid_input():
    """Test that my_function raises ValueError on invalid input."""
    from sempy_labs import my_function

    with pytest.raises(ValueError, match="Invalid"):
        my_function(invalid_param="bad value")
```

---

## CI/CD Integration

The project uses GitHub Actions for CI. See `.github/workflows/build.yaml`:

```yaml
- name: Test with pytest
  shell: bash -el {0}
  run: |
    pytest -s tests/
```

---

## Pre-Commit Test Checklist

Before committing code changes:

```bash
# Run all tests
pytest -s tests/

# Run tests for modified area
pytest -s tests/ -k relevant_test_pattern

# Check for any failures
# If failures, fix code and re-run
```

---

## Common Test Issues

### Import Errors

If tests fail with import errors:

```bash
# Ensure package is installed in editable mode
pip install -e .
```

### Missing Dependencies

If tests fail with missing package:

```bash
# Install test dependencies
pip install -e ".[test]"
```

### Environment Issues

If tests behave unexpectedly:

```bash
# Recreate conda environment
conda env remove -n fabric
conda env create -f environment.yml
conda activate fabric
pip install -e .
```
