# Semantic Link Labs Development Agent Guide

This guide provides comprehensive instructions for AI agents working on the Semantic Link Labs (sempy_labs) codebase — an open-source Python library that extends the capabilities of [Semantic Link](https://learn.microsoft.com/en-us/fabric/data-science/semantic-link-overview) for Microsoft Fabric notebooks.

## Project Overview

Semantic Link Labs is a community-driven extension library designed for use in [Microsoft Fabric notebooks](https://learn.microsoft.com/fabric/data-engineering/how-to-use-notebook). It provides additional functionalities to work with:

- **Semantic models (datasets)** — Management, analysis, and migration tools
- **Workspaces** — Fabric and Power BI workspace operations
- **Lakehouses** — Lakehouse table management and shortcuts
- **Reports** — Report analysis, rebinding, and BPA
- **Capacities** — Capacity management and migration
- **And many more** — Notebooks, gateways, connections, etc.

### Key Features

| Feature Category | Description |
|-----------------|-------------|
| **TOM Wrapper** | Tabular Object Model wrapper for semantic model management |
| **Best Practice Analyzer** | Model and Report BPA for quality checks |
| **Direct Lake** | Migration, schema sync, and guardrails for Direct Lake models |
| **Vertipaq Analyzer** | Semantic model memory and performance analysis |
| **Admin APIs** | Admin-level operations for tenant management |
| **Migration Tools** | Capacity and model migration utilities |

### Dependency on Semantic Link

Semantic Link Labs depends on the `semantic-link-sempy` package as specified in `pyproject.toml`:

```toml
dependencies = [
    "semantic-link-sempy>=0.13.0",
    ...
]
```

The library uses the public APIs from `sempy.fabric` for:
- `FabricRestClient` — HTTP client for REST API calls
- `create_tom_server` — TOM server connection
- Standard Fabric context functions

**Important**: Only use public APIs from `sempy` as documented at:
- [Semantic Link Python API Reference](https://learn.microsoft.com/en-us/python/api/semantic-link-sempy/sempy.fabric)

---

## Source Code Layout

Main source code is located under `src/sempy_labs/`:

| Path | Description |
|------|-------------|
| `__init__.py` | Main module exports |
| `_helper_functions.py` | Shared utility functions (2800+ lines) |
| `_authentication.py` | Service Principal authentication |
| `_utils.py` | Item type mappings and constants |
| `_icons.py` | Icons, emojis, and UI constants |

### Core Submodules

| Submodule | Purpose |
|-----------|---------|
| `tom/` | TOM (Tabular Object Model) wrapper for semantic models |
| `admin/` | Admin API functions |
| `report/` | Report operations (BPA, rebind, export) |
| `lakehouse/` | Lakehouse operations (shortcuts, tables, schemas) |
| `directlake/` | Direct Lake model management |
| `migration/` | Model and capacity migration tools |

### Feature Modules (top-level)

| File | Feature |
|------|---------|
| `_model_bpa.py` | Model Best Practice Analyzer |
| `_model_bpa_rules.py` | BPA rule definitions |
| `_vertipaq.py` | Vertipaq Analyzer |
| `_workspaces.py` | Workspace management |
| `_capacities.py` | Azure capacity operations |
| `_refresh_semantic_model.py` | Model refresh operations |
| `_translations.py` | Model metadata translations |
| `_delta_analyzer.py` | Delta table analysis |

### Other Key Directories

| Directory | Purpose |
|-----------|---------|
| `tests/` | Unit tests |
| `notebooks/` | Sample helper notebooks |
| `docs/` | Sphinx documentation source |
| `.github/` | Issue templates and CI workflows |

---

## Development Guidelines

### Core Principles

1. **Backward compatibility** — ALWAYS maintain backward compatibility for existing functions.
2. **Minimal changes** — Do NOT change existing code unless strongly necessary.
3. **Understand before coding** — Fully understand requirements, context, and constraints before writing code.
4. **Follow existing patterns** — Match the coding style and patterns already in the codebase.
5. **Document everything** — Every public function must have complete docstrings.

---

## Python Coding Standards

### Naming and Type Annotations

- Use descriptive, meaningful names for functions, variables, and classes.
- Include type hints for all function parameters and return values.
- Use the `typing` module for complex annotations (e.g., `List[str]`, `Dict[str, int]`, `Optional[T]`).
- Use `Union[str, UUID]` for parameters that accept both name and ID.

### Function Design

- Write clear docstrings for each function.
- Break down complex functions into smaller helper functions.
- Follow the Single Responsibility Principle.
- Handle edge cases explicitly with clear exception handling.

### Code Clarity

- ALWAYS prioritize readability over cleverness.
- Document design decisions explaining *why*, not just *what*.
- Write concise, efficient, and idiomatic Python code.

---

## Public Function Conventions

### Required Decorator

All public functions MUST have the `@log` decorator from sempy:

```python
from sempy._utils._log import log

@log
def my_public_function(...):
    ...
```

### Naming Conventions

Function names should start with a verb indicating the action:

| Prefix | Use Case |
|--------|----------|
| `list_` | Retrieves a collection of items |
| `get_` | Retrieves a single item or property |
| `create_` | Creates a new resource |
| `update_` | Modifies an existing resource |
| `delete_` | Removes a resource |
| `resolve_` | Converts name to ID or vice versa |
| `run_` | Executes an analysis or process |

### Standard Parameters

Use these common parameter patterns for consistency:

| Parameter | Type | Use Case |
|-----------|------|----------|
| `workspace` | `Optional[str \| UUID] = None` | Fabric workspace name or ID |
| `dataset` | `str \| UUID` | Semantic model name or ID |
| `lakehouse` | `Optional[str \| UUID] = None` | Lakehouse name or ID |

### Default Workspace Resolution

Always document that `None` workspace resolves to attached lakehouse workspace:

```python
workspace : str | uuid.UUID, default=None
    The Fabric workspace name or ID.
    Defaults to None which resolves to the workspace of the attached lakehouse
    or if no lakehouse attached, resolves to the workspace of the notebook.
```

### Return Values

- ALWAYS return `pandas.DataFrame` for functions that return tabular data.
- Use `_create_dataframe` helper to initialize empty DataFrames with proper column types.

### Service Principal Support

For functions supporting Service Principal authentication, add this to the docstring:

```python
"""
...
Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).
...
"""
```

### Example: Well-Formed Public Function

```python
from sempy._utils._log import log
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    _base_api,
    _create_dataframe,
)
import sempy_labs._icons as icons
from typing import Optional
from uuid import UUID
import pandas as pd


@log
def list_items(
    item_type: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Returns a list of items in the specified workspace.

    This is a wrapper function for the following API: `Items - List Items <https://learn.microsoft.com/rest/api/fabric/core/items/list-items>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    item_type : str, default=None
        Filter by item type. If None, returns all item types.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the items in the workspace.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    columns = {
        "Item Id": "string",
        "Item Name": "string",
        "Item Type": "string",
    }
    df = _create_dataframe(columns=columns)

    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/items",
        uses_pagination=True,
        client="fabric_sp",
    )

    # Process responses and build dataframe...

    return df
```

---

## REST API Client Pattern

### Using `_base_api` Helper

The `_base_api` function in `_helper_functions.py` is the standard way to make REST API calls:

```python
from sempy_labs._helper_functions import _base_api

# Simple GET request - MUST call .json() to get dict
response = _base_api(
    request="/v1/workspaces/{workspace_id}/items/{item_id}",
    client="fabric_sp",
).json()  # <-- Don't forget .json()!

# POST with payload
response = _base_api(
    request="/v1/workspaces/{workspace_id}/items",
    method="post",
    payload={"displayName": name},
    status_codes=[201, 202],
    client="fabric_sp",
)

# With pagination - returns list of dicts, no .json() needed
responses = _base_api(
    request=url,
    uses_pagination=True,
    client="fabric_sp",
)

# Long-running operation - returns dict directly, no .json() needed
result = _base_api(
    request=url,
    method="post",
    lro_return_json=True,  # Returns JSON when LRO completes
    client="fabric_sp",
)
```

### `_base_api` Return Types (IMPORTANT)

| Parameters | Return Type | Usage |
|------------|-------------|-------|
| Default | `Response` | **Must call `.json()` to get dict** |
| `uses_pagination=True` | `list[dict]` | Iterate directly |
| `lro_return_json=True` | `dict` | Use directly |
| `lro_return_status_code=True` | `int` | HTTP status code |

### Client Types

| Client | Use Case |
|--------|----------|
| `fabric` | Standard Fabric API (default credentials) |
| `fabric_sp` | Fabric API with Service Principal support |
| `azure` | Azure Resource Manager API |
| `graph` | Microsoft Graph API |
| `onelake` | OneLake storage API |

### API Documentation Links

Always reference the official API docs in docstrings:

```python
"""
This is a wrapper function for the following API: `Items - List Items <https://learn.microsoft.com/rest/api/fabric/core/items/list-items>`_.
"""
```

---

## TOM Wrapper Usage

The TOM wrapper (`tom/`) provides a Pythonic interface to the Tabular Object Model:

```python
from sempy_labs.tom import connect_semantic_model

with connect_semantic_model(
    dataset="My Model",
    readonly=False,  # Set to True for read-only operations
    workspace="My Workspace"
) as tom:
    # Access model objects
    for measure in tom.all_measures():
        print(measure.Name)

    # Modify model (requires readonly=False)
    tom.model.Tables["Sales"].Measures["Revenue"].Description = "Total revenue"

    # Changes are saved when context exits
```

### TOM Methods

The `TOMWrapper` class provides helper methods:

| Method | Returns |
|--------|---------|
| `all_columns()` | Iterator of all columns |
| `all_measures()` | Iterator of all measures |
| `all_tables()` | Access `model.Tables` |
| `all_partitions()` | Iterator of all partitions |
| `all_hierarchies()` | Iterator of all hierarchies |
| `all_calculation_items()` | Iterator of calculation items |

---

## Helper Functions

Common helper functions in `_helper_functions.py`:

### Workspace/Item Resolution

```python
from sempy_labs._helper_functions import (
    resolve_workspace_id,
    resolve_workspace_name_and_id,
    resolve_dataset_id,
    resolve_dataset_name_and_id,
    resolve_item_id,
    resolve_item_name,
)
```

### DataFrame Utilities

```python
from sempy_labs._helper_functions import (
    _create_dataframe,        # Create empty DataFrame with typed columns
    _update_dataframe_datatypes,  # Update column types
)
```

### Validation

```python
from sempy_labs._helper_functions import (
    _is_valid_uuid,           # Check if string is valid UUID
)
```

### Icons and Messages

```python
import sempy_labs._icons as icons

print(f"{icons.green_dot} Success message")
print(f"{icons.red_dot} Error message")
print(f"{icons.warning} Warning message")
print(f"{icons.in_progress} In progress...")
```

---

## Documentation Standards

### Docstrings

Use **numpydoc style** for all docstrings with these sections:

1. **Short description** — One-line summary
2. **Long description** — Extended explanation (optional)
3. **Parameters** — Document each parameter with type and default
4. **Returns** / **Yields** — Document return value
5. **Raises** — Document exceptions (if applicable)

### Parameter Documentation Format

```python
Parameters
----------
item_type : str, default=None
    Filter by item type. If None, returns all item types.
workspace : str | uuid.UUID, default=None
    The Fabric workspace name or ID.
    Defaults to None which resolves to the workspace of the attached lakehouse
    or if no lakehouse attached, resolves to the workspace of the notebook.
```

### Code Style

- Follow **PEP 8** style guide
- Use 4 spaces for indentation (no tabs)
- Maximum line length: 200 characters (as per `pyproject.toml`)
- Use **black** for code formatting
- Use **LF** (Unix-style) line endings

---

## Adding New Functions

### Step 1: Choose the Right Module

| Function Type | Location |
|--------------|----------|
| General workspace/item operations | `_helper_functions.py` or dedicated `_*.py` |
| Admin operations | `admin/` submodule |
| Report operations | `report/` submodule |
| Lakehouse operations | `lakehouse/` submodule |
| Direct Lake operations | `directlake/` submodule |
| TOM operations | `tom/_model.py` |

### Step 2: Implement the Function

Follow the coding standards and patterns described above.

### Step 3: Export the Function

Add to the module's `__init__.py`:

```python
from ._my_module import my_new_function

__all__ = [
    ...,
    "my_new_function",
]
```

### Step 4: Add to Main Package

If it's a top-level function, add to `src/sempy_labs/__init__.py`:

```python
from ._my_module import my_new_function

__all__ = [
    ...,
    "my_new_function",
]
```

---

## Testing

### Test Framework

- **Framework**: pytest
- **Location**: `tests/` directory
- **Configuration**: `pyproject.toml`

### Running Tests

```bash
# Run all tests
pytest -s tests/

# Run specific test
pytest -s tests/ -k test_name
```

### Writing Tests

Tests should be minimal but effective:

```python
import pytest

def test_my_function():
    """Test my_function basic behavior."""
    from sempy_labs import my_function

    result = my_function(input_value)

    assert isinstance(result, pd.DataFrame)
    assert not result.empty
```

---

## Building Documentation

Documentation is built with Sphinx and hosted on ReadTheDocs.

### Local Build

```bash
cd docs
sphinx-apidoc -f -o source ../src/sempy_labs/
make html
```

### Documentation Configuration

- **Config**: `docs/source/conf.py`
- **Theme**: sphinx_rtd_theme
- **Style**: numpydoc

---

## Pre-Commit Checklist

Before committing code changes:

```bash
# Check style (if enabled)
# flake8 src/sempy_labs tests

# Check types (if enabled)
# mypy src/sempy_labs tests

# Run tests
pytest -s tests/

# Install package locally to verify
pip install -e .
```

---

## Quick Commands

| Task | Command |
|------|---------|
| Install package in dev mode | `pip install -e .` |
| Run all tests | `pytest -s tests/` |
| Run specific test | `pytest -s tests/ -k test_name` |
| Build docs locally | `cd docs && make html` |
| Format code with black | `black src/sempy_labs tests` |

---

## Important Files

| File | Purpose |
|------|---------|
| `pyproject.toml` | Package configuration and dependencies |
| `environment.yml` | Conda environment for development |
| `.readthedocs.yaml` | ReadTheDocs build configuration |
| `.github/workflows/build.yaml` | CI/CD pipeline |
| `src/sempy_labs/__init__.py` | Main package exports |
| `src/sempy_labs/_helper_functions.py` | Shared utilities |

---

## Skills Reference

For detailed workflows, refer to these skill files:

| Skill | File | Purpose |
|-------|------|---------|
| Code Style | [SKILL.md](.claude/skills/code-style/SKILL.md) | Linting with flake8, formatting with black |
| Build Docs | [SKILL.md](.claude/skills/build-docs/SKILL.md) | Building and validating Sphinx documentation |
| Run Tests | [SKILL.md](.claude/skills/run-tests/SKILL.md) | Running pytest tests locally |
| Write Tests | [SKILL.md](.claude/skills/write-tests/SKILL.md) | Writing unit tests with pytest |
| Add Function | [SKILL.md](.claude/skills/add-function/SKILL.md) | Adding new API wrapper functions |
| TOM Operations | [SKILL.md](.claude/skills/tom-operations/SKILL.md) | Working with TOM wrapper for semantic models |
| REST API Patterns | [SKILL.md](.claude/skills/rest-api-patterns/SKILL.md) | Implementing REST API wrapper functions |
| Direct Lake | [SKILL.md](.claude/skills/direct-lake-operations/SKILL.md) | Direct Lake model operations and migration |
| Task Management | [SKILL.md](.claude/skills/task-management/SKILL.md) | Plan, track, and checkpoint multi-step workloads |

---

## External Resources

| Resource | URL |
|----------|-----|
| Documentation | [ReadTheDocs](https://semantic-link-labs.readthedocs.io/) |
| GitHub Wiki | [Wiki](https://github.com/microsoft/semantic-link-labs/wiki) |
| Code Examples | [Examples](https://github.com/microsoft/semantic-link-labs/wiki/Code-Examples) |
| Fabric REST API | [Docs](https://learn.microsoft.com/rest/api/fabric/) |
| Power BI REST API | [Docs](https://learn.microsoft.com/rest/api/power-bi/) |
| Semantic Link API | [Docs](https://learn.microsoft.com/en-us/python/api/semantic-link-sempy/sempy.fabric) |
