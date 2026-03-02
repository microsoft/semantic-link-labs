---
name: code-style
description: Guide for running code style linters and formatters. Use this when asked to check code style, run linters, or fix formatting issues.
---

# Code Style and Linting

This skill covers code style checking and formatting workflows for the Semantic Link Labs project.

## When to Use This Skill

Use this skill when you need to:
- Check code for style violations
- Format code with black
- Verify type correctness with mypy
- Fix linting errors before committing
- Ensure code follows project standards

---

## Formatting Tools

| Tool | Purpose | Configuration |
|------|---------|---------------|
| **black** | Code formatting | `pyproject.toml` |
| **flake8** | Style checking | `pyproject.toml` |
| **mypy** | Type checking | `pyproject.toml` |

---

## Code Formatting with Black

The project uses `black` for code formatting.

### Running Black

```bash
# Format all source files
black src/sempy_labs tests

# Check without modifying (dry run)
black --check src/sempy_labs tests

# Show diff of changes
black --diff src/sempy_labs tests

# Format specific file
black src/sempy_labs/_workspaces.py
```

### Black Configuration

From `pyproject.toml`, max line length is configured via flake8 settings.

---

## Style Checking with Flake8

### Running Flake8

```bash
# Check all source files
flake8 src/sempy_labs tests

# Check specific file
flake8 src/sempy_labs/_helper_functions.py

# Show source code for each error
flake8 --show-source src/sempy_labs

# Show statistics summary
flake8 --statistics src/sempy_labs
```

### Flake8 Configuration

From `pyproject.toml`:
```toml
[tool.flake8]
max-line-length = 200
```

### Common Flake8 Errors

| Code | Description | Fix |
|------|-------------|-----|
| E501 | Line too long (>200 chars) | Break line or shorten code |
| F401 | Imported but unused | Remove unused import |
| F841 | Variable assigned but never used | Remove or use the variable |
| E302 | Expected 2 blank lines | Add blank lines between functions |
| E303 | Too many blank lines | Remove extra blank lines |
| W291 | Trailing whitespace | Remove trailing spaces |
| E711 | Comparison to None | Use `is None` instead of `== None` |

---

## Type Checking with Mypy

### Running Mypy

```bash
# Check all source files
mypy src/sempy_labs

# Check specific file
mypy src/sempy_labs/_workspaces.py

# Show error codes
mypy --show-error-codes src/sempy_labs
```

### Mypy Configuration

From `pyproject.toml`:
```toml
[[tool.mypy.overrides]]
module = "sempy.*,Microsoft.*,System.*,anytree.*,powerbiclient.*,synapse.ml.services.*,polib.*,jsonpath_ng.*"
ignore_missing_imports = true
```

### Common Mypy Errors

| Error | Description | Fix |
|-------|-------------|-----|
| `Incompatible types` | Type mismatch | Fix type annotation or value |
| `Missing return statement` | Function missing return | Add return statement |
| `Argument has incompatible type` | Wrong argument type | Pass correct type |
| `has no attribute` | Attribute doesn't exist | Check spelling or type |

### Adding Type Ignore Comments

When mypy raises false positives:

```python
# Ignore specific error
result = some_function()  # type: ignore[return-value]

# Ignore all errors on line
result = some_function()  # type: ignore
```

---

## Import Organization

### Import Order

Organize imports in this order (with blank lines between groups):

1. Standard library imports
2. Third-party imports
3. Local application imports

### Example Import Structure

```python
# Standard library
import os
import json
from typing import Optional, List
from uuid import UUID

# Third-party
import pandas as pd
import requests

# Semantic Link (dependency)
import sempy.fabric as fabric
from sempy._utils._log import log

# Local imports
import sempy_labs._icons as icons
from sempy_labs._helper_functions import (
    resolve_workspace_id,
    _base_api,
)
```

---

## Pre-Commit Style Checklist

Before committing code changes:

```bash
# Format code
black src/sempy_labs tests

# Check for style issues
flake8 src/sempy_labs tests

# Check types (optional)
mypy src/sempy_labs

# If any errors, fix and re-run
```

---

## Common Style Fixes

### Line Too Long

Break long lines using parentheses or backslash:

```python
# Before
result = some_very_long_function_call(parameter1, parameter2, parameter3, parameter4, parameter5)

# After
result = some_very_long_function_call(
    parameter1,
    parameter2,
    parameter3,
    parameter4,
    parameter5
)
```

### F-String Line Breaks

```python
# Before
print(f"{icons.green_dot} The '{item_name}' {item_type} has been successfully created in the '{workspace_name}' workspace.")

# After (if too long)
print(
    f"{icons.green_dot} The '{item_name}' {item_type} has been successfully "
    f"created in the '{workspace_name}' workspace."
)
```

### Dictionary Formatting

```python
# Before
payload = {"displayName": name, "description": description, "definition": definition}

# After
payload = {
    "displayName": name,
    "description": description,
    "definition": definition,
}
```

---

## IDE Integration

### VS Code Settings

Recommended `.vscode/settings.json`:

```json
{
    "python.formatting.provider": "black",
    "python.linting.flake8Enabled": true,
    "python.linting.mypyEnabled": true,
    "editor.formatOnSave": true,
    "[python]": {
        "editor.defaultFormatter": "ms-python.black-formatter"
    }
}
```
