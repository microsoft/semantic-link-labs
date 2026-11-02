---
name: add-function
description: Guide for adding new functions to the library. Use this when implementing new API wrappers or utility functions.
---

# Adding New Functions

This skill covers the workflow for adding new functions to the Semantic Link Labs library.

## When to Use This Skill

Use this skill when you need to:
- Add a new API wrapper function
- Create a new utility function
- Extend existing functionality with new features
- Add functions to submodules (admin, report, lakehouse, etc.)

---

## Function Categories

| Category | Location | Purpose |
|----------|----------|---------|
| **Top-level functions** | `src/sempy_labs/_*.py` | Main library exports |
| **Admin functions** | `src/sempy_labs/admin/` | Admin API operations |
| **Report functions** | `src/sempy_labs/report/` | Report operations |
| **Lakehouse functions** | `src/sempy_labs/lakehouse/` | Lakehouse operations |
| **Direct Lake functions** | `src/sempy_labs/directlake/` | Direct Lake model operations |
| **TOM methods** | `src/sempy_labs/tom/_model.py` | TOMWrapper class methods |

---

## Step 0: Find the API Documentation

Before implementing an API wrapper, find the relevant API documentation:

```bash
# Use the API search tool
cd .claude/skills/rest-api-patterns/scripts
python search_public_api_doc.py "your search query"

# Examples:
python search_public_api_doc.py "workspace users" --source fabric
python search_public_api_doc.py "dataset refresh" --source powerbi
```

See the [REST API Patterns](../rest-api-patterns/SKILL.md) skill for more details.

---

## Step 1: Choose the Right Location

### Top-Level Function

For general-purpose functions exported from `sempy_labs`:

```python
# src/sempy_labs/_my_feature.py
```

### Submodule Function

For functions belonging to a specific domain:

```python
# src/sempy_labs/admin/_my_admin_function.py
# src/sempy_labs/lakehouse/_my_lakehouse_function.py
# src/sempy_labs/report/_my_report_function.py
```

---

## Step 2: Create the Function

### Required Imports

```python
import pandas as pd
from typing import Optional, List
from uuid import UUID

# Logging decorator from sempy
from sempy._utils._log import log

# Helper functions
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_workspace_id,
    _base_api,
    _create_dataframe,
)

# Icons for user messages
import sempy_labs._icons as icons
```

### Function Template

```python
@log
def my_new_function(
    item: str | UUID,
    workspace: Optional[str | UUID] = None,
    option: str = "default",
) -> pd.DataFrame:
    """
    Short description of what the function does.

    Extended description with more details about the function's behavior,
    use cases, and any important notes.

    This is a wrapper function for the following API: `API Name <https://learn.microsoft.com/rest/api/...>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    item : str | uuid.UUID
        The name or ID of the item.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    option : str, default="default"
        An option that controls function behavior.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the results.
        Columns include: 'Column1', 'Column2', 'Column3'.

    Raises
    ------
    ValueError
        If the item does not exist.
    FabricHTTPException
        If the API request fails.
    """

    # Resolve workspace
    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    # Define result DataFrame structure
    columns = {
        "Column1": "string",
        "Column2": "string",
        "Column3": "int",
    }
    df = _create_dataframe(columns=columns)

    # Make API call
    responses = _base_api(
        request=f"/v1/workspaces/{workspace_id}/items",
        uses_pagination=True,
        client="fabric_sp",
    )

    # Process responses
    rows = []
    for r in responses:
        for item in r.get("value", []):
            rows.append({
                "Column1": item.get("id"),
                "Column2": item.get("name"),
                "Column3": item.get("count", 0),
            })

    if rows:
        df = pd.DataFrame(rows)

    return df
```

---

## Step 3: Export the Function

### From Module File

Add to the module's `__init__.py`:

```python
# src/sempy_labs/admin/__init__.py (example for admin submodule)

from ._my_admin_function import my_new_function

__all__ = [
    ...,
    "my_new_function",
]
```

### From Main Package

For top-level functions, add to `src/sempy_labs/__init__.py`:

```python
from ._my_feature import my_new_function

__all__ = [
    ...,
    "my_new_function",
]
```

---

## Common Patterns

### Functions That Modify Resources

```python
@log
def create_item(
    name: str,
    workspace: Optional[str | UUID] = None,
) -> None:
    """
    Creates a new item.
    ...
    """
    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    payload = {
        "displayName": name,
    }

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/items",
        method="post",
        payload=payload,
        status_codes=[201, 202],
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The '{name}' item has been successfully created "
        f"in the '{workspace_name}' workspace."
    )
```

### Functions That Delete Resources

```python
@log
def delete_item(
    item: str | UUID,
    workspace: Optional[str | UUID] = None,
) -> None:
    """
    Deletes an item.
    ...
    """
    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    item_id = resolve_item_id(item=item, type="ItemType", workspace=workspace_id)

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/items/{item_id}",
        method="delete",
        client="fabric_sp",
    )

    print(
        f"{icons.green_dot} The item has been successfully deleted "
        f"from the '{workspace_name}' workspace."
    )
```

### Functions With Long-Running Operations

```python
@log
def long_running_operation(
    item: str | UUID,
    workspace: Optional[str | UUID] = None,
) -> dict:
    """
    Performs a long-running operation.
    ...
    """
    workspace_id = resolve_workspace_id(workspace)
    item_id = resolve_item_id(item=item, type="ItemType", workspace=workspace_id)

    # lro_return_json handles polling for completion
    result = _base_api(
        request=f"/v1/workspaces/{workspace_id}/items/{item_id}/operation",
        method="post",
        lro_return_json=True,
        client="fabric_sp",
    )

    return result
```

---

## Step 4: Add Tests

Create tests for the new function:

```python
# tests/test_my_feature.py

import pytest
import pandas as pd


def test_my_new_function_returns_dataframe():
    """Test that my_new_function returns a DataFrame."""
    from sempy_labs import my_new_function

    # This might require mocking for unit tests
    result = my_new_function()

    assert isinstance(result, pd.DataFrame)


def test_my_new_function_with_workspace():
    """Test my_new_function with specific workspace."""
    from sempy_labs import my_new_function

    result = my_new_function(workspace="Test Workspace")

    assert isinstance(result, pd.DataFrame)
```

---

## Step 5: Document the Function

Ensure the docstring follows numpydoc style:

1. ✅ Short description (one line)
2. ✅ Extended description (if needed)
3. ✅ API reference link (for wrapper functions)
4. ✅ Service Principal note (if supported)
5. ✅ All parameters documented with types
6. ✅ Return value documented
7. ✅ Exceptions documented (if applicable)

---

## Checklist Before Committing

- [ ] Function follows naming conventions (`list_`, `get_`, `create_`, etc.)
- [ ] `@log` decorator is applied
- [ ] Complete docstring with numpydoc style
- [ ] Type hints for all parameters and return value
- [ ] Uses standard helper functions (`_base_api`, `resolve_*`, etc.)
- [ ] Function exported in `__init__.py`
- [ ] Tests written for the new function
- [ ] Code formatted with black
- [ ] No linting errors
- [ ] Documentation builds without warnings

---

## Example: Complete New Function

See [_workspaces.py](../../src/sempy_labs/_workspaces.py) for well-implemented examples:

- `list_workspace_users` — List function returning DataFrame
- `update_workspace_user` — Update function with parameters
- `delete_user_from_workspace` — Delete function with confirmation message

---

## API Documentation Resources

When wrapping REST APIs, reference the official documentation:

| API | Documentation |
|-----|---------------|
| Fabric Core API | [https://learn.microsoft.com/rest/api/fabric/core/](https://learn.microsoft.com/rest/api/fabric/core/) |
| Fabric Admin API | [https://learn.microsoft.com/rest/api/fabric/admin/](https://learn.microsoft.com/rest/api/fabric/admin/) |
| Power BI REST API | [https://learn.microsoft.com/rest/api/power-bi/](https://learn.microsoft.com/rest/api/power-bi/) |
| Azure Management API | [https://learn.microsoft.com/rest/api/resources/](https://learn.microsoft.com/rest/api/resources/) |
