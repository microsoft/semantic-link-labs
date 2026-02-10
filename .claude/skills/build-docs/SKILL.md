---
name: build-docs
description: Guide for building documentation and validating docstrings. Use this when asked to build docs, check docstrings, or validate documentation.
---

# Building and Validating Documentation

This skill covers documentation building and docstring validation workflows for the Semantic Link Labs project.

## When to Use This Skill

Use this skill when you need to:
- Build documentation locally
- Validate docstrings are properly formatted
- Check for documentation warnings or errors
- Ensure new functions have proper documentation
- Preview documentation before pushing

---

## Documentation Framework

| Component | Details |
|-----------|---------|
| **Framework** | Sphinx with numpydoc |
| **Theme** | sphinx_rtd_theme |
| **Hosting** | ReadTheDocs |
| **Source location** | `docs/source/` |
| **Build output** | `docs/build/html/` |
| **Config** | `docs/source/conf.py` |

---

## Building Documentation Locally

### Prerequisites

Install documentation dependencies:

```bash
pip install -r docs/requirements.txt
```

### Build Commands

```bash
# Navigate to docs directory
cd docs

# Generate API documentation from source code
sphinx-apidoc -f -o source ../src/sempy_labs/

# Build HTML documentation
make html

# Or on Windows
make.bat html
```

### View Built Documentation

Open `docs/build/html/index.html` in a browser.

---

## ReadTheDocs Configuration

The project uses `.readthedocs.yaml` for automated builds:

```yaml
version: 2
build:
  os: ubuntu-22.04
  tools:
    python: "3.12"
  jobs:
    pre_build:
      - sphinx-apidoc -f -o docs/source src/sempy_labs/
sphinx:
  configuration: docs/source/conf.py
python:
   install:
   - requirements: docs/requirements.txt
```

---

## Docstring Standards

### Docstring Style

This project uses **numpydoc style** for all docstrings.

### Required Sections

1. **Short description** — One-line summary of what the function does
2. **Extended description** — Detailed explanation (optional but recommended)
3. **Parameters** — Document each parameter with type and description
4. **Returns** — Document return value(s)

### Optional Sections

- **Raises** — Document exceptions that may be raised
- **Examples** — Provide usage examples
- **Notes** — Additional context or implementation details
- **See Also** — Related functions

### Example Docstring

```python
@log
def list_workspaces(
    capacity: Optional[str | UUID] = None,
    workspace_state: Optional[str] = None,
) -> pd.DataFrame:
    """
    Lists workspaces for the organization.

    This is a wrapper function for the following API: `Workspaces - List Workspaces <https://learn.microsoft.com/rest/api/fabric/admin/workspaces/list-workspaces>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    capacity : str | uuid.UUID, default=None
        Returns only the workspaces in the specified Capacity.
    workspace_state : str, default=None
        Return only the workspace with the requested state.
        You can find the possible states in `Workspace States <https://learn.microsoft.com/rest/api/fabric/admin/workspaces/list-workspaces?tabs=HTTP#workspacestate>`_.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of workspaces for the organization.
        Columns include: 'Id', 'Name', 'State', 'Type', 'Capacity Id'.

    Raises
    ------
    FabricHTTPException
        If the API request fails.

    Examples
    --------
    >>> import sempy_labs as labs
    >>> df = labs.list_workspaces()
    >>> df = labs.list_workspaces(capacity="My Capacity")
    """
    pass
```

---

## Parameter Documentation Patterns

### Standard Parameter Formats

```python
# Simple parameter
item_type : str
    The type of item to filter by.

# Parameter with default
item_type : str, default=None
    The type of item to filter by. If None, returns all types.

# Union type parameter
workspace : str | uuid.UUID, default=None
    The Fabric workspace name or ID.
    Defaults to None which resolves to the workspace of the attached lakehouse
    or if no lakehouse attached, resolves to the workspace of the notebook.

# Boolean parameter
readonly : bool, default=True
    If True, opens in read-only mode. If False, allows modifications.

# List parameter
columns : List[str], default=None
    A list of column names to include. If None, includes all columns.
```

### API Reference Links

Always include links to API documentation:

```python
"""
This is a wrapper function for the following API: `Items - List Items <https://learn.microsoft.com/rest/api/fabric/core/items/list-items>`_.
"""
```

### Service Principal Note

For functions supporting Service Principal authentication:

```python
"""
Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).
"""
```

---

## Common Documentation Issues

### Missing or Incomplete Docstrings

**Symptom**: Sphinx warning about missing docstring.

**Fix**: Add complete numpydoc-style docstring with all required sections.

### Type Annotation Mismatches

**Symptom**: Warning about type mismatch between signature and docstring.

**Fix**: Ensure docstring parameter types match function signature type hints.

```python
# Function signature
def my_func(workspace: Optional[str | UUID] = None) -> pd.DataFrame:

# Docstring should match
"""
Parameters
----------
workspace : str | uuid.UUID, default=None
    ...

Returns
-------
pandas.DataFrame
    ...
"""
```

### Indentation Errors

**Symptom**: Warning about unexpected indentation.

**Fix**: Use consistent 4-space indentation in docstrings.

### Broken Links

**Symptom**: Warning about broken reference.

**Fix**: Verify URLs are correct and use proper RST link syntax:
```python
`Link Text <https://example.com>`_
```

---

## Sphinx Configuration

Key settings in `docs/source/conf.py`:

```python
# Extensions
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx.ext.intersphinx',
]

# Napoleon settings for numpydoc
napoleon_numpy_docstring = True

# Mock imports for packages not available during build
autodoc_mock_imports = [
    'delta', 'synapse', 'jwt', 'semantic-link-sempy',
    'pyspark', 'powerbiclient'
]
```

---

## Validating Documentation

### Check for Warnings

```bash
cd docs
make html 2>&1 | grep -i warning
```

### Clean Build

```bash
cd docs
make clean
make html
```

### Verify Specific Module

```bash
# Generate docs for specific module
sphinx-apidoc -f -o source ../src/sempy_labs/admin/
make html
```

---

## Pre-Commit Documentation Check

Before committing changes with new or modified functions:

1. **Verify docstring completeness**:
   - Short description present
   - All parameters documented with types
   - Return value documented
   - API reference link included (if applicable)

2. **Build documentation locally**:
   ```bash
   cd docs && make html
   ```

3. **Check for warnings** in build output

4. **Preview the generated HTML** to ensure proper rendering
