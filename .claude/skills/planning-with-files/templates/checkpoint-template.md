# Overall Goal

[Describe the end state you want to achieve. Be specific about what will exist when the work is complete.]

Example:
> Add wrapper functions for all Fabric Admin Workspace APIs to the `sempy_labs.admin` module.

---

# Guidelines

[List constraints, coding standards, and references to follow.]

Example:
> - Follow sempy_labs code styling and documentation standards
> - Use `@log` decorator from `sempy._utils._log`
> - Use `_base_api` helper for all REST calls
> - Add unit tests for every public exported function
> - Reference: [Fabric Admin API Documentation](https://learn.microsoft.com/rest/api/fabric/admin/)

---

# Notes

[Document important considerations, decisions, and gotchas discovered during planning or execution.]

Example:
> 1. Some APIs require admin permissions - document in docstrings
> 2. Use `resolve_workspace_name_and_id` for workspace parameter handling
> 3. Return pandas DataFrame for list operations

---

# Implementation Plan

## Summary

[Provide scope overview]

**Total items:** ~[N] functions across [M] API categories

### Dependency Graph

```
Stage 1: [Base Layer] (no dependencies)
    |
Stage 2: [Second Layer] (depends on Stage 1)
    |
Stage 3: [Third Layer] (depends on Stages 1-2)
    |
Stage N: [Final Layer] (depends on all previous)
```

---

## Stage 1: [Stage Name] (STATUS)

**Source:** [API documentation URL or source file]
**Target:** [Target file path, e.g., `src/sempy_labs/admin/_workspaces.py`]
**Dependencies:** None (base layer) | Stage N (list dependencies)

### Functions/Items:

- [ ] `function_name_1` - Brief description
- [ ] `function_name_2` - Brief description
- [ ] `function_name_3` - Brief description
- [ ] Unit tests

### Notes:
[Stage-specific notes, if any]

---

## Stage 2: [Stage Name] (STATUS)

**Source:** [API documentation URL]
**Target:** [Target file path]
**Dependencies:** Stage 1 ([specific items used])

### Functions/Items:

- [ ] `function_name_1`
- [ ] `function_name_2`
- [ ] Unit tests

### Helper functions (if needed):
- [ ] `_helper_function_1` (for `_helper_functions.py`)
- [ ] `_helper_function_2`

---

## Stage N: [Final Stage Name]

**Source:** [API documentation URL]
**Target:** [Target file path]
**Dependencies:** Stages 1-N-1

### Functions/Items:

- [ ] `function_name_1`
- [ ] `function_name_2`
- [ ] Unit tests

---

# Code Style Requirements

[List specific requirements for this work]

1. Use `@log` decorator from `sempy._utils._log`
2. Add proper type hints with `Optional`, `Union`, `UUID` support
3. Follow numpy-style docstrings
4. Use `_base_api` helper for REST calls
5. Return `pandas.DataFrame` for list operations
6. Use `_create_dataframe` for empty DataFrame initialization

---

# Testing Requirements

[List testing requirements]

1. Create unit tests in `tests/` directory
2. Test edge cases (empty responses, invalid inputs)
3. Use appropriate mocking for API calls

---

# File Naming Convention

[Document naming patterns used]

- Public functions: `_{module}.py` (e.g., `_workspaces.py`)
- Shared utilities: `_helper_functions.py`
- Tests: `test_{module}.py`
