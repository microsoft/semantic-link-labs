---
name: direct-lake-operations
description: Guide for working with Direct Lake semantic models. Use this when implementing Direct Lake-related features or troubleshooting.
---

# Direct Lake Operations

This skill covers working with Direct Lake semantic models in Semantic Link Labs.

## When to Use This Skill

Use this skill when you need to:
- Migrate models to Direct Lake
- Check and fix Direct Lake fallback issues
- Sync schema between lakehouse and model
- Work with Direct Lake guardrails
- Warm Direct Lake cache

---

## Overview

Direct Lake is a storage mode for Power BI semantic models that reads data directly from Delta tables in OneLake, providing fast query performance without data import.

### Key Files

| File | Purpose |
|------|---------|
| `src/sempy_labs/directlake/` | Direct Lake submodule |
| `_dl_helper.py` | Core Direct Lake utilities |
| `_guardrails.py` | Guardrail checking |
| `_directlake_schema_sync.py` | Schema synchronization |
| `_warm_cache.py` | Cache warming utilities |

---

## Direct Lake Functions

### Core Functions

| Function | Purpose |
|----------|---------|
| `check_fallback_reason` | Check why model falls back to DirectQuery |
| `get_direct_lake_lakehouse` | Get lakehouse connected to model |
| `get_direct_lake_source` | Get SQL endpoint and lakehouse info |
| `generate_direct_lake_semantic_model` | Create new Direct Lake model |

### Schema Management

| Function | Purpose |
|----------|---------|
| `direct_lake_schema_compare` | Compare model schema with lakehouse |
| `direct_lake_schema_sync` | Sync model schema from lakehouse |

### Guardrails

| Function | Purpose |
|----------|---------|
| `get_direct_lake_guardrails` | Get guardrail limits for model |
| `get_sku_size` | Get SKU capacity info |
| `get_directlake_guardrails_for_sku` | Get guardrails for specific SKU |

### Cache Management

| Function | Purpose |
|----------|---------|
| `warm_direct_lake_cache_isresident` | Warm cache using IsResident |
| `warm_direct_lake_cache_perspective` | Warm cache using perspective |

### Connection Updates

| Function | Purpose |
|----------|---------|
| `update_direct_lake_model_connection` | Update lakehouse connection |
| `update_direct_lake_model_lakehouse_connection` | Legacy connection update |

---

## Checking Fallback Reasons

Direct Lake models can fall back to DirectQuery mode. Use `check_fallback_reason` to diagnose:

```python
from sempy_labs.directlake import check_fallback_reason

# Check specific model
result = check_fallback_reason(
    dataset="My Direct Lake Model",
    workspace="My Workspace"
)

# Returns DataFrame with fallback reasons per table
print(result)
```

### Common Fallback Reasons

| Reason | Cause | Solution |
|--------|-------|----------|
| `ColumnNotInPartition` | Column missing from Delta table | Add column to lakehouse table |
| `ParquetTypeMismatch` | Data type mismatch | Update lakehouse table schema |
| `UnsupportedFilter` | DAX filter not supported | Modify DAX query |
| `Guardrail` | Exceeded capacity limits | Upgrade capacity or reduce data |

---

## Schema Synchronization

### Compare Schemas

```python
from sempy_labs.directlake import direct_lake_schema_compare

# Compare model schema with lakehouse tables
comparison = direct_lake_schema_compare(
    dataset="My Direct Lake Model",
    workspace="My Workspace"
)

# Shows differences between model and lakehouse
print(comparison)
```

### Sync Schema

```python
from sempy_labs.directlake import direct_lake_schema_sync

# Sync model schema from lakehouse
direct_lake_schema_sync(
    dataset="My Direct Lake Model",
    workspace="My Workspace",
    add_columns=True,      # Add new columns
    remove_columns=False,  # Keep columns not in lakehouse
)
```

---

## Guardrails

Direct Lake has limits based on capacity SKU:

```python
from sempy_labs.directlake import get_direct_lake_guardrails

# Get guardrails for a model
guardrails = get_direct_lake_guardrails(
    dataset="My Direct Lake Model",
    workspace="My Workspace"
)

# Shows limits vs current values
print(guardrails)
```

### Guardrail Limits

| Guardrail | Description |
|-----------|-------------|
| Max Rows Per Table | Maximum rows per table |
| Max Size Per Table | Maximum table size in GB |
| Max Columns Per Table | Maximum columns per table |
| Max Model Size | Maximum total model size |

---

## Generating Direct Lake Models

Create a new Direct Lake model from lakehouse tables:

```python
from sempy_labs.directlake import generate_direct_lake_semantic_model

# Generate model from lakehouse
generate_direct_lake_semantic_model(
    dataset="New Direct Lake Model",
    lakehouse="My Lakehouse",
    workspace="My Workspace",
    lakehouse_workspace="Lakehouse Workspace",  # Optional
)
```

---

## Updating Connections

Change the lakehouse a Direct Lake model points to:

```python
from sempy_labs.directlake import update_direct_lake_model_connection

# Update to different lakehouse
update_direct_lake_model_connection(
    dataset="My Direct Lake Model",
    workspace="My Workspace",
    target_lakehouse="New Lakehouse",
    target_workspace="Target Workspace",
)
```

---

## Cache Warming

Warm the Direct Lake cache after model refresh:

### Using IsResident

```python
from sempy_labs.directlake import warm_direct_lake_cache_isresident

# Warm columns that were previously in memory
warm_direct_lake_cache_isresident(
    dataset="My Direct Lake Model",
    workspace="My Workspace"
)
```

### Using Perspective

```python
from sempy_labs.directlake import warm_direct_lake_cache_perspective

# Warm columns defined in a perspective
warm_direct_lake_cache_perspective(
    dataset="My Direct Lake Model",
    perspective="Cache Warming",
    workspace="My Workspace"
)
```

---

## Migration to Direct Lake

The library provides migration tools in `src/sempy_labs/migration/`:

### Migration Workflow

1. **Migrate calculated tables** to lakehouse as Delta tables
2. **Create Direct Lake model** with same structure
3. **Migrate measures and other objects**
4. **Validate and test**

### Example Migration

```python
from sempy_labs.migration import (
    migrate_calctables_to_lakehouse,
    migrate_model_objects_to_semantic_model,
)

# Step 1: Migrate calculated tables to lakehouse
migrate_calctables_to_lakehouse(
    dataset="Source Import Model",
    workspace="My Workspace",
    lakehouse="Target Lakehouse",
    lakehouse_workspace="Lakehouse Workspace",
)

# Step 2: Migrate other objects (measures, etc.)
migrate_model_objects_to_semantic_model(
    source_dataset="Source Import Model",
    target_dataset="Target Direct Lake Model",
    source_workspace="My Workspace",
    target_workspace="My Workspace",
)
```

---

## Best Practices

### Do's

- ✅ Always check guardrails before deploying
- ✅ Use schema sync after lakehouse changes
- ✅ Warm cache after scheduled refreshes
- ✅ Monitor fallback reasons regularly

### Don'ts

- ❌ Don't ignore fallback warnings
- ❌ Don't exceed guardrail limits
- ❌ Don't modify lakehouse schema without syncing model
- ❌ Don't use unsupported DAX patterns

---

## Troubleshooting

### Model Falls Back to DirectQuery

1. Run `check_fallback_reason()` to identify cause
2. Check column mappings with `direct_lake_schema_compare()`
3. Verify data types match between model and lakehouse
4. Check guardrails with `get_direct_lake_guardrails()`

### Schema Mismatch

1. Run `direct_lake_schema_compare()` to see differences
2. Run `direct_lake_schema_sync()` to fix mismatches
3. Verify partition entity mappings

### Performance Issues

1. Check cache state with `warm_direct_lake_cache_isresident()`
2. Verify partitions are properly configured
3. Review query patterns for unsupported filters

---

## Related Resources

| Resource | URL |
|----------|-----|
| Direct Lake Overview | [Microsoft Docs](https://learn.microsoft.com/power-bi/enterprise/directlake-overview) |
| Guardrails | [Microsoft Docs](https://learn.microsoft.com/power-bi/enterprise/directlake-guardrails) |
| Migration Notebook | [Notebook](https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Migration%20to%20Direct%20Lake.ipynb) |
| API Docs | [ReadTheDocs](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html) |
