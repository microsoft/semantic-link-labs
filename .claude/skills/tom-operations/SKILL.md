---
name: tom-operations
description: Guide for working with the TOM (Tabular Object Model) wrapper. Use this when modifying semantic models programmatically.
---

# TOM (Tabular Object Model) Operations

This skill covers working with the TOM wrapper in Semantic Link Labs for programmatic semantic model management.

## When to Use This Skill

Use this skill when you need to:
- Read or modify semantic model metadata
- Add/remove tables, columns, measures, relationships
- Work with calculation groups and items
- Manage partitions and data sources
- Implement model management features

---

## Overview

The TOM wrapper provides a Pythonic interface to the Tabular Object Model (TOM), which is the object model for Analysis Services semantic models.

### Key Files

| File | Purpose |
|------|---------|
| `src/sempy_labs/tom/__init__.py` | Module exports |
| `src/sempy_labs/tom/_model.py` | TOMWrapper class implementation |

---

## Basic Usage

### Connecting to a Semantic Model

```python
from sempy_labs.tom import connect_semantic_model

# Read-only connection
with connect_semantic_model(
    dataset="My Semantic Model",
    readonly=True,
    workspace="My Workspace"
) as tom:
    # Read model metadata
    for measure in tom.all_measures():
        print(f"Measure: {measure.Name}")

# Read-write connection (requires XMLA read/write enabled)
with connect_semantic_model(
    dataset="My Semantic Model",
    readonly=False,
    workspace="My Workspace"
) as tom:
    # Modify model
    measure = tom.model.Tables["Sales"].Measures["Revenue"]
    measure.Description = "Total revenue in USD"
    # Changes are saved when context exits
```

### Important Notes

1. **XMLA Endpoint**: Read-write operations require XMLA read/write enabled on the capacity
2. **Context Manager**: Always use `with` statement - changes are saved on exit
3. **Service Principal**: Supported via `service_principal_authentication` context

---

## TOMWrapper Properties and Methods

### Core Properties

```python
with connect_semantic_model(dataset, workspace=workspace) as tom:
    # Access the model
    model = tom.model

    # Get dataset info
    dataset_id = tom._dataset_id
    dataset_name = tom._dataset_name
    workspace_id = tom._workspace_id
    workspace_name = tom._workspace_name

    # Check compatibility level
    compat_level = tom._compat_level
```

### Iterator Methods

| Method | Returns |
|--------|---------|
| `all_columns()` | All columns in all tables |
| `all_calculated_columns()` | All calculated columns |
| `all_calculated_tables()` | All calculated tables |
| `all_calculation_groups()` | All calculation groups |
| `all_measures()` | All measures |
| `all_partitions()` | All partitions |
| `all_hierarchies()` | All hierarchies |
| `all_levels()` | All hierarchy levels |
| `all_calculation_items()` | All calculation items |
| `all_functions()` | All user-defined functions |

### Example: Iterating Objects

```python
with connect_semantic_model(dataset, workspace=workspace) as tom:
    # List all measures
    for measure in tom.all_measures():
        print(f"Table: {measure.Parent.Name}, Measure: {measure.Name}")

    # List all columns
    for column in tom.all_columns():
        print(f"Table: {column.Table.Name}, Column: {column.Name}, Type: {column.DataType}")

    # List all partitions
    for partition in tom.all_partitions():
        print(f"Table: {partition.Table.Name}, Partition: {partition.Name}")
```

---

## Reading Model Metadata

### Tables

```python
with connect_semantic_model(dataset, workspace=workspace) as tom:
    for table in tom.model.Tables:
        print(f"Table: {table.Name}")
        print(f"  Description: {table.Description}")
        print(f"  Is Hidden: {table.IsHidden}")
```

### Columns

```python
with connect_semantic_model(dataset, workspace=workspace) as tom:
    for column in tom.all_columns():
        print(f"Column: {column.Name}")
        print(f"  Table: {column.Table.Name}")
        print(f"  Data Type: {column.DataType}")
        print(f"  Is Hidden: {column.IsHidden}")
```

### Measures

```python
with connect_semantic_model(dataset, workspace=workspace) as tom:
    for measure in tom.all_measures():
        print(f"Measure: {measure.Name}")
        print(f"  Table: {measure.Parent.Name}")
        print(f"  Expression: {measure.Expression}")
        print(f"  Format String: {measure.FormatString}")
```

### Relationships

```python
with connect_semantic_model(dataset, workspace=workspace) as tom:
    for rel in tom.model.Relationships:
        print(f"From: {rel.FromTable.Name}[{rel.FromColumn.Name}]")
        print(f"To: {rel.ToTable.Name}[{rel.ToColumn.Name}]")
        print(f"Cross Filter: {rel.CrossFilteringBehavior}")
```

---

## Modifying Model Metadata

### Update Measure Properties

```python
with connect_semantic_model(dataset, readonly=False, workspace=workspace) as tom:
    measure = tom.model.Tables["Sales"].Measures["Revenue"]
    measure.Description = "Total revenue"
    measure.FormatString = "$#,##0.00"
    measure.DisplayFolder = "Financial Metrics"
```

### Update Column Properties

```python
with connect_semantic_model(dataset, readonly=False, workspace=workspace) as tom:
    column = tom.model.Tables["Date"].Columns["Month"]
    column.IsHidden = False
    column.Description = "Calendar month"
    column.SortByColumn = tom.model.Tables["Date"].Columns["MonthNumber"]
```

### Update Table Properties

```python
with connect_semantic_model(dataset, readonly=False, workspace=workspace) as tom:
    table = tom.model.Tables["Sales"]
    table.Description = "Sales transactions"
    table.IsHidden = False
```

---

## Adding Model Objects

### Add a Measure

```python
import Microsoft.AnalysisServices.Tabular as TOM

with connect_semantic_model(dataset, readonly=False, workspace=workspace) as tom:
    table = tom.model.Tables["Sales"]

    new_measure = TOM.Measure()
    new_measure.Name = "Total Sales"
    new_measure.Expression = "SUM(Sales[Amount])"
    new_measure.FormatString = "$#,##0.00"
    new_measure.Description = "Sum of all sales amounts"

    table.Measures.Add(new_measure)
```

### Add a Calculated Column

```python
import Microsoft.AnalysisServices.Tabular as TOM

with connect_semantic_model(dataset, readonly=False, workspace=workspace) as tom:
    table = tom.model.Tables["Products"]

    calc_column = TOM.Column()
    calc_column.Name = "Profit Margin"
    calc_column.Type = TOM.ColumnType.Calculated
    calc_column.Expression = "[Revenue] - [Cost]"
    calc_column.DataType = TOM.DataType.Decimal

    table.Columns.Add(calc_column)
```

### Add a Relationship

```python
import Microsoft.AnalysisServices.Tabular as TOM

with connect_semantic_model(dataset, readonly=False, workspace=workspace) as tom:
    from_column = tom.model.Tables["Sales"].Columns["ProductKey"]
    to_column = tom.model.Tables["Products"].Columns["ProductKey"]

    relationship = TOM.SingleColumnRelationship()
    relationship.Name = f"{from_column.Table.Name}_{to_column.Table.Name}"
    relationship.FromColumn = from_column
    relationship.ToColumn = to_column
    relationship.CrossFilteringBehavior = TOM.CrossFilteringBehavior.OneDirection

    tom.model.Relationships.Add(relationship)
```

---

## TOMWrapper Helper Methods

The TOMWrapper class includes many helper methods for common operations:

### Display Methods (return DataFrames)

```python
with connect_semantic_model(dataset, workspace=workspace) as tom:
    # List all measures
    df = tom.list_measures()

    # List all columns
    df = tom.list_columns()

    # List all relationships
    df = tom.list_relationships()

    # List all partitions
    df = tom.list_partitions()
```

### Add Methods

```python
with connect_semantic_model(dataset, readonly=False, workspace=workspace) as tom:
    # Add measure
    tom.add_measure(
        table_name="Sales",
        measure_name="Total Sales",
        expression="SUM(Sales[Amount])",
        format_string="$#,##0.00"
    )

    # Add calculated column
    tom.add_calculated_column(
        table_name="Products",
        column_name="Margin",
        expression="[Revenue] - [Cost]"
    )
```

### Update Methods

```python
with connect_semantic_model(dataset, readonly=False, workspace=workspace) as tom:
    # Update measure expression
    tom.update_measure(
        table_name="Sales",
        measure_name="Total Sales",
        expression="SUMX(Sales, Sales[Quantity] * Sales[Price])"
    )
```

---

## Service Principal Authentication

```python
from sempy_labs import service_principal_authentication
from sempy_labs.tom import connect_semantic_model

# Using Service Principal
with service_principal_authentication(
    tenant_id="...",
    client_id="...",
    client_secret="..."
):
    with connect_semantic_model(
        dataset="My Model",
        readonly=False,
        workspace="My Workspace"
    ) as tom:
        # Operations use Service Principal credentials
        for measure in tom.all_measures():
            print(measure.Name)
```

---

## Azure Analysis Services Support

The TOMWrapper also supports Azure Analysis Services:

```python
with connect_semantic_model(
    dataset="MyDatabase",
    workspace="asazure://westus2.asazure.windows.net/myserver",
    readonly=True
) as tom:
    for table in tom.model.Tables:
        print(table.Name)
```

---

## Best Practices

### Do's

- ✅ Always use context manager (`with` statement)
- ✅ Use `readonly=True` when only reading metadata
- ✅ Use iterator methods (`all_measures()`, etc.) for efficient traversal
- ✅ Save changes by exiting the context normally

### Don'ts

- ❌ Don't open read-write connections when only reading
- ❌ Don't forget to handle exceptions (changes won't save on exception)
- ❌ Don't hold connections open longer than necessary

---

## Error Handling

```python
from sempy.fabric.exceptions import FabricHTTPException

try:
    with connect_semantic_model(
        dataset="My Model",
        readonly=False,
        workspace="My Workspace"
    ) as tom:
        # Modifications...
        pass
except FabricHTTPException as e:
    print(f"API error: {e}")
except Exception as e:
    print(f"Error: {e}")
    # Changes NOT saved due to exception
```

---

## Additional Resources

| Resource | URL |
|----------|-----|
| TOM Reference | [Microsoft Docs](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular) |
| Sample Notebook | [Tabular Object Model.ipynb](https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Tabular%20Object%20Model.ipynb) |
| API Documentation | [ReadTheDocs](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html) |
