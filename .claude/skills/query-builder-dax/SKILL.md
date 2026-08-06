---
name: query-builder-dax
description: Guide for the DAX query shape produced by the Query Builder in the DAX test widget. Use this when modifying how the Query Builder generates DAX, or when generating an EVALUATE query from columns, measures, filters and sorting.
---

# SKILL.md — Query Builder DAX Generation

## Purpose

Document the canonical DAX query structure produced by the Query Builder in
the DAX performance test widget (`_build_summarize_dax` in
`src/sempy_labs/semantic_model/_test_dax.py`).

The pattern follows the well-known "Using DAX as a Query Language" approach
(Michael Kovalsky, Elegant BI:
<https://www.elegantbi.com/post/daxquerylanguage>), built on
`SUMMARIZECOLUMNS`.

Use this skill when changing how Query Builder state (columns, measures,
filters, sorting) is converted to DAX, so the generated query stays valid and
consistent.

---

## The canonical query shape

```dax
EVALUATE
SUMMARIZECOLUMNS(
    'Geography'[Area],
    'Geography'[Country],
    'Product'[Product],
    FILTER(KEEPFILTERS(VALUES('Product'[Product Category])), 'Product'[Product Category] = "Bicycles"),
    "Revenue", [Revenue]
)
ORDER BY 'Geography'[Area], 'Geography'[Country]
```

`SUMMARIZECOLUMNS` implies the aggregation (the SQL `GROUP BY` is not needed)
and the model relationships handle the joins.

---

## Element order (STRICT — wrong order errors out)

Inside `SUMMARIZECOLUMNS(...)` the elements MUST appear in this order:

| # | Element | Where it goes | Syntax |
|---|---------|---------------|--------|
| 1 | **Attributes (columns)** | First | `'Table'[Column]` — one per group-by column, left to right |
| 2 | **Filters (on columns)** | After all attributes | `FILTER(KEEPFILTERS(VALUES('Table'[Column])), <predicate>)` — one per filter, any order |
| 3 | **Measures** | After all filters | `"Display Name", [Measure]` — display name in quotes, measure in brackets |

Then, OUTSIDE `SUMMARIZECOLUMNS`:

| Element | Where it goes | Syntax |
|---------|---------------|--------|
| **Sorting** | Final clause, after the table expression | `ORDER BY 'Table'[Column] [ASC\|DESC], ...` (defaults to ascending) |

---

## Columns (attributes)

- Each group-by column from the builder's *Columns & Measures* pane becomes an
  attribute, in pane order (top to bottom = left to right in the result).
- Always fully qualify: `'Table'[Column]`. Escape `'` → `''` in the table name
  and `]` → `]]` in the column name.

## Measures

- Each measure from the *Columns & Measures* pane becomes a measure element.
- Syntax is `"Display Name", [Measure]`. The display name only renames the
  output column; it does not rename the model measure.
- Custom/test measures can be added with a `DEFINE MEASURE ... ` block placed
  **before** `EVALUATE`, then referenced in the measures section. (The Query
  Builder currently only references existing model measures.)

## Filters

Filters come from the builder's *Filters* pane. Split by object kind:

- **Column filters** go **inside** `SUMMARIZECOLUMNS`, after the attributes,
  each as:
  ```dax
  FILTER(KEEPFILTERS(VALUES('Table'[Column])), <predicate>)
  ```
  `KEEPFILTERS(VALUES(...))` preserves any existing filter context on that
  column while applying the new condition.

- **Measure filters** cannot live inside `SUMMARIZECOLUMNS` (measures are not
  yet projected at that point). Wrap the whole table instead:
  ```dax
  FILTER(
      SUMMARIZECOLUMNS( ... ),
      [Measure] > 100
  )
  ```
  Multiple measure predicates are combined with `&&`.

### Predicate operators

`_qb_build_predicate` maps a filter item (`ref`, `kind`, `data_type`, `op`,
`value`, `value2`) to a boolean predicate:

| Filter family | Operators → DAX |
|---------------|-----------------|
| text | `eq` `=`, `ne` `<>`, `contains` `CONTAINSSTRING(ref, "v")`, `startswith` `LEFT(ref, n) = "v"` |
| numeric / datetime / measure | `eq` `=`, `ne` `<>`, `gt` `>`, `ge` `>=`, `lt` `<`, `le` `<=`, `between` `ref >= lo && ref <= hi` |
| boolean | `istrue` `ref = TRUE()`, `isfalse` `ref = FALSE()` |
| any | `blank` `ISBLANK(ref)`, `notblank` `NOT ISBLANK(ref)` |

Value literals:
- numeric → bare number (quoted string if not numeric),
- datetime → `DATE(y, m, d)` when the value matches `YYYY-MM-DD`, else a quoted
  string,
- text → quoted string (`"` escaped as `""`).

## Sorting (ORDER BY)

- The `ORDER BY` clause is the final part of the query, placed **after** the
  (possibly `FILTER`-wrapped) `SUMMARIZECOLUMNS` table expression.
- It is driven by the builder's *Order By* pane, which mirrors the
  columns/measures from the *Columns & Measures* pane. Each Order By item has:
  - a **toggle** (on/off, default **off**) — only enabled items appear in
    `ORDER BY`;
  - a **direction** icon — A-Z = ascending (`ASC`), Z-A = descending (`DESC`);
  - independent **reorder** support (the pane order = clause order).
- The serialized state carries an `order_by` list of items
  (`ref`, `kind`, `name`, `table`, `enabled`, `dir`). `_build_summarize_dax`
  emits `ORDER BY <ref> ASC|DESC, ...` for the enabled items, in pane order.
- Measures can be used in `ORDER BY` because each measure is projected as a
  column in the result.
- If the `order_by` key is **absent** (legacy state), the builder falls back to
  ordering by all attribute columns ascending. If present but no item is
  enabled, no `ORDER BY` is emitted.

---

## Edge cases

- **Measures only (no columns):** emit `SUMMARIZECOLUMNS("Name", [Measure], ...)`
  with no attributes and no `ORDER BY`. Column filters still apply inside;
  measure filters still wrap the outside.
- **Nothing usable:** return an empty string (no columns and no measures).
- **TOPN:** to cap rows, wrap the table in `TOPN(n, <table>)` (respects the
  `ORDER BY`). Not currently emitted by the Query Builder.

---

## Keep in sync

The JS front-end (`onBuildClick`) serializes builder state to JSON with
snake_case keys; the Python helpers read them. When adding operators or field
metadata, update BOTH:

- JS `QB_OPS` / chip serialization in `_test_dax.py` (widget_js),
- Python `_qb_build_predicate` / `_classify_filter_type` / `_build_summarize_dax`.

Field objects use keys: `kind`, `table`, `name`, `data_type`, `ref`.
Filter objects additionally: `op`, `value`, `value2`.
Order By objects additionally: `enabled` (bool), `dir` (`"asc"`/`"desc"`).
