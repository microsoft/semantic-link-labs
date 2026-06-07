---
name: dax-optimization
description: Methodology and an executable rule catalog for diagnosing and optimizing DAX query performance. Use this when analyzing a slow DAX query, interpreting trace timings / DAX query plans, reducing column cardinality, or extending the performance-analysis rules used by the interactive DAX test widget (sempy_labs.semantic_model.test).
---

# DAX Optimization

This skill explains how to determine optimization techniques for a DAX query and
its components, and documents the **executable rule catalog**
(`dax_optimization_rules.json`) that powers the *Performance analysis* tab of the
interactive DAX test widget.

The runtime rule engine lives in
`src/sempy_labs/semantic_model/_dax_optimization.py` and loads its rules from
`src/sempy_labs/semantic_model/_dax_optimization_rules.json` (the canonical copy
that is packaged and executed). The JSON in this skill folder is the same schema
and is the human-facing reference; keep the two in sync when adding rules.

## When to Use This Skill

- Diagnosing why a DAX query is slow.
- Interpreting Formula Engine (FE) vs Storage Engine (SE) timings from a trace.
- Reading a DAX query plan (logical/physical) for `CallbackDataID`, spools, scans.
- Deciding whether high column cardinality is the bottleneck.
- Adding, editing, or reviewing the performance-analysis rules.

---

## The Inputs

The analysis is computed from up to six artifacts. Each rule declares which
artifacts it `requires`; a rule is skipped if any required artifact is missing.

| Input | Source | What it provides |
|-------|--------|------------------|
| **DAX query** | The editor text | Syntax-level patterns (IFERROR, FILTER over a full table, nested iterators, raw `/` division). |
| **Model metadata** | TOM (`connect_semantic_model`) | Tables, columns, measures, relationships, data types. |
| **Query dependencies** | `INFO.CALCDEPENDENCY` | The exact tables/columns the query references. |
| **Trace details** | Server-side trace | `QueryEnd`, `VertiPaqSEQueryEnd`, cache matches → total/FE/SE duration, CPU, SE query count, parallelism. |
| **DAX query plan** | Trace `DAXQueryPlan` events | Logical/physical plan text → `CallbackDataID`, `Spool`, scan operators. |
| **Vertipaq Analyzer** | `vertipaq_analyzer(...)` | Column cardinality, size, encoding, data types. **Only used when the cardinalities of the `Data` columns are not all `1`** — otherwise there is nothing meaningful to analyze and Vertipaq rules are skipped. |

---

## Optimization Methodology

Work top-down, from the cheapest signal to the most detailed.

### 1. Establish the engine balance (FE vs SE)

The Storage Engine (VertiPaq) is multi-threaded and fast; the Formula Engine is
single-threaded. From the trace:

- `Total Duration = QueryEnd.Duration`
- `SE Duration = sum of VertiPaqSEQueryEnd.Duration` (excluding `Internal` subqueries)
- `FE Duration = Total − SE`

Then:
- **SE-bound** (`SE% ≥ 70%`): the query spends its time scanning data → attack
  **data volume and cardinality** (rule `SE_BOUND`).
- **FE-bound** (`FE% ≥ 70%`): the query spends its time in single-threaded logic
  → push work to the SE, remove callbacks, reduce materialization (rule
  `FE_BOUND`).

### 2. Look for `CallbackDataID` (the #1 red flag)

`CallbackDataID` in the **physical plan** means the SE had to call back into the
FE mid-scan. It disables VertiPaq optimizations and is usually caused by:

- `IF` / `IFERROR` / `ISERROR` / error handling inside an iterator,
- division by `/` (wrap in `DIVIDE`),
- rounding / date arithmetic / conditional logic evaluated row-by-row.

Rules: `CALLBACK_DATA_ID`, `USES_IFERROR`, `DIVISION_WITHOUT_DIVIDE`.

### 3. Count and size the Storage Engine queries

- **Many SE queries** (`≥ 10`) usually means fusion failed — simplify filter
  context and use variables to compute base values once (`MANY_SE_QUERIES`).
- **A single slow scan** (`≥ 50 ms`) points at a specific large/high-cardinality
  table — inspect its xmSQL in the plan (`SLOW_SE_SCAN`).
- **Low parallelism** (`SE CPU / SE Duration < 1.2x` over a meaningful SE
  duration) means scans are effectively single-threaded (`LOW_SE_PARALLELISM`).

### 4. Inspect the query plan for materialization

Large/many **spools** materialize intermediate results in the FE and cost memory
and time (`LARGE_SPOOL`). Reduce them with variables and earlier filtering.

### 5. Reduce cardinality (Vertipaq)

Cardinality drives dictionary size, scan cost, and `DISTINCTCOUNT`/join cost.
Focus on **columns the query actually references**:

- **High-cardinality columns** (`≥ 1,000,000` unique values): split datetime
  into date+time, bucket/round numerics, drop unused keys (`HIGH_CARDINALITY_COLUMN`).
- **High-cardinality floating point** columns are especially expensive — convert
  to fixed decimal/integer or round (`FLOAT_HIGH_CARDINALITY_COLUMN`).

### 6. Simplify the DAX itself

- **Nested iterators** multiply row evaluations (`NESTED_ITERATORS`).
- **Many iterators** (`≥ 5`) raise the chance of row-by-row work (`MANY_ITERATORS`).
- **FILTER over a whole table** scans every row — filter on columns instead
  (`FILTER_FULL_TABLE`).
- **Many referenced columns** (`≥ 15`) widen datacaches — project only what's
  needed (`MANY_REFERENCED_COLUMNS`).

### 7. Diagnostics

If the trace or plan wasn't captured, the engine emits an informational finding
(`NO_TRACE_CAPTURED`, `NO_QUERY_PLAN_CAPTURED`) telling the user to run the query
first so the full analysis can be produced.

---

## The Rules JSON Schema

Each entry in `rules` is one rule:

```jsonc
{
  "id": "CALLBACK_DATA_ID",          // stable identifier
  "title": "…",                      // short headline
  "category": "Query plan",          // grouping label
  "severity": "high|medium|low|info",
  "requires": ["query_plan"],        // artifacts that must be present
  "kind": "scalar|for_each",         // evaluation mode
  "condition": { … },                // scalar rules: evaluated against metrics
  "collection": "high_cardinality_columns", // for_each rules: list to iterate
  "where": { … },                    // for_each rules: per-item filter
  "max_findings": 8,                 // for_each rules: cap on emitted findings
  "message": "… {placeholder} …",    // templated; {tokens} filled from context
  "recommendation": "…",
  "references": ["https://…"]
}
```

### Conditions

A condition is a tree of:

- **Leaf** — `{"metric": "se_pct", "op": ">=", "value": 0.7}` for scalar rules,
  or `{"field": "cardinality", "op": ">=", "value": 1000000}` inside a
  `for_each` `where`.
- **Composite** — `{"all": [ … ]}`, `{"any": [ … ]}`, `{"not": { … }}`.

Operators: `>`, `>=`, `<`, `<=`, `==`, `!=`, `contains`, `not_contains`,
`regex`, `in`, `not_in`. Unknown operators / type errors evaluate to `false`,
so a malformed rule can never crash the analysis.

### Available metrics (scalar context)

`has_query`, `query_length`, `iterator_count`, `nested_iterator`,
`uses_iferror`, `uses_divide_function`, `uses_division_operator`,
`filter_full_table_count`, `cold_cache`, `has_trace`, `total_duration_ms`,
`se_duration_ms`, `fe_duration_ms`, `cpu_time_ms`, `se_pct`, `fe_pct`,
`se_query_count`, `se_internal_count`, `se_cache_match_count`, `se_cpu_ms`,
`se_parallelism`, `has_query_plan`, `callback_dataid_count`,
`encode_callback_count`, `spool_count`, `referenced_column_count`,
`referenced_table_count`, `has_dependencies`, `vertipaq_available`,
`vertipaq_skipped_trivial`, `max_data_column_cardinality`,
`high_cardinality_data_column_count`. Display helpers: `se_pct_display`,
`fe_pct_display`, `se_parallelism_display`.

### Available collections (`for_each` context)

| Collection | Item fields |
|------------|-------------|
| `high_cardinality_columns` | `table`, `column`, `cardinality`, `cardinality_display`, `data_type`, `is_floating_point`, `data_size`, `encoding` |
| `slow_se_queries` | `subclass`, `duration`, `cpu` |

`message` placeholders for `for_each` rules can reference any item field as well
as any scalar metric.

---

## Adding or Editing a Rule

1. Add the rule object to **both** JSON copies (package + this skill folder).
2. If the rule needs a new metric or collection, add it in
   `build_context()` in `_dax_optimization.py`.
3. Keep `severity` honest: reserve `high` for things that clearly dominate
   runtime (e.g. `CallbackDataID`, `IFERROR`).
4. Provide an actionable `recommendation` and at least one authoritative
   `reference` (SQLBI or Microsoft Learn).
5. Validate: `python -c "import json,sys; json.load(open('src/sempy_labs/semantic_model/_dax_optimization_rules.json'))"`.

---

## References

- SQLBI — Understanding DAX query plans: https://www.sqlbi.com/articles/understanding-dax-query-plans/
- SQLBI — Optimizing high-cardinality columns in VertiPaq: https://www.sqlbi.com/articles/optimizing-high-cardinality-columns-in-vertipaq/
- SQLBI — Error handling in DAX measures: https://www.sqlbi.com/articles/error-handling-in-dax-measures/
- Microsoft Learn — DIVIDE function: https://learn.microsoft.com/dax/divide-function-dax
- Microsoft Learn — Data reduction techniques for import modeling: https://learn.microsoft.com/power-bi/guidance/import-modeling-data-reduction
