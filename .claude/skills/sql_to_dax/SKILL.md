---
name: sql_to_dax
description: Guide for translating SQL aggregation expressions into DAX measures.
---

# SKILL.md — SQL to DAX Metric Translation

## Purpose

Translate analytical SQL aggregation expressions into equivalent DAX measures.

The goal is semantic equivalence, not syntactic similarity.

The generated DAX should:

- Follow Power BI / Tabular best practices
- Prefer iterator functions when row context is required
- Use DIVIDE instead of `/ NULLIF(...,0)`
- Fully qualify columns using `'table'[column]`
- Use CALCULATE where filter context translation is required
- Preserve aggregation semantics exactly
- Avoid SQL constructs unsupported in DAX by rewriting logically

---

# SQL Identifier Parsing Rules

The SQL source may reference columns using any of the following formats:

```sql
column_name
```

```sql
table.column_name
```

```sql
table.`column name`
```

The translator must normalize all forms into valid DAX column references.

---

## Identifier Normalization Rules

| SQL Format | DAX Format |
|---|---|
| column_name | 'table'[column_name] |
| table.column_name | 'table'[column_name] |
| table.`column name` | 'table'[column name] |

---

## Backtick Handling

SQL backticks must be removed during translation.

### SQL
```sql
dim_product.`standard cost`
```

### DAX
```dax
'dim_product'[standard cost]
```

---

## Unqualified Column Resolution

If a column is referenced without a table qualifier:

```sql
SUM(SALES_AMOUNT)
```

The translator should:

1. Infer the table from model metadata if available
2. Prefer the primary fact table in the expression
3. Fully qualify the final DAX output

### DAX
```dax
SUM('fact_sales'[SALES_AMOUNT])
```

> **Important — bare identifiers inside scalar aggregates must NOT become iterators.**
>
> When a SQL aggregate wraps a single bare identifier (e.g. `SUM(ORIGINAL_SALES_AMOUNT)`,
> `SUM(COST_OF_GOODS_SOLD)`) and that identifier is not declared as a
> column in the model metadata, the translator must still emit the
> scalar aggregation form against the default/owning table:
>
> ```dax
> SUM('fact_returns'[ORIGINAL_SALES_AMOUNT])
> ```
>
> It must **not** fall through to the iterator form:
>
> ```dax
> SUMX('fact_returns', ORIGINAL_SALES_AMOUNT)   -- INVALID
> ```
>
> Iterators (`SUMX`, `AVERAGEX`, etc.) are reserved for cases where the
> aggregate argument contains arithmetic or references multiple
> columns. A single bare token is always a scalar aggregation.

---

## Mixed Identifier Formats

Expressions may mix styles.

### SQL
```sql
SUM(fact_sales.`sales amount` - DISCOUNT_AMOUNT)
```

### DAX
```dax
SUMX(
    'fact_sales',
    'fact_sales'[sales amount] -
    'fact_sales'[DISCOUNT_AMOUNT]
)
```

---

# Core Translation Rules

---

## 1. Aggregate Functions

### SQL
```sql
SUM(column)
```

### DAX
```dax
SUM('table'[column])
```

---

### SQL
```sql
AVG(column)
```

### DAX
```dax
AVERAGE('table'[column])
```

---

### SQL
```sql
COUNT(DISTINCT column)
```

### DAX
```dax
DISTINCTCOUNT('table'[column])
```

---

## 2. Arithmetic Inside Aggregations

If arithmetic occurs inside SUM/AVG/etc., use iterator functions.

### SQL
```sql
SUM(price * quantity)
```

### DAX
```dax
SUMX(
    'table',
    'table'[price] * 'table'[quantity]
)
```

---

### SQL
```sql
SUM(revenue - discount)
```

### DAX
```dax
SUMX(
    'table',
    'table'[revenue] - 'table'[discount]
)
```

---

### SQL
```sql
AVG(quantity * cost)
```

### DAX
```dax
AVERAGEX(
    'table',
    'table'[quantity] * 'table'[cost]
)
```

---

# Iterator Function Rules

Use iterator functions when:

- Multiple columns participate in row-level arithmetic
- Expressions exist inside aggregate functions
- Mixed table references occur inside aggregation

| SQL Aggregate | DAX Iterator |
|---|---|
| SUM(expr) | SUMX(table, expr) |
| AVG(expr) | AVERAGEX(table, expr) |
| MIN(expr) | MINX(table, expr) |
| MAX(expr) | MAXX(table, expr) |

---

# Safe Division

---

## SQL NULLIF Pattern

### SQL
```sql
SUM(sales) / NULLIF(SUM(cost), 0)
```

### DAX
```dax
DIVIDE(
    SUM('table'[sales]),
    SUM('table'[cost])
)
```

---

## Nested NULLIF

### SQL
```sql
365 / NULLIF(metric, 0)
```

### DAX
```dax
DIVIDE(
    365,
    [metric]
)
```

---

# Percentage Calculations

---

### SQL
```sql
(metric / total) * 100
```

### DAX
```dax
DIVIDE(
    [metric],
    [total]
) * 100
```

---

# ROUND Translation

---

### SQL
```sql
ROUND(expression, 2)
```

### DAX
```dax
ROUND(expression, 2)
```

---

# DIV0 Translation

DIV0 means divide-by-zero-safe division.

### SQL
```sql
DIV0(a, b)
```

### DAX
```dax
DIVIDE(a, b)
```

---

# Window Function Translation

---

## Rolling Window SUM

Translate `<agg>(<inner>) OVER (ORDER BY <col> ROWS BETWEEN N PRECEDING AND CURRENT ROW)`
into a `CALCULATE` wrapping the inner aggregation with a `DATESINPERIOD`
filter over the ORDER BY column.

Rules:

- The window's ORDER BY column becomes the date column passed to
  `DATESINPERIOD`. It is resolved through the column map and fully
  qualified as `'table'[column]`.
- `MAX(<order_col>)` is used as the anchor date.
- `ROWS BETWEEN N PRECEDING AND CURRENT ROW` becomes `-N, DAY`. The skill
  preserves the literal `N` from the SQL (e.g. `89 PRECEDING` → `-89, DAY`)
  rather than rounding up to the inclusive day count.
- When the inner expression is itself an aggregate (a non-standard but
  common Snowflake/BigQuery pattern such as `SUM(SUM(...)) OVER (...)`),
  the redundant outer aggregate is stripped and only the inner
  aggregation body is preserved.
- The inner aggregation body is translated using the normal aggregation
  rules (including the additive distribution rule that turns
  `SUM(a - b)` into `(SUM(a) - SUM(b))`).

### SQL
```sql
SUM(metric)
OVER (
    ORDER BY DATE_KEY
    ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
)
```

### DAX
```dax
CALCULATE(
    [metric],
    DATESINPERIOD(
        'dim_date'[date_key],
        MAX('dim_date'[date_key]),
        -89,
        DAY
    )
)
```

---

## Rolling Window over a Nested Aggregate

### SQL
```sql
SUM(
    SUM(EXTENDED_AMOUNT - DISCOUNT_AMOUNT)
) OVER (
    ORDER BY DATE_KEY
    ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
)
```

### DAX
```dax
CALCULATE(
    (
        SUM('fact_sales'[sales_amount])
        - SUM('fact_sales'[discount_amount])
    ),
    DATESINPERIOD(
        'dim_date'[date_key],
        MAX('dim_date'[date_key]),
        -89,
        DAY
    )
)
```

Equivalently (when distribution is not applied), the inner body may be
emitted in iterator form:

```dax
CALCULATE(
    SUMX(
        'fact_sales',
        'fact_sales'[sales_amount] - 'fact_sales'[discount_amount]
    ),
    DATESINPERIOD(
        'dim_date'[date_key],
        MAX('dim_date'[date_key]),
        -89,
        DAY
    )
)
```

---

## Unbounded Window

### SQL
```sql
MAX(metric) OVER ()
```

### DAX
```dax
CALCULATE(
    MAX('table'[metric]),
    ALL('table')
)
```

---

# CASE WHEN Translation

---

## Conditional DISTINCTCOUNT

### SQL
```sql
COUNT(DISTINCT CASE
    WHEN order_count > 1
    THEN customer_key
END)
```

### DAX
```dax
CALCULATE(
    DISTINCTCOUNT('table'[customer_key]),
    'table'[order_count] > 1
)
```

---

# Multi-Table Arithmetic

When expressions reference multiple tables:

- Preserve table qualification
- Use iterator functions
- Choose the fact table as the iterator table when possible

---

## Iterator Table Selection from Relationships

When an X-function (`SUMX`, `AVERAGEX`, `COUNTX`, `MINX`, `MAXX`) is needed and
the inner expression references two tables that participate in a relationship,
choose the iterator table as the **"from" side** of that relationship
(typically the many / fact side). Wrap any column reference to the
**"to" side** (typically the one / dimension side) in `RELATED(...)`.

This rule is independent of the table that the measure is *defined on* — what
matters is which table is on the many side of the relationship linking the
two referenced tables.

### Example

Given a relationship: `fact_sales (Many) → dim_product (One)`

### SQL
```sql
SUM(dim_product.standard_cost * fact_sales.quantity_sold)
```

### DAX
```dax
SUMX(
    'fact_sales',
    RELATED('dim_product'[standard_cost]) *
    'fact_sales'[quantity_sold]
)
```

Even when this measure is authored on the `dim_product` table, the iterator
table is still `fact_sales` because `fact_sales` is the "from" side of the
relationship.

---

### SQL
```sql
SUM(dim_product.standard_cost * fact_sales.quantity)
```

### DAX
```dax
SUMX(
    'fact_sales',
    RELATED('dim_product'[standard_cost]) *
    'fact_sales'[quantity]
)
```

---

### SQL
```sql
SUM(
    (EXTENDED_AMOUNT - DISCOUNT_AMOUNT)
    - (STANDARD_COST * QUANTITY)
)
```

### DAX
```dax
SUMX(
    'fact_sales',
    ('fact_sales'[EXTENDED_AMOUNT] - 'fact_sales'[DISCOUNT_AMOUNT])
    -
    (
        RELATED('dim_product'[STANDARD_COST]) *
        'fact_sales'[QUANTITY]
    )
)
```

---

# KPI Translation Patterns

---

## Profit

### SQL
```sql
SUM(revenue - cost)
```

### DAX
```dax
SUMX(
    'fact',
    'fact'[revenue] - 'fact'[cost]
)
```

---

## Margin %

### SQL
```sql
SUM(profit)
/
NULLIF(SUM(revenue), 0)
```

### DAX
```dax
DIVIDE(
    [Profit],
    [Revenue]
)
```

---

## Return Rate %

### SQL
```sql
SUM(return_amount)
/
NULLIF(SUM(original_sales_amount), 0)
```

### DAX
```dax
DIVIDE(
    SUM('fact_returns'[return_amount]),
    SUM('fact_sales'[original_sales_amount])
)
```

---

# Table Qualification Rules

---

## Always Qualify Columns

Preferred:
```dax
'fact_sales'[sales_amount]
```

Avoid:
```dax
[sales_amount]
```

---

# Relationship Translation

---

## SQL Join Semantics

When SQL implies dimension lookup:

### SQL
```sql
dim_product.standard_cost
```

inside fact aggregation becomes:

### DAX
```dax
RELATED('dim_product'[standard_cost])
```

---

# Translation Heuristics

---

## Detect Iterator Requirement

Use X-iterators when:

- Expression contains operators inside aggregation
- More than one column appears inside SUM/AVG/etc.
- Arithmetic mixes dimensions and facts

---

## Detect Measure References

Nested aggregates inside a window function are flattened — the redundant
outer aggregate is dropped and only the inner aggregate body is wrapped
in `CALCULATE`.

### SQL
```sql
SUM(SUM(revenue)) OVER (
    ORDER BY DATE_KEY
    ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
)
```

### DAX
```dax
CALCULATE(
    SUM('fact_sales'[revenue]),
    DATESINPERIOD(
        'dim_date'[date_key],
        MAX('dim_date'[date_key]),
        -89,
        DAY
    )
)
```

---

# Common SQL → DAX Mappings

| SQL | DAX |
|---|---|
| SUM(col) | SUM(table[col]) |
| AVG(col) | AVERAGE(table[col]) |
| COUNT(DISTINCT col) | DISTINCTCOUNT(table[col]) |
| NULLIF(x,0) | DIVIDE(... ) |
| ROUND(x,n) | ROUND(x,n) |
| CASE WHEN | CALCULATE/FILTER |
| OVER(...) | CALCULATE + time intelligence |
| SUM(a*b) | SUMX(table,a*b) |

---

# Example Translations

---

## Example 1

### SQL
```sql
SUM(store_sales.ss_sales_price * store_sales.ss_quantity)
```

### DAX
```dax
SUMX(
    'store_sales',
    'store_sales'[ss_sales_price] *
    'store_sales'[ss_quantity]
)
```

---

## Example 2

### SQL
```sql
SUM(ATTRIBUTED_REVENUE)
/
NULLIF(SUM(SPEND_AMOUNT), 0)
```

### DAX
```dax
DIVIDE(
    SUM('fact_marketing'[ATTRIBUTED_REVENUE]),
    SUM('fact_marketing'[SPEND_AMOUNT])
)
```

---

## Example 3

### SQL
```sql
(
  COUNT(DISTINCT CASE
    WHEN CUSTOMER_ORDER_COUNT > 1
    THEN CUSTOMER_KEY
  END)
  /
  NULLIF(COUNT(DISTINCT CUSTOMER_KEY), 0)
) * 100
```

### DAX
```dax
DIVIDE(
    CALCULATE(
        DISTINCTCOUNT('customer'[CUSTOMER_KEY]),
        'customer'[CUSTOMER_ORDER_COUNT] > 1
    ),
    DISTINCTCOUNT('customer'[CUSTOMER_KEY])
) * 100
```

---

# Output Requirements

Generated DAX must:

- Be valid Power BI DAX syntax
- Use proper indentation
- Use uppercase DAX functions
- Prefer DIVIDE over `/`
- Prefer iterators over invalid scalar arithmetic
- Preserve business semantics exactly
- Avoid unnecessary CALCULATE wrappers
- Use RELATED for dimension attribute access
- Use measures when semantic reuse is implied

---

# Important Semantic Differences

SQL is row-set based.

DAX is filter-context based.

Correct translation often requires:

- Iterator functions
- Context transition
- Relationship navigation
- Measure decomposition

Do not attempt direct token replacement.

