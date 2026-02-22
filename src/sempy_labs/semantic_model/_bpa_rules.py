rules = [
    {
        "Category": "Performance",
        "Scope": "Column",
        "Severity": "Warning",
        "Name": "Do not use floating point data types",
        "Expression": "obj.DataType == TOM.DataType.Double",
        "Description": 'The "Double" floating point data type should be avoided, as it can result in unpredictable roundoff errors and decreased performance in certain scenarios. Use "Int64" or "Decimal" where appropriate (but note that "Decimal" is limited to 4 digits after the decimal sign).',
        "URL": None,
    },
    {
        "Category": "Performance",
        "Scope": "Column",
        "Severity": "Warning",
        "Name": "Avoid using calculated columns",
        "Expression": "obj.Type == TOM.ColumnType.Calculated",
        "Description": "Calculated columns do not compress as well as data columns so they take up more memory. They also slow down processing times for both the table as well as process recalc. Offload calculated column logic to your data warehouse and turn these calculated columns into data columns.",
        "URL": "https://www.elegantbi.com/post/top10bestpractices",
    },
    {
        "Category": "Performance",
        "Scope": "Relationship",
        "Severity": "Warning",
        "Name": "Check if bi-directional and many-to-many relationships are valid",
        "Expression": """ (
            obj.FromCardinality == TOM.RelationshipEndCardinality.Many
            and obj.ToCardinality == TOM.RelationshipEndCardinality.Many
        )
        or str(obj.CrossFilteringBehavior) == "BothDirections" """,
        "Description": "Bi-directional and many-to-many relationships may cause performance degradation or even have unintended consequences. Make sure to check these specific relationships to ensure they are working as designed and are actually necessary.",
        "URL": "https://www.sqlbi.com/articles/bidirectional-relationships-and-ambiguity-in-dax",
    },
    {
        "Category": "Performance",
        "Scope": "Row Level Security",
        "Severity": "Info",
        "Name": "Check if dynamic row level security (RLS) is necessary",
        "Expression": """ any(
            re.search(pattern, obj.FilterExpression, flags=re.IGNORECASE)
            for pattern in ["USERPRINCIPALNAME()", "USERNAME()"]
        )""",
        "Description": "Usage of dynamic row level security (RLS) can add memory and performance overhead. Please research the pros/cons of using it.",
        "URL": "https://docs.microsoft.com/power-bi/admin/service-admin-rls",
    },
    {
        "Category": "Performance",
        "Scope": "Table",
        "Severity": "Warning",
        "Name": "Avoid using many-to-many relationships on tables used for dynamic row level security",
        "Expression": """ any(
            r.FromCardinality == TOM.RelationshipEndCardinality.Many
            and r.ToCardinality == TOM.RelationshipEndCardinality.Many
            for r in tom.used_in_relationships(object=obj)
        )
        and any(t.Name == obj.Name for t in tom.all_rls())""",
        "Description": "Using many-to-many relationships on tables which use dynamic row level security can cause serious query performance degradation. This pattern's performance problems compound when snowflaking multiple many-to-many relationships against a table which contains row level security. Instead, use one of the patterns shown in the article below where a single dimension table relates many-to-one to a security table.",
        "URL": "https://www.elegantbi.com/post/dynamicrlspatterns",
    },
    {
        "Category": "Performance",
        "Scope": "Relationship",
        "Severity": "Warning",
        "Name": "Many-to-many relationships should be single-direction",
        "Expression": """ (
            obj.FromCardinality == TOM.RelationshipEndCardinality.Many
            and obj.ToCardinality == TOM.RelationshipEndCardinality.Many
        )
        and obj.CrossFilteringBehavior == TOM.CrossFilteringBehavior.BothDirections""",
    },
    {
        "Category": "Performance",
        "Scope": "Column",
        "Severity": "Warning",
        "Name": "Set IsAvailableInMdx to false on non-attribute columns",
        "Expression": """tom.is_direct_lake() is False
        and obj.IsAvailableInMDX
        and (obj.IsHidden or obj.Parent.IsHidden)
        and obj.SortByColumn is None
        and not any(tom.used_in_sort_by(column=obj))
        and not any(tom.used_in_hierarchies(column=obj))""",
        "Description": "To speed up processing time and conserve memory after processing, attribute hierarchies should not be built for columns that are never used for slicing by MDX clients. In other words, all hidden columns that are not used as a Sort By Column or referenced in user hierarchies should have their IsAvailableInMdx property set to false. The IsAvailableInMdx property is not relevant for Direct Lake models.",
        "URL": "https://blog.crossjoin.co.uk/2018/07/02/isavailableinmdx-ssas-tabular",
    },
    {
        "Category": "Performance",
        "Scope": "Partition",
        "Severity": "Warning",
        "Name": "Set 'Data Coverage Definition' property on the DirectQuery partition of a hybrid table",
        "Expression": """ tom.is_hybrid_table(
            table_name=obj.Parent.Name
        )
        and obj.Mode == TOM.ModeType.DirectQuery
        and obj.DataCoverageDefinition is None""",
        "Description": "Setting the 'Data Coverage Definition' property may lead to better performance because the engine knows when it can only query the import-portion of the table and when it needs to query the DirectQuery portion of the table.",
        "URL": "https://learn.microsoft.com/analysis-services/tom/table-partitions?view=asallproducts-allversions",
    },
    {
        "Category": "Performance",
        "Scope": "Model",
        "Severity": "Warning",
        "Name": "Dual mode is only relevant for dimension tables if DirectQuery is used for the corresponding fact table",
        "Expression": """ not any(
            p.Mode == TOM.ModeType.DirectQuery for p in tom.all_partitions()
        )
        and any(p.Mode == TOM.ModeType.Dual for p in tom.all_partitions())""",
        "Description": "Only use Dual mode for dimension tables/partitions where a corresponding fact table is in DirectQuery. Using Dual mode in other circumstances (i.e. rest of the model is in Import mode) may lead to performance issues especially if the number of measures in the model is high.",
    },
    {
        "Category": "Performance",
        "Scope": "Table",
        "Severity": "Warning",
        "Name": "Set dimensions tables to dual mode instead of import when using DirectQuery on fact tables",
        "Expression": """ sum(
            1 for p in obj.Partitions if p.Mode == TOM.ModeType.Import
        )
        == 1
        and obj.Partitions.Count == 1
        and tom.has_hybrid_table()
        and any(
            r.ToCardinality == TOM.RelationshipEndCardinality.One
            and r.ToTable.Name == obj.Name
            for r in tom.used_in_relationships(object=obj)
        )""",
        "Description": "When using DirectQuery, dimension tables should be set to Dual mode in order to improve query performance.",
        "URL": "https://learn.microsoft.com/power-bi/transform-model/desktop-storage-mode#propagation-of-the-dual-setting",
    },
    {
        "Category": "Performance",
        "Scope": "Partition",
        "Severity": "Warning",
        "Name": "Minimize Power Query transformations",
        "Expression": """ obj.SourceType
        == TOM.PartitionSourceType.M
        and any(
            item in obj.Source.Expression
            for item in [
                'Table.Combine("',
                'Table.Join("',
                'Table.NestedJoin("',
                'Table.AddColumn("',
                'Table.Group("',
                'Table.Sort("',
                'Table.Pivot("',
                'Table.Unpivot("',
                'Table.UnpivotOtherColumns("',
                'Table.Distinct("',
                '[Query=(""SELECT',
                "Value.NativeQuery",
                "OleDb.Query",
                "Odbc.Query",
            ]
        )""",
        "Description": "Minimize Power Query transformations in order to improve model processing performance. It is a best practice to offload these transformations to the data warehouse if possible. Also, please check whether query folding is occurring within your model. Please reference the article below for more information on query folding.",
        "URL": "https://docs.microsoft.com/power-query/power-query-folding",
    },
    {
        "Category": "Performance",
        "Scope": "Table",
        "Severity": "Warning",
        "Name": "Consider a star-schema instead of a snowflake architecture",
        "Expression": """ obj.CalculationGroup is None
        and (
            any(
                r.FromTable.Name == obj.Name
                for r in tom.used_in_relationships(object=obj)
            )
            and any(
                r.ToTable.Name == obj.Name
                for r in tom.used_in_relationships(object=obj)
            )
        )""",
        "Description": "Generally speaking, a star-schema is the optimal architecture for tabular models. That being the case, there are valid cases to use a snowflake approach. Please check your model and consider moving to a star-schema architecture.",
        "URL": "https://docs.microsoft.com/power-bi/guidance/star-schema",
    },
    {
        "Category": "Performance",
        "Scope": "Model",
        "Severity": "Warning",
        "Name": "Avoid using views when using Direct Lake mode",
        "Expression": """ tom.is_direct_lake_using_view()""",
        "Description": "In Direct Lake mode, views will always fall back to DirectQuery. Thus, in order to obtain the best performance use lakehouse tables instead of views.",
        "URL": "https://learn.microsoft.com/fabric/get-started/direct-lake-overview#fallback",
    },
    {
        "Category": "Performance",
        "Scope": "Measure",
        "Severity": "Warning",
        "Name": "Avoid adding 0 to a measure",
        "Expression": """ obj.Expression.replace(" ", "").startswith("0+")
            or obj.Expression.replace(" ", "").endswith("+0")
            or re.search(
                r"DIVIDE\s*\(\s*[^,]+,\s*[^,]+,\s*0\s*\)",
                obj.Expression,
                flags=re.IGNORECASE,
            )
            or re.search(
                r"IFERROR\s*\(\s*[^,]+,\s*0\s*\)",
                obj.Expression,
                flags=re.IGNORECASE,
            )""",
        "Description": "Adding 0 to a measure in order for it not to show a blank value may negatively impact performance.",
    },
    {
        "Category": "Performance",
        "Scope": "Table",
        "Severity": "Warning",
        "Name": "Reduce usage of calculated tables",
        "Expression": """ tom.is_field_parameter(
            table_name=obj.Name
        )
        is False
        and tom.is_calculated_table(table_name=obj.Name)""",
        "Description": "Migrate calculated table logic to your data warehouse. Reliance on calculated tables will lead to technical debt and potential misalignments if you have multiple models on your platform.",
    },
    {
        "Category": "Performance",
        "Scope": "Column",
        "Severity": "Warning",
        "Name": "Reduce usage of calculated columns that use the RELATED function",
        "Expression": """ obj.Type
        == TOM.ColumnType.Calculated
        and re.search(r"related\s*\(", obj.Expression, flags=re.IGNORECASE)""",
        "Description": "Calculated columns do not compress as well as data columns and may cause longer processing times. As such, calculated columns should be avoided if possible. One scenario where they may be easier to avoid is if they use the RELATED function.",
        "URL": "https://www.sqlbi.com/articles/storage-differences-between-calculated-columns-and-calculated-tables",
    },
    {
        "Category": "Performance",
        "Scope": "Model",
        "Severity": "Warning",
        "Name": "Avoid excessive bi-directional or many-to-many relationships",
        "Expression": """ (
            (
                sum(
                    1
                    for r in obj.Relationships
                    if r.CrossFilteringBehavior
                    == TOM.CrossFilteringBehavior.BothDirections
                )
                + sum(
                    1
                    for r in obj.Relationships
                    if (r.FromCardinality == TOM.RelationshipEndCardinality.Many)
                    and (r.ToCardinality == TOM.RelationshipEndCardinality.Many)
                )
            )
            / max(int(obj.Relationships.Count), 1)
        )
        > 0.3""",
        "Description": "Limit use of b-di and many-to-many relationships. This rule flags the model if more than 30% of relationships are bi-di or many-to-many.",
        "URL": "https://www.sqlbi.com/articles/bidirectional-relationships-and-ambiguity-in-dax",
    },
    # ('Performance', 'Column', 'Warning', 'Avoid bi-directional or many-to-many relationships against high-cardinality columns',
    # lambda obj, TOM, re: ((str(r.FromCardinality) == 'Many' and str(r.ToCardinality == 'Many'))  or (str(r.CrossFilteringBehavior) == 'BothDirections') for r in tom.used_in_relationships(object = obj)) and tom.cardinality(column = obj) > 100000,
    # 'For best performance, it is recommended to avoid using bi-directional relationships against high-cardinality columns',
    # ),
    {
        "Category": "Performance",
        "Scope": "Table",
        "Severity": "Warning",
        "Name": "Remove auto-date table",
        "Expression": """ tom.is_calculated_table(
            table_name=obj.Name
        )
        and (
            obj.Name.startswith("DateTableTemplate_")
            or obj.Name.startswith("LocalDateTable_")
            )""",
        "Description": "Avoid using auto-date tables. Make sure to turn off auto-date table in the settings in Power BI Desktop. This will save memory resources.",
        "URL": "https://www.youtube.com/watch?v=xu3uDEHtCrg",
    },
    {
        "Category": "Performance",
        "Scope": "Table",
        "Severity": "Warning",
        "Name": "Date/calendar tables should be marked as a date table",
        "Expression": """ (
            re.search(r"date", obj.Name, flags=re.IGNORECASE)
            or re.search(r"calendar", obj.Name, flags=re.IGNORECASE)
        )
        and str(obj.DataCategory) != "Time" """,
        "Description": "This rule looks for tables that contain the words 'date' or 'calendar' as they should likely be marked as a date table.",
        "URL": "https://docs.microsoft.com/power-bi/transform-model/desktop-date-tables",
    },
    {
        "Category": "Performance",
        "Scope": "Table",
        "Severity": "Warning",
        "Name": "Large tables should be partitioned",
        "Expression": """ tom.is_direct_lake() is False
        and int(obj.Partitions.Count) == 1
        and tom.row_count(object=obj) > 25000000""",
        "Description": "Large tables should be partitioned in order to optimize processing. This is not relevant for semantic models in Direct Lake mode as they can only have one partition per table.",
    },
    {
        "Category": "Performance",
        "Scope": "Row Level Security",
        "Severity": "Warning",
        "Name": "Limit row level security (RLS) logic",
        "Expression": """ any(
            item in obj.FilterExpression.lower()
            for item in [
                "right(",
                "left(",
                "filter(",
                "upper(",
                "lower(",
                "find(",
            ]
        )""",
        "Description": "Try to simplify the DAX used for row level security. Usage of the functions within this rule can likely be offloaded to the upstream systems (data warehouse).",
    },
    {
        "Category": "Performance",
        "Scope": "Model",
        "Severity": "Warning",
        "Name": "Model should have a date table",
        "Expression": """ not any(
            (c.IsKey and c.DataType == TOM.DataType.DateTime)
            and str(t.DataCategory) == "Time"
            for t in obj.Tables
            for c in t.Columns)""",
        "Description": "Generally speaking, models should generally have a date table. Models that do not have a date table generally are not taking advantage of features such as time intelligence or may not have a properly structured architecture.",
    },
    # ('Performance', 'Measure', 'Warning', 'Measures using time intelligence and model is using Direct Query',
    # lambda obj, TOM, re: any(str(p.Mode) == 'DirectQuery' for p in tom.all_partitions()) and any(re.search(pattern + '\s*\(', obj.Expression, flags=re.IGNORECASE) for pattern in ['CLOSINGBALANCEMONTH', 'CLOSINGBALANCEQUARTER', 'CLOSINGBALANCEYEAR', \
    #    'DATEADD', 'DATESBETWEEN', 'DATESINPERIOD', 'DATESMTD', 'DATESQTD', 'DATESYTD', 'ENDOFMONTH', 'ENDOFQUARTER', 'ENDOFYEAR', 'FIRSTDATE', 'FIRSTNONBLANK', 'FIRSTNONBLANKVALUE', 'LASTDATE', 'LASTNONBLANK', 'LASTNONBLANKVALUE', \
    #    'NEXTDAY', 'NEXTMONTH', 'NEXTQUARTER', 'NEXTYEAR', 'OPENINGBALANCEMONTH', 'OPENINGBALANCEQUARTER', 'OPENINGBALANCEYEAR', 'PARALLELPERIOD', 'PREVIOUSDAY', 'PREVIOUSMONTH', 'PREVIOUSQUARTER', 'PREVIOUSYEAR', 'SAMEPERIODLASTYEAR', \
    #    'STARTOFMONTH', 'STARTOFQUARTER', 'STARTOFYEAR', 'TOTALMTD', 'TOTALQTD', 'TOTALYTD']),
    # 'At present, time intelligence functions are known to not perform as well when using Direct Query. If you are having performance issues, you may want to try alternative solutions such as adding columns in the fact table that show previous year or previous month data.',
    # },
    {
        "Category": "Error Prevention",
        "Scope": "Calculation Item",
        "Severity": "Error",
        "Name": "Calculation items must have an expression",
        "Expression": """ len(obj.Expression) == 0""",
        "Description": "Calculation items must have an expression. Without an expression, they will not show any values.",
    },
    # ('Error Prevention', ['Table', 'Column', 'Measure', 'Hierarchy', 'Partition'], 'Error', 'Avoid invalid characters in names',
    # lambda obj, TOM, re: obj.Name
    # 'This rule identifies if a name for a given object in your model (i.e. table/column/measure) which contains an invalid character. Invalid characters will cause an error when deploying the model (and failure to deploy). This rule has a fix expression which converts the invalid character into a space, resolving the issue.',
    # ),
    # ('Error Prevention', ['Table', 'Column', 'Measure', 'Hierarchy'], 'Error', 'Avoid invalid characters in descriptions',
    # lambda obj, TOM, re: obj.Description
    # 'This rule identifies if a description for a given object in your model (i.e. table/column/measure) which contains an invalid character. Invalid characters will cause an error when deploying the model (and failure to deploy). This rule has a fix expression which converts the invalid character into a space, resolving the issue.',
    # ),
    {
        "Category": "Error Prevention",
        "Scope": "Relationship",
        "Severity": "Warning",
        "Name": "Relationship columns should be of the same data type",
        "Expression": """ obj.FromColumn.DataType
        != obj.ToColumn.DataType""",
        "Description": "Columns used in a relationship should be of the same data type. Ideally, they will be of integer data type (see the related rule '[Formatting] Relationship columns should be of integer data type'). Having columns within a relationship which are of different data types may lead to various issues.",
    },
    {
        "Category": "Error Prevention",
        "Scope": "Column",
        "Severity": "Error",
        "Name": "Data columns must have a source column",
        "Expression": """ obj.Type == TOM.ColumnType.Data
        and len(obj.SourceColumn) == 0""",
        "Description": "Data columns must have a source column. A data column without a source column will cause an error when processing the model.",
    },
    {
        "Category": "Error Prevention",
        "Scope": "Column",
        "Severity": "Warning",
        "Name": "Set IsAvailableInMdx to true on necessary columns",
        "Expression": """ tom.is_direct_lake() is False
        and obj.IsAvailableInMDX is False
        and (
            any(tom.used_in_sort_by(column=obj))
            or any(tom.used_in_hierarchies(column=obj))
            or obj.SortByColumn is not None
        )""",
        "Description": "In order to avoid errors, ensure that attribute hierarchies are enabled if a column is used for sorting another column, used in a hierarchy, used in variations, or is sorted by another column. The IsAvailableInMdx property is not relevant for Direct Lake models.",
    },
    {
        "Category": "Error Prevention",
        "Scope": "Table",
        "Severity": "Error",
        "Name": "Avoid the USERELATIONSHIP function and RLS against the same table",
        "Expression": """ any(
            re.search(
                r"USERELATIONSHIP\s*\(\s*.+?(?=])\]\s*,\s*'*"
                + re.escape(obj.Name)
                + r"'*\[",
                m.Expression,
                flags=re.IGNORECASE,
            )
            for m in tom.all_measures()
        )
        and any(r.Table.Name == obj.Name for r in tom.all_rls())""",
        "Description": "The USERELATIONSHIP function may not be used against a table which also leverages row-level security (RLS). This will generate an error when using the particular measure in a visual. This rule will highlight the table which is used in a measure's USERELATIONSHIP function as well as RLS.",
        "URL": "https://blog.crossjoin.co.uk/2013/05/10/userelationship-and-tabular-row-security",
    },
    {
        "Category": "DAX Expressions",
        "Scope": "Measure",
        "Severity": "Warning",
        "Name": "Avoid using the IFERROR function",
        "Expression": """ re.search(
            r"iferror\s*\(", obj.Expression, flags=re.IGNORECASE
        )""",
        "Description": "Avoid using the IFERROR function as it may cause performance degradation. If you are concerned about a divide-by-zero error, use the DIVIDE function as it naturally resolves such errors as blank (or you can customize what should be shown in case of such an error).",
        "URL": "https://www.elegantbi.com/post/top10bestpractices",
    },
    {
        "Category": "DAX Expressions",
        "Scope": "Measure",
        "Severity": "Warning",
        "Name": "Use the TREATAS function instead of INTERSECT for virtual relationships",
        "Expression": """ re.search(
            r"intersect\s*\(", obj.Expression, flags=re.IGNORECASE
        )""",
        "Description": "The TREATAS function is more efficient and provides better performance than the INTERSECT function when used in virutal relationships.",
        "URL": "https://www.sqlbi.com/articles/propagate-filters-using-treatas-in-dax",
    },
    {
        "Category": "DAX Expressions",
        "Scope": "Measure",
        "Severity": "Warning",
        "Name": "The EVALUATEANDLOG function should not be used in production models",
        "Expression": """ re.search(
            r"evaluateandlog\s*\(",
            obj.Expression,
            flags=re.IGNORECASE,
        )""",
        "Description": "The EVALUATEANDLOG function is meant to be used only in development/test environments and should not be used in production models.",
        "URL": "https://pbidax.wordpress.com/2022/08/16/introduce-the-dax-evaluateandlog-function",
    },
    {
        "Category": "DAX Expressions",
        "Scope": "Measure",
        "Severity": "Warning",
        "Name": "Measures should not be direct references of other measures",
        "Expression": """ any(
            obj.Expression == f"[{m.Name}]" for m in tom.all_measures()
        )""",
        "Description": "This rule identifies measures which are simply a reference to another measure. As an example, consider a model with two measures: [MeasureA] and [MeasureB]. This rule would be triggered for MeasureB if MeasureB's DAX was MeasureB:=[MeasureA]. Such duplicative measures should be removed.",
    },
    {
        "Category": "DAX Expressions",
        "Scope": "Measure",
        "Severity": "Warning",
        "Name": "No two measures should have the same definition",
        "Expression": """ any(
            re.sub(r"\s+", "", obj.Expression) == re.sub(r"\s+", "", m.Expression)
            and obj.Name != m.Name
            for m in tom.all_measures()
        )""",
        "Description": "Two measures with different names and defined by the same DAX expression should be avoided to reduce redundancy.",
    },
    {
        "Category": "DAX Expressions",
        "Scope": "Measure",
        "Severity": "Warning",
        "Name": "Avoid addition or subtraction of constant values to results of divisions",
        "Expression": """ re.search(
            r"DIVIDE\s*\((\s*.*?)\)\s*[+-]\s*1|\/\s*.*(?=[-+]\s*1)",
            obj.Expression,
            flags=re.IGNORECASE,
        )""",
        "Description": "Adding a constant value may lead to performance degradation.",
    },
    {
        "Category": "DAX Expressions",
        "Scope": "Measure",
        "Severity": "Warning",
        "Name": "Avoid using '1-(x/y)' syntax",
        "Expression": """ re.search(
            r"[0-9]+\s*[-+]\s*[\(]*\s*SUM\s*\(\s*\'*[A-Za-z0-9 _]+\'*\s*\[[A-Za-z0-9 _]+\]\s*\)\s*/",
            obj.Expression,
            flags=re.IGNORECASE,
        )
        or re.search(
            r"[0-9]+\s*[-+]\s*DIVIDE\s*\(",
            obj.Expression,
            flags=re.IGNORECASE,
        )""",
        "Description": "Instead of using the '1-(x/y)' or '1+(x/y)' syntax to achieve a percentage calculation, use the basic DAX functions (as shown below). Using the improved syntax will generally improve the performance. The '1+/-...' syntax always returns a value whereas the solution without the '1+/-...' does not (as the value may be 'blank'). Therefore the '1+/-...' syntax may return more rows/columns which may result in a slower query speed.    Let's clarify with an example:    Avoid this: 1 - SUM ( 'Sales'[CostAmount] ) / SUM( 'Sales'[SalesAmount] )  Better: DIVIDE ( SUM ( 'Sales'[SalesAmount] ) - SUM ( 'Sales'[CostAmount] ), SUM ( 'Sales'[SalesAmount] ) )  Best: VAR x = SUM ( 'Sales'[SalesAmount] ) RETURN DIVIDE ( x - SUM ( 'Sales'[CostAmount] ), x )",
    },
    {
        "Category": "DAX Expressions",
        "Scope": "Measure",
        "Severity": "Warning",
        "Name": "Filter measure values by columns, not tables",
        "Expression": """ re.search(
            r"CALCULATE\s*\(\s*[^,]+,\s*FILTER\s*\(\s*\'*[A-Za-z0-9 _]+\'*\s*,\s*\[[^\]]+\]",
            obj.Expression,
            flags=re.IGNORECASE,
        )
        or re.search(
            r"CALCULATETABLE\s*\(\s*[^,]*,\s*FILTER\s*\(\s*\'*[A-Za-z0-9 _]+\'*\s*,\s*\[",
            obj.Expression,
            flags=re.IGNORECASE,
        )""",
        "Description": "Instead of using this pattern FILTER('Table',[Measure]>Value) for the filter parameters of a CALCULATE or CALCULATETABLE function, use one of the options below (if possible). Filtering on a specific column will produce a smaller table for the engine to process, thereby enabling faster performance. Using the VALUES function or the ALL function depends on the desired measure result.\nOption 1: FILTER(VALUES('Table'[Column]),[Measure] > Value)\nOption 2: FILTER(ALL('Table'[Column]),[Measure] > Value)",
        "URL": "https://docs.microsoft.com/power-bi/guidance/dax-avoid-avoid-filter-as-filter-argument",
    },
    {
        "Category": "DAX Expressions",
        "Scope": "Measure",
        "Severity": "Warning",
        "Name": "Filter column values with proper syntax",
        "Expression": """ re.search(
            r"CALCULATE\s*\(\s*[^,]+,\s*FILTER\s*\(\s*'*[A-Za-z0-9 _]+'*\s*,\s*'*[A-Za-z0-9 _]+'*\[[A-Za-z0-9 _]+\]",
            obj.Expression,
            flags=re.IGNORECASE,
        )
        or re.search(
            r"CALCULATETABLE\s*\([^,]*,\s*FILTER\s*\(\s*'*[A-Za-z0-9 _]+'*\s*,\s*'*[A-Za-z0-9 _]+'*\[[A-Za-z0-9 _]+\]",
            obj.Expression,
            flags=re.IGNORECASE,
        )""",
        "Description": "Instead of using this pattern FILTER('Table','Table'[Column]=\"Value\") for the filter parameters of a CALCULATE or CALCULATETABLE function, use one of the options below. As far as whether to use the KEEPFILTERS function, see the second reference link below.\nOption 1: KEEPFILTERS('Table'[Column]=\"Value\")\nOption 2: 'Table'[Column]=\"Value\"",
        "URL": "https://docs.microsoft.com/power-bi/guidance/dax-avoid-avoid-filter-as-filter-argument  Reference: https://www.sqlbi.com/articles/using-keepfilters-in-dax",
    },
    {
        "Category": "DAX Expressions",
        "Scope": "Measure",
        "Severity": "Warning",
        "Name": "Use the DIVIDE function for division",
        "Expression": """ re.search(
            r"\]\s*\/(?!\/)(?!\*)|\)\s*\/(?!\/)(?!\*)",
            obj.Expression,
            flags=re.IGNORECASE,
        )""",
        "Description": 'Use the DIVIDE  function instead of using "/". The DIVIDE function resolves divide-by-zero cases. As such, it is recommended to use to avoid errors.',
        "URL": "https://docs.microsoft.com/power-bi/guidance/dax-divide-function-operator",
    },
    {
        "Category": "DAX Expressions",
        "Scope": [
            "Measure",
            "Calculated Table",
            "Calculated Column",
            "Calculation Item",
        ],
        "Severity": "Error",
        "Name": "Column references should be fully qualified",
        "Expression": """ any(
            tom.unqualified_columns(object=obj, dependencies=dep)
        )""",
        "Description": "Using fully qualified column references makes it easier to distinguish between column and measure references, and also helps avoid certain errors. When referencing a column in DAX, first specify the table name, then specify the column name in square brackets.",
        "URL": "https://www.elegantbi.com/post/top10bestpractices",
    },
    {
        "Category": "DAX Expressions",
        "Scope": [
            "Measure",
            "Calculated Table",
            "Calculated Column",
            "Calculation Item",
        ],
        "Severity": "Error",
        "Name": "Measure references should be unqualified",
        "Expression": """ any(
            tom.fully_qualified_measures(object=obj, dependencies=dep)
        )""",
        "Description": "Using unqualified measure references makes it easier to distinguish between column and measure references, and also helps avoid certain errors. When referencing a measure using DAX, do not specify the table name. Use only the measure name in square brackets.",
        "URL": "https://www.elegantbi.com/post/top10bestpractices",
    },
    {
        "Category": "DAX Expressions",
        "Scope": "Relationship",
        "Severity": "Warning",
        "Name": "Inactive relationships that are never activated",
        "Expression": """ obj.IsActive is False
        and not any(
            re.search(
                r"USERELATIONSHIP\s*\(\s*\'*"
                + re.escape(obj.FromTable.Name)
                + r"'*\["
                + re.escape(obj.FromColumn.Name)
                + r"\]\s*,\s*'*"
                + re.escape(obj.ToTable.Name)
                + r"'*\["
                + re.escape(obj.ToColumn.Name)
                + r"\]",
                m.Expression,
                flags=re.IGNORECASE,
            )
            for m in tom.all_measures()
        )""",
        "Description": "Inactive relationships are activated using the USERELATIONSHIP function. If an inactive relationship is not referenced in any measure via this function, the relationship will not be used. It should be determined whether the relationship is not necessary or to activate the relationship via this method.",
        "URL": "https://dax.guide/userelationship",
    },
    {
        "Category": "Maintenance",
        "Scope": "Column",
        "Severity": "Warning",
        "Name": "Remove unnecessary columns",
        "Expression": """ (
            obj.IsHidden or obj.Parent.IsHidden
        )
        and not any(tom.used_in_relationships(object=obj))
        and not any(tom.used_in_hierarchies(column=obj))
        and not any(tom.used_in_sort_by(column=obj))
        and any(tom.depends_on(object=obj, dependencies=dep))""",
        "Description": "Hidden columns that are not referenced by any DAX expressions, relationships, hierarchy levels or Sort By-properties should be removed.",
    },
    {
        "Category": "Maintenance",
        "Scope": "Measure",
        "Severity": "Warning",
        "Name": "Remove unnecessary measures",
        "Expression": """ obj.IsHidden
        and not any(tom.referenced_by(object=obj, dependencies=dep))""",
        "Description": "Hidden measures that are not referenced by any DAX expressions should be removed for maintainability.",
    },
    {
        "Category": "Maintenance",
        "Scope": "Table",
        "Severity": "Warning",
        "Name": "Ensure tables have relationships",
        "Expression": """ any(
            tom.used_in_relationships(object=obj)) is False
            and obj.CalculationGroup is None
            and not tom.is_field_parameter(table_name=obj.Name)""",
        "Description": "This rule highlights tables which are not connected to any other table in the model with a relationship.",
    },
    {
        "Category": "Maintenance",
        "Scope": "Table",
        "Severity": "Warning",
        "Name": "Calculation groups with no calculation items",
        "Expression": """ obj.CalculationGroup is not None
        and not any(obj.CalculationGroup.CalculationItems)""",
        "Description": "Calculation groups have no function unless they have calculation items.",
    },
    {
        "Category": "Maintenance",
        "Scope": ["Column", "Measure", "Table"],
        "Severity": "Info",
        "Name": "Visible objects with no description",
        "Expression": """ obj.IsHidden is False
        and len(obj.Description) == 0""",
        "Description": "Add descriptions to objects. These descriptions are shown on hover within the Field List in Power BI Desktop. Additionally, you can leverage these descriptions to create an automated data dictionary.",
    },
    {
        "Category": "Formatting",
        "Scope": "Column",
        "Severity": "Warning",
        "Name": "Provide format string for 'Date' columns",
        "Expression": """ (
            re.search(r"date", obj.Name, flags=re.IGNORECASE)
        )
        and (obj.DataType == TOM.DataType.DateTime)
        and (
            obj.FormatString.lower()
            not in [
                "mm/dd/yyyy",
                "mm-dd-yyyy",
                "dd/mm/yyyy",
                "dd-mm-yyyy",
                "yyyy-mm-dd",
                "yyyy/mm/dd",
            ]
        )""",
        "Description": 'Columns of type "DateTime" that have "Date" in their names should be formatted.',
    },
    {
        "Category": "Formatting",
        "Scope": "Column",
        "Severity": "Warning",
        "Name": "Do not summarize numeric columns",
        "Expression": """ (
            (obj.DataType == TOM.DataType.Int64)
            or (obj.DataType == TOM.DataType.Decimal)
            or (obj.DataType == TOM.DataType.Double)
        )
        and (str(obj.SummarizeBy) != "None")
        and not ((obj.IsHidden) or (obj.Parent.IsHidden))""",
        "Description": 'Numeric columns (integer, decimal, double) should have their SummarizeBy property set to "None" to avoid accidental summation in Power BI (create measures instead).',
    },
    {
        "Category": "Formatting",
        "Scope": "Measure",
        "Severity": "Info",
        "Name": "Provide format string for measures",
        "Expression": """ obj.IsHidden is False
        and len(obj.FormatString) == 0
        and not obj.FormatStringDefinition """,
        "Description": "Visible measures should have their format string property assigned.",
    },
    {
        "Category": "Formatting",
        "Scope": "Column",
        "Severity": "Info",
        "Name": "Add data category for columns",
        "Expression": """ len(obj.DataCategory) == 0
        and any(
            obj.Name.lower().startswith(item.lower())
            for item in [
                "country",
                "city",
                "continent",
                "latitude",
                "longitude",
            ]
        )""",
        "Description": "Add Data Category property for appropriate columns.",
        "URL": "https://docs.microsoft.com/power-bi/transform-model/desktop-data-categorization",
    },
    {
        "Category": "Formatting",
        "Scope": "Measure",
        "Severity": "Warning",
        "Name": "Percentages should be formatted with thousands separators and 1 decimal",
        "Expression": """ "%" in obj.FormatString
        and obj.FormatString != "#,0.0%;-#,0.0%;#,0.0%" """,
        "Description": "For a better user experience, percengage measures should be formatted with a '%' sign.",
    },
    {
        "Category": "Formatting",
        "Scope": "Measure",
        "Severity": "Warning",
        "Name": "Whole numbers should be formatted with thousands separators and no decimals",
        "Expression": """ "$" not in obj.FormatString
        and "%" not in obj.FormatString
        and obj.FormatString not in ["#,0", "#,0.0"] """,
        "Description": "For a better user experience, whole numbers should be formatted with commas.",
    },
    {
        "Category": "Formatting",
        "Scope": "Column",
        "Severity": "Info",
        "Name": "Hide foreign keys",
        "Expression": """ obj.IsHidden is False
        and any(
            r.FromColumn.Name == obj.Name
            and r.FromCardinality == TOM.RelationshipEndCardinality.Many
            for r in tom.used_in_relationships(object=obj)
        )""",
        "Description": "Foreign keys should always be hidden as they should not be used by end users.",
    },
    {
        "Category": "Formatting",
        "Scope": "Column",
        "Severity": "Info",
        "Name": "Mark primary keys",
        "Expression": """ any(
            r.ToTable.Name == obj.Table.Name
            and r.ToColumn.Name == obj.Name
            and r.ToCardinality == TOM.RelationshipEndCardinality.One
            for r in tom.used_in_relationships(object=obj)
        )
        and obj.IsKey is False
        and obj.Table.DataCategory != "Time" """,
        "Description": "Set the 'Key' property to 'True' for primary key columns within the column properties.",
    },
    {
        "Category": "Formatting",
        "Scope": "Column",
        "Severity": "Info",
        "Name": "Month (as a string) must be sorted",
        "Expression": """ (
            re.search(r"month", obj.Name, flags=re.IGNORECASE)
        )
        and not (re.search(r"months", obj.Name, flags=re.IGNORECASE))
        and (obj.DataType == TOM.DataType.String)
        and len(str(obj.SortByColumn)) == 0 """,
        "Description": "This rule highlights month columns which are strings and are not sorted. If left unsorted, they will sort alphabetically (i.e. April, August...). Make sure to sort such columns so that they sort properly (January, February, March...).",
    },
    {
        "Category": "Formatting",
        "Scope": "Relationship",
        "Severity": "Warning",
        "Name": "Relationship columns should be of integer data type",
        "Expression": """ obj.FromColumn.DataType
        != TOM.DataType.Int64
        or obj.ToColumn.DataType != TOM.DataType.Int64 """,
        "Description": "It is a best practice for relationship columns to be of integer data type. This applies not only to data warehousing but data modeling as well.",
    },
    {
        "Category": "Formatting",
        "Scope": "Column",
        "Severity": "Warning",
        "Name": "Provide format string for 'Month' columns",
        "Expression": """ re.search(
            r"month", obj.Name, flags=re.IGNORECASE
        )
        and obj.DataType == TOM.DataType.DateTime
        and obj.FormatString != "MMMM yyyy" """,
        "Description": 'Columns of type "DateTime" that have "Month" in their names should be formatted as "MMMM yyyy".',
    },
    {
        "Category": "Formatting",
        "Scope": "Column",
        "Severity": "Info",
        "Name": "Format flag columns as Yes/No value strings",
        "Expression": """ obj.Name.lower().startswith("is")
        and obj.DataType == TOM.DataType.Int64
        and not (obj.IsHidden or obj.Parent.IsHidden)
        or obj.Name.lower().endswith(" flag")
        and obj.DataType != TOM.DataType.String
        and not (obj.IsHidden or obj.Parent.IsHidden)""",
        "Description": "Flags must be properly formatted as Yes/No as this is easier to read than using 0/1 integer values.",
    },
    {
        "Category": "Formatting",
        "Scope": ["Table", "Column", "Measure", "Partition", "Hierarchy"],
        "Severity": "Error",
        "Name": "Objects should not start or end with a space",
        "Expression": """ obj.Name[0] == " "
        or obj.Name[-1] == " " """,
        "Description": "Objects should not start or end with a space. This usually happens by accident and is difficult to find.",
    },
    {
        "Category": "Formatting",
        "Scope": ["Table", "Column", "Measure", "Partition", "Hierarchy"],
        "Severity": "Info",
        "Name": "First letter of objects must be capitalized",
        "Expression": """ obj.Name[0] != obj.Name[0].upper() """,
        "Description": "The first letter of object names should be capitalized to maintain professional quality.",
    },
    {
        "Category": "Naming Conventions",
        "Scope": ["Table", "Column", "Measure", "Partition", "Hierarchy"],
        "Severity": "Warning",
        "Name": "Object names must not contain special characters",
        "Expression": """re.search(r"[\\t\\r\\n]", obj.Name) """,
        "Description": "Object names should not include tabs, line breaks, etc.",
    },
]
