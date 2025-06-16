import sempy
import pandas as pd
import re
from typing import Optional
from sempy._utils._log import log


@log
def model_bpa_rules(
    dependencies: Optional[pd.DataFrame] = None,
    **kwargs,
) -> pd.DataFrame:
    """
    Shows the default rules for the semantic model BPA used by the run_model_bpa function.

    Parameters
    ----------
    dependencies : pd.DataFrame, default=None
        A pandas dataframe with the output of the 'get_model_calc_dependencies' function.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe containing the default rules for the run_model_bpa function.
    """

    sempy.fabric._client._utils._init_analysis_services()
    import Microsoft.AnalysisServices.Tabular as TOM

    if "dataset" in kwargs:
        print(
            "The 'dataset' parameter has been deprecated. Please remove this parameter from the function going forward."
        )
        del kwargs["dataset"]
    if "workspace" in kwargs:
        print(
            "The 'workspace' parameter has been deprecated. Please remove this parameter from the function going forward."
        )
        del kwargs["workspace"]

    rules = pd.DataFrame(
        [
            (
                "Performance",
                "Column",
                "Warning",
                "Do not use floating point data types",
                lambda obj, tom: obj.DataType == TOM.DataType.Double,
                'The "Double" floating point data type should be avoided, as it can result in unpredictable roundoff errors and decreased performance in certain scenarios. Use "Int64" or "Decimal" where appropriate (but note that "Decimal" is limited to 4 digits after the decimal sign).',
            ),
            (
                "Performance",
                "Column",
                "Warning",
                "Avoid using calculated columns",
                lambda obj, tom: obj.Type == TOM.ColumnType.Calculated,
                "Calculated columns do not compress as well as data columns so they take up more memory. They also slow down processing times for both the table as well as process recalc. Offload calculated column logic to your data warehouse and turn these calculated columns into data columns.",
                "https://www.elegantbi.com/post/top10bestpractices",
            ),
            (
                "Performance",
                "Relationship",
                "Warning",
                "Check if bi-directional and many-to-many relationships are valid",
                lambda obj, tom: (
                    obj.FromCardinality == TOM.RelationshipEndCardinality.Many
                    and obj.ToCardinality == TOM.RelationshipEndCardinality.Many
                )
                or str(obj.CrossFilteringBehavior) == "BothDirections",
                "Bi-directional and many-to-many relationships may cause performance degradation or even have unintended consequences. Make sure to check these specific relationships to ensure they are working as designed and are actually necessary.",
                "https://www.sqlbi.com/articles/bidirectional-relationships-and-ambiguity-in-dax",
            ),
            (
                "Performance",
                "Row Level Security",
                "Info",
                "Check if dynamic row level security (RLS) is necessary",
                lambda obj, tom: any(
                    re.search(pattern, obj.FilterExpression, flags=re.IGNORECASE)
                    for pattern in ["USERPRINCIPALNAME()", "USERNAME()"]
                ),
                "Usage of dynamic row level security (RLS) can add memory and performance overhead. Please research the pros/cons of using it.",
                "https://docs.microsoft.com/power-bi/admin/service-admin-rls",
            ),
            (
                "Performance",
                "Table",
                "Warning",
                "Avoid using many-to-many relationships on tables used for dynamic row level security",
                lambda obj, tom: any(
                    r.FromCardinality == TOM.RelationshipEndCardinality.Many
                    and r.ToCardinality == TOM.RelationshipEndCardinality.Many
                    for r in tom.used_in_relationships(object=obj)
                )
                and any(t.Name == obj.Name for t in tom.all_rls()),
                "Using many-to-many relationships on tables which use dynamic row level security can cause serious query performance degradation. This pattern's performance problems compound when snowflaking multiple many-to-many relationships against a table which contains row level security. Instead, use one of the patterns shown in the article below where a single dimension table relates many-to-one to a security table.",
                "https://www.elegantbi.com/post/dynamicrlspatterns",
            ),
            (
                "Performance",
                "Relationship",
                "Warning",
                "Many-to-many relationships should be single-direction",
                lambda obj, tom: (
                    obj.FromCardinality == TOM.RelationshipEndCardinality.Many
                    and obj.ToCardinality == TOM.RelationshipEndCardinality.Many
                )
                and obj.CrossFilteringBehavior
                == TOM.CrossFilteringBehavior.BothDirections,
            ),
            (
                "Performance",
                "Column",
                "Warning",
                "Set IsAvailableInMdx to false on non-attribute columns",
                lambda obj, tom: tom.is_direct_lake() is False
                and obj.IsAvailableInMDX
                and (obj.IsHidden or obj.Parent.IsHidden)
                and obj.SortByColumn is None
                and not any(tom.used_in_sort_by(column=obj))
                and not any(tom.used_in_hierarchies(column=obj)),
                "To speed up processing time and conserve memory after processing, attribute hierarchies should not be built for columns that are never used for slicing by MDX clients. In other words, all hidden columns that are not used as a Sort By Column or referenced in user hierarchies should have their IsAvailableInMdx property set to false. The IsAvailableInMdx property is not relevant for Direct Lake models.",
                "https://blog.crossjoin.co.uk/2018/07/02/isavailableinmdx-ssas-tabular",
            ),
            (
                "Performance",
                "Partition",
                "Warning",
                "Set 'Data Coverage Definition' property on the DirectQuery partition of a hybrid table",
                lambda obj, tom: tom.is_hybrid_table(table_name=obj.Parent.Name)
                and obj.Mode == TOM.ModeType.DirectQuery
                and obj.DataCoverageDefinition is None,
                "Setting the 'Data Coverage Definition' property may lead to better performance because the engine knows when it can only query the import-portion of the table and when it needs to query the DirectQuery portion of the table.",
                "https://learn.microsoft.com/analysis-services/tom/table-partitions?view=asallproducts-allversions",
            ),
            (
                "Performance",
                "Model",
                "Warning",
                "Dual mode is only relevant for dimension tables if DirectQuery is used for the corresponding fact table",
                lambda obj, tom: not any(
                    p.Mode == TOM.ModeType.DirectQuery for p in tom.all_partitions()
                )
                and any(p.Mode == TOM.ModeType.Dual for p in tom.all_partitions()),
                "Only use Dual mode for dimension tables/partitions where a corresponding fact table is in DirectQuery. Using Dual mode in other circumstances (i.e. rest of the model is in Import mode) may lead to performance issues especially if the number of measures in the model is high.",
            ),
            (
                "Performance",
                "Table",
                "Warning",
                "Set dimensions tables to dual mode instead of import when using DirectQuery on fact tables",
                lambda obj, tom: sum(
                    1 for p in obj.Partitions if p.Mode == TOM.ModeType.Import
                )
                == 1
                and obj.Partitions.Count == 1
                and tom.has_hybrid_table()
                and any(
                    r.ToCardinality == TOM.RelationshipEndCardinality.One
                    and r.ToTable.Name == obj.Name
                    for r in tom.used_in_relationships(object=obj)
                ),
                "When using DirectQuery, dimension tables should be set to Dual mode in order to improve query performance.",
                "https://learn.microsoft.com/power-bi/transform-model/desktop-storage-mode#propagation-of-the-dual-setting",
            ),
            (
                "Performance",
                "Partition",
                "Warning",
                "Minimize Power Query transformations",
                lambda obj, tom: obj.SourceType == TOM.PartitionSourceType.M
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
                ),
                "Minimize Power Query transformations in order to improve model processing performance. It is a best practice to offload these transformations to the data warehouse if possible. Also, please check whether query folding is occurring within your model. Please reference the article below for more information on query folding.",
                "https://docs.microsoft.com/power-query/power-query-folding",
            ),
            (
                "Performance",
                "Table",
                "Warning",
                "Consider a star-schema instead of a snowflake architecture",
                lambda obj, tom: obj.CalculationGroup is None
                and (
                    any(
                        r.FromTable.Name == obj.Name
                        for r in tom.used_in_relationships(object=obj)
                    )
                    and any(
                        r.ToTable.Name == obj.Name
                        for r in tom.used_in_relationships(object=obj)
                    )
                ),
                "Generally speaking, a star-schema is the optimal architecture for tabular models. That being the case, there are valid cases to use a snowflake approach. Please check your model and consider moving to a star-schema architecture.",
                "https://docs.microsoft.com/power-bi/guidance/star-schema",
            ),
            (
                "Performance",
                "Model",
                "Warning",
                "Avoid using views when using Direct Lake mode",
                lambda obj, tom: tom.is_direct_lake_using_view(),
                "In Direct Lake mode, views will always fall back to DirectQuery. Thus, in order to obtain the best performance use lakehouse tables instead of views.",
                "https://learn.microsoft.com/fabric/get-started/direct-lake-overview#fallback",
            ),
            (
                "Performance",
                "Measure",
                "Warning",
                "Avoid adding 0 to a measure",
                lambda obj, tom: obj.Expression.replace(" ", "").startswith("0+")
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
                ),
                "Adding 0 to a measure in order for it not to show a blank value may negatively impact performance.",
            ),
            (
                "Performance",
                "Table",
                "Warning",
                "Reduce usage of calculated tables",
                lambda obj, tom: tom.is_field_parameter(table_name=obj.Name) is False
                and tom.is_calculated_table(table_name=obj.Name),
                "Migrate calculated table logic to your data warehouse. Reliance on calculated tables will lead to technical debt and potential misalignments if you have multiple models on your platform.",
            ),
            (
                "Performance",
                "Column",
                "Warning",
                "Reduce usage of calculated columns that use the RELATED function",
                lambda obj, tom: obj.Type == TOM.ColumnType.Calculated
                and re.search(r"related\s*\(", obj.Expression, flags=re.IGNORECASE),
                "Calculated columns do not compress as well as data columns and may cause longer processing times. As such, calculated columns should be avoided if possible. One scenario where they may be easier to avoid is if they use the RELATED function.",
                "https://www.sqlbi.com/articles/storage-differences-between-calculated-columns-and-calculated-tables",
            ),
            (
                "Performance",
                "Model",
                "Warning",
                "Avoid excessive bi-directional or many-to-many relationships",
                lambda obj, tom: (
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
                            if (
                                r.FromCardinality == TOM.RelationshipEndCardinality.Many
                            )
                            and (r.ToCardinality == TOM.RelationshipEndCardinality.Many)
                        )
                    )
                    / max(int(obj.Relationships.Count), 1)
                )
                > 0.3,
                "Limit use of b-di and many-to-many relationships. This rule flags the model if more than 30% of relationships are bi-di or many-to-many.",
                "https://www.sqlbi.com/articles/bidirectional-relationships-and-ambiguity-in-dax",
            ),
            # ('Performance', 'Column', 'Warning', 'Avoid bi-directional or many-to-many relationships against high-cardinality columns',
            # lambda obj, tom: ((str(r.FromCardinality) == 'Many' and str(r.ToCardinality == 'Many'))  or (str(r.CrossFilteringBehavior) == 'BothDirections') for r in tom.used_in_relationships(object = obj)) and tom.cardinality(column = obj) > 100000,
            # 'For best performance, it is recommended to avoid using bi-directional relationships against high-cardinality columns',
            # ),
            (
                "Performance",
                "Table",
                "Warning",
                "Remove auto-date table",
                lambda obj, tom: tom.is_calculated_table(table_name=obj.Name)
                and (
                    obj.Name.startswith("DateTableTemplate_")
                    or obj.Name.startswith("LocalDateTable_")
                ),
                "Avoid using auto-date tables. Make sure to turn off auto-date table in the settings in Power BI Desktop. This will save memory resources.",
                "https://www.youtube.com/watch?v=xu3uDEHtCrg",
            ),
            (
                "Performance",
                "Table",
                "Warning",
                "Date/calendar tables should be marked as a date table",
                lambda obj, tom: (
                    re.search(r"date", obj.Name, flags=re.IGNORECASE)
                    or re.search(r"calendar", obj.Name, flags=re.IGNORECASE)
                )
                and str(obj.DataCategory) != "Time",
                "This rule looks for tables that contain the words 'date' or 'calendar' as they should likely be marked as a date table.",
                "https://docs.microsoft.com/power-bi/transform-model/desktop-date-tables",
            ),
            (
                "Performance",
                "Table",
                "Warning",
                "Large tables should be partitioned",
                lambda obj, tom: tom.is_direct_lake() is False
                and int(obj.Partitions.Count) == 1
                and tom.row_count(object=obj) > 25000000,
                "Large tables should be partitioned in order to optimize processing. This is not relevant for semantic models in Direct Lake mode as they can only have one partition per table.",
            ),
            (
                "Performance",
                "Row Level Security",
                "Warning",
                "Limit row level security (RLS) logic",
                lambda obj, tom: any(
                    item in obj.FilterExpression.lower()
                    for item in [
                        "right(",
                        "left(",
                        "filter(",
                        "upper(",
                        "lower(",
                        "find(",
                    ]
                ),
                "Try to simplify the DAX used for row level security. Usage of the functions within this rule can likely be offloaded to the upstream systems (data warehouse).",
            ),
            (
                "Performance",
                "Model",
                "Warning",
                "Model should have a date table",
                lambda obj, tom: not any(
                    (c.IsKey and c.DataType == TOM.DataType.DateTime)
                    and str(t.DataCategory) == "Time"
                    for t in obj.Tables
                    for c in t.Columns
                ),
                "Generally speaking, models should generally have a date table. Models that do not have a date table generally are not taking advantage of features such as time intelligence or may not have a properly structured architecture.",
            ),
            # ('Performance', 'Measure', 'Warning', 'Measures using time intelligence and model is using Direct Query',
            # lambda obj, tom: any(str(p.Mode) == 'DirectQuery' for p in tom.all_partitions()) and any(re.search(pattern + '\s*\(', obj.Expression, flags=re.IGNORECASE) for pattern in ['CLOSINGBALANCEMONTH', 'CLOSINGBALANCEQUARTER', 'CLOSINGBALANCEYEAR', \
            #    'DATEADD', 'DATESBETWEEN', 'DATESINPERIOD', 'DATESMTD', 'DATESQTD', 'DATESYTD', 'ENDOFMONTH', 'ENDOFQUARTER', 'ENDOFYEAR', 'FIRSTDATE', 'FIRSTNONBLANK', 'FIRSTNONBLANKVALUE', 'LASTDATE', 'LASTNONBLANK', 'LASTNONBLANKVALUE', \
            #    'NEXTDAY', 'NEXTMONTH', 'NEXTQUARTER', 'NEXTYEAR', 'OPENINGBALANCEMONTH', 'OPENINGBALANCEQUARTER', 'OPENINGBALANCEYEAR', 'PARALLELPERIOD', 'PREVIOUSDAY', 'PREVIOUSMONTH', 'PREVIOUSQUARTER', 'PREVIOUSYEAR', 'SAMEPERIODLASTYEAR', \
            #    'STARTOFMONTH', 'STARTOFQUARTER', 'STARTOFYEAR', 'TOTALMTD', 'TOTALQTD', 'TOTALYTD']),
            # 'At present, time intelligence functions are known to not perform as well when using Direct Query. If you are having performance issues, you may want to try alternative solutions such as adding columns in the fact table that show previous year or previous month data.',
            # ),
            (
                "Error Prevention",
                "Calculation Item",
                "Error",
                "Calculation items must have an expression",
                lambda obj, tom: len(obj.Expression) == 0,
                "Calculation items must have an expression. Without an expression, they will not show any values.",
            ),
            # ('Error Prevention', ['Table', 'Column', 'Measure', 'Hierarchy', 'Partition'], 'Error', 'Avoid invalid characters in names',
            # lambda obj, tom: obj.Name
            # 'This rule identifies if a name for a given object in your model (i.e. table/column/measure) which contains an invalid character. Invalid characters will cause an error when deploying the model (and failure to deploy). This rule has a fix expression which converts the invalid character into a space, resolving the issue.',
            # ),
            # ('Error Prevention', ['Table', 'Column', 'Measure', 'Hierarchy'], 'Error', 'Avoid invalid characters in descriptions',
            # lambda obj, tom: obj.Description
            # 'This rule identifies if a description for a given object in your model (i.e. table/column/measure) which contains an invalid character. Invalid characters will cause an error when deploying the model (and failure to deploy). This rule has a fix expression which converts the invalid character into a space, resolving the issue.',
            # ),
            (
                "Error Prevention",
                "Relationship",
                "Warning",
                "Relationship columns should be of the same data type",
                lambda obj, tom: obj.FromColumn.DataType != obj.ToColumn.DataType,
                "Columns used in a relationship should be of the same data type. Ideally, they will be of integer data type (see the related rule '[Formatting] Relationship columns should be of integer data type'). Having columns within a relationship which are of different data types may lead to various issues.",
            ),
            (
                "Error Prevention",
                "Column",
                "Error",
                "Data columns must have a source column",
                lambda obj, tom: obj.Type == TOM.ColumnType.Data
                and len(obj.SourceColumn) == 0,
                "Data columns must have a source column. A data column without a source column will cause an error when processing the model.",
            ),
            (
                "Error Prevention",
                "Column",
                "Warning",
                "Set IsAvailableInMdx to true on necessary columns",
                lambda obj, tom: tom.is_direct_lake() is False
                and obj.IsAvailableInMDX is False
                and (
                    any(tom.used_in_sort_by(column=obj))
                    or any(tom.used_in_hierarchies(column=obj))
                    or obj.SortByColumn is not None
                ),
                "In order to avoid errors, ensure that attribute hierarchies are enabled if a column is used for sorting another column, used in a hierarchy, used in variations, or is sorted by another column. The IsAvailableInMdx property is not relevant for Direct Lake models.",
            ),
            (
                "Error Prevention",
                "Table",
                "Error",
                "Avoid the USERELATIONSHIP function and RLS against the same table",
                lambda obj, tom: any(
                    re.search(
                        r"USERELATIONSHIP\s*\(\s*.+?(?=])\]\s*,\s*'*"
                        + re.escape(obj.Name)
                        + r"'*\[",
                        m.Expression,
                        flags=re.IGNORECASE,
                    )
                    for m in tom.all_measures()
                )
                and any(r.Table.Name == obj.Name for r in tom.all_rls()),
                "The USERELATIONSHIP function may not be used against a table which also leverages row-level security (RLS). This will generate an error when using the particular measure in a visual. This rule will highlight the table which is used in a measure's USERELATIONSHIP function as well as RLS.",
                "https://blog.crossjoin.co.uk/2013/05/10/userelationship-and-tabular-row-security",
            ),
            (
                "DAX Expressions",
                "Measure",
                "Warning",
                "Avoid using the IFERROR function",
                lambda obj, tom: re.search(
                    r"iferror\s*\(", obj.Expression, flags=re.IGNORECASE
                ),
                "Avoid using the IFERROR function as it may cause performance degradation. If you are concerned about a divide-by-zero error, use the DIVIDE function as it naturally resolves such errors as blank (or you can customize what should be shown in case of such an error).",
                "https://www.elegantbi.com/post/top10bestpractices",
            ),
            (
                "DAX Expressions",
                "Measure",
                "Warning",
                "Use the TREATAS function instead of INTERSECT for virtual relationships",
                lambda obj, tom: re.search(
                    r"intersect\s*\(", obj.Expression, flags=re.IGNORECASE
                ),
                "The TREATAS function is more efficient and provides better performance than the INTERSECT function when used in virutal relationships.",
                "https://www.sqlbi.com/articles/propagate-filters-using-treatas-in-dax",
            ),
            (
                "DAX Expressions",
                "Measure",
                "Warning",
                "The EVALUATEANDLOG function should not be used in production models",
                lambda obj, tom: re.search(
                    r"evaluateandlog\s*\(",
                    obj.Expression,
                    flags=re.IGNORECASE,
                ),
                "The EVALUATEANDLOG function is meant to be used only in development/test environments and should not be used in production models.",
                "https://pbidax.wordpress.com/2022/08/16/introduce-the-dax-evaluateandlog-function",
            ),
            (
                "DAX Expressions",
                "Measure",
                "Warning",
                "Measures should not be direct references of other measures",
                lambda obj, tom: any(
                    obj.Expression == f"[{m.Name}]" for m in tom.all_measures()
                ),
                "This rule identifies measures which are simply a reference to another measure. As an example, consider a model with two measures: [MeasureA] and [MeasureB]. This rule would be triggered for MeasureB if MeasureB's DAX was MeasureB:=[MeasureA]. Such duplicative measures should be removed.",
            ),
            (
                "DAX Expressions",
                "Measure",
                "Warning",
                "No two measures should have the same definition",
                lambda obj, tom: any(
                    re.sub(r"\s+", "", obj.Expression)
                    == re.sub(r"\s+", "", m.Expression)
                    and obj.Name != m.Name
                    for m in tom.all_measures()
                ),
                "Two measures with different names and defined by the same DAX expression should be avoided to reduce redundancy.",
            ),
            (
                "DAX Expressions",
                "Measure",
                "Warning",
                "Avoid addition or subtraction of constant values to results of divisions",
                lambda obj, tom: re.search(
                    r"DIVIDE\s*\((\s*.*?)\)\s*[+-]\s*1|\/\s*.*(?=[-+]\s*1)",
                    obj.Expression,
                    flags=re.IGNORECASE,
                ),
                "Adding a constant value may lead to performance degradation.",
            ),
            (
                "DAX Expressions",
                "Measure",
                "Warning",
                "Avoid using '1-(x/y)' syntax",
                lambda obj, tom: re.search(
                    r"[0-9]+\s*[-+]\s*[\(]*\s*SUM\s*\(\s*\'*[A-Za-z0-9 _]+\'*\s*\[[A-Za-z0-9 _]+\]\s*\)\s*/",
                    obj.Expression,
                    flags=re.IGNORECASE,
                )
                or re.search(
                    r"[0-9]+\s*[-+]\s*DIVIDE\s*\(",
                    obj.Expression,
                    flags=re.IGNORECASE,
                ),
                "Instead of using the '1-(x/y)' or '1+(x/y)' syntax to achieve a percentage calculation, use the basic DAX functions (as shown below). Using the improved syntax will generally improve the performance. The '1+/-...' syntax always returns a value whereas the solution without the '1+/-...' does not (as the value may be 'blank'). Therefore the '1+/-...' syntax may return more rows/columns which may result in a slower query speed.    Let's clarify with an example:    Avoid this: 1 - SUM ( 'Sales'[CostAmount] ) / SUM( 'Sales'[SalesAmount] )  Better: DIVIDE ( SUM ( 'Sales'[SalesAmount] ) - SUM ( 'Sales'[CostAmount] ), SUM ( 'Sales'[SalesAmount] ) )  Best: VAR x = SUM ( 'Sales'[SalesAmount] ) RETURN DIVIDE ( x - SUM ( 'Sales'[CostAmount] ), x )",
            ),
            (
                "DAX Expressions",
                "Measure",
                "Warning",
                "Filter measure values by columns, not tables",
                lambda obj, tom: re.search(
                    r"CALCULATE\s*\(\s*[^,]+,\s*FILTER\s*\(\s*\'*[A-Za-z0-9 _]+\'*\s*,\s*\[[^\]]+\]",
                    obj.Expression,
                    flags=re.IGNORECASE,
                )
                or re.search(
                    r"CALCULATETABLE\s*\(\s*[^,]*,\s*FILTER\s*\(\s*\'*[A-Za-z0-9 _]+\'*\s*,\s*\[",
                    obj.Expression,
                    flags=re.IGNORECASE,
                ),
                "Instead of using this pattern FILTER('Table',[Measure]>Value) for the filter parameters of a CALCULATE or CALCULATETABLE function, use one of the options below (if possible). Filtering on a specific column will produce a smaller table for the engine to process, thereby enabling faster performance. Using the VALUES function or the ALL function depends on the desired measure result.\nOption 1: FILTER(VALUES('Table'[Column]),[Measure] > Value)\nOption 2: FILTER(ALL('Table'[Column]),[Measure] > Value)",
                "https://docs.microsoft.com/power-bi/guidance/dax-avoid-avoid-filter-as-filter-argument",
            ),
            (
                "DAX Expressions",
                "Measure",
                "Warning",
                "Filter column values with proper syntax",
                lambda obj, tom: re.search(
                    r"CALCULATE\s*\(\s*[^,]+,\s*FILTER\s*\(\s*'*[A-Za-z0-9 _]+'*\s*,\s*'*[A-Za-z0-9 _]+'*\[[A-Za-z0-9 _]+\]",
                    obj.Expression,
                    flags=re.IGNORECASE,
                )
                or re.search(
                    r"CALCULATETABLE\s*\([^,]*,\s*FILTER\s*\(\s*'*[A-Za-z0-9 _]+'*\s*,\s*'*[A-Za-z0-9 _]+'*\[[A-Za-z0-9 _]+\]",
                    obj.Expression,
                    flags=re.IGNORECASE,
                ),
                "Instead of using this pattern FILTER('Table','Table'[Column]=\"Value\") for the filter parameters of a CALCULATE or CALCULATETABLE function, use one of the options below. As far as whether to use the KEEPFILTERS function, see the second reference link below.\nOption 1: KEEPFILTERS('Table'[Column]=\"Value\")\nOption 2: 'Table'[Column]=\"Value\"",
                "https://docs.microsoft.com/power-bi/guidance/dax-avoid-avoid-filter-as-filter-argument  Reference: https://www.sqlbi.com/articles/using-keepfilters-in-dax",
            ),
            (
                "DAX Expressions",
                "Measure",
                "Warning",
                "Use the DIVIDE function for division",
                lambda obj, tom: re.search(
                    r"\]\s*\/(?!\/)(?!\*)|\)\s*\/(?!\/)(?!\*)",
                    obj.Expression,
                    flags=re.IGNORECASE,
                ),
                'Use the DIVIDE  function instead of using "/". The DIVIDE function resolves divide-by-zero cases. As such, it is recommended to use to avoid errors.',
                "https://docs.microsoft.com/power-bi/guidance/dax-divide-function-operator",
            ),
            (
                "DAX Expressions",
                [
                    "Measure",
                    "Calculated Table",
                    "Calculated Column",
                    "Calculation Item",
                ],
                "Error",
                "Column references should be fully qualified",
                lambda obj, tom: any(
                    tom.unqualified_columns(object=obj, dependencies=dependencies)
                ),
                "Using fully qualified column references makes it easier to distinguish between column and measure references, and also helps avoid certain errors. When referencing a column in DAX, first specify the table name, then specify the column name in square brackets.",
                "https://www.elegantbi.com/post/top10bestpractices",
            ),
            (
                "DAX Expressions",
                [
                    "Measure",
                    "Calculated Table",
                    "Calculated Column",
                    "Calculation Item",
                ],
                "Error",
                "Measure references should be unqualified",
                lambda obj, tom: any(
                    tom.fully_qualified_measures(object=obj, dependencies=dependencies)
                ),
                "Using unqualified measure references makes it easier to distinguish between column and measure references, and also helps avoid certain errors. When referencing a measure using DAX, do not specify the table name. Use only the measure name in square brackets.",
                "https://www.elegantbi.com/post/top10bestpractices",
            ),
            (
                "DAX Expressions",
                "Relationship",
                "Warning",
                "Inactive relationships that are never activated",
                lambda obj, tom: obj.IsActive is False
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
                ),
                "Inactive relationships are activated using the USERELATIONSHIP function. If an inactive relationship is not referenced in any measure via this function, the relationship will not be used. It should be determined whether the relationship is not necessary or to activate the relationship via this method.",
                "https://dax.guide/userelationship",
            ),
            (
                "Maintenance",
                "Column",
                "Warning",
                "Remove unnecessary columns",
                lambda obj, tom: (obj.IsHidden or obj.Parent.IsHidden)
                and not any(tom.used_in_relationships(object=obj))
                and not any(tom.used_in_hierarchies(column=obj))
                and not any(tom.used_in_sort_by(column=obj))
                and any(tom.depends_on(object=obj, dependencies=dependencies)),
                "Hidden columns that are not referenced by any DAX expressions, relationships, hierarchy levels or Sort By-properties should be removed.",
            ),
            (
                "Maintenance",
                "Measure",
                "Warning",
                "Remove unnecessary measures",
                lambda obj, tom: obj.IsHidden
                and not any(tom.referenced_by(object=obj, dependencies=dependencies)),
                "Hidden measures that are not referenced by any DAX expressions should be removed for maintainability.",
            ),
            (
                "Maintenance",
                "Table",
                "Warning",
                "Ensure tables have relationships",
                lambda obj, tom: any(tom.used_in_relationships(object=obj)) is False
                and obj.CalculationGroup is None,
                "This rule highlights tables which are not connected to any other table in the model with a relationship.",
            ),
            (
                "Maintenance",
                "Table",
                "Warning",
                "Calculation groups with no calculation items",
                lambda obj, tom: obj.CalculationGroup is not None
                and not any(obj.CalculationGroup.CalculationItems),
                "Calculation groups have no function unless they have calculation items.",
            ),
            (
                "Maintenance",
                ["Column", "Measure", "Table"],
                "Info",
                "Visible objects with no description",
                lambda obj, tom: obj.IsHidden is False and len(obj.Description) == 0,
                "Add descriptions to objects. These descriptions are shown on hover within the Field List in Power BI Desktop. Additionally, you can leverage these descriptions to create an automated data dictionary.",
            ),
            (
                "Formatting",
                "Column",
                "Warning",
                "Provide format string for 'Date' columns",
                lambda obj, tom: (re.search(r"date", obj.Name, flags=re.IGNORECASE))
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
                ),
                'Columns of type "DateTime" that have "Date" in their names should be formatted.',
            ),
            (
                "Formatting",
                "Column",
                "Warning",
                "Do not summarize numeric columns",
                lambda obj, tom: (
                    (obj.DataType == TOM.DataType.Int64)
                    or (obj.DataType == TOM.DataType.Decimal)
                    or (obj.DataType == TOM.DataType.Double)
                )
                and (str(obj.SummarizeBy) != "None")
                and not ((obj.IsHidden) or (obj.Parent.IsHidden)),
                'Numeric columns (integer, decimal, double) should have their SummarizeBy property set to "None" to avoid accidental summation in Power BI (create measures instead).',
            ),
            (
                "Formatting",
                "Measure",
                "Info",
                "Provide format string for measures",
                lambda obj, tom: obj.IsHidden is False and len(obj.FormatString) == 0,
                "Visible measures should have their format string property assigned.",
            ),
            (
                "Formatting",
                "Column",
                "Info",
                "Add data category for columns",
                lambda obj, tom: len(obj.DataCategory) == 0
                and any(
                    obj.Name.lower().startswith(item.lower())
                    for item in [
                        "country",
                        "city",
                        "continent",
                        "latitude",
                        "longitude",
                    ]
                ),
                "Add Data Category property for appropriate columns.",
                "https://docs.microsoft.com/power-bi/transform-model/desktop-data-categorization",
            ),
            (
                "Formatting",
                "Measure",
                "Warning",
                "Percentages should be formatted with thousands separators and 1 decimal",
                lambda obj, tom: "%" in obj.FormatString
                and obj.FormatString != "#,0.0%;-#,0.0%;#,0.0%",
                "For a better user experience, percengage measures should be formatted with a '%' sign.",
            ),
            (
                "Formatting",
                "Measure",
                "Warning",
                "Whole numbers should be formatted with thousands separators and no decimals",
                lambda obj, tom: "$" not in obj.FormatString
                and "%" not in obj.FormatString
                and obj.FormatString not in ["#,0", "#,0.0"],
                "For a better user experience, whole numbers should be formatted with commas.",
            ),
            (
                "Formatting",
                "Column",
                "Info",
                "Hide foreign keys",
                lambda obj, tom: obj.IsHidden is False
                and any(
                    r.FromColumn.Name == obj.Name
                    and r.FromCardinality == TOM.RelationshipEndCardinality.Many
                    for r in tom.used_in_relationships(object=obj)
                ),
                "Foreign keys should always be hidden as they should not be used by end users.",
            ),
            (
                "Formatting",
                "Column",
                "Info",
                "Mark primary keys",
                lambda obj, tom: any(
                    r.ToTable.Name == obj.Table.Name
                    and r.ToColumn.Name == obj.Name
                    and r.ToCardinality == TOM.RelationshipEndCardinality.One
                    for r in tom.used_in_relationships(object=obj)
                )
                and obj.IsKey is False
                and obj.Table.DataCategory != "Time",
                "Set the 'Key' property to 'True' for primary key columns within the column properties.",
            ),
            (
                "Formatting",
                "Column",
                "Info",
                "Month (as a string) must be sorted",
                lambda obj, tom: (re.search(r"month", obj.Name, flags=re.IGNORECASE))
                and not (re.search(r"months", obj.Name, flags=re.IGNORECASE))
                and (obj.DataType == TOM.DataType.String)
                and len(str(obj.SortByColumn)) == 0,
                "This rule highlights month columns which are strings and are not sorted. If left unsorted, they will sort alphabetically (i.e. April, August...). Make sure to sort such columns so that they sort properly (January, February, March...).",
            ),
            (
                "Formatting",
                "Relationship",
                "Warning",
                "Relationship columns should be of integer data type",
                lambda obj, tom: obj.FromColumn.DataType != TOM.DataType.Int64
                or obj.ToColumn.DataType != TOM.DataType.Int64,
                "It is a best practice for relationship columns to be of integer data type. This applies not only to data warehousing but data modeling as well.",
            ),
            (
                "Formatting",
                "Column",
                "Warning",
                "Provide format string for 'Month' columns",
                lambda obj, tom: re.search(r"month", obj.Name, flags=re.IGNORECASE)
                and obj.DataType == TOM.DataType.DateTime
                and obj.FormatString != "MMMM yyyy",
                'Columns of type "DateTime" that have "Month" in their names should be formatted as "MMMM yyyy".',
            ),
            (
                "Formatting",
                "Column",
                "Info",
                "Format flag columns as Yes/No value strings",
                lambda obj, tom: obj.Name.lower().startswith("is")
                and obj.DataType == TOM.DataType.Int64
                and not (obj.IsHidden or obj.Parent.IsHidden)
                or obj.Name.lower().endswith(" flag")
                and obj.DataType != TOM.DataType.String
                and not (obj.IsHidden or obj.Parent.IsHidden),
                "Flags must be properly formatted as Yes/No as this is easier to read than using 0/1 integer values.",
            ),
            (
                "Formatting",
                ["Table", "Column", "Measure", "Partition", "Hierarchy"],
                "Error",
                "Objects should not start or end with a space",
                lambda obj, tom: obj.Name[0] == " " or obj.Name[-1] == " ",
                "Objects should not start or end with a space. This usually happens by accident and is difficult to find.",
            ),
            (
                "Formatting",
                ["Table", "Column", "Measure", "Partition", "Hierarchy"],
                "Info",
                "First letter of objects must be capitalized",
                lambda obj, tom: obj.Name[0] != obj.Name[0].upper(),
                "The first letter of object names should be capitalized to maintain professional quality.",
            ),
            (
                "Naming Conventions",
                ["Table", "Column", "Measure", "Partition", "Hierarchy"],
                "Warning",
                "Object names must not contain special characters",
                lambda obj, tom: re.search(r"[\t\r\n]", obj.Name),
                "Object names should not include tabs, line breaks, etc.",
            ),
        ],
        columns=[
            "Category",
            "Scope",
            "Severity",
            "Rule Name",
            "Expression",
            "Description",
            "URL",
        ],
    )

    return rules
