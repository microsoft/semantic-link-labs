import sempy
import sempy.fabric as fabric
import pandas as pd
import re, unicodedata, warnings, datetime
import numpy as np
from IPython.display import display, HTML
from pyspark.sql import SparkSession
from sempy_labs._model_dependencies import get_measure_dependencies
from sempy_labs._helper_functions import format_dax_object_name, resolve_lakehouse_name
from sempy_labs.lakehouse._get_lakehouse_tables import get_lakehouse_tables
from sempy_labs.lakehouse._lakehouse import lakehouse_attached
from typing import List, Optional, Union
from sempy._utils._log import log


def model_bpa_rules():
    """
    Shows the default rules for the semantic model BPA used by the run_model_bpa function.

    Parameters
    ----------


    Returns
    -------
    pandas.DataFrame
        A pandas dataframe containing the default rules for the run_model_bpa function.
    """

    df_rules = pd.DataFrame(
        [
            (
                "Performance",
                "Column",
                "Warning",
                "Do not use floating point data types",
                lambda df: df["Data Type"] == "Double",
                'The "Double" floating point data type should be avoided, as it can result in unpredictable roundoff errors and decreased performance in certain scenarios. Use "Int64" or "Decimal" where appropriate (but note that "Decimal" is limited to 4 digits after the decimal sign).',
            ),
            (
                "Performance",
                "Column",
                "Warning",
                "Avoid using calculated columns",
                lambda df: df["Type"] == "Calculated",
                "Calculated columns do not compress as well as data columns so they take up more memory. They also slow down processing times for both the table as well as process recalc. Offload calculated column logic to your data warehouse and turn these calculated columns into data columns.",
                "https://www.elegantbi.com/post/top10bestpractices",
            ),
            (
                "Performance",
                "Relationship",
                "Warning",
                "Check if bi-directional and many-to-many relationships are valid",
                lambda df: (df["Multiplicity"] == "m:m")
                | (df["Cross Filtering Behavior"] == "BothDirections"),
                "Bi-directional and many-to-many relationships may cause performance degradation or even have unintended consequences. Make sure to check these specific relationships to ensure they are working as designed and are actually necessary.",
                "https://www.sqlbi.com/articles/bidirectional-relationships-and-ambiguity-in-dax",
            ),
            (
                "Performance",
                "Row Level Security",
                "Info",
                "Check if dynamic row level security (RLS) is necessary",
                lambda df: df["Is Dynamic"],
                "Usage of dynamic row level security (RLS) can add memory and performance overhead. Please research the pros/cons of using it.",
                "https://docs.microsoft.com/power-bi/admin/service-admin-rls",
            ),
            (
                "Performance",
                "Table",
                "Warning",
                "Avoid using many-to-many relationships on tables used for dynamic row level security",
                lambda df: (df["Used in M2M Relationship"] == True)
                & (df["Used in Dynamic RLS"] == True),
                "Using many-to-many relationships on tables which use dynamic row level security can cause serious query performance degradation. This pattern's performance problems compound when snowflaking multiple many-to-many relationships against a table which contains row level security. Instead, use one of the patterns shown in the article below where a single dimension table relates many-to-one to a security table.",
                "https://www.elegantbi.com/post/dynamicrlspatterns",
            ),
            (
                "Performance",
                "Relationship",
                "Warning",
                "Many-to-many relationships should be single-direction",
                lambda df: (df["Multiplicity"] == "m:m")
                & (df["Cross Filtering Behavior"] == "BothDirections"),
            ),
            (
                "Performance",
                "Column",
                "Warning",
                "Set IsAvailableInMdx to false on non-attribute columns",
                lambda df: (df["Is Direct Lake"] == False)
                & (df["Is Available in MDX"] == True)
                & ((df["Hidden"] == True) | (df["Parent Is Hidden"] == True))
                & (df["Used in Sort By"] == False)
                & (df["Used in Hierarchy"] == False)
                & (df["Sort By Column"] == None),
                "To speed up processing time and conserve memory after processing, attribute hierarchies should not be built for columns that are never used for slicing by MDX clients. In other words, all hidden columns that are not used as a Sort By Column or referenced in user hierarchies should have their IsAvailableInMdx property set to false. The IsAvailableInMdx property is not relevant for Direct Lake models.",
                "https://blog.crossjoin.co.uk/2018/07/02/isavailableinmdx-ssas-tabular",
            ),
            # ('Performance', 'Partition', 'Warning', "Set 'Data Coverage Definition' property on the DirectQuery partition of a hybrid table",
            #  lambda df: (df['Data Coverage Definition Expression'].isnull()) & (df['Mode'] == 'DirectQuery') & (df['Import Partitions'] > 0) & (df['Has Date Table']),
            #  "Setting the 'Data Coverage Definition' property may lead to better performance because the engine knows when it can only query the import-portion of the table and when it needs to query the DirectQuery portion of the table.",
            #  "https://learn.microsoft.com/analysis-services/tom/table-partitions?view=asallproducts-allversions",
            # ),
            (
                "Performance",
                "Table",
                "Warning",
                "Set dimensions tables to dual mode instead of import when using DirectQuery on fact tables",
                lambda df: (df["Import Partitions"] == 1)
                & (df["Model Has DQ"])
                & (df["Used in Relationship x:1"]),
                "https://learn.microsoft.com/power-bi/transform-model/desktop-storage-mode#propagation-of-the-dual-setting",
            ),
            (
                "Performance",
                "Partition",
                "Warning",
                "Minimize Power Query transformations",
                lambda df: (df["Source Type"] == "M")
                & (
                    ('Table.Combine("' in df["Query"])
                    | ('Table.Join("' in df["Query"])
                    | ('Table.NestedJoin("' in df["Query"])
                    | ('Table.AddColumn("' in df["Query"])
                    | ('Table.Group("' in df["Query"])
                    | ('Table.Sort("' in df["Query"])
                    | ('Table.Sort("' in df["Query"])
                    | ('Table.Pivot("' in df["Query"])
                    | ('Table.Unpivot("' in df["Query"])
                    | ('Table.UnpivotOtherColumns("' in df["Query"])
                    | ('Table.Distinct("' in df["Query"])
                    | ('[Query=(""SELECT' in df["Query"])
                    | ("Value.NativeQuery" in df["Query"])
                    | ("OleDb.Query" in df["Query"])
                    | ("Odbc.Query" in df["Query"])
                ),
                "Minimize Power Query transformations in order to improve model processing performance. It is a best practice to offload these transformations to the data warehouse if possible. Also, please check whether query folding is occurring within your model. Please reference the article below for more information on query folding.",
                "https://docs.microsoft.com/power-query/power-query-folding",
            ),
            (
                "Performance",
                "Table",
                "Warning",
                "Consider a star-schema instead of a snowflake architecture",
                lambda df: (df["Type"] != "Calculation Group")
                & df["Used in Relationship Both Sides"],
                "Generally speaking, a star-schema is the optimal architecture for tabular models. That being the case, there are valid cases to use a snowflake approach. Please check your model and consider moving to a star-schema architecture.",
                "https://docs.microsoft.com/power-bi/guidance/star-schema",
            ),
            (
                "Performance",
                "Table",
                "Warning",
                "Reduce usage of calculated tables",
                lambda df: df["Type"] == "Calculated Table",
                "Migrate calculated table logic to your data warehouse. Reliance on calculated tables will lead to technical debt and potential misalignments if you have multiple models on your platform.",
            ),
            (
                "Performance",
                "Column",
                "Warning",
                "Reduce usage of calculated columns that use the RELATED function",
                lambda df: (df["Type"] == "Calculated")
                & (df["Source"].str.contains(r"related\s*\(", case=False)),
                "Calculated columns do not compress as well as data columns and may cause longer processing times. As such, calculated columns should be avoided if possible. One scenario where they may be easier to avoid is if they use the RELATED function.",
                "https://www.sqlbi.com/articles/storage-differences-between-calculated-columns-and-calculated-tables",
            ),
            (
                "Performance",
                "Model",
                "Warning",
                "Avoid excessive bi-directional or many-to-many relationships",
                lambda df: (
                    df["M2M or BiDi Relationship Count"] / df["Relationship Count"]
                )
                > 0.3,
                "Limit use of b-di and many-to-many relationships. This rule flags the model if more than 30% of relationships are bi-di or many-to-many.",
                "https://www.sqlbi.com/articles/bidirectional-relationships-and-ambiguity-in-dax",
            ),
            (
                "Performance",
                "Column",
                "Warning",
                "Avoid bi-directional or many-to-many relationships against high-cardinality columns",
                lambda df: df["Used in M2M/BiDi Relationship"]
                & df["Column Cardinality"]
                > 100000,
                "For best performance, it is recommended to avoid using bi-directional relationships against high-cardinality columns",
            ),
            (
                "Performance",
                "Table",
                "Warning",
                "Remove auto-date table",
                lambda df: (df["Type"] == "Calculated Table")
                & (
                    (df["Name"].str.startswith("DateTableTemplate_"))
                    | (df["Name"].str.startswith("LocalDateTable_"))
                ),
                "Avoid using auto-date tables. Make sure to turn off auto-date table in the settings in Power BI Desktop. This will save memory resources.",
                "https://www.youtube.com/watch?v=xu3uDEHtCrg",
            ),
            (
                "Performance",
                "Table",
                "Warning",
                "Date/calendar tables should be marked as a date table",
                lambda df: (
                    (df["Name"].str.contains(r"date", case=False))
                    | (df["Name"].str.contains(r"calendar", case=False))
                )
                & (df["Data Category"] != "Time"),
                "This rule looks for tables that contain the words 'date' or 'calendar' as they should likely be marked as a date table.",
                "https://docs.microsoft.com/power-bi/transform-model/desktop-date-tables",
            ),
            (
                "Performance",
                "Table",
                "Warning",
                "Large tables should be partitioned",
                lambda df: (df["Is Direct Lake"] == False)
                & (df["Partition Count"] == 1)
                & (df["Row Count"] > 25000000),
                "Large tables should be partitioned in order to optimize processing. This is not relevant for semantic models in Direct Lake mode as they can only have one partition per table.",
            ),
            (
                "Performance",
                "Row Level Security",
                "Warning",
                "Limit row level security (RLS) logic",
                lambda df: df["Filter Expression"].str.contains(
                    "|".join(["right", "left", "filter", "upper", "lower", "find"]),
                    case=False,
                ),
                "Try to simplify the DAX used for row level security. Usage of the functions within this rule can likely be offloaded to the upstream systems (data warehouse).",
            ),
            (
                "Performance",
                "Model",
                "Warning",
                "Model should have a date table",
                lambda df: df["Has Date Table"],
                "Generally speaking, models should generally have a date table. Models that do not have a date table generally are not taking advantage of features such as time intelligence or may not have a properly structured architecture.",
            ),
            (
                "Performance",
                "Measure",
                "Warning",
                "Measures using time intelligence and model is using Direct Query",
                lambda df: df["DQ Date Function Used"],
                "At present, time intelligence functions are known to not perform as well when using Direct Query. If you are having performance issues, you may want to try alternative solutions such as adding columns in the fact table that show previous year or previous month data.",
            ),
            (
                "Error Prevention",
                "Calculation Item",
                "Error",
                "Calculation items must have an expression",
                lambda df: df["Expression"].str.len() == 0,
                "Calculation items must have an expression. Without an expression, they will not show any values.",
            ),
            (
                "Error Prevention",
                ["Table", "Column", "Measure", "Hierarchy", "Partition"],
                "Error",
                "Avoid invalid characters in names",
                lambda df: df["Name"].apply(
                    lambda x: any(
                        unicodedata.category(char) == "Cc" and not char.isspace()
                        for char in x
                    )
                ),
                "This rule identifies if a name for a given object in your model (i.e. table/column/measure) which contains an invalid character. Invalid characters will cause an error when deploying the model (and failure to deploy). This rule has a fix expression which converts the invalid character into a space, resolving the issue.",
            ),
            (
                "Error Prevention",
                ["Table", "Column", "Measure", "Hierarchy"],
                "Error",
                "Avoid invalid characters in descriptions",
                lambda df: df["Description"].apply(
                    lambda x: any(
                        unicodedata.category(char) == "Cc" and not char.isspace()
                        for char in x
                    )
                ),
                "This rule identifies if a description for a given object in your model (i.e. table/column/measure) which contains an invalid character. Invalid characters will cause an error when deploying the model (and failure to deploy). This rule has a fix expression which converts the invalid character into a space, resolving the issue.",
            ),
            (
                "Error Prevention",
                "Relationship",
                "Warning",
                "Relationship columns should be of the same data type",
                lambda df: df["From Column Data Type"] != df["To Column Data Type"],
                "Columns used in a relationship should be of the same data type. Ideally, they will be of integer data type (see the related rule '[Formatting] Relationship columns should be of integer data type'). Having columns within a relationship which are of different data types may lead to various issues.",
            ),
            (
                "Error Prevention",
                "Column",
                "Error",
                "Data columns must have a source column",
                lambda df: (df["Type"] == "Data") & (df["Source"].str.len() == 0),
                "Data columns must have a source column. A data column without a source column will cause an error when processing the model.",
            ),
            (
                "Error Prevention",
                "Column",
                "Warning",
                "Set IsAvailableInMdx to true on necessary columns",
                lambda df: (df["Is Direct Lake"] == False)
                & (df["Is Available in MDX"] == False)
                & (
                    (df["Used in Sort By"] == True)
                    | (df["Used in Hierarchy"] == True)
                    | (df["Sort By Column"] != None)
                ),
                "In order to avoid errors, ensure that attribute hierarchies are enabled if a column is used for sorting another column, used in a hierarchy, used in variations, or is sorted by another column. The IsAvailableInMdx property is not relevant for Direct Lake models.",
            ),
            (
                "Error Prevention",
                "Table",
                "Error",
                "Avoid the USERELATIONSHIP function and RLS against the same table",
                lambda df: (df["USERELATIONSHIP Used"] == True)
                & (df["Used in RLS"] == True),
                "The USERELATIONSHIP function may not be used against a table which also leverages row-level security (RLS). This will generate an error when using the particular measure in a visual. This rule will highlight the table which is used in a measure's USERELATIONSHIP function as well as RLS.",
                "https://blog.crossjoin.co.uk/2013/05/10/userelationship-and-tabular-row-security",
            ),
            (
                "DAX Expressions",
                "Measure",
                "Warning",
                "Avoid using the IFERROR function",
                lambda df: df["Measure Expression"].str.contains(
                    r"irerror\s*\(", case=False
                ),
                "Avoid using the IFERROR function as it may cause performance degradation. If you are concerned about a divide-by-zero error, use the DIVIDE function as it naturally resolves such errors as blank (or you can customize what should be shown in case of such an error).",
                "https://www.elegantbi.com/post/top10bestpractices",
            ),
            (
                "DAX Expressions",
                "Measure",
                "Warning",
                "Use the TREATAS function instead of INTERSECT for virtual relationships",
                lambda df: df["Measure Expression"].str.contains(
                    r"intersect\s*\(", case=False
                ),
                "The TREATAS function is more efficient and provides better performance than the INTERSECT function when used in virutal relationships.",
                "https://www.sqlbi.com/articles/propagate-filters-using-treatas-in-dax",
            ),
            (
                "DAX Expressions",
                "Measure",
                "Warning",
                "The EVALUATEANDLOG function should not be used in production models",
                lambda df: df["Measure Expression"].str.contains(
                    r"evaluateandlog\s*\(", case=False
                ),
                "The EVALUATEANDLOG function is meant to be used only in development/test environments and should not be used in production models.",
                "https://pbidax.wordpress.com/2022/08/16/introduce-the-dax-evaluateandlog-function",
            ),
            (
                "DAX Expressions",
                "Measure",
                "Warning",
                "Measures should not be direct references of other measures",
                lambda df: df["Measure Expression"]
                .str.strip()
                .isin(df["Measure Object"]),
                "This rule identifies measures which are simply a reference to another measure. As an example, consider a model with two measures: [MeasureA] and [MeasureB]. This rule would be triggered for MeasureB if MeasureB's DAX was MeasureB:=[MeasureA]. Such duplicative measures should be removed.",
            ),
            (
                "DAX Expressions",
                "Measure",
                "Warning",
                "No two measures should have the same definition",
                lambda df: df["Measure Expression"]
                .apply(lambda x: re.sub(r"\s+", "", x))
                .duplicated(keep=False),
                "Two measures with different names and defined by the same DAX expression should be avoided to reduce redundancy.",
            ),
            (
                "DAX Expressions",
                "Measure",
                "Warning",
                "Avoid addition or subtraction of constant values to results of divisions",
                lambda df: df["Measure Expression"].str.contains(
                    "(?i)DIVIDE\\s*\\((\\s*.*?)\\)\\s*[+-]\\s*1"
                    or "\\/\\s*.*(?=[-+]\\s*1)",
                    regex=True,
                ),
            ),
            (
                "DAX Expressions",
                "Measure",
                "Warning",
                "Avoid using '1-(x/y)' syntax",
                lambda df: df["Measure Expression"].str.contains(
                    "[0-9]+\\s*[-+]\\s*[\\(]*\\s*(?i)SUM\\s*\\(\\s*'*[A-Za-z0-9 _]+'*\\s*\\[[A-Za-z0-9 _]+\\]\\s*\\)\\s*\\/"
                    or "[0-9]+\\s*[-+]\\s*(?i)DIVIDE\\s*\\(",
                    regex=True,
                ),
                "Instead of using the '1-(x/y)' or '1+(x/y)' syntax to achieve a percentage calculation, use the basic DAX functions (as shown below). Using the improved syntax will generally improve the performance. The '1+/-...' syntax always returns a value whereas the solution without the '1+/-...' does not (as the value may be 'blank'). Therefore the '1+/-...' syntax may return more rows/columns which may result in a slower query speed.    Let's clarify with an example:    Avoid this: 1 - SUM ( 'Sales'[CostAmount] ) / SUM( 'Sales'[SalesAmount] )  Better: DIVIDE ( SUM ( 'Sales'[SalesAmount] ) - SUM ( 'Sales'[CostAmount] ), SUM ( 'Sales'[SalesAmount] ) )  Best: VAR x = SUM ( 'Sales'[SalesAmount] ) RETURN DIVIDE ( x - SUM ( 'Sales'[CostAmount] ), x )",
            ),
            (
                "DAX Expressions",
                "Measure",
                "Warning",
                "Filter measure values by columns, not tables",
                lambda df: df["Measure Expression"].str.contains(
                    "(?i)CALCULATE\\s*\\(\\s*[^,]+,\\s*(?i)FILTER\\s*\\(\\s*'*[A-Za-z0-9 _]+'*\\s*,\\s*\\[[^\\]]+\\]"
                    or "(?i)CALCULATETABLE\\s*\\([^,]*,\\s*(?i)FILTER\\s*\\(\\s*'*[A-Za-z0-9 _]+'*\\s*,\\s*\\[",
                    regex=True,
                ),
                "Instead of using this pattern FILTER('Table',[Measure]>Value) for the filter parameters of a CALCULATE or CALCULATETABLE function, use one of the options below (if possible). Filtering on a specific column will produce a smaller table for the engine to process, thereby enabling faster performance. Using the VALUES function or the ALL function depends on the desired measure result.\nOption 1: FILTER(VALUES('Table'[Column]),[Measure] > Value)\nOption 2: FILTER(ALL('Table'[Column]),[Measure] > Value)",
                "https://docs.microsoft.com/power-bi/guidance/dax-avoid-avoid-filter-as-filter-argument",
            ),
            (
                "DAX Expressions",
                "Measure",
                "Warning",
                "Filter column values with proper syntax",
                lambda df: df["Measure Expression"].str.contains(
                    "(?i)CALCULATE\\s*\\(\\s*[^,]+,\\s*(?i)FILTER\\s*\\(\\s*'*[A-Za-z0-9 _]+'*\\s*,\\s*'*[A-Za-z0-9 _]+'*\\[[A-Za-z0-9 _]+\\]"
                    or "(?i)CALCULATETABLE\\s*\\([^,]*,\\s*(?i)FILTER\\s*\\(\\s*'*[A-Za-z0-9 _]+'*\\s*,\\s*'*[A-Za-z0-9 _]+'*\\[[A-Za-z0-9 _]+\\]",
                    regex=True,
                ),
                "Instead of using this pattern FILTER('Table','Table'[Column]=\"Value\") for the filter parameters of a CALCULATE or CALCULATETABLE function, use one of the options below. As far as whether to use the KEEPFILTERS function, see the second reference link below.\nOption 1: KEEPFILTERS('Table'[Column]=\"Value\")\nOption 2: 'Table'[Column]=\"Value\"",
                "https://docs.microsoft.com/power-bi/guidance/dax-avoid-avoid-filter-as-filter-argument  Reference: https://www.sqlbi.com/articles/using-keepfilters-in-dax",
            ),
            (
                "DAX Expressions",
                "Measure",
                "Warning",
                "Use the DIVIDE function for division",
                lambda df: df["Measure Expression"].str.contains(
                    '\\]\\s*\\/(?!\\/)(?!\\*)" or "\\)\\s*\\/(?!\\/)(?!\\*)', regex=True
                ),
                'Use the DIVIDE  function instead of using "/". The DIVIDE function resolves divide-by-zero cases. As such, it is recommended to use to avoid errors.',
                "https://docs.microsoft.com/power-bi/guidance/dax-divide-function-operator",
            ),
            (
                "DAX Expressions",
                "Measure",
                "Error",
                "Column references should be fully qualified",
                lambda df: df["Has Unqualified Column Reference"],
                "Using fully qualified column references makes it easier to distinguish between column and measure references, and also helps avoid certain errors. When referencing a column in DAX, first specify the table name, then specify the column name in square brackets.",
                "https://www.elegantbi.com/post/top10bestpractices",
            ),
            (
                "DAX Expressions",
                "Measure",
                "Error",
                "Measure references should be unqualified",
                lambda df: df["Has Fully Qualified Measure Reference"],
                "Using unqualified measure references makes it easier to distinguish between column and measure references, and also helps avoid certain errors. When referencing a measure using DAX, do not specify the table name. Use only the measure name in square brackets.",
                "https://www.elegantbi.com/post/top10bestpractices",
            ),
            (
                "DAX Expressions",
                "Relationship",
                "Warning",
                "Inactive relationships that are never activated",
                lambda df: df["Inactive without USERELATIONSHIP"],
                "Inactive relationships are activated using the USERELATIONSHIP function. If an inactive relationship is not referenced in any measure via this function, the relationship will not be used. It should be determined whether the relationship is not necessary or to activate the relationship via this method.",
                "https://dax.guide/userelationship",
            ),
            (
                "Maintenance",
                "Column",
                "Warning",
                "Remove unnecessary columns",
                lambda df: (df["Hidden"] | df["Parent Is Hidden"])
                & ~df["Used in Relationship"]
                & ~df["Used in Sort By"]
                & ~df["Used in Hierarchy"]
                & (df["Referenced By"] == 0)
                & ~(df["Used in RLS"]),  # usedInOLS
                "Hidden columns that are not referenced by any DAX expressions, relationships, hierarchy levels or Sort By-properties should be removed.",
            ),
            (
                "Maintenance",
                "Measure",
                "Warning",
                "Remove unnecessary measures",
                lambda df: df["Measure Hidden"] & (df["Referenced By"] == 0),
                "Hidden measures that are not referenced by any DAX expressions should be removed for maintainability.",
            ),
            # ('Maintenance', 'Role', 'Warning', 'Remove roles with no members',
            #  lambda df: df['Member Count'] == 0,
            # ),
            (
                "Maintenance",
                "Table",
                "Warning",
                "Ensure tables have relationships",
                lambda df: (df["Used in Relationship"] == False)
                & (df["Type"] != "Calculation Group"),
                "This rule highlights tables which are not connected to any other table in the model with a relationship.",
            ),
            (
                "Maintenance",
                "Table",
                "Warning",
                "Calculation groups with no calculation items",
                lambda df: (df["Type"] == "Calculation Group")
                & (df["Has Calculation Items"]),
            ),
            (
                "Maintenance",
                "Column",
                "Info",
                "Visible objects with no description",
                lambda df: (df["Hidden"] == False) & (df["Description"].str.len() == 0),
                "Calculation groups have no function unless they have calculation items.",
            ),
            (
                "Formatting",
                "Column",
                "Warning",
                "Provide format string for 'Date' columns",
                lambda df: (df["Column Name"].str.contains(r"date", case=False))
                & (df["Data Type"] == "DateTime")
                & (df["Format String"] != "mm/dd/yyyy"),
                'Columns of type "DateTime" that have "Month" in their names should be formatted as "mm/dd/yyyy".',
            ),
            (
                "Formatting",
                "Column",
                "Warning",
                "Do not summarize numeric columns",
                lambda df: (
                    (df["Data Type"] == "Int64")
                    | (df["Data Type"] == "Decimal")
                    | (df["Data Type"] == "Double")
                )
                & (df["Summarize By"] != "None")
                & ~((df["Hidden"]) | (df["Parent Is Hidden"])),
                'Numeric columns (integer, decimal, double) should have their SummarizeBy property set to "None" to avoid accidental summation in Power BI (create measures instead).',
            ),
            (
                "Formatting",
                "Measure",
                "Info",
                "Provide format string for measures",
                lambda df: ~((df["Measure Hidden"]) | (df["Parent Is Hidden"]))
                & (df["Format String"].str.len() == 0),
                "Visible measures should have their format string property assigned.",
            ),
            (
                "Formatting",
                "Column",
                "Info",
                "Add data category for columns",
                lambda df: (df["Data Category"] == "")
                & (
                    (
                        (
                            (df["Column Name"].str.contains(r"country", case=False))
                            | (df["Column Name"].str.contains(r"city", case=False))
                            | (df["Column Name"].str.contains(r"continent", case=False))
                        )
                        & (df["Data Type"] == "String")
                    )
                    | (
                        (
                            (df["Column Name"].str.contains(r"latitude", case=False))
                            | (df["Column Name"].str.contains(r"longitude", case=False))
                        )
                        & (df["Data Type"] == "String")
                    )
                ),
                "Add Data Category property for appropriate columns.",
                "https://docs.microsoft.com/power-bi/transform-model/desktop-data-categorization",
            ),
            (
                "Formatting",
                "Measure",
                "Warning",
                "Percentages should be formatted with thousands separators and 1 decimal",
                lambda df: (df["Format String"].str.contains("%"))
                & (df["Format String"] != "#,0.0%;-#,0.0%;#,0.0%"),
            ),
            (
                "Formatting",
                "Measure",
                "Warning",
                "Whole numbers should be formatted with thousands separators and no decimals",
                lambda df: (~df["Format String"].str.contains("$"))
                & ~(df["Format String"].str.contains("%"))
                & ~((df["Format String"] == "#,0") | (df["Format String"] == "#,0.0")),
            ),
            (
                "Formatting",
                "Column",
                "Info",
                "Hide foreign keys",
                lambda df: (df["Foreign Key"]) & (df["Hidden"] == False),
                "Foreign keys should always be hidden.",
            ),
            (
                "Formatting",
                "Column",
                "Info",
                "Mark primary keys",
                lambda df: (df["Primary Key"]) & (df["Key"] == False),
                "Set the 'Key' property to 'True' for primary key columns within the column properties.",
            ),
            (
                "Formatting",
                "Column",
                "Info",
                "Month (as a string) must be sorted",
                lambda df: (df["Column Name"].str.contains(r"month", case=False))
                & ~(df["Column Name"].str.contains(r"months", case=False))
                & (df["Data Type"] == "String")
                & (df["Sort By Column"] == ""),
                "This rule highlights month columns which are strings and are not sorted. If left unsorted, they will sort alphabetically (i.e. April, August...). Make sure to sort such columns so that they sort properly (January, February, March...).",
            ),
            (
                "Formatting",
                "Relationship",
                "Warning",
                "Relationship columns should be of integer data type",
                lambda df: (df["From Column Data Type"] != "Int64")
                | (df["To Column Data Type"] != "Int64"),
                "It is a best practice for relationship columns to be of integer data type. This applies not only to data warehousing but data modeling as well.",
            ),
            (
                "Formatting",
                "Column",
                "Warning",
                'Provide format string for "Month" columns',
                lambda df: (df["Column Name"].str.contains(r"month", case=False))
                & (df["Data Type"] == "DateTime")
                & (df["Format String"] != "MMMM yyyy"),
                'Columns of type "DateTime" that have "Month" in their names should be formatted as "MMMM yyyy".',
            ),
            (
                "Formatting",
                "Column",
                "Info",
                "Format flag columns as Yes/No value strings",
                lambda df: (
                    df["Column Name"].str.startswith("Is")
                    & (df["Data Type"] == "Int64")
                    & ~(df["Hidden"] | df["Parent Is Hidden"])
                )
                | (
                    df["Column Name"].str.endswith(" Flag")
                    & (df["Data Type"] != "String")
                    & ~(df["Hidden"] | df["Parent Is Hidden"])
                ),
                "Flags must be properly formatted as Yes/No as this is easier to read than using 0/1 integer values.",
            ),
            # ('Formatting', ['Table', 'Column', 'Measure', 'Partition', 'Hierarchy'], 'Error', 'Objects should not start or end with a space',
            #  lambda df: (df['Name'].str[0] == ' ') | (df['Name'].str[-1] == ' '),
            #  'Objects should not start or end with a space. This usually happens by accident and is difficult to find.',
            # ),
            (
                "Formatting",
                ["Table", "Column", "Measure", "Partition", "Hierarchy"],
                "Info",
                "First letter of objects must be capitalized",
                lambda df: df["Name"].str[0].str.upper() != df["Name"].str[0],
                "The first letter of object names should be capitalized to maintain professional quality.",
            ),
            (
                "Naming Conventions",
                ["Table", "Column", "Measure", "Partition", "Hierarchy"],
                "Warning",
                "Object names must not contain special characters",
                lambda df: df["Name"].str.contains(r"[\t\r\n]"),
                "Object names should not include tabs, line breaks, etc.",
            ),  # ,
            # ('Error Prevention', ['Table'], 'Error', 'Avoid invalid characters in names',
            #  lambda df: df['Name'].str.char.iscontrol() & ~ df['Name'].str.char.isspace(),
            # )#,
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

    df_rules["Severity"] = (
        df_rules["Severity"]
        .replace("Warning", "⚠️")
        .replace("Error", "\u274C")
        .replace("Info", "ℹ️")
    )

    pd.set_option("display.max_colwidth", 1000)

    return df_rules


@log
def run_model_bpa(
    dataset: str,
    rules_dataframe: Optional[pd.DataFrame] = None,
    workspace: Optional[str] = None,
    export: Optional[bool] = False,
    return_dataframe: Optional[bool] = False,
    **kwargs,
):
    """
    Displays an HTML visualization of the results of the Best Practice Analyzer scan for a semantic model.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    rules_dataframe : pandas.DataFrame, default=None
        A pandas dataframe containing rules to be evaluated.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    export : bool, default=False
        If True, exports the resulting dataframe to a delta table in the lakehouse attached to the notebook.
    return_dataframe : bool, default=False
        If True, returns a pandas dataframe instead of the visualization.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe in HTML format showing semantic model objects which violated the best practice analyzer rules.
    """

    if "extend" in kwargs:
        print(
            "The 'extend' parameter has been deprecated. Please remove this parameter from the function going forward."
        )
        del kwargs["extend"]

    warnings.filterwarnings(
        "ignore",
        message="This pattern is interpreted as a regular expression, and has match groups.",
    )

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    if rules_dataframe is None:
        rules_dataframe = model_bpa_rules()

    dfT = fabric.list_tables(dataset=dataset, workspace=workspace, extended=True)
    dfT = dfT.drop_duplicates()
    dfC = fabric.list_columns(
        dataset=dataset,
        workspace=workspace,
        extended=True,
        additional_xmla_properties=["Parent.DataCategory", "Parent.IsHidden"],
    )
    dfC = dfC[~dfC["Column Name"].str.startswith("RowNumber-")]

    dfM = fabric.list_measures(
        dataset=dataset,
        workspace=workspace,
        additional_xmla_properties=["Parent.IsHidden"],
    )
    dfR = fabric.list_relationships(
        dataset=dataset,
        workspace=workspace,
        additional_xmla_properties=["FromCardinality", "ToCardinality"],
    )
    dfP = fabric.list_partitions(
        dataset=dataset,
        workspace=workspace,
        additional_xmla_properties=["DataCoverageDefinition.Expression"],
    )
    dfH = fabric.list_hierarchies(dataset=dataset, workspace=workspace)
    dfRole = fabric.get_roles(dataset=dataset, workspace=workspace)
    dfRM = fabric.get_roles(dataset=dataset, workspace=workspace, include_members=True)
    dfRLS = fabric.get_row_level_security_permissions(
        dataset=dataset, workspace=workspace
    )
    # dfTr = fabric.list_translations(dataset = datasetName, workspace = workspaceName)
    # dfE = fabric.list_expressions(dataset = datasetName, workspace = workspaceName)
    dfCI = fabric.list_calculation_items(dataset=dataset, workspace=workspace)
    # dfDS = fabric.list_datasources(dataset = datasetName, workspace = workspaceName)
    # dfPersp = fabric.list_perspectives(dataset = datasetName, workspace = workspaceName)
    dfD = fabric.list_datasets(mode="rest", workspace=workspace)
    dfD = dfD[dfD["Dataset Name"] == dataset]
    # datasetOwner = dfD['Configured By'].iloc[0]
    md = get_measure_dependencies(dataset, workspace)
    isDirectLake = any(r["Mode"] == "DirectLake" for i, r in dfP.iterrows())
    dfC["Is Direct Lake"] = isDirectLake
    dfT["Is Direct Lake"] = isDirectLake

    cols = ["From Cardinality", "To Cardinality"]

    for col in cols:
        if not col in dfR:
            dfR[col] = None

    cols = ["Parent Is Hidden"]

    for col in cols:
        if not col in dfM:
            dfM[col] = None

    # Data Coverage Definition rule
    dfP_imp = dfP[dfP["Mode"] == "Import"]
    dfTP = dfP_imp.groupby("Table Name")["Partition Name"].count().reset_index()
    dfTP.rename(columns={"Partition Name": "Import Partitions"}, inplace=True)
    dfP = pd.merge(
        dfP, dfTP[["Table Name", "Import Partitions"]], on="Table Name", how="left"
    )
    dfP["Import Partitions"].fillna(0, inplace=True)
    dfC_DateKey = dfC[
        (dfC["Parent Data Category"] == "Time")
        & (dfC["Data Type"] == "DateTime")
        & (dfC["Key"])
    ]
    hasDateTable = False

    if len(dfC_DateKey) > 0:
        hasDateTable = True

    dfP["Has Date Table"] = hasDateTable

    # Set dims to dual mode
    dfR_one = dfR[dfR["To Cardinality"] == "One"]
    dfTP = dfP_imp.groupby("Table Name")["Partition Name"].count().reset_index()
    dfTP.rename(columns={"Partition Name": "Import Partitions"}, inplace=True)
    dfT = pd.merge(dfT, dfTP, left_on="Name", right_on="Table Name", how="left")
    dfT.drop(columns=["Table Name"], inplace=True)
    dfT["Import Partitions"].fillna(0, inplace=True)
    hasDQ = any(r["Mode"] == "DirectQuery" for i, r in dfP.iterrows())
    dfT["Model Has DQ"] = hasDQ
    dfT["Used in Relationship x:1"] = dfT["Name"].isin(dfR_one["To Table"])

    dfF = fabric.evaluate_dax(
        dataset=dataset,
        workspace=workspace,
        dax_string="""
        SELECT [FUNCTION_NAME] 
        FROM $SYSTEM.MDSCHEMA_FUNCTIONS
        WHERE [INTERFACE_NAME] = 'DATETIME'        
        """,
    )

    dfC["Name"] = dfC["Column Name"]
    dfH["Name"] = dfH["Hierarchy Name"]
    dfM["Name"] = dfM["Measure Name"]
    dfP["Name"] = dfP["Partition Name"]
    dfRole["Name"] = dfRole["Role"]
    dfD["Name"] = dfD["Dataset Name"]
    dfH["Description"] = dfH["Hierarchy Description"]
    dfM["Description"] = dfM["Measure Description"]
    dfH["Hierarchy Object"] = format_dax_object_name(
        dfH["Table Name"], dfH["Hierarchy Name"]
    )

    dfCI["Calculation Object"] = format_dax_object_name(
        dfCI["Calculation Group Name"], dfCI["Calculation Item Name"]
    )

    dfRole["Member Count"] = dfRM["Role"].isin(dfRole["Role"]).sum()
    dfRLS["Is Dynamic"] = dfRLS["Filter Expression"].str.contains(
        r"userprincipalname\s*\(", case=False
    ) | dfRLS["Filter Expression"].str.contains(r"username\s*\(", case=False)

    # Partition Count
    partition_count = (
        dfP.groupby("Table Name").size().reset_index(name="Partition Count")
    )
    dfT = pd.merge(
        dfT, partition_count, left_on="Name", right_on="Table Name", how="left"
    ).drop("Table Name", axis=1)
    dfT["Partition Count"] = dfT["Partition Count"].fillna(0).astype(int)

    dfT = dfT.merge(
        dfP[["Table Name", "Partition Name"]],
        how="left",
        left_on="Name",
        right_on="Table Name",
    )
    dfT["First Partition Name"] = dfT.groupby("Name")["Partition Name"].transform(
        "first"
    )
    dfT.drop("Table Name", axis=1, inplace=True)

    dfC["Sort By Column Object"] = format_dax_object_name(
        dfC["Table Name"], dfC["Sort By Column"]
    )
    dfC["Column Object"] = format_dax_object_name(dfC["Table Name"], dfC["Column Name"])
    dfM["Measure Object"] = "[" + dfM["Measure Name"] + "]"
    dfM["Measure Fully Qualified"] = format_dax_object_name(
        dfM["Table Name"], dfM["Measure Name"]
    )
    dfM["Measure Fully Qualified No Spaces"] = (
        dfM["Table Name"] + "[" + dfM["Measure Name"] + "]"
    )
    # dfM['Measure Fully Qualified No Spaces'] = dfM.apply(lambda row: row['Table Name'] + '[' + row['Measure Name'] + ']' if ' ' not in row['Table Name'] else '', axis=1)
    dfC["Column Unqualified"] = "[" + dfC["Column Name"] + "]"
    dfC["Column Object No Spaces"] = dfC.apply(
        lambda row: (
            row["Table Name"] + "[" + row["Column Name"] + "]"
            if " " not in row["Table Name"]
            else ""
        ),
        axis=1,
    )
    dfC["Used in Sort By"] = dfC["Column Object"].isin(dfC["Sort By Column Object"])
    dfH["Column Object"] = format_dax_object_name(dfH["Table Name"], dfH["Column Name"])
    dfC["Used in Hierarchy"] = dfC["Column Object"].isin(dfH["Column Object"])
    dfR["From Object"] = format_dax_object_name(dfR["From Table"], dfR["From Column"])
    dfR["To Object"] = format_dax_object_name(dfR["To Table"], dfR["To Column"])
    dfT["Used in Relationship"] = dfT["Name"].isin(dfR["From Table"]) | dfT[
        "Name"
    ].isin(dfR["To Table"])
    dfT["Used in Relationship Both Sides"] = dfT["Name"].isin(dfR["From Table"]) & dfT[
        "Name"
    ].isin(dfR["To Table"])
    dfC["Used in Relationship"] = dfC["Column Object"].isin(dfR["From Object"]) | dfC[
        "Column Object"
    ].isin(dfR["To Object"])

    dfR_filt = dfR[
        (dfR["Cross Filtering Behavior"] == "BothDirections")
        | (dfR["Multiplicity"] == "m:m")
    ]
    dfC["Used in M2M/BiDi Relationship"] = dfC["Column Object"].isin(
        dfR_filt["From Object"]
    ) | dfC["Column Object"].isin(dfR_filt["To Object"])
    dfC["Foreign Key"] = dfC["Column Object"].isin(
        dfR[dfR["From Cardinality"] == "Many"]["From Object"]
    )
    dfC["Primary Key"] = dfC["Column Object"].isin(
        dfR[dfR["To Cardinality"] == "One"]["To Object"]
    )
    dfT["Used in M2M Relationship"] = dfT["Name"].isin(
        dfR[dfR["Multiplicity"] == "m:m"][["From Table"]]
    ) | dfT["Name"].isin(dfR[dfR["Multiplicity"] == "m:m"][["To Table"]])
    dfT["Used in Dynamic RLS"] = dfT["Name"].isin(dfRLS[dfRLS["Is Dynamic"]]["Table"])
    dfT["Used in RLS"] = dfT["Name"].isin(
        dfRLS.loc[dfRLS["Filter Expression"].str.len() > 0, "Table"]
    )
    dfC["Primary Key"] = dfC["Column Object"].isin(
        dfR.loc[dfR["To Cardinality"] == "One", "To Object"]
    )
    dfD["Has Date Table"] = any(
        (r["Parent Data Category"] == "Time")
        & (r["Data Type"] == "DateTime")
        & (r["Key"] == True)
        for i, r in dfC.iterrows()
    )
    # dfC['In Date Table'] = dfC['Table Name'].isin(dfT.loc[dfT['Data Category'] == "Time", 'Name'])
    dfD["Relationship Count"] = len(dfR)
    dfD["M2M or BiDi Relationship Count"] = len(
        dfR[
            (dfR["Multiplicity"] == "m:m")
            | (dfR["Cross Filtering Behavior"] == "BothDirections")
        ]
    )
    dfD["Calculation Group Count"] = len(dfT[dfT["Type"] == "Calculation Group"])
    dfT["Has Calculation Items"] = np.where(
        (dfT["Type"] == "Calculation Group")
        & dfT["Name"].isin(dfCI["Calculation Group Name"]),
        True,
        False,
    )
    dfP["Partition Object"] = format_dax_object_name(
        dfP["Table Name"], dfP["Partition Name"]
    )
    dfRLS["RLS Object"] = format_dax_object_name(dfRLS["Role"], dfRLS["Table"])

    function_pattern = "|".join(dfF["FUNCTION_NAME"].map(re.escape))

    dfM["DQ Date Function Used"] = any(dfP["Mode"] == "DirectQuery") & dfM[
        "Measure Expression"
    ].str.contains(f"({function_pattern})\\s*\\(", case=False, regex=True)

    md["Reference"] = (
        "'" + md["Referenced Table"] + "'[" + md["Referenced Object"] + "]"
    )

    dfC["Referenced By"] = (
        md[
            (md["Referenced Object Type"] == "Column")
            & (md["Reference"].isin(dfC["Column Object"]))
        ]
        .groupby("Reference")
        .size()
        .reset_index(name="Count")["Count"]
    )
    dfC["Referenced By"].fillna(0, inplace=True)
    dfC["Referenced By"] = dfC["Referenced By"].fillna(0).astype(int)

    dfM["Referenced By"] = (
        md[
            (md["Referenced Object Type"] == "Measure")
            & (md["Referenced Object"].isin(dfM["Measure Name"]))
        ]
        .groupby("Referenced Object")
        .size()
        .reset_index(name="Count")["Count"]
    )
    dfM["Referenced By"].fillna(0, inplace=True)
    dfM["Referenced By"] = dfM["Referenced By"].fillna(0).astype(int)

    pattern = "[^\( ][a-zA-Z0-9_()-]+\[[^\[]+\]|'[^']+'\[[^\[]+\]|\[[^\[]+\]"

    dfM["Has Fully Qualified Measure Reference"] = False
    dfM["Has Unqualified Column Reference"] = False

    for i, r in dfM.iterrows():
        tName = r["Table Name"]
        mName = r["Measure Name"]
        expr = r["Measure Expression"]

        matches = re.findall(pattern, expr)

        for m in matches:
            if m[0] == "[":
                if (m in dfC["Column Unqualified"].values) and (
                    dfC[dfC["Table Name"] == tName]["Column Unqualified"] == m
                ).any():
                    dfM.at[i, "Has Unqualified Column Reference"] = True
            else:
                if (m in dfM["Measure Fully Qualified"].values) | (
                    m in dfM["Measure Fully Qualified No Spaces"].values
                ):
                    dfM.at[i, "Has Fully Qualified Measure Reference"] = True

    dfR["Inactive without USERELATIONSHIP"] = False
    for i, r in dfR[dfR["Active"] == False].iterrows():
        fromTable = r["From Table"]
        fromColumn = r["From Column"]
        toTable = r["To Table"]
        toColumn = r["To Column"]

        dfM_filt = dfM[
            dfM["Measure Expression"].str.contains(
                "(?i)USERELATIONSHIP\s*\(\s*'*"
                + fromTable
                + "'*\["
                + fromColumn
                + "\]\s*,\s*'*"
                + toTable
                + "'*\["
                + toColumn
                + "\]",
                regex=True,
            )
        ]
        if len(dfM_filt) == 0:
            dfR.at[i, "Inactive without USERELATIONSHIP"] = True

    dfC["Used in RLS"] = (
        dfC["Column Object No Spaces"].isin(dfRLS["Filter Expression"])
        | dfC["Column Object"].isin(dfRLS["Filter Expression"])
        | dfC.apply(
            lambda row: any(
                row["Column Name"] in expr
                for expr in dfRLS.loc[
                    dfRLS["Table"] == row["Table Name"], "Filter Expression"
                ]
            ),
            axis=1,
        )
    )

    # Merge dfR and dfC based on 'From Object' and 'Column Object'
    merged_from = pd.merge(
        dfR, dfC, left_on="From Object", right_on="Column Object", how="left"
    )
    merged_to = pd.merge(
        dfR, dfC, left_on="To Object", right_on="Column Object", how="left"
    )

    dfR["From Column Data Type"] = merged_from["Data Type"]
    dfR["To Column Data Type"] = merged_to["Data Type"]

    # Check if USERELATIONSHIP objects are used in a given column, table
    userelationship_pattern = re.compile(
        r"USERELATIONSHIP\s*\(\s*(.*?)\s*,\s*(.*?)\s*\)", re.DOTALL | re.IGNORECASE
    )

    # Function to extract objects within USERELATIONSHIP function
    def extract_objects(measure_expression):
        matches = userelationship_pattern.findall(measure_expression)
        if matches:
            return [obj.strip() for match in matches for obj in match]
        else:
            return []

    dfM["USERELATIONSHIP Objects"] = dfM["Measure Expression"].apply(extract_objects)
    flat_object_list = [
        item for sublist in dfM["USERELATIONSHIP Objects"] for item in sublist
    ]
    dfC["USERELATIONSHIP Used"] = dfC["Column Object"].isin(flat_object_list) | dfC[
        "Column Object No Spaces"
    ].isin(flat_object_list)
    dfT["USERELATIONSHIP Used"] = dfT["Name"].isin(
        dfC[dfC["USERELATIONSHIP Used"]]["Table Name"]
    )
    dfR["Relationship Name"] = (
        format_dax_object_name(dfR["From Table"], dfR["From Column"])
        + " -> "
        + format_dax_object_name(dfR["To Table"], dfR["To Column"])
    )
    dfH = dfH[
        [
            "Name",
            "Description",
            "Table Name",
            "Hierarchy Name",
            "Hierarchy Description",
            "Hierarchy Object",
        ]
    ].drop_duplicates()

    scope_to_dataframe = {
        "Table": (dfT, ["Name"]),
        "Partition": (dfP, ["Partition Object"]),
        "Column": (dfC, ["Column Object"]),
        "Hierarchy": (dfH, ["Hierarchy Object"]),
        "Measure": (dfM, ["Measure Name"]),
        "Calculation Item": (dfCI, ["Calculation Object"]),
        "Relationship": (dfR, ["Relationship Name"]),
        "Row Level Security": (dfRLS, ["RLS Object"]),
        "Role": (dfRole, ["Role"]),
        "Model": (dfD, ["Dataset Name"]),
    }

    def execute_rule(row):
        scopes = row["Scope"]

        # support both str and list as scope type
        if isinstance(scopes, str):
            scopes = [scopes]

        # collect output dataframes
        df_outputs = []

        for scope in scopes:
            # common fields for each scope
            (df, violation_cols_or_func) = scope_to_dataframe[scope]

            if scope in ["Hierarchy", "Measure"] and len(df) == 0:
                continue
            # execute rule and subset df
            df_violations = df[row["Expression"](df)]

            # subset the right output columns (e.g. Table Name & Column Name)
            if isinstance(violation_cols_or_func, list):
                violation_func = lambda violations: violations[violation_cols_or_func]
            else:
                violation_func = violation_cols_or_func

            # build output data frame
            df_output = violation_func(df_violations).copy()

            df_output.columns = ["Object Name"]
            df_output["Rule Name"] = row["Rule Name"]
            df_output["Category"] = row["Category"]

            df_output["Object Type"] = scope
            df_output["Severity"] = row["Severity"]
            df_output["Description"] = row["Description"]
            df_output["URL"] = row["URL"]

            df_outputs.append(df_output)

        return df_outputs

    # flatten list of lists
    flatten_dfs = [
        df for dfs in rules_dataframe.apply(execute_rule, axis=1).tolist() for df in dfs
    ]

    finalDF = pd.concat(flatten_dfs, ignore_index=True)

    if export:
        lakeAttach = lakehouse_attached()
        if lakeAttach == False:
            print(
                f"In order to save the Best Practice Analyzer results, a lakehouse must be attached to the notebook. Please attach a lakehouse to this notebook."
            )
            return
        dfExport = finalDF.copy()
        delta_table_name = "modelbparesults"

        lakehouse_id = fabric.get_lakehouse_id()
        lakehouse = resolve_lakehouse_name(
            lakehouse_id=lakehouse_id, workspace=workspace
        )

        lakeT = get_lakehouse_tables(lakehouse=lakehouse, workspace=workspace)
        lakeT_filt = lakeT[lakeT["Table Name"] == delta_table_name]

        dfExport["Severity"].replace("⚠️", "Warning", inplace=True)
        dfExport["Severity"].replace("\u274C", "Error", inplace=True)
        dfExport["Severity"].replace("ℹ️", "Info", inplace=True)

        spark = SparkSession.builder.getOrCreate()
        query = f"SELECT MAX(RunId) FROM {lakehouse}.{delta_table_name}"

        if len(lakeT_filt) == 0:
            runId = 1
        else:
            dfSpark = spark.sql(query)
            maxRunId = dfSpark.collect()[0][0]
            runId = maxRunId + 1

        now = datetime.datetime.now()
        dfExport["Workspace Name"] = workspace
        dfExport["Dataset Name"] = dataset
        dfExport["Timestamp"] = now
        dfExport["RunId"] = runId

        dfExport["RunId"] = dfExport["RunId"].astype("int")

        colName = "Workspace Name"
        dfExport.insert(0, colName, dfExport.pop(colName))
        colName = "Dataset Name"
        dfExport.insert(1, colName, dfExport.pop(colName))

        dfExport.columns = dfExport.columns.str.replace(" ", "_")
        spark_df = spark.createDataFrame(dfExport)
        spark_df.write.mode("append").format("delta").saveAsTable(delta_table_name)
        print(
            f"\u2022 Model Best Practice Analyzer results for the '{dataset}' semantic model have been appended to the '{delta_table_name}' delta table."
        )

    if return_dataframe:
        return finalDF

    pd.set_option("display.max_colwidth", 100)

    finalDF = (
        finalDF[
            [
                "Category",
                "Rule Name",
                "Object Type",
                "Object Name",
                "Severity",
                "Description",
                "URL",
            ]
        ]
        .sort_values(["Category", "Rule Name", "Object Type", "Object Name"])
        .set_index(["Category", "Rule Name"])
    )

    bpa2 = finalDF.reset_index()
    bpa_dict = {
        cat: bpa2[bpa2["Category"] == cat].drop("Category", axis=1)
        for cat in bpa2["Category"].drop_duplicates().values
    }

    styles = """
    <style>
        .tab { overflow: hidden; border: 1px solid #ccc; background-color: #f1f1f1; }
        .tab button { background-color: inherit; float: left; border: none; outline: none; cursor: pointer; padding: 14px 16px; transition: 0.3s; }
        .tab button:hover { background-color: #ddd; }
        .tab button.active { background-color: #ccc; }
        .tabcontent { display: none; padding: 6px 12px; border: 1px solid #ccc; border-top: none; }
        .tabcontent.active { display: block; }
        .tooltip { position: relative; display: inline-block; }
        .tooltip .tooltiptext { visibility: hidden; width: 300px; background-color: #555; color: #fff; text-align: center; border-radius: 6px; padding: 5px; position: absolute; z-index: 1; bottom: 125%; left: 50%; margin-left: -110px; opacity: 0; transition: opacity 0.3s; }
        .tooltip:hover .tooltiptext { visibility: visible; opacity: 1; }
    </style>
    """

    # JavaScript for tab functionality
    script = """
    <script>
    function openTab(evt, tabName) {
        var i, tabcontent, tablinks;
        tabcontent = document.getElementsByClassName("tabcontent");
        for (i = 0; i < tabcontent.length; i++) {
            tabcontent[i].style.display = "none";
        }
        tablinks = document.getElementsByClassName("tablinks");
        for (i = 0; i < tablinks.length; i++) {
            tablinks[i].className = tablinks[i].className.replace(" active", "");
        }
        document.getElementById(tabName).style.display = "block";
        evt.currentTarget.className += " active";
    }
    </script>
    """

    # JavaScript for dynamic tooltip positioning
    dynamic_script = """
    <script>
    function adjustTooltipPosition(event) {
        var tooltip = event.target.querySelector('.tooltiptext');
        var rect = tooltip.getBoundingClientRect();
        var topSpace = rect.top;
        var bottomSpace = window.innerHeight - rect.bottom;

        if (topSpace < bottomSpace) {
            tooltip.style.bottom = '125%';
        } else {
            tooltip.style.bottom = 'auto';
            tooltip.style.top = '125%';
        }
    }
    </script>
    """

    # HTML for tabs
    tab_html = '<div class="tab">'
    content_html = ""
    for i, (title, df) in enumerate(bpa_dict.items()):
        if df.shape[0] == 0:
            continue

        tab_id = f"tab{i}"
        active_class = ""
        if i == 0:
            active_class = "active"

        summary = " + ".join(
            [f"{idx} ({v})" for idx, v in df["Severity"].value_counts().items()]
        )
        tab_html += f'<button class="tablinks {active_class}" onclick="openTab(event, \'{tab_id}\')"><b>{title}</b><br/>{summary}</button>'
        content_html += f'<div id="{tab_id}" class="tabcontent {active_class}">'

        # Adding tooltip for Rule Name using Description column
        content_html += '<table border="1">'
        content_html += "<tr><th>Rule Name</th><th>Object Type</th><th>Object Name</th><th>Severity</th></tr>"
        for _, row in df.iterrows():
            content_html += f"<tr>"
            if pd.notnull(row["URL"]):
                content_html += f'<td class="tooltip" onmouseover="adjustTooltipPosition(event)"><a href="{row["URL"]}">{row["Rule Name"]}</a><span class="tooltiptext">{row["Description"]}</span></td>'
            elif pd.notnull(row["Description"]):
                content_html += f'<td class="tooltip" onmouseover="adjustTooltipPosition(event)">{row["Rule Name"]}<span class="tooltiptext">{row["Description"]}</span></td>'
            else:
                content_html += f'<td>{row["Rule Name"]}</td>'
            content_html += f'<td>{row["Object Type"]}</td>'
            content_html += f'<td>{row["Object Name"]}</td>'
            content_html += f'<td>{row["Severity"]}</td>'
            content_html += f"</tr>"
        content_html += "</table>"

        content_html += "</div>"
    tab_html += "</div>"

    # Display the tabs, tab contents, and run the script
    return display(HTML(styles + tab_html + content_html + script))
