import sempy
import sempy.fabric as fabric
import pandas as pd
from typing import List, Optional, Union
from IPython.display import display
import sempy_labs._icons as icons
from .._helper_functions import (
    _read_delta_table,
    _run_spark_sql_query,
)


def optimize_semantic_model(dataset: str, workspace: Optional[str] = None):

    from ._model_bpa import run_model_bpa
    from .directlake._dl_helper import check_fallback_reason
    from ._helper_functions import format_dax_object_name

    modelBPA = run_model_bpa(
        dataset=dataset, workspace=workspace, return_dataframe=True
    )
    dfC = fabric.list_columns(dataset=dataset, workspace=workspace, extended=True)
    dfC["Column Object"] = format_dax_object_name(dfC["Table Name"], dfC["Column Name"])
    dfC["Total Size"] = dfC["Total Size"].astype("int")
    dfP = fabric.list_partitions(dataset=dataset, workspace=workspace)

    modelBPA_col = modelBPA[modelBPA["Object Type"] == "Column"]
    modelBPA_col = pd.merge(
        modelBPA_col,
        dfC[["Column Object", "Total Size"]],
        left_on="Object Name",
        right_on="Column Object",
        how="left",
    )

    isDirectLake = any(r["Mode"] == "DirectLake" for i, r in dfP.iterrows())

    if isDirectLake:
        fallback = check_fallback_reason(dataset=dataset, workspace=workspace)
        fallback_filt = fallback[fallback["FallbackReasonID"] == 2]

        if len(fallback_filt) > 0:
            print(
                f"{icons.yellow_dot} The '{dataset}' semantic model is a Direct Lake semantic model which contains views. "
                "Since views always fall back to DirectQuery, it is recommended to only use lakehouse tables and not views."
            )

    # Potential model reduction estimate
    ruleNames = [
        "Remove unnecessary columns",
        "Set IsAvailableInMdx to false on non-attribute columns",
    ]

    for rule in ruleNames:
        df = modelBPA_col[modelBPA_col["Rule Name"] == rule]
        df_filt = df[["Object Name", "Total Size"]].sort_values(
            by="Total Size", ascending=False
        )
        totSize = df["Total Size"].sum()
        if len(df_filt) > 0:
            print(
                f"{icons.yellow_dot} Potential savings of {totSize} bytes from following the '{rule}' rule."
            )
            display(df_filt)
        else:
            print(f"{icons.green_dot} The '{rule}' rule has been followed.")


def generate_aggs(
    dataset: str,
    table_name: str,
    columns: Union[str, List[str]],
    workspace: Optional[str] = None,
    lakehouse_workspace: Optional[str] = None,
):

    from ._helper_functions import (
        get_direct_lake_sql_endpoint,
        create_abfss_path,
        format_dax_object_name,
        resolve_lakehouse_id,
    )

    sempy.fabric._client._utils._init_analysis_services()
    import Microsoft.AnalysisServices.Tabular as TOM
    import System

    # columns = {
    # 'SalesAmount': 'Sum',
    # 'ProductKey': 'GroupBy',
    # 'OrderDateKey': 'GroupBy'
    #  }

    if workspace is None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    if lakehouse_workspace is None:
        lakehouse_workspace = workspace
        lakehouse_workspace_id = workspace_id
    else:
        lakehouse_workspace_id = fabric.resolve_workspace_id(lakehouse_workspace)

    if isinstance(columns, str):
        columns = [columns]

    columnValues = columns.keys()

    aggTypes = ["Sum", "Count", "Min", "Max", "GroupBy"]
    aggTypesAggregate = ["Sum", "Count", "Min", "Max"]
    numericTypes = ["Int64", "Double", "Decimal"]

    if any(value not in aggTypes for value in columns.values()):
        raise ValueError(
            f"{icons.red_dot} Invalid aggregation type(s) have been specified in the 'columns' parameter. Valid aggregation types: {aggTypes}."
        )

    dfC = fabric.list_columns(dataset=dataset, workspace=workspace)
    dfP = fabric.list_partitions(dataset=dataset, workspace=workspace)
    dfM = fabric.list_measures(dataset=dataset, workspace=workspace)
    dfR = fabric.list_relationships(dataset=dataset, workspace=workspace)
    if not any(r["Mode"] == "DirectLake" for i, r in dfP.iterrows()):
        raise ValueError(
            f"{icons.red_dot} The '{dataset}' semantic model within the '{workspace}' workspace is not in Direct Lake mode. This function is only relevant for Direct Lake semantic models."
        )

    dfC_filtT = dfC[dfC["Table Name"] == table_name]

    if len(dfC_filtT) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{table_name}' table does not exist in the '{dataset}' semantic model within the '{workspace}' workspace."
        )

    dfC_filt = dfC[
        (dfC["Table Name"] == table_name) & (dfC["Column Name"].isin(columnValues))
    ]

    if len(columns) != len(dfC_filt):
        raise ValueError(
            f"{icons.red_dot} Columns listed in '{columnValues}' do not exist in the '{table_name}' table in the '{dataset}' semantic model within the '{workspace}' workspace."
        )

    # Check if doing sum/count/min/max etc. on a non-number column
    for cm, agg in columns.items():
        dfC_col = dfC_filt[dfC_filt["Column Name"] == cm]
        dataType = dfC_col["Data Type"].iloc[0]
        if agg in aggTypesAggregate and dataType not in numericTypes:
            raise ValueError(
                f"{icons.red_dot} The '{cm}' column in the '{table_name}' table is of '{dataType}' data type. Only columns of '{numericTypes}' data types"
                f" can be aggregated as '{aggTypesAggregate}' aggregation types."
            )

    # Create/update lakehouse delta agg table
    aggSuffix = "_agg"
    aggTableName = f"{table_name}{aggSuffix}"
    aggLakeTName = aggTableName.lower().replace(" ", "_")
    dfP = fabric.list_partitions(dataset=dataset, workspace=workspace)
    dfP_filt = dfP[dfP["Table Name"] == table_name]
    lakeTName = dfP_filt["Query"].iloc[0]

    sqlEndpointId = get_direct_lake_sql_endpoint(dataset=dataset, workspace=workspace)

    dfI = fabric.list_items(workspace=lakehouse_workspace, type="SQLEndpoint")
    dfI_filt = dfI[(dfI["Id"] == sqlEndpointId)]

    if len(dfI_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The lakehouse (SQL Endpoint) used by the '{dataset}' semantic model does not reside in"
            f" the '{lakehouse_workspace}' workspace. Please update the lakehouse_workspace parameter."
        )

    lakehouseName = dfI_filt["Display Name"].iloc[0]
    lakehouse_id = resolve_lakehouse_id(
        lakehouse=lakehouseName, workspace=lakehouse_workspace
    )

    # Generate SQL query
    query = "SELECT"
    groupBy = "\nGROUP BY"
    for cm, agg in columns.items():
        colFilt = dfC_filt[dfC_filt["Column Name"] == cm]
        sourceCol = colFilt["Source"].iloc[0]

        if agg == "GroupBy":
            query = f"{query}\n{sourceCol},"
            groupBy = f"{groupBy}\n{sourceCol},"
        else:
            query = f"{query}\n{agg}({sourceCol}) AS {sourceCol},"

    query = query[:-1]

    fromTablePath = create_abfss_path(
        lakehouse_id=lakehouse_id,
        lakehouse_workspace_id=lakehouse_workspace_id,
        delta_table_name=lakeTName,
    )

    df = _read_delta_table(fromTablePath)
    tempTableName = f"delta_table_{lakeTName}"
    df.createOrReplaceTempView(tempTableName)
    sqlQuery = f"{query} \n FROM {tempTableName} {groupBy}"

    sqlQuery = sqlQuery[:-1]
    print(sqlQuery)

    # Save query to spark dataframe
    spark_df = _run_spark_sql_query(sqlQuery)
    f"\nCreating/updating the '{aggLakeTName}' table in the lakehouse..."
    # Write spark dataframe to delta table
    aggFilePath = create_abfss_path(
        lakehouse_id=lakehouse_id,
        lakehouse_workspace_id=lakehouse_workspace_id,
        delta_table_name=aggLakeTName,
    )
    spark_df.write.mode("overwrite").format("delta").save(aggFilePath)
    f"{icons.green_dot} The '{aggLakeTName}' table has been created/updated in the lakehouse."

    # Create/update semantic model agg table
    tom_server = fabric.create_tom_server(
        dataset=dataset, readonly=False, workspace=workspace
    )
    m = tom_server.Databases.GetByName(dataset).Model
    print(f"\n{icons.in_progress} Updating the '{dataset}' semantic model...")
    dfC_agg = dfC[dfC["Table Name"] == aggTableName]

    if len(dfC_agg) == 0:
        print(f"{icons.in_progress} Creating the '{aggTableName}' table...")
        exp = m.Expressions["DatabaseQuery"]
        tbl = TOM.Table()
        tbl.Name = aggTableName
        tbl.IsHidden = True

        ep = TOM.EntityPartitionSource()
        ep.Name = aggTableName
        ep.EntityName = aggLakeTName
        ep.ExpressionSource = exp

        part = TOM.Partition()
        part.Name = aggTableName
        part.Source = ep
        part.Mode = TOM.ModeType.DirectLake

        tbl.Partitions.Add(part)

        for i, r in dfC_filt.iterrows():
            scName = r["Source"]
            cName = r["Column Name"]
            dType = r["Data Type"]

            col = TOM.DataColumn()
            col.Name = cName
            col.IsHidden = True
            col.SourceColumn = scName
            col.DataType = System.Enum.Parse(TOM.DataType, dType)

            tbl.Columns.Add(col)
            print(
                f"{icons.green_dot} The '{aggTableName}'[{cName}] column has been added to the '{dataset}' semantic model."
            )

        m.Tables.Add(tbl)
        print(
            f"{icons.green_dot} The '{aggTableName}' table has been added to the '{dataset}' semantic model."
        )
    else:
        print(f"{icons.in_progress} Updating the '{aggTableName}' table's columns...")
        # Remove existing columns
        for t in m.Tables:
            tName = t.Name
            for c in t.Columns:
                cName = c.Name
                if t.Name == aggTableName:
                    m.Tables[tName].Columns.Remove(cName)
        # Add columns
        for i, r in dfC_filt.iterrows():
            scName = r["Source"]
            cName = r["Column Name"]
            dType = r["Data Type"]

            col = TOM.DataColumn()
            col.Name = cName
            col.IsHidden = True
            col.SourceColumn = scName
            col.DataType = System.Enum.Parse(TOM.DataType, dType)

            m.Tables[aggTableName].Columns.Add(col)
            print(
                f"{icons.green_dot} The '{aggTableName}'[{cName}] column has been added."
            )

    # Create relationships
    relMap = {"m": "Many", "1": "One", "0": "None"}

    print(f"\n{icons.in_progress} Generating necessary relationships...")
    for i, r in dfR.iterrows():
        fromTable = r["From Table"]
        fromColumn = r["From Column"]
        toTable = r["To Table"]
        toColumn = r["To Column"]
        cfb = r["Cross Filtering Behavior"]
        sfb = r["Security Filtering Behavior"]
        mult = r["Multiplicity"]

        crossFB = System.Enum.Parse(TOM.CrossFilteringBehavior, cfb)
        secFB = System.Enum.Parse(TOM.SecurityFilteringBehavior, sfb)
        fromCardinality = System.Enum.Parse(
            TOM.RelationshipEndCardinality, relMap.get(mult[0])
        )
        toCardinality = System.Enum.Parse(
            TOM.RelationshipEndCardinality, relMap.get(mult[-1])
        )

        rel = TOM.SingleColumnRelationship()
        rel.FromCardinality = fromCardinality
        rel.ToCardinality = toCardinality
        rel.IsActive = r["Active"]
        rel.CrossFilteringBehavior = crossFB
        rel.SecurityFilteringBehavior = secFB
        rel.RelyOnReferentialIntegrity = r["Rely On Referential Integrity"]

        if fromTable == table_name:
            try:
                rel.FromColumn = m.Tables[aggTableName].Columns[fromColumn]
                m.Relationships.Add(rel)
                print(
                    f"{icons.green_dot} '{aggTableName}'[{fromColumn}] -> '{toTable}'[{toColumn}] relationship has been added."
                )
            except Exception as e:
                print(
                    f"{icons.red_dot} '{aggTableName}'[{fromColumn}] -> '{toTable}'[{toColumn}] relationship has not been created."
                )
                print(f"Exception occured: {e}")
        elif toTable == table_name:
            try:
                rel.ToColumn = m.Tables[aggTableName].Columns[toColumn]
                m.Relationships.Add(rel)
                print(
                    f"{icons.green_dot} '{fromTable}'[{fromColumn}] -> '{aggTableName}'[{toColumn}] relationship has been added."
                )
            except Exception as e:
                print(
                    f"{icons.red_dot} '{fromTable}'[{fromColumn}] -> '{aggTableName}'[{toColumn}] relationship has not been created."
                )
                print(f"Exception occured: {e}")
    "Relationship creation is complete."

    # Create IF measure
    f"\n{icons.in_progress} Creating measure to check if the agg table can be used..."
    aggChecker = "IF("
    dfR_filt = dfR[
        (dfR["From Table"] == table_name) & (~dfR["From Column"].isin(columnValues))
    ]

    for i, r in dfR_filt.iterrows():
        toTable = r["To Table"]
        aggChecker = f"{aggChecker}\nISCROSSFILTERED('{toTable}') ||"

    aggChecker = aggChecker[:-3]
    aggChecker = f"{aggChecker},1,0)"
    print(aggChecker)

    # Todo: add IFISFILTERED clause for columns
    f"\n{icons.in_progress} Creating the base measures in the agg table..."
    # Create base agg measures
    dep = fabric.evaluate_dax(
        dataset=dataset,
        workspace=workspace,
        dax_string="""
        SELECT 
         [TABLE] AS [Table Name]
        ,[OBJECT] AS [Object Name]
        ,[OBJECT_TYPE] AS [Object Type]
        ,[REFERENCED_TABLE] AS [Referenced Table]
        ,[REFERENCED_OBJECT] AS [Referenced Object]
        ,[REFERENCED_OBJECT_TYPE] AS [Referenced Object Type]
        FROM $SYSTEM.DISCOVER_CALC_DEPENDENCY
        WHERE [OBJECT_TYPE] = 'MEASURE'
        """,
    )

    baseMeasures = dep[
        (dep["Referenced Object Type"] == "COLUMN")
        & (dep["Referenced Table"] == table_name)
        & (dep["Referenced Object"].isin(columnValues))
    ]
    for i, r in baseMeasures.iterrows():
        tName = r["Table Name"]
        mName = r["Object Name"]
        cName = r["Referenced Object"]
        dfM_filt = dfM[dfM["Measure Name"] == mName]
        expr = dfM_filt["Measure Expression"].iloc[0]

        colFQNonAgg = format_dax_object_name(tName, cName)
        colFQAgg = format_dax_object_name(aggTableName, cName)
        colNQNonAgg = f"{tName}[{cName}]"

        if " " in tName:
            newExpr = expr.replace(colFQNonAgg, colFQAgg)
        else:
            newExpr = expr.replace(colFQNonAgg, colFQAgg).replace(colNQNonAgg, colFQAgg)
        print(expr)
        print(newExpr)

        aggMName = f"{mName}{aggSuffix}"
        measure = TOM.Measure()
        measure.Name = aggMName
        measure.IsHidden = True
        measure.Expression = newExpr
        m.Tables[aggTableName].Measures.Add(measure)
        f"The '{aggMName}' measure has been created in the '{aggTableName}' table."

    # Update base detail measures

    # m.SaveChanges()


# Identify views used within Direct Lake model
# workspace = 'MK Demo 6'
# lakehouse = 'MyLakehouse'
# dataset = 'MigrationTest'
# lakehouse_workspace = workspace

# dfView = pd.DataFrame(columns=['Workspace Name', 'Lakehouse Name', 'View Name'])
# dfP = fabric.list_partitions(dataset = dataset, workspace = workspace)
# isDirectLake = any(r['Mode'] == 'DirectLake' for i, r in dfP.iterrows())

# spark = _create_spark_session()
# views = spark.sql(f"SHOW VIEWS IN {lakehouse}").collect()
# for view in views:
#    viewName = view['viewName']
#    isTemporary = view['isTemporary']
#    new_data = {'Workspace Name': workspace, 'Lakehouse Name': lakehouse, 'View Name': viewName}
#    dfView = pd.concat([dfView, pd.DataFrame(new_data, index=[0])], ignore_index=True)
# dfView
# lakeT = get_lakehouse_tables(lakehouse, lakehouse_workspace)
# if not dfP['Query'].isin(lakeT['Table Name'].values):
#    if
