import sempy
import sempy.fabric as fabric
import pandas as pd
sempy.fabric._client._utils._init_analysis_services()
import Microsoft.AnalysisServices.Tabular as TOM

def set_data_coverage_definition(dataset: str, filter_column: str, filterOperation: str, filterValue: str, workspaceName: str | None = None):

    if workspaceName == None:
        workspaceId = fabric.get_workspace_id()
        workspaceName = fabric.resolve_workspace_name(workspaceId)
    else:
        workspaceId = fabric.resolve_workspace_id(workspaceName)

    filterOperation = filterOperation.upper()

    dfC = fabric.list_columns(dataset = dataset, workspace = workspaceName, additional_xmla_properties=['Parent.DataCategory'])
    dfD = fabric.list_datasets(workspace = workspaceName, additional_xmla_properties=['CompatibilityLevel'])
    dfP = fabric.list_partitions(dataset = dataset, workspace = workspaceName)
    dfR = fabric.list_relationships(dataset = dataset, workspace = workspaceName)
    dfC = pd.merge(dfC, dfP[['Table Name', 'Mode']], on='Table Name', how='left')
    dfD = dfD[dfD['Dataset Name'] == dataset]

    dfP_filt = dfP[dfP['Mode'].isin(['Import', 'DirectQuery'])]
    tmc = pd.DataFrame(dfP_filt.groupby('Table Name')['Mode'].nunique()).reset_index()

    # Check if composite tables exist
    compositeTables = tmc[tmc['Mode'] == 2]
    if len(compositeTables) == 0:
        print(f"The '{dataset}' has no composite tables.")
        return
    
    factTable = compositeTables['Table Name'].iloc[0]

    # Check if there is a date table
    dfC_DateKey = dfC[(dfC['Parent Data Category'] == 'Time') & (dfC['Data Type'] == 'DateTime') & (dfC['Key'])]
    dateTables = dfC_DateKey[['Table Name', 'Mode']]
    if len(dateTables) == 0:
        print(f"The '{dataset}' has no date tables.")
        return

    # Check if the date table is in dual mode
    dualModeDate = dateTables[dateTables['Mode'] == 'Dual']
    if len(dualModeDate) == 0:
        print(f"The date table(s) in the '{dataset}' semantic model are not in dual mode")
        return

    dateTable = dualModeDate['Table Name'].iloc[0]

    # Check if filterColumn is valid
    dfC_filtcol = dfC[(dfC['Table Name'] == dateTable) & (dfC['Column Name'] == filter_column)]
    if len(dfC_filtcol) == 0:
        print(f"The 'filterColumn' parameter '{filter_column}' is not a valid column in the '{dateTable}' table.")
        return

    operator = ['<', '>', '=', '<=', '>=']
    filterColumnDT = dfC_filtcol['Data Type'].iloc[0]

    # Check if there is a relationship between the fact and date table
    dfR_filt = dfR[(dfR['From Table'] == factTable) & (dfR['To Table'] == dateTable)]
    if len(dfR_filt) == 0:
        print(f"There is no relationship between the fact and date table.")
        return

    compatLevel = dfD['Compatiblity Level'].iloc[0]
    if compatLevel < 1603:
        print(f"The compatibility of the model is '{compatLevel}. The compatibility level must be at least 1603 to use this feature.")
        return

    # Get DQ partition
    dfP_filt = dfP[(dfP['Table Name'] == factTable) & (dfP['Mode'] == 'DirectQuery')]
    dqPartition = dfP_filt['Partition Name'].iloc[0]

    tom_server = fabric.create_tom_server(readonly=False, workspace=workspaceName)
    m = tom_server.Databases.GetByName(dataset).Model

    if filterOperation == 'IN':
        filterCondition = f"RELATED('{dateTable}'[{filter_column}]) {filterOperation} {{{filterValue}}}"
    elif filterOperation in operator and filterColumnDT != 'Int64':
        print(f"If specifying a filterOperation value of {operator}, the filterColumn must be an 'Int64' data type.")
        return
    elif filterOperation in operator:
        filterCondition = f"RELATED('{dateTable}'[{filter_column}]) {filterOperation} {filterValue}"
    else:
        print(f"'{filterOperation}' is not a valid filter operation.")
        return

    dcd = TOM.DataCoverageDefinition()
    dcd.Expression = filterCondition
    print(f"All settings validated. Setting data coverage definition...")
    m.Tables[factTable].Partitions[dqPartition].DataCoverageDefinition = dcd
    m.SaveChanges()
    print(f"The data coverage definition '{filterCondition}' has been set for the '{dqPartition}' partition within the '{factTable}' table.")

    
