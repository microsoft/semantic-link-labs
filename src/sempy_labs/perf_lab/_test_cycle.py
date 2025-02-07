import pandas as pd
import sempy.fabric as fabric
from sempy.fabric.exceptions import WorkspaceNotFoundException, FabricHTTPException
from sempy.fabric import FabricDataFrame

from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import col, lit
from datetime import date, datetime
from pyspark.sql.types import StructType, StructField, StringType
from typing import Optional, Tuple, Callable, List
from uuid import UUID
import warnings
import time
import re

import sempy_labs._icons as icons
from sempy_labs import deploy_semantic_model, clear_cache
from sempy_labs.perf_lab import  _get_or_create_workspace
from sempy_labs._refresh_semantic_model import refresh_semantic_model
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_lakehouse_name_and_id,
    resolve_dataset_name_and_id,
    generate_guid,
)
_sample_queries = [
("Total Sales (Card)", """EVALUATE
SUMMARIZE(
    Sales,
    "Total Sales", SUM(Sales[Sales])
)"""),
# ----------------------------
("Sales Over Time (Line Chart)", """EVALUATE
SUMMARIZE(
    'Date',
    'Date'[Date],
    "Sales Over Time", CALCULATE(SUM(Sales[Sales]))
)"""),
# ----------------------------
("Sales by Product Category (Column Chart)", """EVALUATE
SUMMARIZE(
    'Product',
    'Product'[ProductCategory],
    "Sales by Product Category", CALCULATE(SUM(Sales[Sales]))
)"""),
# ----------------------------
("Sales by Location (Map)", """EVALUATE
SUMMARIZE(
    Geography,
    Geography[StateOrTerritory],
    "Sales by Location", CALCULATE(SUM(Sales[Sales]))
)"""),
# ----------------------------
("Total Profit (Card)", """EVALUATE
SUMMARIZE(
    Sales,
	Geography[Country],
    "Total Profit", [Profit]
)"""),
# ----------------------------
("Monthly Sales Trends (Line Chart)", """EVALUATE
SUMMARIZE(
    'Date',
    'Date'[MonthYear],
    "Monthly Sales Trends", CALCULATE(SUM(Sales[Sales]))
)"""),
# ----------------------------
("Period Comparison", """DEFINE
MEASURE Sales[Sum of Quantity] = SUM(Sales[Sales])
MEASURE Sales[Sum of Quantity PM] = CALCULATE([Sum of Quantity],PREVIOUSMONTH('date'[Date]))
MEASURE Sales[Sum of Quantity PM Delta] = [Sum of Quantity] - [Sum of Quantity PM] 
MEASURE Sales[Sum of Quantity PM %] = [Sum of Quantity PM Delta] / [Sum of Quantity]

EVALUATE
SUMMARIZECOLUMNS(
    'date'[Monthly] ,
    TREATAS({DATE(2023,1,1),DATE(2023,2,1),DATE(2023,3,1)} , 'Date'[Monthly] ) ,
    "Sales" , [Sum of Quantity],
    "Sales PM" ,  [Sum of Quantity PM],
    "Sales PM Delta", [Sum of Quantity PM Delta] ,
    "Sales PM % " , [Sum of Quantity PM %]
)

ORDER BY [Monthly]"""),
# ----------------------------
("Running Total", """DEFINE
MEASURE Sales[Sum of Sales] =  SUM(Sales[sales])
MEASURE Sales[Sum of Sales YTD] = TOTALYTD([Sum of Sales],'date'[Date])
MEASURE Sales[Sum of Sales QTD] = TOTALQTD([Sum of Sales],'date'[Date]) 

EVALUATE
SUMMARIZECOLUMNS(
    'Date'[Monthly],
    TREATAS({DATE(2023,1,1)} , 'Date'[Monthly] ),
    "Sales" , [Sum of Sales],
    "Sales YTD" , [Sum of Sales YTD],
    "Sales QTD" , [Sum of Sales QTD]
)
ORDER BY [Monthly] """),
# ----------------------------
    ]


def _get_test_definitions(
    dax_queries: list[str] | list[(str, str)],
    target_dataset: str | UUID,
    target_workspace: Optional[str | UUID] = None,
    master_dataset: Optional[str | UUID] = None,
    master_workspace: Optional[str | UUID] = None,
    data_source: Optional[str | UUID] = None,
    data_source_workspace: Optional[str | UUID] = None,
    data_source_type: Optional[str] = "Lakehouse",

) -> DataFrame:
    """
    Generates a spark dataframe with test definitions.

    Parameters
    ----------
    dax_queries: list[str]
        A predefined list of DAX queries included in the test cycle.
        This can be a simple list of query expressions,
        or a list of (Query_Id, Query_Text) tuples.
    target_dataset : str | uuid.UUID
        The semantic model name or ID designating the model that the 
        test cycle should use to run the DAX queries.
    target_workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID where the target dataset is located.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    master_dataset : str | uuid.UUID, default=None
        The master semantic model name or ID for the target_dataset. If not 
        specified, the test cycle cannot clone the master to create the target_dataset.
        In this case, the target_dataset must already exist.
    master_workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID where the master dataset is located.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    data_source : str | uuid.UUID, default=None
        The name or ID of the lakehouse or other artifact that serves as the data source for the target_dataset.
        Defaults to None which resolves to the lakehouse or warehouse referenced in the data source shared expression.
    data_source_workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID where the data source is located.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    data_source_type : str, default=Lakehouse
        The type of the data source. Currently, the only supported type is Lakehouse.

    Returns
    -------
    DataFrame
        A Spark dataframe for the test definitions table.
    """
    # Parameter validation
    if data_source_type != "Lakehouse":
        raise ValueError("Unrecognized data source type specified. The only valid option for now is 'Lakehouse'.")

    try:
        (target_workspace_name, target_workspace_id) = resolve_workspace_name_and_id(target_workspace)
    except:
        (target_workspace_name, target_workspace_id) = (target_workspace, None)

    try:
        (target_dataset_name, target_dataset_id) = resolve_dataset_name_and_id(dataset=target_dataset, workspace=target_workspace_name)
    except:
        (target_dataset_name, target_dataset_id) = (target_dataset, None)

    if master_dataset == "" or master_dataset is None:
        (master_dataset_name, master_dataset_id) = (None, None)
        (master_workspace_name, master_workspace_id) = (None, None)
    else:
        (master_workspace_name, master_workspace_id) = resolve_workspace_name_and_id(master_workspace)
        (master_dataset_name, master_dataset_id) = resolve_dataset_name_and_id(dataset=master_dataset, workspace=master_workspace_id)

    (data_source_workspace_name, data_source_workspace_id) = resolve_workspace_name_and_id(data_source_workspace)
    (data_source_name, data_source_id) = resolve_lakehouse_name_and_id(lakehouse=data_source, workspace=data_source_workspace_id)


    spark = SparkSession.builder \
        .appName("PerfLabGenerator") \
        .getOrCreate()

    # A table to return the test definitions in the form of a spark dataframe.
    schema = StructType([
        StructField("QueryId", StringType(), nullable=False),
        StructField("QueryText", StringType(), nullable=False),
        StructField("MasterWorkspace", StringType(), nullable=True),
        StructField("MasterDataset", StringType(), nullable=True),
        StructField("TargetWorkspace", StringType(), nullable=True),
        StructField("TargetDataset", StringType(), nullable=False),
        StructField("DatasourceName", StringType(), nullable=True),
        StructField("DatasourceWorkspace", StringType(), nullable=True),
        StructField("DatasourceType", StringType(), nullable=True),
    ])

    data = []
    for i in range(len(dax_queries)):
        q = dax_queries[i]
        if isinstance(q, str):
            data.append((
                f"Q{i}",
                q,
                master_workspace_name,
                master_dataset_name,
                target_workspace_name,
                target_dataset_name,
                data_source_name,
                data_source_workspace_name,
                data_source_type))
        elif isinstance(q, tuple):
            data.append((
                q[0],
                q[1],
                master_workspace_name,
                master_dataset_name,
                target_workspace_name,
                target_dataset_name,
                data_source_name,
                data_source_workspace_name,
                data_source_type))

    return spark.createDataFrame(data, schema=schema)


def _get_test_definitions_from_trace_events(
    target_dataset_prefix: str | UUID,
    target_workspace: Optional[str | UUID] = None,
    master_dataset: Optional[str | UUID] = None,
    master_workspace: Optional[str | UUID] = None,
    data_source: Optional[str | UUID] = None,
    data_source_workspace: Optional[str | UUID] = None,
    data_source_type: Optional[str] = "Lakehouse",
    timeout: Optional[int] = 300,
) -> DataFrame:
    """
        Generates a spark dataframe with test definitions.

    Parameters
    ----------
    target_dataset_prefix : str | uuid.UUID
        The semantic model name or ID designating the model that the 
        test cycle should use to run a DAX query. This function generates
        a unique name for each unique DAX query in the form of {target_dataset_prefix}_{sequence_number}
    target_workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID where the target dataset is located.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    master_dataset : str | uuid.UUID, default=None
        The master semantic model name or ID for the target_dataset. If not 
        specified, the test cycle cannot clone the master to create the target_dataset.
        In this case, the target_dataset must already exist.
    master_workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID where the master dataset is located.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    data_source : str | uuid.UUID, default=None
        The name or ID of the lakehouse or other artifact that serves as the data source for the target_dataset.
        Defaults to None which resolves to the lakehouse or warehouse referenced in the data source shared expression.
    data_source_workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID where the data source is located.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    data_source_type : str, default=Lakehouse
        The type of the data source. Currently, the only supported type is Lakehouse.    timeout : int, default=30
        The max time duration to capture trace events after execution of the DAX queries.

    Returns
    -------
    DataFrame
        A Spark dataframe for the test definitions table.
    """

   # Parameter validation
    if data_source_type != "Lakehouse":
        raise ValueError("Unrecognized data source type specified. The only valid option for now is 'Lakehouse'.")

    try:
        (target_workspace_name, target_workspace_id) = resolve_workspace_name_and_id(target_workspace)
    except:
        (target_workspace_name, target_workspace_id) = (target_workspace, None)

    if master_dataset == "" or master_dataset is None:
        (master_dataset_name, master_dataset_id) = (None, None)
        (master_workspace_name, master_workspace_id) = (None, None)
    else:
        (master_workspace_name, master_workspace_id) = resolve_workspace_name_and_id(master_workspace)
        (master_dataset_name, master_dataset_id) = resolve_dataset_name_and_id(dataset=master_dataset, workspace=master_workspace_id)

    (data_source_workspace_name, data_source_workspace_id) = resolve_workspace_name_and_id(data_source_workspace)
    (data_source_name, data_source_id) = resolve_lakehouse_name_and_id(lakehouse=data_source, workspace=data_source_workspace_id)

    spark = SparkSession.builder.getOrCreate()
    warnings.filterwarnings("ignore", category=UserWarning)

    event_schema = {
        "QueryEnd": ["TextData"],
    }

    # A table to return the test definitions in the form of a spark dataframe.
    schema = StructType([
        StructField("QueryId", StringType(), nullable=False),
        StructField("QueryText", StringType(), nullable=False),
        StructField("MasterWorkspace", StringType(), nullable=True),
        StructField("MasterDataset", StringType(), nullable=True),
        StructField("TargetWorkspace", StringType(), nullable=True),
        StructField("TargetDataset", StringType(), nullable=False),
        StructField("DatasourceName", StringType(), nullable=True),
        StructField("DatasourceWorkspace", StringType(), nullable=True),
        StructField("DatasourceType", StringType(), nullable=True),
    ])

    data = []
    test_definitions = None

    with fabric.create_trace_connection(dataset=master_dataset_id, workspace=master_workspace_id) as trace_connection:
        with trace_connection.create_trace(event_schema) as trace:
            trace.start()
            # Loop until the timeout expires or until a query with '{"Stop"}' in the query text is received.
            time_in_secs = timeout
            step_size = 1
            print(f"{icons.in_progress} Entering a trace loop for up to {time_in_secs} seconds to capture DAX queries executed against the '{master_dataset_name}' semantic model. Execute 'EVALUATE {{\"Stop\"}}' to exit the trace loop.")
            while time_in_secs > 0:
                df = trace.get_trace_logs()
                if not df.empty:
                    for _, row in df.iterrows():
                        if row['Text Data'].find("""{"Stop"}""") != -1:                            
                            time_in_secs = 0
                    
                if time_in_secs > 0:
                    time.sleep(step_size)
                    time_in_secs -= step_size
            
            df = trace.stop()
            if not df.empty:
                row_count = len(df[~df['Text Data'].str.contains('{"Stop"}')])
                print(f"{icons.green_dot} Trace loop exited. Trace stopped. {row_count} DAX queries captured.")
            else:
                print(f"{icons.yellow_dot} Trace loop exited. Trace stopped. 0 DAX queries captured.")

            i = 0
            for _, row in df.iterrows():
                if row['Text Data'].find("""{"Stop"}""") == -1:
                    i += 1
                    data.append((
                        f"Q{i}",
                        row['Text Data'].replace("\n\n", "\n"),
                        master_workspace_name,
                        master_dataset_name,
                        target_workspace_name,
                        f"{target_dataset_prefix}_{i}",
                        data_source_name,
                        data_source_workspace_name,
                        data_source_type))

    return spark.createDataFrame(data, schema=schema)

def _provision_test_models( 
    test_definitions: DataFrame,
    capacity_id: Optional[UUID] = None,
    refresh_clone: Optional[bool] = True,
    ):
    """
    Creates test models from the master models specified in the test defintions dataframe.

    Parameters
    ----------
    test_definitions : DataFrame
        A spark dataframe with the query, semantic model, and data source definitions for a test cycle.
        The test definitions dataframe must have the following columns.
        +----------+----------+----------------+-------------+---------------+-------------+---------------+-------------------+--------------+
        | QueryId|   QueryText| MasterWorkspace|MasterDataset|TargetWorkspace|TargetDataset| DatasourceName|DatasourceWorkspace|DatasourceType|
        +----------+----------+----------------+-------------+---------------+-------------+---------------+-------------------+--------------+
    capacity_id : uuid.UUID, default=None
        The ID of the capacity on which to place new workspaces.
        Defaults to None which resolves to the capacity of the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the capacity of the workspace of the notebook.
    refresh_clone : bool, default=True
        If set to True, this will initiate a full refresh of the cloned semantic model in the target workspace.

    """

    for row in test_definitions.dropDuplicates(['MasterWorkspace','MasterDataset','TargetWorkspace', 'TargetDataset']).collect():
        master_dataset = row['MasterDataset']
        master_workspace = row['MasterWorkspace']
        target_dataset = row['TargetDataset']
        target_workspace = row['TargetWorkspace']

        # Skip this row if master and target are not defined.
        if master_dataset == "" or master_dataset is None \
            or target_dataset == "" or target_dataset is None:
            continue

        # Make sure the target_workspace exists.
        try:
            (target_workspace_name, target_workspace_id) = resolve_workspace_name_and_id(target_workspace)
        except WorkspaceNotFoundException:
            try:
                (target_workspace_name, target_workspace_id) = _get_or_create_workspace(
                    workspace=target_workspace,
                    capacity_id=capacity_id,
                    description="A semantic model for query tests in a perf lab."
                )
            except FabricHTTPException as e:
                print(f"{icons.red_dot} Unable to create target workspace '{target_workspace}' for test semantic model '{target_dataset}'. {e.response.reason}")
                continue

        dfD = fabric.list_datasets(workspace=target_workspace_id, mode="rest")
        dfD_filt = dfD[dfD["Dataset Name"] == target_dataset]
        if dfD_filt.empty:
            deploy_semantic_model(
                source_dataset=master_dataset,
                source_workspace=master_workspace,
                target_dataset=target_dataset,
                target_workspace=target_workspace,
                refresh_target_dataset=refresh_clone,
                overwrite=False,
            )
        else:
            print(f"{icons.green_dot} The test semantic model '{target_dataset}' already exists in the workspace '{target_workspace_name}'.")

def _initialize_test_cycle(
    test_definitions: DataFrame,
    test_run_id: Optional[str] = None,
    test_description: Optional[str] = None
) -> DataFrame:
    """
    Generate a unique test id and timestamp for the current test cycle and adds this information to the test_definitions dataframe.

    Parameters
    ----------
    test_definitions : DataFrame
        A spark dataframe with the query, semantic model, and data source definitions for a test cycle.
        The test definitions dataframe must have the following columns.
        +----------+----------+----------------+-------------+---------------+-------------+---------------+-------------------+--------------+
        | QueryId|   QueryText| MasterWorkspace|MasterDataset|TargetWorkspace|TargetDataset| DatasourceName|DatasourceWorkspace|DatasourceType|
        +----------+----------+----------------+-------------+---------------+-------------+---------------+-------------------+--------------+
    test_run_id: str, Default = None
        An optional id for the test run to be included in the test definitions.
    test_description: str, Default = None
        An optional description to be included in the test definitions.

    Returns
    -------
    DataFrame
        A test cycle-initialized Spark dataframe containing the test definitions augmented with test cycle Id and timestamp.
        The returned dataframe includes the following columns:
        +-------+---------+----------------+-------------+---------------+-------------+--------------+-------------------+--------------+---------+----------------+
        |QueryId|QueryText| MasterWorkspace|MasterDataset|TargetWorkspace|TargetDataset|DatasourceName|DatasourceWorkspace|DatasourceType|TestRunId|TestRunTimestamp|
        --------+---------+----------------+-------------+---------------+-------------+--------------+-------------------+--------------+---------+----------------+
    """
    if test_run_id is None:
        test_run_id = generate_guid()

    return test_definitions \
        .withColumn("TestRunId", lit(test_run_id)) \
        .withColumn("TestRunTimestamp", lit(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))) \
        .withColumn("TestRunDescription", lit(test_description))

def _get_test_cycle_id(
    test_cycle_definitions: DataFrame,
) -> DataFrame:
    """
    Generate a unique test id and timestamp for the current test cycle and adds this information to the test_definitions dataframe.

    Parameters
    ----------
    test_cycle_definitions : DataFrame
        A test cycle-initialized Spark dataframe containing the test definitions augmented with test cycle Id and timestamp.
        The returned dataframe includes the following columns:
        +-------+---------+----------------+-------------+---------------+-------------+--------------+-------------------+--------------+---------+----------------+
        |QueryId|QueryText| MasterWorkspace|MasterDataset|TargetWorkspace|TargetDataset|DatasourceName|DatasourceWorkspace|DatasourceType|TestRunId|TestRunTimestamp|
        --------+---------+----------------+-------------+---------------+-------------+--------------+-------------------+--------------+---------+----------------+

    Returns
    -------
    str
        Retrieves the first value of the TestRunId column from the test_cycle_definitions dataFrame as the id of the test cycle.
     """
    if test_cycle_definitions is None:
        return None
    else:
        return test_cycle_definitions.select("TestRunId").first()[0]

def _tag_dax_queries(
    query_dict: dict,
) -> List:
    """
    Add the query name to the query text for later identification in Profiler trace events.

    Parameters
    ----------
    query_dict : dict
        A dictionary with the query name and query text records.
        The test definitions dataframe must have the following columns.
        +----------+----------+
        | QueryId|   QueryText|
        +----------+----------+

    Returns
    -------
    list
        A list of dictionaries. Each dictionary in the list contains keys like QueryId and QueryText.
        Use a simple for loop to iterate over this list and access the QueryText for each dictionary.
    """
    query_dicts = [q.asDict() for q in query_dict]
    tag = "perflabquerynametagx"
    for q in query_dicts:
        query_text = q['QueryText']
        query_name = q['QueryId'].replace('"', '""')
        match = re.search(r'^DEFINE.*$', query_text, re.MULTILINE)
        if match:
            q['QueryText'] = query_text.replace("DEFINE",
                f"DEFINE\n var {tag} = \"{query_name}\"\n", 1)
        else:
            q['QueryText'] = f"DEFINE\n var {tag} = \"{query_name}\"\n{query_text}"


    return query_dicts

def _get_query_text(
    query_dict: dict,
    query_id: str,
    default: Optional[str] = ""
) -> str:
    """
    Gets the QueryText value for a given QueryId.

    Parameters
    ----------
    query_dict : dict
        A dictionary with the query name and query text records.
        The test definitions dataframe must have the following columns.
        +----------+----------+
        | QueryId|   QueryText|
        +----------+----------+
    query_id: str
        The QueryId for which to return the QueryText.
    default: str, Default = ""
        The value to return if the QueryId was not found in the dictionary.
    Returns
    -------
    str
        The QueryText for a given QueryId or the default value.
    """
    
    for query in query_dict:
        if query['QueryId'] == query_id:
            return query['QueryText']
    return default

def _get_query_name(
    query_text: str
)->str:
    """
    Extracts the string assigned to the perflabquerynametagx variable from the DAX query text.

    Parameters
    ----------
    query_text : str
        The text containing the perflabquerynametagx assignment.

    Returns:
    str
        The extracted query name, or None if no match is found.
    """
    pattern = r'(?<=var perflabquerynametagx = ").*?(?=")'
    match = re.search(pattern, query_text)
    if match:
        return match.group()
    else:
        return None

def _trace_dax_queries(
    dataset: str,
    dax_queries: dict,
    clear_query_cache: Optional[bool] = True,
    timeout: Optional[int] = 60,
    workspace: Optional[str] = None,
) -> Tuple[DataFrame, dict]:
    """
    Runs each DAX query and returns the QueryBegin and QueryEnd trace events together with the query results.

    Parameters
    ----------
    dataset : str
        The mame of the semantic model.
    dax_queries : dict
        The dax queries to run in a dictionary format. Here is an example:
        {
            "Sales Amount Test", 'EVALUATE SUMMARIZECOLUMNS("Sales Amount", [Sales Amount])',
            "Order Quantity with Product", """ """EVALUATE SUMMARIZECOLUMNS('Product'[Color], "Order Qty", [Order Qty])""" """,
        }
    clear_query_cache : bool, Default=True
        Clear the query cache before running each query.
    timeout : int, default=60
        The max time duration to capture trace events after execution of the DAX queries.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    Tuple[DataFrame, dict]
        A Spark dataframe with the trace events for each DAX query in the dictionary.
        A dictionary of FabricDataFrames with the query results for each DAX query.
    """

    spark = SparkSession.builder.getOrCreate()
    warnings.filterwarnings("ignore", category=UserWarning)

    base_cols = ["EventClass", "EventSubclass", "CurrentTime", "NTUserName", "TextData"]
    begin_cols = base_cols + ["StartTime"]
    end_cols = base_cols + ["StartTime", "EndTime", "Duration", "CpuTime", "Success"]

    event_schema = {
        "QueryBegin": begin_cols,
        "QueryEnd": end_cols,
    }

    event_schema["DirectQueryEnd"] = ["EventClass", "TextData", "Duration", "StartTime", "EndTime", "Error"]

    event_schema["VertiPaqSEQueryBegin"] = begin_cols
    event_schema["VertiPaqSEQueryEnd"] = end_cols
    event_schema["VertiPaqSEQueryCacheMatch"] = base_cols
    event_schema["ExecutionMetrics"] = ["EventClass", "ApplicationName", "TextData"]

    query_results = {}
    last_executed_query = ""

    # Establish trace connection
    with fabric.create_trace_connection(dataset=dataset, workspace=workspace) as trace_connection:
        with trace_connection.create_trace(event_schema) as trace:
            trace.start()
            try:
                print(f"{icons.in_progress} Tracing {len(dax_queries)} dax queries...")
                
                # Loop through the DAX queries
                tagged_dax_queries = _tag_dax_queries(dax_queries)
                for query in tagged_dax_queries:
                    query_name = query['QueryId']
                    query_text = query['QueryText']

                    if clear_query_cache:
                        clear_cache(dataset=dataset, workspace=workspace)

                    fabric.evaluate_dax(
                        dataset=dataset, workspace=workspace, dax_string="EVALUATE {1}")

                    result = fabric.evaluate_dax(
                        dataset=dataset, workspace=workspace, dax_string=query_text)

                    # Add results to output
                    query_results[query_name] = result
                    print(f"{icons.green_dot} The '{query_name}' query has completed with {result.shape[0]} rows.")
                    last_executed_query = query_name

            except Exception as e:
                print(f"{icons.red_dot} {e}")

            print(f"{icons.in_progress} Gathering the trace events for this query batch.")

            # Fetch periodically until timeout is expired
            time_in_secs = timeout
            step_size = 10
            while time_in_secs > 0:
                pd_df = trace.get_trace_logs()
                if not pd_df.empty:
                    for _, row in pd_df.iterrows():
                        if row['Event Class'] == 'QueryEnd' \
                        and _get_query_name(row["Text Data"]) == last_executed_query:
                            time_in_secs = 0
                            print(f"{icons.green_dot} Trace events gathered for this query batch.")
                            break
                
                if time_in_secs > 0:
                    time.sleep(step_size)
                    time_in_secs -= step_size

            trace_df = trace.stop()
            if trace_df is None:
                return (None, query_results)
            
    # Remove all rows where 'Text Data' contains 'EVALUATE {1}'
    trace_df = trace_df[~trace_df['Text Data'].str.contains('EVALUATE {1}')]

    # Add a QueryId column with query names from the 'Text Data'
    trace_df['QueryId'] = trace_df['Text Data'].apply(_get_query_name)

    # Function to replace the query text in trace events with the clean query text from the dax_queries dict.
    def replace_text_data(row):
        if row['Event Class'].startswith('Query'):
            row['Text Data'] = _get_query_text(
                query_dict = dax_queries,
                query_id = _get_query_name(row['Text Data']),
                default = row['Text Data'])
        return row

    trace_df = trace_df.apply(replace_text_data, axis=1)


    spark_trace_df = spark.createDataFrame(trace_df)

    # Rename remove spaces from all columns
    for col in spark_trace_df.columns:
        no_spaces = col.replace(" ", "")
        spark_trace_df = spark_trace_df.withColumnRenamed(col, no_spaces)

    return(spark_trace_df, query_results)

def _queries_toDict(
    test_definitions: DataFrame
) -> dict:
    """
    Returns the QueryId and QueryText columns from a test definitons dataframe in a dictionary.

    Parameters
    ----------
    test_definitions : DataFrame
        A Spark dataframe with test definitions.

    Returns
    -------
    dict
        A dictionary with the QueryId and QueryText columns from a test definitons dataframe.
    """
   
    rows = test_definitions.select(test_definitions.columns[:2]).collect()
    return {row: row for row in rows}

def _warmup_test_models(
     test_definitions: DataFrame,
) -> None:
    """
    Generate a unique test id and timestamp for the current test cycle and adds this information to the test_definitions dataframe.

    Parameters
    ----------
    test_definitions : DataFrame
        A spark dataframe with the query and semantic model definitions for a test cycle.
        The test definitions dataframe must have the following columns.
        +---------+---------------+-------------+
        |QueryText|TargetWorkspace|TargetDataset|
        +---------+---------------+-------------+
    Returns
    -------
    None
    """
    for row in test_definitions.dropDuplicates(['TargetWorkspace', 'TargetDataset']).collect():
        target_dataset = row['TargetDataset']
        target_workspace = row['TargetWorkspace']

        # Skip this row if the target semantic model is not defined.
        if target_dataset == "" or target_dataset is None:
            print(f"{icons.red_dot} No test semantic model specifed as the target dataset. Ignoring this row. Please review your test definitions.")
            continue

        try:
            (target_workspace_name, target_workspace_id) = resolve_workspace_name_and_id(workspace=target_workspace)
        except:
            print(f"{icons.red_dot} Unable to resolve workspace '{target_workspace}' for test semantic model '{target_dataset}'. Ignoring this row. Please review your test definitions.")
            continue 

        try:
            (target_dataset_name, target_dataset_id) = resolve_dataset_name_and_id(dataset=target_dataset, workspace=target_workspace_id)
        except:
            print(f"{icons.red_dot} Unable to find the test semantic model '{target_dataset}' in workspace '{target_workspace}'. Ignoring this row. Please review your test definitions.")
            continue 

        # Filter the DataFrame and select the QueryText column
        df = test_definitions
        queries_df = df.filter((df.TargetWorkspace == target_workspace) & (df.TargetDataset == target_dataset)).select("QueryText")
        for row in queries_df.collect():
            fabric.evaluate_dax(
                dataset=target_dataset, 
                workspace=target_workspace, 
                dax_string=row.QueryText)
        print(f"{icons.green_dot} {queries_df.count()} queries executed to warm up semantic model '{target_dataset}' in workspace '{target_workspace}'.")

def _refresh_test_models(
    test_definitions: DataFrame,
    refresh_type: str = "full",
) -> None:
    """
    Generate a unique test id and timestamp for the current test cycle and adds this information to the test_definitions dataframe.

    Parameters
    ----------
    test_definitions : DataFrame
        A spark dataframe with the query and semantic model definitions for a test cycle.
        The test definitions dataframe must have the following columns.
        +---------+---------------+-------------+
        |QueryText|TargetWorkspace|TargetDataset|
        +---------+---------------+-------------+
    refresh_type : str, Default = full
        The type of processing to perform for each test semantic model.
        Types align with the TMSL refresh command types: full, clearValues, calculate, dataOnly, automatic, and defragment. 
        The add type isn't supported.
        In addition, refresh_type can be set to clearValuesFull, which performs a clearValues refresh followed by a full refresh.

    Returns
    -------
    None
    """
    for row in test_definitions.dropDuplicates(['TargetWorkspace', 'TargetDataset']).collect():
        target_dataset = row['TargetDataset']
        target_workspace = row['TargetWorkspace']

        # Skip this row if the target semantic model is not defined.
        if target_dataset == "" or target_dataset is None:
            print(f"{icons.red_dot} No test semantic model specifed as the target dataset. Ignoring this row. Please review your test definitions.")
            continue

        try:
            (target_workspace_name, target_workspace_id) = resolve_workspace_name_and_id(workspace=target_workspace)
        except:
            print(f"{icons.red_dot} Unable to resolve workspace '{target_workspace}' for test semantic model '{target_dataset}'. Ignoring this row. Please review your test definitions.")
            continue 

        try:
            (target_dataset_name, target_dataset_id) = resolve_dataset_name_and_id(dataset=target_dataset, workspace=target_workspace_id)
        except:
            print(f"{icons.red_dot} Unable to find the test semantic model '{target_dataset}' in workspace '{target_workspace}'. Ignoring this row. Please review your test definitions.")
            continue 

        if refresh_type is None:
            print(f"{icons.red_dot} Unable to refresh test definitions because no refresh type was specified.")
        elif refresh_type == "clearValuesFull":
            # The refresh type 'clearValuesFull' requires 2 refresh_semantic_model calls
            # 1. clearValues, 2. full
            refresh_semantic_model( 
                dataset=target_dataset_id, 
                workspace=target_workspace_id, 
                refresh_type="clearValues")
            refresh_semantic_model( 
                dataset=target_dataset_id, 
                workspace=target_workspace_id, 
                refresh_type="full")
        else:
            # The refresh type is supported by the refresh_semantic_model function.
            refresh_semantic_model( 
                dataset=target_dataset_id, 
                workspace=target_workspace_id, 
                refresh_type=refresh_type)

def run_test_cycle(
    test_cycle_definitions: DataFrame,
    clear_query_cache: Optional[bool] = True,
    refresh_type: Optional[str] = None,
    trace_timeout: Optional[int] = 60,
    tag: Optional[str] = None,
) -> Tuple[DataFrame, dict]:
    """
    Runs each DAX query and returns the traces events, such as QueryBegin and QueryEnd, together with the query results.

    Parameters
    ----------
    test_cycle_definitions : DataFrame
        A spark dataframe with test-cycle augmented test definitions, usually obtained by using the _initialize_test_cycle() function.
    clear_query_cache : bool, Default = True
        Clear the query cache before running each query.
    refresh_type : str, Default = None
        The type of processing to perform for each test semantic model.
        Types align with the TMSL refresh command types: full, clearValues, calculate, dataOnly, automatic, and defragment. 
        The add type isn't supported.
        In addition, refresh_type can be set to clearValuesFull, which performs a clearValues refresh followed by a full refresh.
   trace_timeout : int, default=60
        The max time duration to capture trace events after execution of the DAX queries against a test semantic model.
        The trace_timeout applies on a model-by-model basis. If the test_cycle_definitions use x models,
        the timeout is applied x times, because each test model is traced individually.
    tag : str, default=None
        A string to provide additional information about a particular test cycle, such as cold, incremental, warm, or any other meaningful tag.
        This string is added in a 'Tag' column to all trace events.

    Returns
    -------
    Tuple[DataFrame, dict]
        A Spark dataframe with the trace events for each DAX query in the dictionary.
        A dictionary of FabricDataFrames with the query results for each DAX query.
    """

    cycle_results_tuple = (None, {})
    for row in test_cycle_definitions.dropDuplicates(['TargetWorkspace', 'TargetDataset', 'TestRunId', 'TestRunTimestamp']).collect():
        target_dataset = row['TargetDataset']
        target_workspace = row['TargetWorkspace']
        test_run_id = row['TestRunId']
        test_run_timestamp = row['TestRunTimestamp']

        # Skip this row if the target semantic model is not defined.
        if target_dataset == "" or target_dataset is None:
            print(f"{icons.red_dot} The target dataset info is missing. Ignoring this row. Please review your test definitions.")
            continue

        try:
            (target_workspace_name, target_workspace_id) = resolve_workspace_name_and_id(workspace=target_workspace)
        except:
            print(f"{icons.red_dot} Unable to resolve the target workspace '{target_workspace}' for test semantic model '{target_dataset}'. Ignoring semantic model '{target_dataset}'. Please review your test definitions.")
            continue 

        try:
            (target_dataset_name, target_dataset_id) = resolve_dataset_name_and_id(dataset=target_dataset, workspace=target_workspace_id)
        except:
            print(f"{icons.red_dot} Unable to find semantic model '{target_dataset}' in workspace '{target_workspace}'. Ignoring this semantic model. Please review your test definitions.")
            continue 

        # Prior to tracing the DAX queries for this model, perform the requested refreshes. 
        if refresh_type is not None and refresh_type != "clearValuesFull":
            # The refresh type is supported by the refresh_semantic_model function.
            refresh_semantic_model( 
                dataset=target_dataset_id, 
                workspace=target_workspace_id, 
                refresh_type=refresh_type)
        elif refresh_type == "clearValuesFull":
            # The refresh type 'clearValuesFull' requires 2 refresh_semantic_model calls
            # 1. clearValues, 2. full
            refresh_semantic_model( 
                dataset=target_dataset_id, 
                workspace=target_workspace_id, 
                refresh_type="clearValues")
            refresh_semantic_model( 
                dataset=target_dataset_id, 
                workspace=target_workspace_id, 
                refresh_type="full")

        # Run all queries that use the current target semantic model,
        # and then merge the results with the overall cycle results.
        queries_df = test_cycle_definitions.filter(
            (test_cycle_definitions.TargetWorkspace == target_workspace) \
            & (test_cycle_definitions.TargetDataset == target_dataset))

        (trace_df, query_results) = _trace_dax_queries(
            dataset=target_dataset_name,
            dax_queries=_queries_toDict(queries_df),
            workspace = target_workspace_name,
            clear_query_cache=clear_query_cache,
        )

        if trace_df is not None:
            # Add test cycle info to the trace events.
            trace_df = trace_df \
                .withColumn("TestRunId", lit(test_run_id)) \
                .withColumn("TestRunTimestamp", lit(test_run_timestamp)) \
                .withColumn("DatasetId", lit(target_dataset_id)) \
                .withColumn("WorkspaceId", lit(target_workspace_id)) \
                .withColumn("Tag", lit(tag)) \
                       
            if cycle_results_tuple[0] is None:
                cycle_results_tuple = (trace_df, cycle_results_tuple[1])
            else:
                cycle_results_tuple = (cycle_results_tuple[0].union(trace_df), cycle_results_tuple[1])

        if query_results is not None:
            cycle_results_tuple = (cycle_results_tuple[0], cycle_results_tuple[1] | query_results)

    return cycle_results_tuple