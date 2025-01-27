import pandas as pd
import sempy.fabric as fabric
from sempy.fabric.exceptions import WorkspaceNotFoundException, FabricHTTPException

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit
from datetime import date, datetime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from typing import Optional, Tuple, Callable
from uuid import UUID

import sempy_labs._icons as icons
from sempy_labs import deploy_semantic_model
from sempy_labs.perf_lab import _get_or_create_workspace
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_lakehouse_name_and_id,
    resolve_dataset_name_and_id,
    generate_guid,
)

_sample_queries = [
("Total Sales (Card)", """EVALUATE
SUMMARIZE(
    Sales
    Sales[id],
    \"Total Sales\", SUM(Sales[Sales])
)"""),
# ----------------------------
("Sales Over Time (Line Chart)", """EVALUATE
SUMMARIZE(
    date,
    date[Date],
    \"Sales Over Time\", CALCULATE(SUM(Sales[Sales]))
)"""),
# ----------------------------
("Sales by Product Category (Column Chart)", """EVALUATE
SUMMARIZE(
    productcategory,
    productcategory[ProductCategory],
    \"Sales by Product Category\", CALCULATE(SUM(Sales[Sales]))
)"""),
# ----------------------------
("Sales by Location (Map)", """EVALUATE
SUMMARIZE(
    geo,
    geo[StateOrTerritory],
    \"Sales by Location\", CALCULATE(SUM(Sales[Sales]))
)"""),
# ----------------------------
("Total Profit (Card)", """EVALUATE
SUMMARIZE(
    measuregroup,
    measuregroup[Profit],
    \"Total Profit\", SUM(measuregroup[Profit])
)"""),
# ----------------------------
("Profit Over Time (Line Chart)", """EVALUATE
SUMMARIZE(
    date,
    date[Date],
    \"Profit Over Time\", CALCULATE(SUM(measuregroup[Profit]))
)"""),
# ----------------------------
("Profit by Product Category (Column Chart)", """EVALUATE
SUMMARIZE(
    productcategory,
    productcategory[ProductCategory],
    \"Profit by Product Category\", CALCULATE(SUM(measuregroup[Profit]))
)"""),
# ----------------------------
("Profit by Location (Map)", """EVALUATE
SUMMARIZE(
    geo,
    geo[StateOrTerritory],
    \"Profit by Location\", CALCULATE(SUM(measuregroup[Profit]))
)"""),
# ----------------------------
("Monthly Sales Trends (Line Chart)", """EVALUATE
SUMMARIZE(
    date,
    date[MonthYear],
    \"Monthly Sales Trends\", CALCULATE(SUM(Sales[Sales]))
)"""),
# ----------------------------
("Sales by Month (Column Chart)", """EVALUATE
SUMMARIZE(
    date,
    date[Month],
    \"Sales by Month\", CALCULATE(SUM(Sales[Sales]))
)"""),
# ----------------------------
("Sales by Year (Line Chart)", """EVALUATE
SUMMARIZE(
    date,
    date[Year],
    \"Sales by Year\", CALCULATE(SUM(Sales[Sales]))
)"""),
# ----------------------------
("Monthly Profit Trends (Line Chart)", """EVALUATE
SUMMARIZE(
    date,
    date[MonthYear],
    \"Monthly Profit Trends\", CALCULATE(SUM(measuregroup[Profit]))
)"""),
# ----------------------------
("Profit by Month (Column Chart)", """EVALUATE
SUMMARIZE(
    date,
    date[Month],
    \"Profit by Month\", CALCULATE(SUM(measuregroup[Profit]))
)"""),
# ----------------------------
("Profit by Year (Line Chart)", """EVALUATE
SUMMARIZE(
    date,
    date[Year],
    \"Profit by Year\", CALCULATE(SUM(measuregroup[Profit]))
)"""),
# ----------------------------
("Tamed Beast Query (Skip 'Sales'[id])", """EVALUATE
  SUMMARIZECOLUMNS(
//    'Sales'[id],
    'Sales'[ProductCategoryID],
    'Sales'[GeoID],
    'Sales'[DateID],
    \"Total Sales\", SUM('Sales'[Sales]),
    \"Total Costs\", SUM('Sales'[Costs])
  )"""),
# ----------------------------
("Run out of Memory ('Sales'[id])", """EVALUATE
  SUMMARIZECOLUMNS(
    'Sales'[id],
    'Sales'[ProductCategoryID],
    'Sales'[GeoID],
    'Sales'[DateID],
    \"Total Sales\", SUM('Sales'[Sales]),
    \"Total Costs\", SUM('Sales'[Costs])
  )"""),
# ----------------------------
("Period Comparison", """DEFINE

MEASURE Sales[Sum of Quantity] = 

SUM(Sales[Sales])

MEASURE Sales[Sum of Quantity PM] =

CALCULATE([Sum of Quantity],PREVIOUSMONTH('date'[Date]))

MEASURE Sales[Sum of Quantity PM Delta] =

[Sum of Quantity] - [Sum of Quantity PM] 

MEASURE Sales[Sum of Quantity PM %] =

[Sum of Quantity PM Delta] / [Sum of Quantity]


EVALUATE
SUMMARIZECOLUMNS(
'date'[Monthly] ,
TREATAS({DATE(2023,1,1),DATE(2023,2,1),DATE(2023,3,1)} , 'Date'[Monthly] ) ,
\"Sales\" , [Sum of Quantity],
\"Sales PM\" ,  [Sum of Quantity PM],
\"Sales PM Delta\", [Sum of Quantity PM Delta] ,
\"Sales PM % \" , [Sum of Quantity PM %]
)

ORDER BY [Monthly]"""),
# ----------------------------
("Running Total", """
DEFINE

MEASURE Sales[Sum of Sales] =  SUM(Sales[sales])
MEASURE Sales[Sum of Sales YTD] = TOTALYTD([Sum of Sales],'date'[Date])
MEASURE Sales[Sum of Sales QTD] = TOTALQTD([Sum of Sales],'date'[Date]) 

EVALUATE
SUMMARIZECOLUMNS(
    'date'[Monthly] ,
    TREATAS({DATE(2023,1,1)} , 'date'[Monthly] ) ,
    \"Sales\" , [Sum of Sales],
    \"Sales YTD\" , [Sum of Sales YTD] ,
    \"Sales QTD\" , [Sum of Sales QTD]
)
ORDER BY 
    [Monthly] """),
# ----------------------------
    ]


def _get_test_definitions_df(
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
        A Spark dataframe for the measures table.
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

def _provision_test_models( 
    test_definitions: DataFrame,
    capacity_id: Optional[UUID] = None,
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
                refresh_target_dataset=False,
                overwrite=False,
            )
        else:
            print(f"{icons.green_dot} The test semantic model '{target_dataset}' already exists in the workspace '{target_workspace_name}'.")

def _initialize_test_cycle(
    test_definitions: DataFrame,
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

    Returns
    -------
    DataFrame
        A test cycle-initialized Spark dataframe containing the test definitions augmented with test cycle Id and timestamp.
        The returned dataframe includes the following columns:
        +-------+---------+----------------+-------------+---------------+-------------+--------------+-------------------+--------------+---------+----------------+
        |QueryId|QueryText| MasterWorkspace|MasterDataset|TargetWorkspace|TargetDataset|DatasourceName|DatasourceWorkspace|DatasourceType|TestRunId|TestRunTimestamp|
        --------+---------+----------------+-------------+---------------+-------------+--------------+-------------------+--------------+---------+----------------+
    """

    return test_definitions \
        .withColumn("TestRunId", lit(generate_guid())) \
        .withColumn("TestRunTimestamp", lit(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))