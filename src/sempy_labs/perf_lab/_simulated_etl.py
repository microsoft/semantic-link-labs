import pandas as pd
import sempy.fabric as fabric
from sempy_labs._list_functions import list_tables

from pyspark.sql import DataFrame, SparkSession, Row
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import Column, col, min, max, when, lit
from datetime import datetime, timedelta

import sempy_labs._icons as icons
from typing import Optional, Callable, Tuple
from uuid import UUID
from sempy_labs.tom import connect_semantic_model

from sempy_labs.lakehouse import get_lakehouse_tables
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_dataset_name_and_id,
)

FilterCallback = Callable[[str, str, dict], bool]
def _filter_by_prefix(
    table_name: str, 
    source_table_name: str, 
    filter_properties: Optional[dict] = None
) -> bool:
    """
    Returns a boolean to indicate if the table info should be included in the source table list (True) or not (False).

    Parameters
    ----------
    table_name : str
        The name of the table in a semantic model.
    table_name : str
        The name of the table in a data source.
    filter_properties: dict, default=None
        An arbirary dictionary of key/value pairs that the provision_perf_lab_lakehouse function passes to the table_generator function.
        The _filter_by_prefix sample function expects to find a 'Prefix' key in the filter_properties.

    Returns
    -------
    bool
        Indicates if the table should be included (True) or ignored (False).
    """

    if filter_properties is None:
        return False

    return source_table_name.startswith(
        filter_properties["Prefix"])

def get_source_tables(
    test_definitions: DataFrame,
    filter_properties: Optional[dict] = None,
    filter_function: Optional[FilterCallback] = None
) -> DataFrame:
    """
    Returns a Spark dataframe with information about the source tables that the test semantic models use.

    Parameters
    ----------
    test_definitions : DataFrame
        A spark dataframe with test definitions for a test cycle.
        The test definitions dataframe must have the following columns.
        +----------+----------+----------------+-------------+---------------+-------------+---------------+-------------------+--------------+
        | QueryId|   QueryText| MasterWorkspace|MasterDataset|TargetWorkspace|TargetDataset| DatasourceName|DatasourceWorkspace|DatasourceType|
        +----------+----------+----------------+-------------+---------------+-------------+---------------+-------------------+--------------+
    filter_properties: dict, default=None
        A dictionary of key/value pairs that the _get_source_tables() function passes to the filter_function function.
        The key/value pairs in the dictionary are specific to the filter_function function passed into the _get_source_tables() function.        
    filter_function
        A callback function to which source Delta tables to include in the dataframe returned to the caller.

    Returns
    -------
    DataFrame
        A Spark dataframe containing the filtered source Delta tables.
        The returned dataframe includes the following columns:
        +----------+--------------------+--------------+---------------+----------------+-------------------+---------------+------------+----------+--------------+
        | ModelName|      ModelWorkspace|ModelTableName| DatasourceName|  DatasourceType|DatasourceWorkspace|SourceTableName|SourceFormat|SourceType|SourceLocation|
        +----------+--------------------+--------------+---------------+----------------+-------------------+---------------+------------+----------+--------------+
    """

    spark = SparkSession.builder \
        .appName("PerfLabApp") \
        .getOrCreate()

    # A table to return the source tables in a Spark dataframe.
    schema = StructType([
        StructField("ModelName", StringType(), nullable=False),
        StructField("ModelWorkspace", StringType(), nullable=False),
        StructField("ModelTableName", StringType(), nullable=False),
        StructField("DatasourceName", StringType(), nullable=False),
        StructField("DatasourceWorkspace", StringType(), nullable=False),
        StructField("DatasourceType", StringType(), nullable=False),
        StructField("SourceTableName", StringType(), nullable=False),
        StructField("SourceFormat", StringType(), nullable=False),
        StructField("SourceType", StringType(), nullable=False),
        StructField("SourceLocation", StringType(), nullable=False),
    ])
    rows = []

    for row in test_definitions.dropDuplicates(['TargetWorkspace', 'TargetDataset','DatasourceName','DatasourceWorkspace','DatasourceType']).collect():
        target_dataset = row['TargetDataset']
        target_workspace = row['TargetWorkspace']
        data_source_name = row['DatasourceName']
        data_source_workspace = row['DatasourceWorkspace']
        data_source_type = row['DatasourceType']

        # Skip this row if the data_source_type is invalid.
        if not data_source_type == "Lakehouse":
            print(f"{icons.red_dot} Invalid data source type '{data_source_type}' detected. Ignoring this row. Please review your test definitions.")
            continue 

        # Skip this row if the target semantic model is not defined.
        if target_dataset == "" or target_dataset is None:
            print(f"{icons.red_dot} No test semantic model specifed as the target dataset. Ignoring this row. Please review your test definitions.")
            continue

        # Skip this row if the data_source_name is not defined.
        if data_source_name == "" or data_source_name is None:
            print(f"{icons.red_dot} No data source found for test semantic model '{target_dataset}'. Ignoring this row. Please review your test definitions.")
            continue

        try:
            (target_workspace_name, target_workspace_id) = resolve_workspace_name_and_id(workspace=target_workspace)
        except:
            print(f"{icons.red_dot} Unable to resolve workspace '{target_workspace}' for test semantic model '{target_dataset}'. Ignoring this row. Please review your test definitions.")
            continue 

        try:
            (target_dataset_name, target_dataset_id) = resolve_dataset_name_and_id(dataset=target_dataset, workspace=target_workspace_id)
        except:
            (target_dataset_name, target_dataset_id) = (target_dataset, None)

        dfD = fabric.list_datasets(workspace=target_workspace_name, mode="rest")
        dfD_filt = dfD[dfD["Dataset Name"] == target_dataset_name]
        if dfD_filt.empty:
            print(f"{icons.red_dot} Unable to find test semantic model '{target_dataset_name}'. Please review your test definitions and make sure all test semantic models are provisioned.")
            continue
        else:
            ltbls_df = get_lakehouse_tables(lakehouse=data_source_name, workspace=data_source_workspace)
            with connect_semantic_model(dataset=target_dataset_name, workspace=target_workspace_id, readonly=True) as tom:
                for t in tom.model.Tables:
                    for p in t.Partitions:
                        try:
                            table_name = t.get_Name()
                            source_table_name = p.Source.EntityName

                            st_df = ltbls_df[ltbls_df["Table Name"] == source_table_name]
                            if not st_df.empty:
                                if filter_function is None or filter_function(table_name, source_table_name, filter_properties) == True:
                                    
                                    # Get the first row for the source tables filtered by source table name.
                                    record = st_df.iloc

                                    rows.append((
                                        target_dataset_name,
                                        target_workspace_name,
                                        table_name,
                                        data_source_name,
                                        data_source_workspace,
                                        data_source_type,
                                        source_table_name,
                                        record[0]["Format"],
                                        record[0]["Type"],
                                        record[0]["Location"],
                                    ))                               
                            else:
                                print(f"{icons.red_dot} Delta table '{source_table_name}' not found in data source {data_source_type} '{data_source_name}' in workspace '{data_source_workspace}'.")
                        except:
                            continue                   

    return spark.createDataFrame(rows, schema=schema).dropDuplicates()

def _get_min_max_keys(
    table_name: str,
    table_path: str,
    key_column: Optional[str] = "DIM_DateId"
) -> Tuple[str,str]:
    """
    Gets the min and max values for a specified column from a Spark dataframe.

    Parameters
    ----------
    table_name : str
        The name if a Delta table from which to delete the rows.
    table_path : str
        The full path to the Delta table to read its data.
    key_column : str
        The name of column for which to return the min and max values.

    Returns
    -------
    Tuple[object,object]
        A tuple of the min and max values for the specified column.
    """
    spark = SparkSession.builder.getOrCreate()
    table_df = spark.read.format("delta").load(table_path)

    if key_column in table_df.columns:
        # Get the min and max value from the specified table
        minKey = table_df.agg(min(key_column)).collect()[0][0]
        maxKey = table_df.agg(max(key_column)).collect()[0][0]
        return (minKey,maxKey)

    print(f"{icons.red_dot} The specified key column '{key_column}' was not found in {table_df.columns} of Delta table '{table_name}', path '{table_path}'.\r\nNote that column names are case senstive.")

    return None

def _delete_rows(
    table_name: str,
    table_path: str,
    key_value: str,
    key_column: Optional[str] = "DIM_DateId"
) -> DataFrame:
    """
    Deletes and returns the rows from a Delta table that have a given value in a given column.

    Parameters
    ----------
    table_name : str
        The name if a Delta table from which to delete the rows.
    table_path : str
        The full path to the Delta table to read its data.
    key_value : str
        The filter value for which to filter and delete the rows.
    key_column : str, Default = "DIM_DateId"
        The name of the column for which to filter and delete the rows.

    Returns
    -------
    DataFrame
        A Spark DataFrame with the deleted rows.
    """    
    spark = SparkSession.builder.getOrCreate()
    table_df = spark.read.format("delta").load(table_path)

    rows_df = table_df[table_df[key_column] == key_value]
    rows_df = spark.createDataFrame(rows_df.rdd, schema=rows_df.schema)

    table_df.createOrReplaceTempView(f"{table_name}_view")
    try:
        strDelete = f"DELETE From {table_name}_view WHERE {key_column} = '{key_value}'"
        spark.sql(strDelete)
        print(f"{icons.checked} {rows_df.count()} rows with {key_column} = '{key_value}' removed from table '{table_name}'.")
    except:
        print(f"")
        print(f"{icons.red_dot} Unable to delete the oldest rows from Delta table '{table_name}', path '{table_path}'.")

    return rows_df

def _insert_rows(
        data_df: DataFrame,
        table_name: str,
        table_path: str,
) -> None:
    """
    Inserts the rows from a Spark Dataframe into the specified Delta table.

    Parameters
    ----------
    data_df : DataFrame
        A Spark dataframe with the data to be inserted into the specified Delta table.
    table_name : str
        The name if a Delta table from which to delete the rows.
    table_path : str
        The full path to the Delta table to read its data.

    Returns
    -------
    None
     """
    data_df.write.format("delta").mode("append").save(table_path)
    print(f"{icons.checked} {data_df.count()} rows inserted into table {table_name}, path '{table_path}'.")

    return None

def _update_rows(
    table_name: str,
    table_path: str,
    old_value: str,
    new_value: str,
    column_name: str
) -> DataFrame:
    """
    Updates the rows in a Delta table that match the old value in the specified column.

    Parameters
    ----------
    table_name : str
        The name if a Delta table from which to delete the rows.
    table_path : str
        The full path to the Delta table to read its data.
    old_value : str
        The old value to match in the specified column.
    new_value : str
        The new value in the specified column for the matching rows.
    column_name : str
        The name of the column to filter the rows and update the value.

    Returns
    -------
    DataFrame
        A Spark DataFrame with the updated rows.
    """ 
    return _update_delta_table(
        table_name,
        table_path,
        col(column_name) == old_value,
        new_value,
        column_name,
        )

def _update_delta_table(
    table_name: str,
    table_path: str,
    condition: Column,
    new_value: str,
    column_name: str
) -> DataFrame:
    """
    Updates the rows from a Delta table that match the specified condition expression.

    Parameters
    ----------
    table_name : str
        The name if a Delta table from which to delete the rows.
    table_path : str
        The full path to the Delta table to read its data.
    condition : Column
        The column expression to determine which rows to update in the Delta table, using various functions like col, when, lit, and others from the pyspark.sql.functions module.
    new_value : str
        The new value in the specified column for the matching rows.
    column_name : str
        The name of the column to update.

    Returns
    -------
    DataFrame
        A Spark DataFrame with the updated rows.
    """  

    spark = SparkSession.builder.getOrCreate()
    try:
        # Load the Delta table
        delta_table = DeltaTable.forPath(spark, table_path)

        # Update the column based on the condition
        delta_table.update(
            condition,
            {column_name: when(condition, new_value).otherwise(col(column_name))}
        )

        # Load the updated rows as a DataFrame
        rows_df = spark.read.format("delta").load(table_path) \
            .filter(col(column_name) == new_value)

        print(f"{icons.checked} {rows_df.count()} rows updated in table {table_name}.")
        return rows_df
    
    except Exception as e:
        print(f"{icons.red_dot} Unable to delete the oldest rows from Delta table '{table_name}', path '{table_path}' because an error occurred: {e}.")
        return None
    
def _sliding_window_update(
    table_name: str,
    table_path: str,
    old_value: str,
    new_value: str,
    key_column: str,
    compress: Optional[bool] = True,
) -> DataFrame:
    """
    Deletes the rows in a Delta table that match the old value in the specified column,
    then optionally compats the Delta table, and then reinserts the old rows with the old value replaced by new value.
    This is very similar to a normal update operation, with the exception that 
    this method optionally optimizes the Delta table between deletes and inserts.
    Parameters
    ----------
    table_name : str
        The name if a Delta table from which to delete the rows.
    table_path : str
        The full path to the Delta table to read its data.
    old_value : str
        The old value to match in the specified column.
    new_value : str
        The new value in the specified column for the matching rows.
    key_column : str
        The name of the column to filter the rows to delete and insert.
    compress : bool, Default = True
        A flag that indicates if the Delta table should be optimized between deletes and inserts.
    Returns
    -------
    DataFrame
        A Spark DataFrame with the reinserted rows.
    """ 

    deleted_rows = _delete_rows(
        table_name,
        table_path,
        old_value,
        key_column,
        )
    
    if compress:
        # SPARK optimize the table
        spark = SparkSession.builder.getOrCreate()
        delta_table = DeltaTable.forPath(spark, table_path)
        delta_table.optimize().executeCompaction()
        print(f"{icons.checked} Delta table {table_name} optimized.")

    updated_rows = deleted_rows.withColumn(key_column, lit(new_value))
    _insert_rows(
        updated_rows,
        table_name,
        table_path,
        )

    return updated_rows

UpdateTableCallback = Callable[[Row, dict], None]
def _delete_reinsert_rows(
    source_table_info: Row,
    custom_properties: Optional[dict] = None
) -> None:
    """
    Deletes and reinserts rows in a Delta table and optimizes the Delta table between deletes and inserts.
    Parameters
    ----------
    source_table_info: Row
        A Spark DataFrame row with the following columns:
        +---------------+----------------+-------------------+---------------+--------------+
        | DatasourceName|  DatasourceType|DatasourceWorkspace|SourceTableName|SourceLocation|
        +---------------+----------------+-------------------+---------------+--------------+
     custom_properties: dict, default=None
        A dictionary of key/value pairs specific to the callback function.

    Returns
    -------
    None
    """

    key_column = custom_properties["key_column"]
    optimize_table = custom_properties["Optimize"]

    minmax = _get_min_max_keys(
        source_table_info['SourceTableName'], 
        source_table_info['SourceLocation'], 
        key_column)

    if not minmax == None:
         # Calculate the date id for the next (max+1) day for the new rows.
        new_date = datetime.strptime(str(minmax[1]), "%Y%m%d") + timedelta(days=1)     
        _sliding_window_update(
                table_name=source_table_info['SourceTableName'],
                table_path=source_table_info['SourceLocation'],
                old_value= minmax[0],
                new_value=int(new_date.strftime("%Y%m%d")),
                key_column=key_column,
                compress = optimize_table,
                )

    return None

def simulate_etl(
    source_tables_info: DataFrame,
    update_properties: Optional[dict] = None,
    update_function: Optional[UpdateTableCallback] = None
) -> None:
    """
    Simulates an ETL process by rolling the tables in the tables_df forward by using the specified update_function.

    Parameters
    ----------
    source_tables_info : DataFrame
        A spark dataframe with information about the model tables and source tables, usually obtained by using the get_source_tables() function.
    update_properties: dict, Default=None
        A dictionary of key/value pairs that the simulate_etl() function passes to the update_function callback.
        The key/value pairs in the dictionary are specific to the update_function implementation.
    update_function: UpdateTableCallback, Default=None
        A callback function to process each source table.

    Returns
    -------
    None
    """
    if not update_function is None:
        for row in source_tables_info.dropDuplicates(["SourceLocation", "DatasourceName", "DatasourceType", "DatasourceWorkspace", "SourceTableName"]).collect():
            update_function(row, update_properties)
    else:
            raise ValueError("Unable to process tables without an UpdateTableCallback. Please set the update_function parameter.")
    
    print(f"{icons.green_dot} ETL processing completed.")
    return None