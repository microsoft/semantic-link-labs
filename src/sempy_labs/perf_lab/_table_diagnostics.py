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
from sempy_labs.perf_lab import _refresh_test_models

from sempy_labs.lakehouse import get_lakehouse_tables
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_dataset_name_and_id,
)

def get_storage_table_column_segments(
    test_cycle_definitions: DataFrame,
    tables_info: DataFrame,
    refresh_type: str = "full",
)->DataFrame:
    """
    Queries the INFO.STORAGETABLECOLUMNSEGMENTS DAX function for all model tables in the tables_info dataframe.

    Parameters
    ----------
    test_cycle_definitions : DataFrame
        A spark dataframe with test-cycle augmented test definitions, usually obtained by using the _initialize_test_cycle() function.
    tables_info : DataFrame
        A Spark dataframe with information about the model tables and source tables, usually obtained by using the get_source_tables() function.
    refresh_type : str, Default = full
        The type of processing to perform for each test semantic model before gathering column segment data.
        Types align with the TMSL refresh command types: full, clearValues, calculate, dataOnly, automatic, and defragment. 
        The add type isn't supported.
        In addition, refresh_type can be set to clearValuesFull, which performs a clearValues refresh followed by a full refresh.

    Returns
    -------
    DataFrame
        A Spark dataframe containing the data retrieved from the INFO.STORAGETABLECOLUMNSEGMENTS DAX function.
    """   
    spark = SparkSession.builder.getOrCreate()

    if refresh_type is not None:
        _refresh_test_models(
            test_definitions = test_cycle_definitions,
            refresh_type = refresh_type,
        )

    # Initialize an empty DataFrame
    results_df = None
    for row in test_cycle_definitions.dropDuplicates(['TargetWorkspace', 'TargetDataset']).collect():
        target_dataset = row['TargetDataset']
        target_workspace = row['TargetWorkspace']

        # Skip this row if the target semantic model is not defined.
        if target_dataset == "" or target_dataset is None:
            print(f"{icons.red_dot} The target dataset info is missing. Ignoring this row. Please review your test definitions.")
            continue

        try:
            (target_workspace_name, target_workspace_id) = resolve_workspace_name_and_id(workspace=target_workspace)
        except:
            print(f"{icons.red_dot} Unable to resolve the target workspace '{target_workspace}' for test semantic model '{target_dataset}'. Ignoring this row. Please review your test definitions.")
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
            # Format the dax query, but include only the tables that are interesting for the current model.
            model_table_rows = tables_info.where(
                (tables_info['ModelName'] == target_dataset) & (tables_info['ModelWorkspace'] == target_workspace))
            dax_query = """
            EVALUATE FILTER(INFO.STORAGETABLECOLUMNSEGMENTS(), 
            """
            or_str = ""
            for t in model_table_rows.dropDuplicates(['ModelTableName']).collect():
                dax_query += f"""{or_str} left([TABLE_ID], {len(t['ModelTableName']) + 2}) = \"{t['ModelTableName']} (\"
                """
                or_str = "||"

            dax_query += ")"

            dax_fdf = fabric.evaluate_dax(
                workspace=target_workspace_name, 
                dataset=target_dataset_name, 
                dax_string=dax_query)
            
            # Add some more information from the test_cycle_definitions
            dax_df = spark.createDataFrame(dax_fdf)
            dax_df = dax_df.withColumn("[WORKSPACE_NAME]", lit(target_workspace_id)) \
                .withColumn("[WORKSPACE_ID]", lit(target_dataset_name)) \
                .withColumn("[DATASET_NAME]", lit(target_dataset_name)) \
                .withColumn("[DATASET_ID]", lit(target_dataset_id)) \
                .withColumn("[TESTRUNID]", lit(row['TestRunId'])) \
                .withColumn("[TESTRUNTIMESTAMP]", lit(row['TestRunTimestamp']))

            # If results_df is None, initialize it with the first DataFrame
            if results_df is None:
                results_df = dax_df
            else:
                # Merge the DataFrame with the existing merged DataFrame
                results_df = results_df.union(dax_df)
    
    return results_df