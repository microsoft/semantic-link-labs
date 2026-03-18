import sempy.fabric as fabric
import pandas as pd
from IPython.display import display, HTML
import zipfile
import os
import uuid
import shutil
import datetime
from sempy_labs._helper_functions import (
    format_dax_object_name,
    save_as_delta_table,
    resolve_workspace_capacity,
    _get_column_aggregate,
    resolve_workspace_name_and_id,
    resolve_dataset_name_and_id,
)
from sempy_labs.lakehouse._get_lakehouse_tables import get_lakehouse_tables
from typing import Optional, Literal
from sempy._utils._log import log
import sempy_labs._icons as icons
from pathlib import Path
from uuid import UUID
from sempy_labs.directlake._sources import get_direct_lake_sources
from sempy_labs.lakehouse._schemas import is_schema_enabled


def get_run_id(lakehouse, schema, workspace, save_table_name):
    has_schema = is_schema_enabled(lakehouse=lakehouse, workspace=workspace)
    tables = get_lakehouse_tables(lakehouse=lakehouse, workspace=workspace)
    if schema and not has_schema:
        raise ValueError(
            f"{icons.red_dot} A schema (export_schema) was provided but the lakehouse does not have schemas enabled."
        )
    if not schema and not has_schema:
        tables = tables[(tables["Table Name"] == save_table_name)]
    elif schema and has_schema:
        tables = tables[
            (tables["Schema Name"] == schema)
            & (tables["Table Name"] == save_table_name)
        ]
    else:
        tables = tables[
            (tables["Schema Name"] == "dbo") & (tables["Table Name"] == save_table_name)
        ]

    if tables.empty:
        run_id = 1
    else:
        run_id = _get_column_aggregate(
            table_name=save_table_name,
            lakehouse=lakehouse,
            workspace=workspace,
            schema_name=schema,
        )

    return run_id


def create_sql_query(columns, schema_name, table_name):

    distinct_counts = ", ".join([f"COUNT(DISTINCT {col}) AS {col}" for col in columns])

    return f"""
    SELECT {distinct_counts}
    FROM {schema_name}.{table_name}
    """


# Calculate missing rows
def calc_missing_rows_dax(
    from_table: str,
    from_column: str,
    to_table: str,
    to_column: str,
    is_active: bool,
    dataset_id: UUID,
    workspace_id: UUID,
):

    from_object = format_dax_object_name(from_table, from_column)
    to_object = format_dax_object_name(to_table, to_column)

    query = f"evaluate\nsummarizecolumns(\n\"1\",calculate(countrows('{from_table}'),isblank({to_object}))\n)"

    if not is_active:
        query = f"evaluate\nsummarizecolumns(\n\"1\",calculate(countrows('{from_table}'),userelationship({from_object},{to_object}),isblank({to_object}))\n)"

    missing_rows = 0
    result = fabric.evaluate_dax(
        dataset=dataset_id, dax_string=query, workspace=workspace_id
    )
    if not result.empty:
        missing_rows = result.iloc[0, 0]

    return missing_rows


# Converting to KB/MB/GB necessitates division by 1024 * 1000.
def format_bytes(size, decimals=2):
    """
    Convert bytes to a human-readable format (KB, MB, GB, etc.)

    :param size: Size in bytes
    :param decimals: Number of decimal places
    :return: Formatted string
    """
    if size == 0:
        return "0 Bytes"

    units = ["Bytes", "KB", "MB", "GB", "TB", "PB", "EB"]
    i = 0

    while size >= 1024 and i < len(units) - 1:
        size /= 1024
        i += 1

    return f"{round(size, decimals)} {units[i]}"


def cast_to_type(value, type_):
    type_mapping = {
        "int": int,
        "decimal": float,
        "bool": lambda v: str(v).strip().lower()
        == "true",  # convert "True"/"False" strings,
        "timestamp": lambda v: pd.to_datetime(
            v,
            format="%Y-%m-%d %H:%M:%S",
            errors="coerce",
            utc=True,
        )
    }

    # Handle null / empty values
    if pd.isna(value) or value in ("nan", "<NA>", None, ""):
        if type_ == "bool":
            return False  # default for bool
        elif type_ == "decimal":
            return 0.0
        else:
            return 0

    return type_mapping[type_](value)


@log
def vertipaq_analyzer(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    export: Optional[Literal["table"]] = None,
    read_stats_from_data: bool = False,
    export_lakehouse: Optional[str | UUID] = None,
    export_workspace: Optional[str | UUID] = None,
    export_schema: Optional[str] = None,
) -> dict[str, pd.DataFrame]:
    """
    Displays an HTML visualization of the `Vertipaq Analyzer <https://www.sqlbi.com/tools/vertipaq-analyzer/>`_ statistics from a semantic model.

    `Vertipaq Analyzer <https://www.sqlbi.com/tools/vertipaq-analyzer/>`_ is an open-sourced tool built by SQLBI. It provides a detailed analysis of the VertiPaq engine, which is the in-memory engine used by Power BI and Analysis Services Tabular models.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str| uuid.UUID, default=None
        The Fabric workspace name or ID in which the semantic model exists.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    export : typing.Literal['table'], default=None
        If set to 'table', the vertipaq analyzer statistics will be exported as delta tables to the lakehouse. The tables will be named vertipaqanalyzer_model, vertipaqanalyzer_table, vertipaqanalyzer_partition, vertipaqanalyzer_column, vertipaqanalyzer_relationship, and vertipaqanalyzer_hierarchy. If None, the statistics will just be displayed in the notebook.
    read_stats_from_data : bool, default=False
        Setting this parameter to True has the function get Column Cardinality and Missing Rows using DAX (Direct Lake semantic models achieve by querying the delta tables). Missing Rows is not calculated for Direct Lake models.
    export_lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID to which the vertipaq analyzer statistics tables will be exported if export is set to 'table'.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    export_workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse to which the vertipaq analyzer statistics tables will be exported if export is set to 'table'.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    export_schema : str, default=None
        The schema to which the vertipaq analyzer statistics tables will be exported if export is set to 'table' and the lakehouse has schemas enabled. If the lakehouse does not have schemas enabled, this parameter will be ignored.

    Returns
    -------
    dict[str, pandas.DataFrame]
        A dictionary of pandas dataframes showing the vertipaq analyzer statistics.
    """

    from sempy_labs.tom import connect_semantic_model

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)
    save_prefix = "vertipaqanalyzer_"
    save_table_name = f"{save_prefix}model"

    if export == "table":
        run_id = get_run_id(
            lakehouse=export_lakehouse,
            schema=export_schema,
            workspace=export_workspace,
            save_table_name=save_table_name,
        )

    vertipaq_map = {
        "Model": {
            "Dataset Name": {
                "data_type": icons.data_type_string,
                "format": icons.no_format,
                "tooltip": "The name of the semantic model",
            },
            "Total Size": {
                "data_type": icons.data_type_long,
                "format": icons.int_format,
                "tooltip": "The size of the semantic model (in bytes)",
            },
            "Table Count": {
                "data_type": icons.data_type_long,
                "format": icons.int_format,
                "tooltip": "The number of tables in the semantic model",
            },
            "Column Count": {
                "data_type": icons.data_type_long,
                "format": icons.int_format,
                "tooltip": "The number of columns in the semantic model",
            },
            "Compatibility Level": {
                "data_type": icons.data_type_long,
                "format": icons.no_format,
                "tooltip": "The compatibility level of the semantic model",
            },
            "Default Mode": {
                "data_type": icons.data_type_string,
                "format": icons.no_format,
                "tooltip": "The default query mode of the semantic model",
            },
        },
        "Tables": {
            "Table Name": {
                "data_type": icons.data_type_string,
                "format": icons.no_format,
                "tooltip": "The name of the table",
            },
            "Type": {
                "data_type": icons.data_type_string,
                "format": icons.no_format,
                "tooltip": "The type of table",
            },
            "Row Count": {
                "data_type": icons.data_type_long,
                "format": icons.int_format,
                "tooltip": "The number of rows in the table",
            },
            "Total Size": {
                "data_type": icons.data_type_long,
                "format": icons.int_format,
                "tooltip": "Data Size + Dictionary Size + Hierarchy Size (in bytes)",
            },
            "Data Size": {
                "data_type": icons.data_type_long,
                "format": icons.int_format,
                "tooltip": "The size of the data for all the columns in this table (in bytes)",
            },
            "Dictionary Size": {
                "data_type": icons.data_type_long,
                "format": icons.int_format,
                "tooltip": "The size of the column's dictionary for all columns in this table (in bytes)",
            },
            "Hierarchy Size": {
                "data_type": icons.data_type_long,
                "format": icons.int_format,
                "tooltip": "The size of hierarchy structures for all columns in this table (in bytes)",
            },
            "Relationship Size": {
                "data_type": icons.data_type_long,
                "format": icons.int_format,
                "tooltip": "The size of relationship structures for this table (in bytes)",
            },
            "User Hierarchy Size": {
                "data_type": icons.data_type_long,
                "format": icons.int_format,
                "tooltip": "The size of user hierarchy structures for this table (in bytes)",
            },
            "Partitions": {
                "data_type": icons.data_type_long,
                "format": icons.int_format,
                "tooltip": "The number of partitions in the table",
            },
            "Columns": {
                "data_type": icons.data_type_long,
                "format": icons.int_format,
                "tooltip": "The number of columns in the table",
            },
            "% DB": {
                "data_type": icons.data_type_double,
                "format": icons.pct_format,
                "tooltip": "The size of the table relative to the size of the semantic model",
            },
        },
        "Partitions": {
            "Table Name": {
                "data_type": icons.data_type_string,
                "format": icons.no_format,
                "tooltip": "The name of the table",
            },
            "Partition Name": {
                "data_type": icons.data_type_string,
                "format": icons.no_format,
                "tooltip": "The name of the partition within the table",
            },
            "Mode": {
                "data_type": icons.data_type_string,
                "format": icons.no_format,
                "tooltip": "The query mode of the partition",
            },
            "Record Count": {
                "data_type": icons.data_type_long,
                "format": icons.int_format,
                "tooltip": "The number of rows in the partition",
            },
            "Segment Count": {
                "data_type": icons.data_type_long,
                "format": icons.int_format,
                "tooltip": "The number of segments within the partition",
            },
            "Records per Segment": {
                "data_type": icons.data_type_double,
                "format": icons.int_format,
                "tooltip": "The number of rows per segment",
            },
            "Direct Lake Type": {
                "data_type": icons.data_type_string,
                "format": icons.no_format,
                "tooltip": "The type of Direct Lake connection (SQL or OneLake)",
            },
            "Source Name": {
                "data_type": icons.data_type_string,
                "format": icons.no_format,
                "tooltip": "The name of the data source for the partition",
            },
            "Source Type": {
                "data_type": icons.data_type_string,
                "format": icons.no_format,
                "tooltip": "The type of data source for the partition",
            },
            "Source Workspace": {
                "data_type": icons.data_type_string,
                "format": icons.no_format,
                "tooltip": "The workspace of the data source for the partition",
            },
            "Source Schema Name": {
                "data_type": icons.data_type_string,
                "format": icons.no_format,
                "tooltip": "The schema name in the source for the partition",
            },
            "Source Table Name": {
                "data_type": icons.data_type_string,
                "format": icons.no_format,
                "tooltip": "The name of the table in the source for the partition",
            },
        },
        "Columns": {
            "Table Name": {
                "data_type": icons.data_type_string,
                "format": icons.no_format,
                "tooltip": "The name of the table",
            },
            "Column Name": {
                "data_type": icons.data_type_string,
                "format": icons.no_format,
                "tooltip": "The name of the column",
            },
            "Source Column": {
                "data_type": icons.data_type_string,
                "format": icons.no_format,
                "tooltip": "The name of the source column",
            },
            "Type": {
                "data_type": icons.data_type_string,
                "format": icons.no_format,
                "tooltip": "The type of column",
            },
            "Cardinality": {
                "data_type": icons.data_type_long,
                "format": icons.int_format,
                "tooltip": "The number of unique rows in the column",
            },
            "Total Size": {
                "data_type": icons.data_type_long,
                "format": icons.int_format,
                "tooltip": "Data Size + Dictionary Size + Hierarchy Size (in bytes)",
            },
            "Data Size": {
                "data_type": icons.data_type_long,
                "format": icons.int_format,
                "tooltip": "The size of the data for the column (in bytes)",
            },
            "Dictionary Size": {
                "data_type": icons.data_type_long,
                "format": icons.int_format,
                "tooltip": "The size of the column's dictionary (in bytes)",
            },
            "Hierarchy Size": {
                "data_type": icons.data_type_long,
                "format": icons.int_format,
                "tooltip": "The size of hierarchy structures (in bytes)",
            },
            "% Table": {
                "data_type": icons.data_type_double,
                "format": icons.pct_format,
                "tooltip": "The size of the column relative to the size of the table",
            },
            "% DB": {
                "data_type": icons.data_type_double,
                "format": icons.pct_format,
                "tooltip": "The size of the column relative to the size of the semantic model",
            },
            "Data Type": {
                "data_type": icons.data_type_string,
                "format": icons.no_format,
                "tooltip": "The data type of the column",
            },
            "Encoding": {
                "data_type": icons.data_type_string,
                "format": icons.no_format,
                "tooltip": "The encoding type for the column",
            },
            "Is Resident": {
                "data_type": icons.data_type_bool,
                "format": icons.no_format,
                "tooltip": "Indicates whether the column is in memory or not",
            },
            "Temperature": {
                "data_type": icons.data_type_double,
                "format": icons.data_type_double,
                "tooltip": "A decimal indicating the frequency and recency of queries against the column",
            },
            "Last Accessed": {
                "data_type": icons.data_type_timestamp,  # icons.data_type_timestamp,
                "format": icons.data_type_timestamp,
                "tooltip": "The time the column was last queried",
            },
        },
        "Hierarchies": {
            "Table Name": {
                "data_type": icons.data_type_string,
                "format": icons.no_format,
                "tooltip": "The name of the table",
            },
            "Hierarchy Name": {
                "data_type": icons.data_type_string,
                "format": icons.no_format,
                "tooltip": "The name of the hierarchy",
            },
            "Used Size": {
                "data_type": icons.data_type_long,
                "format": icons.int_format,
                "tooltip": "The size of user hierarchy structures (in bytes)",
            },
            "Levels": {
                "data_type": icons.data_type_long,
                "format": icons.int_format,
                "tooltip": "The number of levels in the hierarchy",
            },
        },
        "Relationships": {
            "From Object": {
                "data_type": icons.data_type_string,
                "format": icons.no_format,
                "tooltip": "The from table/column in the relationship",
            },
            "To Object": {
                "data_type": icons.data_type_string,
                "format": icons.no_format,
                "tooltip": "The to table/column in the relationship",
            },
            "Multiplicity": {
                "data_type": icons.data_type_string,
                "format": icons.no_format,
                "tooltip": "The cardinality on each side of the relationship",
            },
            "Used Size": {
                "data_type": icons.data_type_long,
                "format": icons.int_format,
                "tooltip": "The size of the relationship (in bytes)",
            },
            "Max From Cardinality": {
                "data_type": icons.data_type_long,
                "format": icons.int_format,
                "tooltip": "The number of unique values in the column used in the from side of the relationship",
            },
            "Max To Cardinality": {
                "data_type": icons.data_type_long,
                "format": icons.int_format,
                "tooltip": "The number of unique values in the column used in the to side of the relationship",
            },
            "Missing Rows": {
                "data_type": icons.data_type_long,
                "format": icons.int_format,
                "tooltip": "The number of rows in the 'from' table which do not map to the key column in the 'to' table",
            },
        },
    }

    sources = get_direct_lake_sources(dataset=dataset_id, workspace=workspace_id)
    is_direct_lake = False
    with connect_semantic_model(
        dataset=dataset_id, workspace=workspace_id, readonly=True
    ) as tom:
        compat_level = tom.model.Model.Database.CompatibilityLevel
        is_direct_lake = tom.is_direct_lake()
        def_mode = str(tom.model.DefaultMode)
        table_count = tom.model.Tables.Count
        column_count = len(list(tom.all_columns()))
        if table_count == 0:
            print(
                f"{icons.warning} The '{dataset_name}' semantic model within the '{workspace_name}' workspace has no tables. Vertipaq Analyzer can only be run if the semantic model has tables."
            )
            return

        tom.set_vertipaq_annotations()

        model_summary = []
        tables = []
        partitions = []
        columns = []
        hierarchies = []
        relationships = []

        for p in tom.all_partitions():
            mode = str(p.Mode)
            direct_lake_type = None
            (
                source,
                source_type,
                source_workspace,
                source_table_name,
                source_schema_name,
            ) = (
                None,
                None,
                None,
                None,
                None,
            )
            if mode == "DirectLake":
                expr = p.Source.ExpressionSource.Expression
                expression_name = p.Source.ExpressionSource.Name
                if "Sql.Database" in expr:
                    direct_lake_type = "SQL"
                elif "AzureStorage.DataLake" in expr:
                    direct_lake_type = "OneLake"
                s = next(
                    s for s in sources if s.get("ExpressionName") == expression_name
                )
                source = s.get("itemName")
                source_type = s.get("itemType")
                source_workspace = s.get("workspaceName")
                source_table_name = p.Source.EntityName
                source_schema_name = p.Source.SchemaName

            partitions.append(
                {
                    "Table Name": p.Parent.Name,
                    "Partition Name": p.Name,
                    "Mode": mode,
                    "Record Count": cast_to_type(
                        tom.get_annotation_value(object=p, name="Vertipaq_RecordCount"),
                        "int",
                    ),
                    "Segment Count": cast_to_type(
                        tom.get_annotation_value(
                            object=p, name="Vertipaq_SegmentCount"
                        ),
                        "int",
                    ),
                    "Records per Segment": cast_to_type(
                        tom.get_annotation_value(
                            object=p, name="Vertipaq_RecordsPerSegment"
                        ),
                        "decimal",
                    ),
                    "Direct Lake Type": direct_lake_type,
                    "Source Name": source,
                    "Source Type": source_type,
                    "Source Workspace": source_workspace,
                    "Source Schema Name": source_schema_name,
                    "Source Table Name": source_table_name,
                }
            )

        for h in tom.all_hierarchies():
            hierarchies.append(
                {
                    "Table Name": h.Parent.Name,
                    "Hierarchy Name": h.Name,
                    "Used Size": cast_to_type(
                        tom.get_annotation_value(object=h, name="Vertipaq_UsedSize"),
                        "decimal",
                    ),
                    "Levels": h.Levels.Count,
                }
            )

        for r in tom.model.Relationships:
            from_object = f"'{r.FromTable.Name}'[{r.FromColumn.Name}]"
            to_object = f"'{r.ToTable.Name}'[{r.ToColumn.Name}]"
            missing_rows = 0

            # Use DAX to calculate missing rows for non-DL models; TODO: update this to include DL models
            if read_stats_from_data and not is_direct_lake:
                missing_rows = calc_missing_rows_dax(
                    from_table=r.FromTable.Name,
                    from_column=r.FromColumn.Name,
                    to_table=r.ToTable.Name,
                    to_column=r.ToColumn.Name,
                    is_active=r.IsActive,
                    dataset_id=dataset_id,
                    workspace_id=workspace_id,
                )

            relationships.append(
                {
                    "From Object": from_object,
                    "To Object": to_object,
                    "Multiplicity": tom.get_annotation_value(
                        object=r, name="Vertipaq_Multiplicity"
                    ),
                    "Used Size": cast_to_type(
                        tom.get_annotation_value(object=r, name="Vertipaq_UsedSize"),
                        "decimal",
                    ),
                    "Max From Cardinality": 0,  # Updated later
                    "Max To Cardinality": 0,  # Updated later
                    "Missing Rows": cast_to_type(missing_rows, "int"),
                }
            )

        for t in tom.model.Tables:
            table_total_size = cast_to_type(
                tom.get_annotation_value(object=t, name="Vertipaq_TotalSize"), "decimal"
            )
            table_type = (
                "Calculation Group"
                if t.CalculationGroup
                else (
                    "Calculated Table"
                    if tom.is_calculated_table(table_name=t.Name)
                    else "Table"
                )
            )
            table_direct_lake = (
                True
                if next(str(p.Mode) for p in t.Partitions) == "DirectLake"
                else False
            )
            tables.append(
                {
                    "Table Name": t.Name,
                    "Type": table_type,
                    "Row Count": cast_to_type(
                        tom.get_annotation_value(object=t, name="Vertipaq_RowCount"),
                        "int",
                    ),
                    "Total Size": table_total_size,
                    "Dictionary Size": cast_to_type(
                        tom.get_annotation_value(
                            object=t, name="Vertipaq_DictionarySize"
                        ),
                        "decimal",
                    ),
                    "Data Size": cast_to_type(
                        tom.get_annotation_value(object=t, name="Vertipaq_DataSize"),
                        "decimal",
                    ),
                    "Hierarchy Size": cast_to_type(
                        tom.get_annotation_value(
                            object=t, name="Vertipaq_HierarchySize"
                        ),
                        "decimal",
                    ),
                    "Relationship Size": cast_to_type(
                        tom.get_annotation_value(
                            object=t, name="Vertipaq_RelationshipSize"
                        ),
                        "decimal",
                    ),
                    "User Hierarchy Size": cast_to_type(
                        tom.get_annotation_value(
                            object=t, name="Vertipaq_UserHierarchySize"
                        ),
                        "decimal",
                    ),
                    "Partitions": t.Partitions.Count,
                    "Columns": t.Columns.Count
                    - 1,  # Subtracting 1 to exclude the RowNumber column
                    "% DB": cast_to_type(
                        tom.get_annotation_value(object=t, name="Vertipaq_%DB"),
                        "decimal",
                    ),
                }
            )
            for c in t.Columns:
                column_total_size = cast_to_type(
                    tom.get_annotation_value(object=c, name="Vertipaq_TotalSize"),
                    "decimal",
                )
                last_accessed = tom.get_annotation_value(
                    object=c, name="Vertipaq_LastAccessed"
                )
                columns.append(
                    {
                        "Table Name": c.Parent.Name,
                        "Column Name": c.Name,
                        "Source Column": (
                            c.SourceColumn if str(c.Type) == "Data" else None
                        ),
                        "Type": str(c.Type),
                        "Cardinality": cast_to_type(
                            tom.get_annotation_value(
                                object=c, name="Vertipaq_Cardinality"
                            ),
                            "int",
                        ),
                        "Total Size": column_total_size,
                        "Data Size": cast_to_type(
                            tom.get_annotation_value(
                                object=c, name="Vertipaq_DataSize"
                            ),
                            "decimal",
                        ),
                        "Dictionary Size": cast_to_type(
                            tom.get_annotation_value(
                                object=c, name="Vertipaq_DictionarySize"
                            ),
                            "decimal",
                        ),
                        "Hierarchy Size": cast_to_type(
                            tom.get_annotation_value(
                                object=c, name="Vertipaq_HierarchySize"
                            ),
                            "decimal",
                        ),
                        "% Table": (
                            round(column_total_size / table_total_size * 100, 2)
                            if table_total_size > 0
                            else 0.0
                        ),
                        "% DB": 0.0,  # Updated later
                        "Data Type": str(c.DataType),
                        "Encoding": str(c.EncodingHint),
                        "Is Resident": cast_to_type(
                            tom.get_annotation_value(
                                object=c, name="Vertipaq_IsResident"
                            ),
                            "bool",
                        ),
                        "Temperature": cast_to_type(
                            tom.get_annotation_value(
                                object=c, name="Vertipaq_Temperature"
                            ),
                            "decimal",
                        ),
                        "Last Accessed": last_accessed,
                    }
                )

        # Set totals
        total_db = sum([t["Total Size"] for t in tables])
        for c in columns:
            c["% DB"] = (
                round(c["Total Size"] / total_db * 100, 2) if total_db > 0 else 0.0
            )

        # Capture cardinality for Direct Lake
        if read_stats_from_data and table_direct_lake:
            partitions_by_table = {p["Table Name"]: p for p in partitions}

            for t in tom.model.Tables:

                p = next(iter(t.Partitions), None)
                if not p or str(p.Mode) != "DirectLake":
                    continue

                par = partitions_by_table.get(t.Name)
                if not par:
                    continue

                entity_name = par.get("Source Table Name")
                schema_name = par.get("Source Schema Name")
                source_name = par.get("Source Name")
                source_type = par.get("Source Type")
                source_workspace = par.get("Source Workspace")
                column_list = [
                    {
                        "ColumnName": c.Name,
                        "SourceColumn": c.SourceColumn,
                        "Cardinality": 0,  # Updated later
                    }
                    for c in t.Columns
                    if str(c.Type) != "RowNumber"
                ]
                source_column_list = [c["SourceColumn"] for c in column_list]
                # Only valid for lakehouse sources
                if source_type == "Lakehouse":
                    aggs = _get_column_aggregate(
                        table_name=entity_name,
                        column_name=source_column_list,
                        lakehouse=source_name,
                        workspace=source_workspace,
                        function="distinct",
                        schema_name=schema_name,
                    )
                elif source_type == "Warehouse":
                    from sempy_labs._sql import ConnectBase

                    query = create_sql_query(
                        columns=source_column_list,
                        schema_name=schema_name,
                        table_name=entity_name,
                    )

                    with ConnectBase(
                        item=source_name, type=source_type, workspace=source_workspace
                    ) as sql:
                        df = sql.query(query)

                    aggs = df.iloc[0].to_dict()
                else:
                    continue

                for col in columns:
                    if col["Table Name"] == t.Name:
                        col["Cardinality"] = aggs.get(
                            col["Source Column"], col["Cardinality"]
                        )

        for r in relationships:
            r["Max From Cardinality"] = next(
                (
                    c["Cardinality"]
                    for c in columns
                    if f"'{c['Table Name']}'[{c['Column Name']}]" == r["From Object"]
                ),
                0,
            )
            r["Max To Cardinality"] = next(
                (
                    c["Cardinality"]
                    for c in columns
                    if f"'{c['Table Name']}'[{c['Column Name']}]" == r["To Object"]
                ),
                0,
            )

        model_summary.append(
            {
                "Dataset Name": dataset_name,
                "Total Size": total_db,
                "Table Count": table_count,
                "Column Count": column_count,
                "Compatibility Level": compat_level,
                "Default Mode": def_mode,
            }
        )

    # Convert lists to dataframes and sort as needed
    config = {
        "Model": {
            "data": model_summary,
            "sortby": None,
            "title": "Model Summary",
            "name": "Model",
        },
        "Tables": {
            "data": tables,
            "sortby": "Total Size",
            "title": "Tables",
            "name": "Table",
        },
        "Partitions": {
            "data": partitions,
            "sortby": "Record Count",
            "title": "Partitions",
            "name": "Partition",
        },
        "Relationships": {
            "data": relationships,
            "sortby": "Used Size",
            "title": "Relationships",
            "name": "Relationship",
        },
        "Hierarchies": {
            "data": hierarchies,
            "sortby": "Used Size",
            "title": "Hierarchies",
            "name": "Hierarchy",
        },
        "Columns": {
            "data": columns,
            "sortby": "Total Size",
            "title": "Columns",
            "name": "Column",
        },
    }

    def _style_columns_based_on_types(df: pd.DataFrame, column_type_mapping):
        format_funcs = {
            "int": lambda x: f"{int(x):,}" if pd.notna(x) and x != "<NA>" else "",
            "pct": lambda x: f"{float(x):.2f}%" if pd.notna(x) and x != "<NA>" else "",
            "double": lambda x: f"{float(x):.4f}" if pd.notna(x) and x != "<NA>" else "",
            "": lambda x: f"{x}",
        }

        for col, dt in column_type_mapping.items():
            if dt in format_funcs and col in df.columns:
                df[col] = df[col].map(format_funcs[dt])

        return df

    def create_dfs(column_formatting: str = "format"):
        dfs = {}
        for name, items in config.items():
            data = items.get("data")
            sort_col = items.get("sortby")
            nm = items.get("name")
            df = pd.DataFrame(data, columns=list(vertipaq_map[name].keys()))

            if sort_col and sort_col in df.columns:
                df = df.sort_values(sort_col, ascending=False, ignore_index=True)

            if name == "Columns":
                df = df[df["Type"] != "RowNumber"]
                keys_to_remove = ["Source Column"]
                df.drop(columns=keys_to_remove, inplace=True)
                for k in keys_to_remove:
                    vertipaq_map[name].pop(k, None)
            if name == "Partitions":
                keys_to_remove = [
                    "Direct Lake Type",
                    "Source Name",
                    "Source Type",
                    "Source Workspace",
                    "Source Schema Name",
                    "Source Table Name",
                ]
                if not is_direct_lake:
                    df.drop(
                        columns=keys_to_remove,
                        inplace=True,
                    )

                    for k in keys_to_remove:
                        vertipaq_map[name].pop(k, None)
            if name == "Relationships" and not read_stats_from_data:
                keys_to_remove = ["Missing Rows"]
                df.drop(columns=keys_to_remove, inplace=True)
                for k in keys_to_remove:
                    vertipaq_map[name].pop(k, None)

            column_type_mapping = {
                k: v[column_formatting] for k, v in vertipaq_map[name].items()
            }
            dataframe = _style_columns_based_on_types(df, column_type_mapping)
            dfs[name] = {
                "data": dataframe,
                "title": items.get("title"),
                "name": nm,
            }
        return dfs

    if export is None:
        dfs = create_dfs(column_formatting="format")
        default_sort = {
            items["title"]: items["sortby"]
            for items in config.values()
            if items.get("sortby")
        }
        visualize_vertipaq(dfs, dataset_name, vertipaq_map, default_sort=default_sort)
        return {
            "Model Summary": dfs["Model"]["data"],
            "Tables": dfs["Tables"]["data"],
            "Partitions": dfs["Partitions"]["data"],
            "Columns": dfs["Columns"]["data"],
            "Relationships": dfs["Relationships"]["data"],
            "Hierarchies": dfs["Hierarchies"]["data"],
        }

    # Export vertipaq to delta tables in lakehouse
    if export == "table":
        dfs = create_dfs(column_formatting="data_type")

        print(
            f"{icons.in_progress} Saving Vertipaq Analyzer to delta tables in the lakehouse...\n"
        )

        now = datetime.datetime.now()

        # Dataset metadata
        df_datasets = fabric.list_datasets(workspace=workspace_id, mode="rest")
        configured_by = df_datasets.loc[
            df_datasets["Dataset Id"] == dataset_id, "Configured By"
        ].iloc[0]

        (capacity_id, capacity_name) = resolve_workspace_capacity(
            workspace=workspace_id
        )

        base_metadata = {
            "Capacity Name": capacity_name,
            "Capacity Id": capacity_id,
            "Workspace Name": workspace_name,
            "Workspace Id": workspace_id,
            "Dataset Name": dataset_name,
            "Dataset Id": dataset_id,
            "Configured By": configured_by,
            "RunId": run_id,
            "Timestamp": now,
        }

        df_map = {
            k: dfs[k]["data"]
            for k in [
                "Columns",
                "Tables",
                "Partitions",
                "Relationships",
                "Hierarchies",
                "Model",
            ]
        }

        ordered_prefix = [
            "Capacity Name",
            "Capacity Id",
            "Workspace Name",
            "Workspace Id",
            "Dataset Name",
            "Dataset Id",
            "Configured By",
        ]

        for obj, df in df_map.items():

            # Add metadata columns
            df = df.assign(**base_metadata)

            # Reorder columns
            df = df[ordered_prefix + [c for c in df.columns if c not in ordered_prefix]]

            # Normalize column names
            df.columns = df.columns.str.replace(" ", "_")

            # Build schema
            schema = {
                "Capacity_Name": icons.data_type_string,
                "Capacity_Id": icons.data_type_string,
                "Workspace_Name": icons.data_type_string,
                "Workspace_Id": icons.data_type_string,
                "Dataset_Name": icons.data_type_string,
                "Dataset_Id": icons.data_type_string,
                "Configured_By": icons.data_type_string,
                **{
                    k.replace(" ", "_"): v["data_type"]
                    for k, v in vertipaq_map[obj].items()
                },
                "RunId": icons.data_type_long,
                "Timestamp": icons.data_type_timestamp,
            }

            delta_table_suffix = f"{save_prefix}{obj}".lower()
            if export_schema:
                delta_table_name = f"{export_schema}/{delta_table_suffix}"
            else:
                delta_table_name = delta_table_suffix

            # Needed to convert from string back to datetime (since the annotation is stored as a string)
            if obj == "Columns":
                df["Last_Accessed"] = pd.to_datetime(
                    df["Last_Accessed"],
                    format="%Y-%m-%d %H:%M:%S",
                    errors="coerce",
                    utc=True,
                )
            save_as_delta_table(
                dataframe=df,
                delta_table_name=delta_table_name,
                write_mode="append",
                schema=schema,
                merge_schema=True,
                lakehouse=export_lakehouse,
                workspace=export_workspace,
            )


def visualize_vertipaq(dataframes, dataset_name, vertipaq_map=None, default_sort=None):

    # Build tooltip lookup from vertipaq_map
    tooltip_lookup = {}
    if vertipaq_map:
        view_name_map = {
            "Model": "Model",
            "Tables": "Table",
            "Partitions": "Partition",
            "Columns": "Column",
            "Hierarchies": "Hierarchy",
            "Relationships": "Relationship",
        }
        for section, cols in vertipaq_map.items():
            vw = view_name_map.get(section, section)
            for col_name, col_info in cols.items():
                tt = col_info.get("tooltip", "")
                if tt:
                    tooltip_lookup[(vw, col_name)] = tt

    # Model summary cards (shown above tabs)
    model_df = dataframes["Model"]["data"]

    # define the dictionary with {"Tab name":df}
    df_dict = {
        "Tables": (dataframes["Tables"]["data"], "Table"),
        "Partitions": (dataframes["Partitions"]["data"], "Partition"),
        "Columns": (dataframes["Columns"]["data"], "Column"),
        "Relationships": (dataframes["Relationships"]["data"], "Relationship"),
        "Hierarchies": (dataframes["Hierarchies"]["data"], "Hierarchy"),
    }

    uid = uuid.uuid4().hex[:8]

    # ── CSS ──────────────────────────────────────────────────────────────
    styles = f"""
    <style>
    .vpx-{uid} {{
        --vpx-accent: #0071e3;
        --vpx-accent-hover: #0077ED;
        --vpx-bg: #ffffff;
        --vpx-bg-secondary: #f5f5f7;
        --vpx-bg-tertiary: #fbfbfd;
        --vpx-border: rgba(0, 0, 0, 0.06);
        --vpx-border-strong: rgba(0, 0, 0, 0.12);
        --vpx-text: #1d1d1f;
        --vpx-text-secondary: #6e6e73;
        --vpx-text-tertiary: #86868b;
        --vpx-shadow-sm: 0 1px 3px rgba(0,0,0,0.04), 0 1px 2px rgba(0,0,0,0.06);
        --vpx-shadow-md: 0 4px 14px rgba(0,0,0,0.08), 0 2px 6px rgba(0,0,0,0.04);
        --vpx-shadow-lg: 0 12px 40px rgba(0,0,0,0.12), 0 4px 12px rgba(0,0,0,0.06);
        --vpx-radius: 12px;
        --vpx-radius-sm: 8px;
        --vpx-transition: 0.25s cubic-bezier(0.4, 0, 0.2, 1);
        font-family: -apple-system, BlinkMacSystemFont, "SF Pro Display", "SF Pro Text", "Helvetica Neue", Helvetica, Arial, sans-serif;
        color: var(--vpx-text);
        -webkit-font-smoothing: antialiased;
        -moz-osx-font-smoothing: grayscale;
        max-width: 100%;
        margin: 0;
        padding: 0;
    }}
    .vpx-{uid} *, .vpx-{uid} *::before, .vpx-{uid} *::after {{
        box-sizing: border-box;
    }}
    /* ── Container ── */
    .vpx-{uid} .vpx-container {{
        background: var(--vpx-bg);
        border-radius: var(--vpx-radius);
        box-shadow: var(--vpx-shadow-lg);
        overflow: hidden;
        border: 1px solid var(--vpx-border);
    }}
    /* ── Header ── */
    .vpx-{uid} .vpx-header {{
        padding: 20px 24px 0 24px;
        background: var(--vpx-bg);
    }}
    .vpx-{uid} .vpx-title {{
        font-size: 22px;
        font-weight: 700;
        letter-spacing: -0.02em;
        color: var(--vpx-text);
        margin: 0 0 16px 0;
        line-height: 1.2;
    }}
    /* ── Model Summary Cards ── */
    .vpx-{uid} .vpx-cards {{
        display: flex;
        gap: 12px;
        padding: 0 24px 16px 24px;
        flex-wrap: wrap;
    }}
    .vpx-{uid} .vpx-card {{
        flex: 1 1 0;
        min-width: 120px;
        background: var(--vpx-bg-secondary);
        border: 1px solid var(--vpx-border);
        border-radius: var(--vpx-radius-sm);
        padding: 14px 16px;
        display: flex;
        flex-direction: column;
        gap: 4px;
    }}
    .vpx-{uid} .vpx-card-label {{
        font-size: 11px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.04em;
        color: var(--vpx-text-tertiary);
    }}
    .vpx-{uid} .vpx-card-value {{
        font-size: 20px;
        font-weight: 700;
        letter-spacing: -0.02em;
        color: var(--vpx-text);
        font-variant-numeric: tabular-nums;
    }}
    /* ── Tab Navigation ── */
    .vpx-{uid} .vpx-tab-bar {{
        display: flex;
        gap: 2px;
        padding: 0 24px;
        background: var(--vpx-bg);
        border-bottom: 1px solid var(--vpx-border);
        overflow-x: auto;
        scrollbar-width: none;
        -ms-overflow-style: none;
    }}
    .vpx-{uid} .vpx-tab-bar::-webkit-scrollbar {{
        display: none;
    }}
    .vpx-{uid} .vpx-tab-btn {{
        position: relative;
        padding: 10px 16px;
        font-size: 13px;
        font-weight: 500;
        color: var(--vpx-text-secondary);
        background: none;
        border: none;
        border-radius: 0;
        cursor: pointer;
        transition: color var(--vpx-transition);
        white-space: nowrap;
        letter-spacing: -0.01em;
        outline: none;
        font-family: inherit;
        display: inline-flex;
        align-items: center;
        gap: 6px;
    }}
    .vpx-{uid} .vpx-tab-btn .vpx-tab-icon {{
        width: 14px;
        height: 14px;
        flex-shrink: 0;
    }}
    .vpx-{uid} .vpx-tab-btn::after {{
        content: '';
        position: absolute;
        bottom: -1px;
        left: 0;
        right: 0;
        height: 2px;
        background: var(--vpx-accent);
        border-radius: 2px 2px 0 0;
        transform: scaleX(0);
        transition: transform var(--vpx-transition);
    }}
    .vpx-{uid} .vpx-tab-btn:hover {{
        color: var(--vpx-text);
    }}
    .vpx-{uid} .vpx-tab-btn.vpx-active {{
        color: var(--vpx-accent);
        font-weight: 600;
    }}
    .vpx-{uid} .vpx-tab-btn.vpx-active::after {{
        transform: scaleX(1);
    }}
    /* ── Toolbar ── */
    .vpx-{uid} .vpx-toolbar {{
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 12px 24px;
        background: var(--vpx-bg-tertiary);
        border-bottom: 1px solid var(--vpx-border);
    }}
    .vpx-{uid} .vpx-search-wrap {{
        position: relative;
        width: 260px;
    }}
    .vpx-{uid} .vpx-search-icon {{
        position: absolute;
        left: 10px;
        top: 50%;
        transform: translateY(-50%);
        width: 14px;
        height: 14px;
        color: var(--vpx-text-tertiary);
        pointer-events: none;
    }}
    .vpx-{uid} .vpx-search {{
        width: 100%;
        padding: 7px 12px 7px 32px;
        font-size: 13px;
        font-family: inherit;
        background: var(--vpx-bg);
        border: 1px solid var(--vpx-border-strong);
        border-radius: var(--vpx-radius-sm);
        color: var(--vpx-text);
        outline: none;
        transition: border-color var(--vpx-transition), box-shadow var(--vpx-transition);
    }}
    .vpx-{uid} .vpx-search::placeholder {{
        color: var(--vpx-text-tertiary);
    }}
    .vpx-{uid} .vpx-search:focus {{
        border-color: var(--vpx-accent);
        box-shadow: 0 0 0 3px rgba(0, 113, 227, 0.15);
    }}
    .vpx-{uid} .vpx-row-count {{
        font-size: 12px;
        font-weight: 500;
        color: var(--vpx-text-tertiary);
        letter-spacing: -0.01em;
    }}
    .vpx-{uid} .vpx-row-count span {{
        font-variant-numeric: tabular-nums;
    }}
    /* ── Toolbar controls ── */
    .vpx-{uid} .vpx-toolbar-controls {{
        display: flex;
        align-items: center;
        gap: 12px;
    }}
    .vpx-{uid} .vpx-bar-toggle {{
        display: inline-flex;
        align-items: center;
        gap: 5px;
        padding: 4px 10px;
        font-size: 11px;
        font-weight: 500;
        font-family: inherit;
        color: var(--vpx-text-secondary);
        background: var(--vpx-bg);
        border: 1px solid var(--vpx-border-strong);
        border-radius: 6px;
        cursor: pointer;
        transition: color var(--vpx-transition), border-color var(--vpx-transition);
        white-space: nowrap;
    }}
    .vpx-{uid} .vpx-bar-toggle:hover {{
        color: var(--vpx-text);
        border-color: var(--vpx-text-tertiary);
    }}
    .vpx-{uid} .vpx-bar-toggle.vpx-bars-active {{
        color: var(--vpx-accent);
        border-color: var(--vpx-accent);
    }}
    .vpx-{uid} .vpx-bar-toggle .vpx-toggle-icon {{
        width: 12px;
        height: 12px;
        flex-shrink: 0;
    }}

    /* ── Tab Content ── */
    .vpx-{uid} .vpx-panel {{
        display: none;
        animation: vpxFadeIn{uid} 0.3s ease;
    }}
    .vpx-{uid} .vpx-panel.vpx-visible {{
        display: block;
    }}
    @keyframes vpxFadeIn{uid} {{
        from {{ opacity: 0; transform: translateY(4px); }}
        to {{ opacity: 1; transform: translateY(0); }}
    }}
    /* ── Table ── */
    .vpx-{uid} .vpx-table-wrap {{
        overflow-x: auto;
        overflow-y: auto;
        max-height: 520px;
    }}
    .vpx-{uid} table {{
        width: max-content;
        min-width: 100%;
        border-collapse: separate;
        border-spacing: 0;
        font-size: 13px;
        line-height: 1.4;
    }}
    .vpx-{uid} thead {{
        position: sticky;
        top: 0;
        z-index: 2;
    }}
    .vpx-{uid} thead th {{
        padding: 10px 20px 10px 16px;
        text-align: left;
        font-weight: 600;
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.04em;
        color: var(--vpx-text-secondary);
        background: var(--vpx-bg-secondary);
        border-bottom: 1px solid var(--vpx-border-strong);
        cursor: pointer;
        user-select: none;
        white-space: nowrap;
        position: relative;
        overflow: visible;
        min-width: fit-content;
        transition: color var(--vpx-transition), background var(--vpx-transition);
    }}
    .vpx-{uid} thead th:hover {{
        color: var(--vpx-text);
        background: #ececee;
    }}
    .vpx-{uid} thead th .vpx-sort-arrow {{
        display: none;
        margin-left: 4px;
        font-size: 10px;
        transition: opacity var(--vpx-transition);
    }}
    .vpx-{uid} thead th.vpx-sort-asc .vpx-sort-arrow,
    .vpx-{uid} thead th.vpx-sort-desc .vpx-sort-arrow {{
        display: inline-block;
        opacity: 1;
        color: var(--vpx-accent);
    }}
    .vpx-{uid} tbody td {{
        padding: 9px 16px;
        border-bottom: 1px solid var(--vpx-border);
        color: var(--vpx-text);
        white-space: nowrap;
        text-align: left;
        transition: background var(--vpx-transition);
    }}
    .vpx-{uid} tbody td.vpx-bar-cell {{
        position: relative;
        overflow: hidden;
    }}
    .vpx-{uid} tbody td.vpx-bar-cell .vpx-bar {{
        position: absolute;
        left: 0;
        top: 0;
        bottom: 0;
        background: rgba(0, 113, 227, 0.08);
        border-right: 2px solid rgba(0, 113, 227, 0.25);
        pointer-events: none;
    }}
    .vpx-{uid} tbody td.vpx-bar-cell .vpx-bar-value {{
        position: relative;
        z-index: 1;
    }}
    .vpx-{uid} .vpx-bars-off .vpx-bar {{
        display: none;
    }}
    .vpx-{uid} tbody tr {{
        transition: background var(--vpx-transition);
    }}
    .vpx-{uid} tbody tr:nth-child(even) {{
        background: var(--vpx-bg-tertiary);
    }}
    .vpx-{uid} tbody tr:hover {{
        background: rgba(0, 113, 227, 0.04);
    }}
    .vpx-{uid} tbody tr:hover td {{
        color: var(--vpx-text);
    }}
    .vpx-{uid} tbody td.vpx-numeric {{
        font-variant-numeric: tabular-nums;
        text-align: left;
        font-feature-settings: "tnum";
    }}
    .vpx-{uid} thead th.vpx-numeric {{
        text-align: left;
    }}
    /* ── Resize handle ── */
    .vpx-{uid} thead th .vpx-resize-handle {{
        position: absolute;
        right: 0;
        top: 0;
        bottom: 0;
        width: 5px;
        cursor: col-resize;
        background: transparent;
        z-index: 3;
    }}
    .vpx-{uid} thead th .vpx-resize-handle:hover,
    .vpx-{uid} thead th .vpx-resize-handle.vpx-resizing {{
        background: var(--vpx-accent);
        opacity: 0.5;
    }}

    /* ── Empty state ── */
    .vpx-{uid} .vpx-empty {{
        text-align: center;
        padding: 40px 24px;
        color: var(--vpx-text-tertiary);
        font-size: 14px;
    }}
    /* ── Footer ── */
    .vpx-{uid} .vpx-footer {{
        padding: 10px 24px;
        font-size: 11px;
        color: var(--vpx-text-tertiary);
        text-align: right;
        border-top: 1px solid var(--vpx-border);
        background: var(--vpx-bg-tertiary);
    }}
    </style>
    """

    # ── Build HTML ────────────────────────────────────────────────────────
    search_svg = (
        '<svg class="vpx-search-icon" viewBox="0 0 20 20" fill="currentColor">'
        '<path fill-rule="evenodd" d="M8 4a4 4 0 100 8 4 4 0 000-8zM2 8a6 6 0 '
        '1110.89 3.476l4.817 4.817a1 1 0 01-1.414 1.414l-4.816-4.816A6 6 0 012 8z"'
        ' clip-rule="evenodd"/></svg>'
    )

    header_title = (
        f"Vertipaq Analyzer &mdash; {dataset_name}"
        if dataset_name
        else "Vertipaq Analyzer"
    )

    html_parts = []
    html_parts.append(f'<div class="vpx-{uid}">')
    html_parts.append('<div class="vpx-container">')
    html_parts.append(
        f'<div class="vpx-header"><div class="vpx-title">{header_title}</div></div>'
    )

    # Model summary cards
    if not model_df.empty:
        html_parts.append('<div class="vpx-cards">')
        row = model_df.iloc[0]
        for col in model_df.columns:
            if col == "Dataset Name":
                continue
            val = row[col]
            if col == "Total Size" and pd.notna(val) and str(val):
                try:
                    numeric_val = float(str(val).replace(",", ""))
                    cell_val = format_bytes(numeric_val)
                except (ValueError, TypeError):
                    cell_val = str(val)
            else:
                cell_val = "" if pd.isna(val) else str(val)
            tt = tooltip_lookup.get(("Model", col), "")
            tip_attr = f' title="{tt}"' if tt else ""
            html_parts.append(
                f'<div class="vpx-card"{tip_attr}>'
                f'<div class="vpx-card-label">{col}</div>'
                f'<div class="vpx-card-value">{cell_val}</div>'
                f'</div>'
            )
        html_parts.append('</div>')

    # Tab icons (monochrome SVGs using currentColor for light/dark mode)
    tab_icons = {
        "Tables": '<svg class="vpx-tab-icon" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="2" width="12" height="12" rx="1.5"/><line x1="2" y1="6" x2="14" y2="6"/><line x1="2" y1="10" x2="14" y2="10"/><line x1="6" y1="6" x2="6" y2="14"/></svg>',
        "Partitions": '<svg class="vpx-tab-icon" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="1.5" width="10" height="4" rx="1"/><rect x="3" y="6.5" width="10" height="4" rx="1"/><rect x="3" y="11.5" width="10" height="3" rx="1"/></svg>',
        "Columns": '<svg class="vpx-tab-icon" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"><line x1="4" y1="14" x2="4" y2="5"/><line x1="8" y1="14" x2="8" y2="2"/><line x1="12" y1="14" x2="12" y2="8"/><line x1="2" y1="14" x2="14" y2="14"/></svg>',
        "Relationships": '<svg class="vpx-tab-icon" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"><circle cx="4" cy="8" r="2.5"/><circle cx="12" cy="8" r="2.5"/><line x1="6.5" y1="8" x2="9.5" y2="8"/></svg>',
        "Hierarchies": '<svg class="vpx-tab-icon" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"><circle cx="8" cy="3" r="1.8"/><circle cx="4" cy="13" r="1.8"/><circle cx="12" cy="13" r="1.8"/><line x1="6.8" y1="4.5" x2="4.8" y2="11.2"/><line x1="9.2" y1="4.5" x2="11.2" y2="11.2"/></svg>',
    }

    # Columns that should show data bars per tab
    data_bar_columns = {
        "Tables": ["Total Size", "Row Count", "Data Size", "Dictionary Size", "Relationship Size", "Hierarchy Size", "User Hierarchy Size"],
        "Columns": ["Cardinality", "Total Size", "Data Size", "Dictionary Size", "Hierarchy Size"],
        "Relationships": ["Used Size"],
        "Partitions": ["Record Count"],
        "Hierarchies": ["Used Size"],
    }

    # Tab bar
    html_parts.append(f'<div class="vpx-tab-bar" id="vpx-tabbar-{uid}">')
    for i, title in enumerate(df_dict.keys()):
        active = " vpx-active" if i == 0 else ""
        icon = tab_icons.get(title, "")
        html_parts.append(
            f'<button class="vpx-tab-btn{active}" '
            f'data-vpx-target="vpx-{uid}-p{i}" '
            f'onclick="vpxSwitch_{uid}(this)">{icon}{title}</button>'
        )

    html_parts.append("</div>")

    # Panels
    for i, (title, (df, _)) in enumerate(df_dict.items()):
        visible = " vpx-visible" if i == 0 else ""
        panel_id = f"vpx-{uid}-p{i}"
        vw = df_dict.get(title)[1]
        row_count = len(df)

        html_parts.append(f'<div id="{panel_id}" class="vpx-panel{visible}">')

        has_bars = title in data_bar_columns and any(
            bc in df.columns for bc in data_bar_columns.get(title, [])
        )

        # Toolbar with search and row count
        html_parts.append('<div class="vpx-toolbar">')
        html_parts.append(
            f'<div class="vpx-search-wrap">{search_svg}'
            f'<input type="text" class="vpx-search" '
            f'placeholder="Filter {title.lower()}\u2026" '
            f"oninput=\"vpxFilter_{uid}(this, '{panel_id}')\" />"
            f"</div>"
        )
        html_parts.append('<div class="vpx-toolbar-controls">')
        if has_bars:
            bar_toggle_icon = (
                '<svg class="vpx-toggle-icon" viewBox="0 0 16 16" fill="none" '
                'stroke="currentColor" stroke-width="1.4" stroke-linecap="round">'
                '<line x1="3" y1="12" x2="3" y2="6"/>'
                '<line x1="7" y1="12" x2="7" y2="3"/>'
                '<line x1="11" y1="12" x2="11" y2="8"/>'
                '<line x1="1" y1="12" x2="13" y2="12"/></svg>'
            )
            html_parts.append(
                f'<button class="vpx-bar-toggle vpx-bars-active" '
                f'onclick="vpxToggleBars_{uid}(this, \'{panel_id}\')" '
                f'title="Toggle data bars">{bar_toggle_icon}Bars</button>'
            )

        html_parts.append(
            f'<div class="vpx-row-count" id="{panel_id}-rc">'
            f'<span>{row_count:,}</span> row{"s" if row_count != 1 else ""}'
            f"</div>"
        )
        html_parts.append("</div>")  # toolbar-controls
        html_parts.append("</div>")  # toolbar

        # Table
        html_parts.append('<div class="vpx-table-wrap">')

        if df.empty:
            html_parts.append('<div class="vpx-empty">No data available</div>')
        else:
            html_parts.append("<table>")

            # Determine numeric columns
            numeric_cols = set()
            for col in df.columns:
                if df[col].dtype.kind in ("i", "f", "u"):
                    numeric_cols.add(col)

            # Header
            presort_col = default_sort.get(title) if default_sort else None
            html_parts.append("<thead><tr>")
            for col in df.columns:
                tt = tooltip_lookup.get((vw, col), "")
                num_cls = " vpx-numeric" if col in numeric_cols else ""
                sort_cls = " vpx-sort-desc" if col == presort_col else ""
                tip_attr = f' title="{tt}"' if tt else ""
                arrow = "&#x25BC;" if col == presort_col else "&#x25B2;"
                html_parts.append(
                    f'<th class="{(num_cls + sort_cls).strip()}"{tip_attr} '
                    f'onclick="vpxSort_{uid}(this)">'
                    f'{col}<span class="vpx-sort-arrow">{arrow}</span>'
                    f'<div class="vpx-resize-handle" '
                    f'onmousedown="vpxResizeStart_{uid}(event, this)"></div></th>'
                )
            html_parts.append("</tr></thead>")

            # Body
            bar_cols = data_bar_columns.get(title, [])
            bar_maxes = {}
            for bc in bar_cols:
                if bc in df.columns:
                    max_val = 0
                    for v in df[bc]:
                        if pd.notna(v) and str(v):
                            try:
                                max_val = max(
                                    max_val,
                                    float(
                                        str(v)
                                        .replace(",", "")
                                        .replace("%", "")
                                    ),
                                )
                            except ValueError:
                                pass
                    if max_val > 0:
                        bar_maxes[bc] = max_val

            html_parts.append("<tbody>")
            for _, row_data in df.iterrows():
                html_parts.append("<tr>")
                for col in df.columns:
                    val = row_data[col]
                    cell_val = "" if pd.isna(val) else str(val)
                    is_bar = col in bar_maxes
                    num = col in numeric_cols
                    cls_parts = []
                    if num:
                        cls_parts.append("vpx-numeric")
                    if is_bar:
                        cls_parts.append("vpx-bar-cell")
                    cls_attr = (
                        f' class="{" ".join(cls_parts)}"' if cls_parts else ""
                    )
                    if is_bar and cell_val:
                        try:
                            num_val = float(
                                cell_val.replace(",", "").replace("%", "")
                            )
                            pct = (num_val / bar_maxes[col]) * 100
                        except ValueError:
                            pct = 0
                        html_parts.append(
                            f"<td{cls_attr}>"
                            f'<div class="vpx-bar" style="width:{pct:.1f}%"></div>'
                            f'<span class="vpx-bar-value">{cell_val}</span></td>'
                        )
                    else:
                        html_parts.append(f"<td{cls_attr}>{cell_val}</td>")
                html_parts.append("</tr>")
            html_parts.append("</tbody>")

            html_parts.append("</table>")

        html_parts.append("</div>")  # table-wrap
        html_parts.append("</div>")  # panel

    html_parts.append(
        f'<div class="vpx-footer">Powered by <a href="https://github.com/microsoft/semantic-link-labs" target="_blank" style="color:inherit;text-decoration:underline;">Semantic Link Labs</a> &bull; <a href="https://www.sqlbi.com/tools/vertipaq-analyzer/" target="_blank" style="color:inherit;text-decoration:underline;">Vertipaq Analyzer</a></div>'
    )
    html_parts.append("</div>")  # container
    html_parts.append("</div>")  # root

    # ── JavaScript ────────────────────────────────────────────────────────
    script = f"""
    <script>
    (function() {{
        /* Tab switching */
        window.vpxSwitch_{uid} = function(btn) {{
            var bar = btn.parentElement;
            var container = bar.closest('.vpx-{uid}');
            bar.querySelectorAll('.vpx-tab-btn').forEach(function(b) {{ b.classList.remove('vpx-active'); }});
            btn.classList.add('vpx-active');
            container.querySelectorAll('.vpx-panel').forEach(function(p) {{ p.classList.remove('vpx-visible'); }});
            var target = container.querySelector('#' + btn.getAttribute('data-vpx-target'));
            if (target) target.classList.add('vpx-visible');
        }};

        /* Filtering */
        window.vpxFilter_{uid} = function(input, panelId) {{
            var panel = document.getElementById(panelId);
            if (!panel) return;
            var q = input.value.toLowerCase();
            var rows = panel.querySelectorAll('tbody tr');
            var shown = 0;
            rows.forEach(function(tr) {{
                var text = tr.textContent.toLowerCase();
                var match = !q || text.indexOf(q) !== -1;
                tr.style.display = match ? '' : 'none';
                if (match) shown++;
            }});
            var rc = document.getElementById(panelId + '-rc');
            if (rc) {{
                var total = rows.length;
                rc.innerHTML = '<span>' + shown.toLocaleString() + '</span>' +
                    (shown !== total ? ' of <span>' + total.toLocaleString() + '</span>' : '') +
                    ' row' + (shown !== 1 ? 's' : '');
            }}
        }};

        /* Column resizing */
        var _vpxResizing_{uid} = false;
        window.vpxResizeStart_{uid} = function(evt, handle) {{
            evt.stopPropagation();
            evt.preventDefault();
            _vpxResizing_{uid} = true;
            var th = handle.parentElement;
            var table = th.closest('table');
            var startX = evt.pageX;
            var startW = th.offsetWidth;
            handle.classList.add('vpx-resizing');
            /* measure natural header widths before freezing layout */
            var ths = table.querySelectorAll('thead th');
            ths.forEach(function(h) {{
                if (!h.getAttribute('data-vpx-minw')) {{
                    h.setAttribute('data-vpx-minw', h.scrollWidth);
                }}
            }});
            /* freeze all column widths so layout is stable */
            ths.forEach(function(h) {{ h.style.width = h.offsetWidth + 'px'; }});
            table.style.tableLayout = 'fixed';
            var minW = parseInt(th.getAttribute('data-vpx-minw')) || 40;
            function onMove(e) {{
                var diff = e.pageX - startX;
                var newW = Math.max(minW, startW + diff);
                th.style.width = newW + 'px';
                th.style.minWidth = newW + 'px';
            }}
            function onUp() {{
                handle.classList.remove('vpx-resizing');
                document.removeEventListener('mousemove', onMove);
                document.removeEventListener('mouseup', onUp);
                setTimeout(function() {{ _vpxResizing_{uid} = false; }}, 0);
            }}
            document.addEventListener('mousemove', onMove);
            document.addEventListener('mouseup', onUp);
        }};

        /* Sorting */
        window.vpxSort_{uid} = function(th) {{
            if (_vpxResizing_{uid}) return;
            var table = th.closest('table');
            if (!table) return;
            var idx = Array.from(th.parentElement.children).indexOf(th);
            var tbody = table.querySelector('tbody');
            var rows = Array.from(tbody.querySelectorAll('tr'));
            var asc = !th.classList.contains('vpx-sort-asc');

            /* Clear sort classes from all headers */
            th.parentElement.querySelectorAll('th').forEach(function(h) {{
                h.classList.remove('vpx-sort-asc', 'vpx-sort-desc');
                h.querySelector('.vpx-sort-arrow').innerHTML = '&#x25B2;';
            }});
            th.classList.add(asc ? 'vpx-sort-asc' : 'vpx-sort-desc');
            th.querySelector('.vpx-sort-arrow').innerHTML = asc ? '&#x25B2;' : '&#x25BC;';

            rows.sort(function(a, b) {{
                var aVal = a.children[idx] ? a.children[idx].textContent.trim() : '';
                var bVal = b.children[idx] ? b.children[idx].textContent.trim() : '';
                /* Try numeric comparison */
                var aNum = parseFloat(aVal.replace(/,/g, '').replace(/%/g, ''));
                var bNum = parseFloat(bVal.replace(/,/g, '').replace(/%/g, ''));
                if (!isNaN(aNum) && !isNaN(bNum)) {{
                    return asc ? aNum - bNum : bNum - aNum;
                }}
                return asc ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);
            }});
            rows.forEach(function(r) {{ tbody.appendChild(r); }});
        }};

        /* Toggle data bars (synced across all tabs) */
        window.vpxToggleBars_{uid} = function(btn, panelId) {{
            var container = btn.closest('.vpx-{uid}');
            if (!container) return;
            var panel = document.getElementById(panelId);
            if (!panel) return;
            var wrap = panel.querySelector('.vpx-table-wrap');
            if (!wrap) return;
            var barsOff = !wrap.classList.contains('vpx-bars-off');
            container.querySelectorAll('.vpx-panel .vpx-table-wrap').forEach(function(w) {{
                if (barsOff) {{ w.classList.add('vpx-bars-off'); }}
                else {{ w.classList.remove('vpx-bars-off'); }}
            }});
            container.querySelectorAll('.vpx-bar-toggle').forEach(function(b) {{
                if (barsOff) {{ b.classList.remove('vpx-bars-active'); }}
                else {{ b.classList.add('vpx-bars-active'); }}
            }});
        }};


    }})();
    </script>
    """

    display(HTML(styles + "\n".join(html_parts) + script))


@log
def import_vertipaq_analyzer(folder_path: str, file_name: str):
    """
    Imports and visualizes the vertipaq analyzer info from a saved .zip file in your lakehouse.

    Parameters
    ----------
    folder_path : str
        The folder within your lakehouse in which the .zip file containing the vertipaq analyzer info has been saved.
    file_name : str
        The file name of the file which contains the vertipaq analyzer info.

    Returns
    -------
    str
       A visualization of the Vertipaq Analyzer statistics.
    """

    pd.options.mode.copy_on_write = True

    zipFilePath = os.path.join(folder_path, file_name)
    extracted_dir = os.path.join(folder_path, "extracted_dataframes")

    with zipfile.ZipFile(zipFilePath, "r") as zip_ref:
        zip_ref.extractall(extracted_dir)

    # Read all CSV files into a dictionary of DataFrames
    dfs = {}
    for file_name in zip_ref.namelist():
        df = pd.read_csv(extracted_dir + "/" + file_name)
        file_path = Path(file_name)
        df_name = file_path.stem
        dfs[df_name] = df

    visualize_vertipaq(dfs, "")

    # Clean up: remove the extracted directory
    shutil.rmtree(extracted_dir)
