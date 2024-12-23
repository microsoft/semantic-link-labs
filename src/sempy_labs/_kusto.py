import sempy.fabric as fabric
import requests
import pandas as pd
from sempy.fabric.exceptions import FabricHTTPException
from sempy._utils._log import log
import sempy_labs._icons as icons
from typing import Optional, List
from uuid import UUID
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    create_abfss_path,
    save_as_delta_table,
)
from sempy_labs._kql_databases import _resolve_cluster_uri


@log
def query_kusto(cluster_uri: str, query: str, database: str) -> pd.DataFrame:
    """
    Shows the KQL querysets within a workspace.

    Parameters
    ----------
    cluster_uri : str
        The Query URI for the KQL database. Example: "https://guid.kusto.fabric.microsoft.com"
    query : str
        The KQL query.
    database : str
        The KQL database name.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the result of the KQL query.
    """

    import notebookutils

    token = notebookutils.credentials.getToken(cluster_uri)

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    payload = {"db": database, "csl": query}

    response = requests.post(
        f"{cluster_uri}/v1/rest/query",
        headers=headers,
        json=payload,
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)

    results = response.json()
    columns_info = results["Tables"][0]["Columns"]
    rows = results["Tables"][0]["Rows"]

    df = pd.DataFrame(rows, columns=[col["ColumnName"] for col in columns_info])

    for col_info in columns_info:
        col_name = col_info["ColumnName"]
        data_type = col_info["DataType"]

        try:
            if data_type == "DateTime":
                df[col_name] = pd.to_datetime(df[col_name])
            elif data_type in ["Int64", "Int32", "Long"]:
                df[col_name] = (
                    pd.to_numeric(df[col_name], errors="coerce")
                    .fillna(0)
                    .astype("int64")
                )
            elif data_type == "Real" or data_type == "Double":
                df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
            else:
                # Convert any other type to string, change as needed
                df[col_name] = df[col_name].astype(str)
        except Exception as e:
            print(
                f"{icons.yellow_dot} Could not convert column {col_name} to {data_type}, defaulting to string: {str(e)}"
            )
            df[col_name] = df[col_name].astype(str)

    return df


def semantic_model_logs(
    cluster_uri: Optional[str] = None,
    dataset: Optional[str | List[str]] = None,
    workspace: Optional[str | UUID] = None,
    report: Optional[str | UUID | List[str] | List[UUID]] = None,
    capacity: Optional[str | List[str]] = None,
    operation_name: Optional[str | List[str]] = None,
    operation_detail_name: Optional[str | List[str]] = None,
    application_name: Optional[str | List[str]] = None,
    executing_user: Optional[str | List[str]] = None,
    duration_ms: Optional[int] = None,
    cpu_time_ms: Optional[int] = None,
    timespan: Optional[int | float] = 1,
    timespan_literal: Optional[str] = "hour",
) -> pd.DataFrame:
    """
    Shows the semantic model logs based on `Workspace Monitoring <https://blog.fabric.microsoft.com/blog/announcing-public-preview-of-workspace-monitoring>`_.

    Requirement: Workspace Monitoring must be enabled for the workspace. See the link above for how to enable it.

    Parameters
    ----------
    cluster_uri : str, default=None
        The Query URI for the KQL database. Example: "https://guid.kusto.fabric.microsoft.com"
    dataset : str | List[str], default=None
        Filter to be applied to the DatasetName column.
    workspace : str | UUID, default=None
        Filter to be applied to the WorkspaceName column.
    report : str | UUID | List[str] | List[UUID], default=None
        Filters the output to a report or list of reports. Must specify a single workspace if specifying a report or list of reports.
    capacity : str | List[str], default=None
        Filters the output to a capacity or list of capacities.
    operation_name : str | List[str], default=None
        Filters the output to an operation or list of operations.
    operation_detail_name : str | List[str], default=None
        Filters the output to a detail operation or list of detail operations.
    application_name : str | List[str], default=None
        Filters the output to an application name.
    executing_user : str | List[str], default=None
        Filters the ouptut to a user or list of users (email addresses).
    duration_ms : int, default=None
        Filter to be applied to the Duration (milliseconds) column.
    cpu_time_ms : int, default=None
        Filter to be applied to the CPU Time (milliseconds) column.
    timespan : int, default=1,
        The timespan (use in conjunction with the timespan_format).
    timespan_literal : str, default="hour"
        The timespan literal format. Valid options: "day", "hour", "minute".

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the semantic model logs based on the filters provided.
    """

    from sempy_labs._kql_databases import list_kql_databases

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    if cluster_uri is None:
        dfK = list_kql_databases(workspace=workspace_id)
        dfK_filt = dfK[dfK["KQL Database Name"] == "Monitoring KQL database"]
        if len(dfK_filt) == 0:
            raise ValueError(
                f"{icons.red_dot} Workspace monitoring is not set up for the '{workspace_name}' workspace."
            )
        cluster_uri = dfK_filt["Query Service URI"].iloc[0]

    timespan_literal = timespan_literal.lower()
    if timespan_literal.startswith("h"):
        timespan_literal = "h"
    elif timespan_literal.startswith("m"):
        timespan_literal = "m"
    elif timespan_literal.startswith("d"):
        timespan_literal = "d"
    else:
        raise ValueError(
            f"{icons.red_dot} The '{timespan_literal} timespan_literal is not supported. Only 'day, 'hour', and 'minute' are supported."
        )

    if report is not None and (workspace is None or not isinstance(workspace, str)):
        raise ValueError(
            f"{icons.red_dot} A report or list of reports may only be specified if a single workspace is specified."
        )

    query = f"SemanticModelLogs\n| where Timestamp > ago({timespan}{timespan_literal})"

    report_json_filter = (
        "tostring(parse_json(dynamic_to_json(ApplicationContext)).Sources[0].ReportId)"
    )
    visual_json_filter = (
        "tostring(parse_json(dynamic_to_json(ApplicationContext)).Sources[0].VisualId)"
    )
    return_columns = [
        "Timestamp",
        "OperationName",
        "OperationDetailName",
        "ItemName",
        "WorkspaceId",
        "WorkspaceName",
        "CapacityId",
        "DurationMs",
        "CpuTimeMs",
        "EventText",
        "Status",
        "ReportId",
        "VisualId",
        "ApplicationName",
        "ExecutingUser",
    ]

    param_dict = {
        "dataset": "ItemName",
        "workspace_name": "WorkspaceName",
        "operation_name": "OperationName",
        "operation_detail_name": "OperationDetailName",
        "application_name": "ApplicationName",
        "duration_ms": "DurationMs",
        "cpu_time_ms": "CpuTimeMs",
        "executing_user": "ExecutingUser",
    }

    if capacity is not None:
        dfC = fabric.list_capacities()
        if isinstance(capacity, str):
            capacity = [capacity]
        capacities = dfC[dfC["Display Name"].isin(capacity)]["Id"].tolist()
        if len(capacities) > 0:
            comma_delimited_string = ", ".join(f'"{item}"' for item in capacities)
            query += f"\nand CapacityId in ({comma_delimited_string})"

    if report is not None:
        dfR = fabric.list_reports(workspace=workspace)
        if isinstance(report, str):
            report = [report]
        reports = dfR[dfR["Name"].isin(report)]["Id"].tolist()
        reports = reports + dfR[dfR["Id"].isin(report)]["Id"].tolist()
        if len(reports) > 0:
            comma_delimited_string = ", ".join(f'"{item}"' for item in reports)
            query += f"\nand {report_json_filter} in ({comma_delimited_string})"

    def _add_to_filter(parameter, filter_name, query):
        if parameter is not None:
            if isinstance(parameter, str):
                parameter = [parameter]
            comma_delimited_string = ", ".join(f'"{item}"' for item in parameter)
            query += f"\nand {filter_name} in ({comma_delimited_string})"
        return query

    for param, filter_name in param_dict.items():
        query = _add_to_filter(
            parameter=locals()[param], filter_name=filter_name, query=query
        )

    query += (
        f"\n| extend ReportId = {report_json_filter}, VisualId = {visual_json_filter}"
    )

    # Add columns to return
    return_cols = ", ".join(return_columns)
    query += f"\n| project {return_cols}"

    return query_kusto(
        cluster_uri=cluster_uri, query=query, database="Monitoring Eventhouse"
    )


def save_semantic_model_logs(
    workspace: Optional[str | UUID] = None,
    frequency: int = 1,
    frequency_literal: str = "hour",
):

    from sempy_labs.lakehouse import get_lakehouse_tables
    from pyspark.sql import SparkSession

    delta_table_name = "SLL_SemanticModelLogs"
    cluster_uri = _resolve_cluster_uri(workspace=workspace)

    query = f"""
        let StartDate = datetime_add('{frequency_literal}', -{frequency}, bin(now(), 1h));
        let EndDate = bin(now(), 1h);
        //let StartDate = datetime_add('day', -4, bin(now(), 1h));
        //let EndDate = bin(now(), 1h);
        SemanticModelLogs
        | where Timestamp between (StartDate .. EndDate)
        | extend ReportId = tostring(parse_json(dynamic_to_json(ApplicationContext)).Sources[0].ReportId)
        | extend VisualId = tostring(parse_json(dynamic_to_json(ApplicationContext)).Sources[0].VisualId)
        | extend UniqueId = hash(strcat((Timestamp), "_", OperationName, "_", OperationDetailName))
    """

    df = query_kusto(
        cluster_uri=cluster_uri, database="Monitoring Eventhouse", query=query
    )
    if df.empty:
        print(f"{icons.yellow_dot} No logs to capture in this time period.")
        return

    lakehouse_id = fabric.get_lakehouse_id()
    lakehouse_workspace_id = fabric.get_workspace_id()
    path = create_abfss_path(
        lakehouse_id, lakehouse_workspace_id, delta_table_name=delta_table_name
    )

    dfLT = get_lakehouse_tables()
    dfLT_filt = dfLT[dfLT["Table Name"] == delta_table_name]

    if len(dfLT_filt) == 1:
        spark = SparkSession.builder.getOrCreate()
        existing_df = spark.read.format("delta").load(path)
        df_spark = spark.createDataFrame(df)

        # Filter out rows that already exist in the Delta table
        incremental_df = df_spark.join(existing_df, "UniqueId", "left_anti")
    else:
        incremental_df = df_spark

    incremental_df_pandas = incremental_df.toPandas()

    # If the resulting DataFrame is not empty, save to Delta table
    if not incremental_df_pandas.empty:
        save_as_delta_table(
            dataframe=incremental_df_pandas,  # Use the filtered DataFrame here
            write_mode="append",
            delta_table_name=delta_table_name,
        )
    else:
        print(f"{icons.yellow_dot} No new logs to capture in this time period.")
