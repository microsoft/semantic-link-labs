import sempy.fabric as fabric
import re
import json
import base64
import pandas as pd
from functools import wraps
import datetime
import time
from pyspark.sql import SparkSession
from typing import Optional, Tuple, List
from uuid import UUID
import sempy_labs._icons as icons
from sempy.fabric.exceptions import FabricHTTPException


def create_abfss_path(
    lakehouse_id: UUID, lakehouse_workspace_id: UUID, delta_table_name: str
):
    """
    Creates an abfss path for a delta table in a Fabric lakehouse.

    Parameters
    ----------
    lakehouse_id : UUID
        ID of the Fabric lakehouse.
    lakehouse_workspace_id : UUID
        ID of the Fabric workspace.
    delta_table_name : str
        Name of the delta table name.

    Returns
    -------
    str
        An abfss path which can be used to save/reference a delta table in a Fabric lakehouse.
    """

    return f"abfss://{lakehouse_workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/{delta_table_name}"


def format_dax_object_name(table: str, column: str):
    """
    Formats a table/column combination to the 'Table Name'[Column Name] format.

    Parameters
    ----------
    table : str
        The name of the table.
    column : str
        The name of the column.

    Returns
    -------
    str
        The fully qualified object name.
    """

    return "'" + table + "'[" + column + "]"


def create_relationship_name(
    from_table: str, from_column: str, to_table: str, to_column: str
):
    """
    Formats a relationship's table/columns into a fully qualified name.

    Parameters
    ----------
    from_table : str
        The name of the table on the 'from' side of the relationship.
    from_column : str
        The name of the column on the 'from' side of the relationship.
    to_table : str
        The name of the table on the 'to' side of the relationship.
    to_column : str
        The name of the column on the 'to' side of the relationship.

    Returns
    -------
    str
        The fully qualified relationship name.
    """

    return (
        format_dax_object_name(from_table, from_column)
        + " -> "
        + format_dax_object_name(to_table, to_column)
    )


def resolve_report_id(report: str, workspace: Optional[str] = None):
    """
    Obtains the ID of the Power BI report.

    Parameters
    ----------
    report : str
        The name of the Power BI report.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    UUID
        The ID of the Power BI report.
    """

    if workspace is None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    obj = fabric.resolve_item_id(item_name=report, type="Report", workspace=workspace)

    return obj


def resolve_report_name(report_id: UUID, workspace: Optional[str] = None):
    """
    Obtains the name of the Power BI report.

    Parameters
    ----------
    report_id : UUID
        The name of the Power BI report.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    str
        The name of the Power BI report.
    """

    if workspace is None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    obj = fabric.resolve_item_name(
        item_id=report_id, type="Report", workspace=workspace
    )

    return obj


def resolve_dataset_id(dataset: str, workspace: Optional[str] = None):
    """
    Obtains the ID of the semantic model.

    Parameters
    ----------
    dataset : str
        The name of the semantic model.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    UUID
        The ID of the semantic model.
    """

    if workspace is None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    obj = fabric.resolve_item_id(
        item_name=dataset, type="SemanticModel", workspace=workspace
    )

    return obj


def resolve_dataset_name(dataset_id: UUID, workspace: Optional[str] = None):
    """
    Obtains the name of the semantic model.

    Parameters
    ----------
    dataset_id : UUID
        The name of the semantic model.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    str
        The name of the semantic model.
    """

    if workspace is None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    obj = fabric.resolve_item_name(
        item_id=dataset_id, type="SemanticModel", workspace=workspace
    )

    return obj


def resolve_lakehouse_name(
    lakehouse_id: Optional[UUID] = None, workspace: Optional[str] = None
):
    """
    Obtains the name of the Fabric lakehouse.

    Parameters
    ----------
    lakehouse_id : UUID, default=None
        The name of the Fabric lakehouse.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    str
        The name of the Fabric lakehouse.
    """

    if workspace is None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    if lakehouse_id is None:
        lakehouse_id = fabric.get_lakehouse_id()

    obj = fabric.resolve_item_name(
        item_id=lakehouse_id, type="Lakehouse", workspace=workspace
    )

    return obj


def resolve_lakehouse_id(lakehouse: str, workspace: Optional[str] = None):
    """
    Obtains the ID of the Fabric lakehouse.

    Parameters
    ----------
    lakehouse : str
        The name of the Fabric lakehouse.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    UUID
        The ID of the Fabric lakehouse.
    """

    if workspace is None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    obj = fabric.resolve_item_id(
        item_name=lakehouse, type="Lakehouse", workspace=workspace
    )

    return obj


def get_direct_lake_sql_endpoint(dataset: str, workspace: Optional[str] = None) -> UUID:
    """
    Obtains the SQL Endpoint ID of the semantic model.

    Parameters
    ----------
    dataset : str
        The name of the semantic model.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    uuid.UUID
        The ID of SQL Endpoint.
    """

    from sempy_labs.tom import connect_semantic_model

    if workspace is None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    # dfP = fabric.list_partitions(dataset=dataset, workspace=workspace)
    # dfP_filt = dfP[dfP["Mode"] == "DirectLake"]

    # if len(dfP_filt) == 0:
    #    raise ValueError(
    #        f"The '{dataset}' semantic model in the '{workspace}' workspace is not in Direct Lake mode."
    #    )

    with connect_semantic_model(
        dataset=dataset, readonly=True, workspace=workspace
    ) as tom:
        sqlEndpointId = None
        for e in tom.model.Expressions:
            if e.Name == "DatabaseQuery":
                expr = e.Expression
                matches = re.findall(r'"([^"]+)"', expr)
                sqlEndpointId = matches[1]

        if sqlEndpointId is None:
            raise ValueError("SQL Endpoint not found.")

        return sqlEndpointId


def generate_embedded_filter(filter: str):
    """
    Converts the filter expression to a filter expression which can be used by a Power BI embedded URL.

    Parameters
    ----------
    filter : str
        The filter expression for an embedded Power BI report.

    Returns
    -------
    str
        A filter expression usable by a Power BI embedded URL.
    """

    pattern = r"'[^']+'\[[^\[]+\]"
    matches = re.findall(pattern, filter)
    for match in matches:
        matchReplace = (
            match.replace("'", "")
            .replace("[", "/")
            .replace("]", "")
            .replace(" ", "_x0020_")
            .replace("@", "_00x40_")
            .replace("+", "_0x2B_")
            .replace("{", "_007B_")
            .replace("}", "_007D_")
        )
        filter = filter.replace(match, matchReplace)

    pattern = r"\[[^\[]+\]"
    matches = re.findall(pattern, filter)
    for match in matches:
        matchReplace = (
            match.replace("'", "")
            .replace("[", "/")
            .replace("]", "")
            .replace(" ", "_x0020_")
            .replace("@", "_00x40_")
            .replace("+", "_0x2B_")
            .replace("{", "_007B_")
            .replace("}", "_007D_")
        )
        filter = filter.replace(match, matchReplace)

    revised_filter = (
        filter.replace("<=", "le")
        .replace(">=", "ge")
        .replace("<>", "ne")
        .replace("!=", "ne")
        .replace("==", "eq")
        .replace("=", "eq")
        .replace("<", "lt")
        .replace(">", "gt")
        .replace(" && ", " and ")
        .replace(" & ", " and ")
        .replace(" || ", " or ")
        .replace(" | ", " or ")
        .replace("{", "(")
        .replace("}", ")")
    )

    return revised_filter


def save_as_delta_table(
    dataframe,
    delta_table_name: str,
    write_mode: str,
    merge_schema: Optional[bool] = False,
    lakehouse: Optional[str] = None,
    workspace: Optional[str] = None,
):
    """
    Saves a pandas dataframe as a delta table in a Fabric lakehouse.

    Parameters
    ----------
    dataframe : pandas.DataFrame
        The dataframe to be saved as a delta table.
    delta_table_name : str
        The name of the delta table.
    write_mode : str
        The write mode for the save operation. Options: 'append', 'overwrite'.
    merge_schema : bool, default=False
        Merges the schemas of the dataframe to the delta table.
    lakehouse : str, default=None
        The Fabric lakehouse used by the Direct Lake semantic model.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    UUID
        The ID of the Power BI report.
    """

    if workspace is None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

    if lakehouse is None:
        lakehouse_id = fabric.get_lakehouse_id()
        lakehouse = resolve_lakehouse_name(
            lakehouse_id=lakehouse_id, workspace=workspace
        )
    else:
        lakehouse_id = resolve_lakehouse_id(lakehouse, workspace)

    writeModes = ["append", "overwrite"]
    write_mode = write_mode.lower()

    if write_mode not in writeModes:
        raise ValueError(
            f"{icons.red_dot} Invalid 'write_type' parameter. Choose from one of the following values: {writeModes}."
        )

    if " " in delta_table_name:
        raise ValueError(
            f"{icons.red_dot} Invalid 'delta_table_name'. Delta tables in the lakehouse cannot have spaces in their names."
        )

    dataframe.columns = dataframe.columns.str.replace(" ", "_")

    spark = SparkSession.builder.getOrCreate()
    spark_df = spark.createDataFrame(dataframe)

    filePath = create_abfss_path(
        lakehouse_id=lakehouse_id,
        lakehouse_workspace_id=workspace_id,
        delta_table_name=delta_table_name,
    )

    if merge_schema:
        spark_df.write.mode(write_mode).format("delta").option(
            "mergeSchema", "true"
        ).save(filePath)
    else:
        spark_df.write.mode(write_mode).format("delta").save(filePath)
    print(
        f"{icons.green_dot} The dataframe has been saved as the '{delta_table_name}' table in the '{lakehouse}' lakehouse within the '{workspace}' workspace."
    )


def language_validate(language: str):
    """
    Validateds that the language specified exists within the supported langauges.

    Parameters
    ----------
    language : str
        The language code.

    Returns
    -------
    bool
        A True/False indication as to whether the language code is supported.
    """

    url = "https://learn.microsoft.com/azure/ai-services/translator/language-support"

    tables = pd.read_html(url)
    df = tables[0]

    df_filt = df[df["Language code"] == language]

    df_filt2 = df[df["Language"] == language.capitalize()]

    if len(df_filt) == 1:
        lang = df_filt["Language"].iloc[0]
    elif len(df_filt2) == 1:
        lang = df_filt2["Language"].iloc[0]
    else:
        raise ValueError(
            f"{icons.red_dot} The '{language}' language is not a valid language code. Please refer to this link for a list of valid language codes: {url}."
        )

    return lang


def resolve_workspace_name_and_id(workspace: Optional[str] = None) -> Tuple[str, str]:
    """
    Obtains the name and ID of the Fabric workspace.

    Parameters
    ----------
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    str, str
        The name and ID of the Fabric workspace.
    """

    if workspace is None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_id = fabric.resolve_workspace_id(workspace)

    return str(workspace), str(workspace_id)


def _extract_json(dataframe: pd.DataFrame) -> dict:

    payload = dataframe["payload"].iloc[0]
    json_file = _decode_b64(payload)

    return json.loads(json_file)


def _conv_b64(file):

    loadJson = json.dumps(file)
    f = base64.b64encode(loadJson.encode("utf-8")).decode("utf-8")

    return f


def _decode_b64(file, format: Optional[str] = "utf-8"):

    result = base64.b64decode(file).decode(format)

    return result


def is_default_semantic_model(dataset: str, workspace: Optional[str] = None) -> bool:
    """
    Identifies whether a semantic model is a default semantic model.

    Parameters
    ----------
    dataset : str
        The name of the semantic model.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    bool
        A True/False value indicating whether the semantic model is a default semantic model.
    """

    workspace = fabric.resolve_workspace_name(workspace)

    dfI = fabric.list_items(workspace=workspace)
    filtered_df = dfI.groupby("Display Name").filter(
        lambda x: set(["Warehouse", "SemanticModel"]).issubset(set(x["Type"]))
        or set(["Lakehouse", "SemanticModel"]).issubset(set(x["Type"]))
    )
    default_semantic_models = filtered_df["Display Name"].unique().tolist()

    return dataset in default_semantic_models


def resolve_item_type(item_id: UUID, workspace: Optional[str] = None) -> str:
    """
    Obtains the item type for a given Fabric Item Id within a Fabric workspace.

    Parameters
    ----------
    item_id : UUID
        The item/artifact Id.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    str
        The item type for the item Id.
    """

    workspace = fabric.resolve_workspace_name(workspace)
    dfI = fabric.list_items(workspace=workspace)
    dfI_filt = dfI[dfI["Id"] == item_id]

    if len(dfI_filt) == 0:
        raise ValueError(
            f"Invalid 'item_id' parameter. The '{item_id}' item was not found in the '{workspace}' workspace."
        )
    item_type = dfI_filt["Type"].iloc[0]

    return item_type


def resolve_dataset_from_report(
    report: str, workspace: Optional[str] = None
) -> Tuple[UUID, str, UUID, str]:
    """
    Obtains the basic semantic model properties from which the report's data is sourced.

    Parameters
    ----------
    report : str
        The name of the Power BI report.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    Tuple[UUID, str, UUID, str]
        The semantic model UUID, semantic model name, semantic model workspace UUID, semantic model workspace name
    """

    workspace = fabric.resolve_workspace_name(workspace)

    dfR = fabric.list_reports(workspace=workspace)
    dfR_filt = dfR[dfR["Name"] == report]
    if len(dfR_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{report}' report does not exist within the '{workspace}' workspace."
        )
    dataset_id = dfR_filt["Dataset Id"].iloc[0]
    dataset_workspace_id = dfR_filt["Dataset Workspace Id"].iloc[0]
    dataset_workspace = fabric.resolve_workspace_name(dataset_workspace_id)
    dataset_name = resolve_dataset_name(
        dataset_id=dataset_id, workspace=dataset_workspace
    )

    return dataset_id, dataset_name, dataset_workspace_id, dataset_workspace


def _add_part(target_dict, path, payload):

    part = {"path": path, "payload": payload, "payloadType": "InlineBase64"}

    target_dict["definition"]["parts"].append(part)


def resolve_workspace_capacity(workspace: Optional[str] = None) -> Tuple[UUID, str]:
    """
    Obtains the capacity Id and capacity name for a given workspace.

    Parameters
    ----------
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    Tuple[UUID, str]
        capacity Id; capacity came.
    """

    workspace = fabric.resolve_workspace_name(workspace)
    dfW = fabric.list_workspaces(filter=f"name eq '{workspace}'")
    capacity_id = dfW["Capacity Id"].iloc[0]
    dfC = fabric.list_capacities()
    dfC_filt = dfC[dfC["Id"] == capacity_id]
    if len(dfC_filt) == 1:
        capacity_name = dfC_filt["Display Name"].iloc[0]
    else:
        capacity_name = None

    return capacity_id, capacity_name


def get_capacity_id(workspace: Optional[str] = None) -> UUID:
    """
    Obtains the Capacity Id for a given workspace.

    Parameters
    ----------
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    UUID
        The capacity Id.
    """

    workspace = fabric.resolve_workspace_name(workspace)
    dfW = fabric.list_workspaces(filter=f"name eq '{workspace}'")
    if len(dfW) == 0:
        raise ValueError(f"{icons.red_dot} The '{workspace}' does not exist'.")

    return dfW["Capacity Id"].iloc[0]


def get_capacity_name(workspace: Optional[str] = None) -> str:
    """
    Obtains the capacity name for a given workspace.

    Parameters
    ----------
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    str
        The capacity name.
    """

    capacity_id = get_capacity_id(workspace)
    dfC = fabric.list_capacities()
    dfC_filt = dfC[dfC["Id"] == capacity_id]
    if len(dfC_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{capacity_id}' capacity Id does not exist."
        )

    return dfC_filt["Display Name"].iloc[0]


def resolve_capacity_name(capacity_id: Optional[UUID] = None) -> str:
    """
    Obtains the capacity name for a given capacity Id.

    Parameters
    ----------
    capacity_id : UUID, default=None
        The capacity Id.
        Defaults to None which resolves to the capacity name of the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the capacity name of the workspace of the notebook.

    Returns
    -------
    str
        The capacity name.
    """

    if capacity_id is None:
        return get_capacity_name()

    dfC = fabric.list_capacities()
    dfC_filt = dfC[dfC["Id"] == capacity_id]

    if len(dfC_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{capacity_id}' capacity Id does not exist."
        )

    return dfC_filt["Display Name"].iloc[0]


def retry(sleep_time: int, timeout_error_message: str):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = datetime.datetime.now()
            timeout = datetime.timedelta(minutes=1)
            while datetime.datetime.now() - start_time <= timeout:
                try:
                    return func(*args, **kwargs)
                except Exception:
                    time.sleep(sleep_time)
            raise TimeoutError(timeout_error_message)

        return wrapper

    return decorator


def lro(
    client,
    response,
    status_codes: Optional[List[str]] = [200, 202],
    sleep_time: Optional[int] = 1,
    return_status_code: Optional[bool] = False,
):

    if response.status_code not in status_codes:
        raise FabricHTTPException(response)
    if response.status_code == status_codes[0]:
        if return_status_code:
            result = response.status_code
        else:
            result = response
    if response.status_code == status_codes[1]:
        operationId = response.headers["x-ms-operation-id"]
        response = client.get(f"/v1/operations/{operationId}")
        response_body = json.loads(response.content)
        while response_body["status"] not in ["Succeeded", "Failed"]:
            time.sleep(sleep_time)
            response = client.get(f"/v1/operations/{operationId}")
            response_body = json.loads(response.content)
        if response_body["status"] != "Succeeded":
            raise FabricHTTPException(response)
        if return_status_code:
            result = response.status_code
        else:
            response = client.get(f"/v1/operations/{operationId}/result")
            result = response

    return result


def pagination(client, response):

    responses = []
    response_json = response.json()
    responses.append(response_json)

    # Check for pagination
    continuation_token = response_json.get("continuationToken")
    continuation_uri = response_json.get("continuationUri")

    # Loop to handle pagination
    while continuation_token is not None:
        response = client.get(continuation_uri)
        response_json = response.json()
        responses.append(response_json)

        # Update the continuation token and URI for the next iteration
        continuation_token = response_json.get("continuationToken")
        continuation_uri = response_json.get("continuationUri")

    return responses
