import sempy.fabric as fabric
import re
import json
import base64
import time
import uuid
from sempy.fabric.exceptions import FabricHTTPException
import pandas as pd
from functools import wraps
import datetime
from typing import Optional, Tuple, List
from uuid import UUID
import sempy_labs._icons as icons
from azure.core.credentials import TokenCredential, AccessToken
import urllib.parse
import numpy as np
from IPython.display import display, HTML
import requests
import sempy_labs._authentication as auth


def _build_url(url: str, params: dict) -> str:
    """
    Build the url with a list of parameters
    """
    url_parts = list(urllib.parse.urlparse(url))
    url_parts[4] = urllib.parse.urlencode(params)
    url = urllib.parse.urlunparse(url_parts)

    return url


def _encode_user(user: str) -> str:

    return urllib.parse.quote(user, safe="@")


def create_abfss_path(
    lakehouse_id: UUID,
    lakehouse_workspace_id: UUID,
    delta_table_name: Optional[str] = None,
) -> str:
    """
    Creates an abfss path for a delta table in a Fabric lakehouse.

    Parameters
    ----------
    lakehouse_id : uuid.UUID
        ID of the Fabric lakehouse.
    lakehouse_workspace_id : uuid.UUID
        ID of the Fabric workspace.
    delta_table_name : str, default=None
        Name of the delta table name.

    Returns
    -------
    str
        An abfss path which can be used to save/reference a delta table in a Fabric lakehouse or lakehouse.
    """

    fp = _get_default_file_path()
    path = f"abfss://{lakehouse_workspace_id}@{fp}/{lakehouse_id}"

    if delta_table_name is not None:
        path += f"/Tables/{delta_table_name}"

    return path


def _get_default_file_path() -> str:

    default_file_storage = _get_fabric_context_setting(name="fs.defaultFS")

    return default_file_storage.split("@")[-1][:-1]


def _split_abfss_path(path: str) -> Tuple[UUID, UUID, str]:

    parsed_url = urllib.parse.urlparse(path)

    workspace_id = parsed_url.netloc.split("@")[0]
    item_id = parsed_url.path.lstrip("/").split("/")[0]
    delta_table_name = parsed_url.path.split("/")[-1]

    return workspace_id, item_id, delta_table_name


def format_dax_object_name(table: str, column: str) -> str:
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
) -> str:
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


def resolve_report_id(
    report: str | UUID, workspace: Optional[str | UUID] = None
) -> UUID:
    """
    Obtains the ID of the Power BI report.

    Parameters
    ----------
    report : str | uuid.UUID
        The name or ID of the Power BI report.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    uuid.UUID
        The ID of the Power BI report.
    """

    return resolve_item_id(item=report, type="Report", workspace=workspace)


def resolve_report_name(report_id: UUID, workspace: Optional[str | UUID] = None) -> str:
    """
    Obtains the name of the Power BI report.

    Parameters
    ----------
    report_id : uuid.UUID
        The name of the Power BI report.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    str
        The name of the Power BI report.
    """

    return fabric.resolve_item_name(
        item_id=report_id, type="Report", workspace=workspace
    )


def delete_item(
    item: str | UUID, type: str, workspace: Optional[str | UUID] = None
) -> None:
    """
    Deletes an item from a Fabric workspace.

    Parameters
    ----------
    item : str | uuid.UUID
        The name or ID of the item to be deleted.
    type : str
        The type of the item to be deleted.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    from sempy_labs._utils import item_types

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (item_name, item_id) = resolve_item_name_and_id(item, type, workspace_id)
    item_type = item_types.get(type)[0].lower()

    fabric.delete_item(item_id=item_id, workspace=workspace_id)

    print(
        f"{icons.green_dot} The '{item_name}' {item_type} has been successfully deleted from the '{workspace_name}' workspace."
    )


def create_item(
    name: str,
    type: str,
    description: Optional[str] = None,
    definition: Optional[dict] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Creates an item in a Fabric workspace.

    Parameters
    ----------
    name : str
        The name of the item to be created.
    type : str
        The type of the item to be created.
    description : str, default=None
        A description of the item to be created.
    definition : dict, default=None
        The definition of the item to be created.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """
    from sempy_labs._utils import item_types

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    item_type = item_types.get(type)[0].lower()
    item_type_url = item_types.get(type)[1]

    payload = {
        "displayName": name,
    }
    if description:
        payload["description"] = description
    if definition:
        payload["definition"] = definition

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/{item_type_url}",
        method="post",
        payload=payload,
        status_codes=[201, 202],
        lro_return_status_code=True,
    )
    print(
        f"{icons.green_dot} The '{name}' {item_type} has been successfully created within the in the '{workspace_name}' workspace."
    )


def get_item_definition(
    item: str | UUID,
    type: str,
    workspace: Optional[str | UUID] = None,
    format: Optional[str] = None,
    return_dataframe: bool = True,
    decode: bool = True,
):

    from sempy_labs._utils import item_types

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    item_id = resolve_item_id(item, type, workspace_id)
    item_type_url = item_types.get(type)[1]
    path = item_types.get(type)[2]

    url = f"/v1/workspaces/{workspace_id}/{item_type_url}/{item_id}/getDefinition"
    if format:
        url += f"?format={format}"

    result = _base_api(
        request=url,
        method="post",
        status_codes=None,
        lro_return_json=True,
    )

    if return_dataframe:
        return pd.json_normalize(result["definition"]["parts"])

    value = next(
        p.get("payload") for p in result["definition"]["parts"] if p.get("path") == path
    )
    if decode:
        json.loads(_decode_b64(value))
    else:
        return value


def resolve_item_id(
    item: str | UUID, type: str, workspace: Optional[str] = None
) -> UUID:

    if _is_valid_uuid(item):
        return item
    else:
        return fabric.resolve_item_id(item_name=item, type=type, workspace=workspace)


def resolve_item_name_and_id(
    item: str | UUID, type: Optional[str] = None, workspace: Optional[str | UUID] = None
) -> Tuple[str, UUID]:

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    if _is_valid_uuid(item):
        item_id = item
        item_name = fabric.resolve_item_name(
            item_id=item_id, type=type, workspace=workspace_id
        )
    else:
        if type is None:
            raise ValueError(
                f"{icons.warning} Must specify a 'type' if specifying a name as the 'item'."
            )
        item_name = item
        item_id = fabric.resolve_item_id(
            item_name=item, type=type, workspace=workspace_id
        )

    return item_name, item_id


def resolve_lakehouse_name_and_id(
    lakehouse: Optional[str | UUID] = None, workspace: Optional[str | UUID] = None
) -> Tuple[str, UUID]:

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    type = "Lakehouse"

    if lakehouse is None:
        lakehouse_id = fabric.get_lakehouse_id()
        lakehouse_name = fabric.resolve_item_name(
            item_id=lakehouse_id, type=type, workspace=workspace_id
        )
    elif _is_valid_uuid(lakehouse):
        lakehouse_id = lakehouse
        lakehouse_name = fabric.resolve_item_name(
            item_id=lakehouse_id, type=type, workspace=workspace_id
        )
    else:
        lakehouse_name = lakehouse
        lakehouse_id = fabric.resolve_item_id(
            item_name=lakehouse, type=type, workspace=workspace_id
        )

    return lakehouse_name, lakehouse_id


def resolve_dataset_name_and_id(
    dataset: str | UUID, workspace: Optional[str | UUID] = None
) -> Tuple[str, UUID]:

    (dataset_name, dataset_id) = resolve_item_name_and_id(
        item=dataset, type="SemanticModel", workspace=workspace
    )

    return dataset_name, dataset_id


def resolve_dataset_id(
    dataset: str | UUID, workspace: Optional[str | UUID] = None
) -> UUID:
    """
    Obtains the ID of the semantic model.

    Parameters
    ----------
    dataset : str | uuid.UUID
        The name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    uuid.UUID
        The ID of the semantic model.
    """

    return resolve_item_id(item=dataset, type="SemanticModel", workspace=workspace)


def resolve_dataset_name(
    dataset_id: UUID, workspace: Optional[str | UUID] = None
) -> str:
    """
    Obtains the name of the semantic model.

    Parameters
    ----------
    dataset_id : uuid.UUID
        The name of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    str
        The name of the semantic model.
    """

    return fabric.resolve_item_name(
        item_id=dataset_id, type="SemanticModel", workspace=workspace
    )


def resolve_lakehouse_name(
    lakehouse_id: Optional[UUID] = None, workspace: Optional[str | UUID] = None
) -> str:
    """
    Obtains the name of the Fabric lakehouse.

    Parameters
    ----------
    lakehouse_id : uuid.UUID, default=None
        The name of the Fabric lakehouse.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    str
        The name of the Fabric lakehouse.
    """

    if lakehouse_id is None:
        lakehouse_id = fabric.get_lakehouse_id()

    return fabric.resolve_item_name(
        item_id=lakehouse_id, type="Lakehouse", workspace=workspace
    )


def resolve_lakehouse_id(
    lakehouse: Optional[str | UUID] = None, workspace: Optional[str | UUID] = None
) -> UUID:
    """
    Obtains the ID of the Fabric lakehouse.

    Parameters
    ----------
    lakehouse : str | uuid.UUID, default=None
        The name or ID of the Fabric lakehouse.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    uuid.UUID
        The ID of the Fabric lakehouse.
    """

    if lakehouse is None:
        lakehouse_id = fabric.get_lakehouse_id()
    elif _is_valid_uuid(lakehouse):
        lakehouse_id = lakehouse
    else:
        lakehouse_id = fabric.resolve_item_id(
            item_name=lakehouse, type="Lakehouse", workspace=workspace
        )

    return lakehouse_id


def get_direct_lake_sql_endpoint(
    dataset: str | UUID, workspace: Optional[str | UUID] = None
) -> UUID:
    """
    Obtains the SQL Endpoint ID of the semantic model.

    Parameters
    ----------
    dataset : str | uuid.UUID
        The name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    uuid.UUID
        The ID of SQL Endpoint.
    """

    from sempy_labs.tom import connect_semantic_model

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
            raise ValueError(f"{icons.red_dot} SQL Endpoint not found.")

        return sqlEndpointId


def generate_embedded_filter(filter: str) -> str:
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
    merge_schema: bool = False,
    schema: Optional[dict] = None,
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
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
    schema : dict, default=None
        A dictionary showing the schema of the columns and their data types.
    lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    from pyspark.sql.types import (
        StringType,
        IntegerType,
        FloatType,
        DateType,
        StructType,
        StructField,
        BooleanType,
        LongType,
        DoubleType,
        TimestampType,
    )

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (lakehouse_name, lakehouse_id) = resolve_lakehouse_name_and_id(
        lakehouse=lakehouse, workspace=workspace_id
    )

    write_modes = ["append", "overwrite"]
    write_mode = write_mode.lower()

    if write_mode not in write_modes:
        raise ValueError(
            f"{icons.red_dot} Invalid 'write_type' parameter. Choose from one of the following values: {write_modes}."
        )

    if " " in delta_table_name:
        raise ValueError(
            f"{icons.red_dot} Invalid 'delta_table_name'. Delta tables in the lakehouse cannot have spaces in their names."
        )

    spark = _create_spark_session()

    type_mapping = {
        "string": StringType(),
        "str": StringType(),
        "integer": IntegerType(),
        "int": IntegerType(),
        "float": FloatType(),
        "date": DateType(),
        "bool": BooleanType(),
        "boolean": BooleanType(),
        "long": LongType(),
        "double": DoubleType(),
        "timestamp": TimestampType(),
    }

    if isinstance(dataframe, pd.DataFrame):
        dataframe.columns = [col.replace(" ", "_") for col in dataframe.columns]
        if schema is None:
            spark_df = spark.createDataFrame(dataframe)
        else:
            schema_map = StructType(
                [
                    StructField(column_name, type_mapping[data_type], True)
                    for column_name, data_type in schema.items()
                ]
            )
            spark_df = spark.createDataFrame(dataframe, schema_map)
    else:
        for col_name in dataframe.columns:
            new_name = col_name.replace(" ", "_")
            dataframe = dataframe.withColumnRenamed(col_name, new_name)
        spark_df = dataframe

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
        f"{icons.green_dot} The dataframe has been saved as the '{delta_table_name}' table in the '{lakehouse_name}' lakehouse within the '{workspace_name}' workspace."
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


def resolve_workspace_name_and_id(
    workspace: Optional[str | UUID] = None,
) -> Tuple[str, str]:
    """
    Obtains the name and ID of the Fabric workspace.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    str, uuid.UUID
        The name and ID of the Fabric workspace.
    """

    if workspace is None:
        workspace_id = fabric.get_workspace_id()
        workspace_name = fabric.resolve_workspace_name(workspace_id)
    elif _is_valid_uuid(workspace):
        workspace_id = workspace
        workspace_name = fabric.resolve_workspace_name(workspace_id)
    else:
        workspace_name = workspace
        workspace_id = fabric.resolve_workspace_id(workspace_name)

    return str(workspace_name), str(workspace_id)


def _extract_json(dataframe: pd.DataFrame) -> dict:

    payload = dataframe["payload"].iloc[0]
    json_file = _decode_b64(payload)

    return json.loads(json_file)


def _conv_b64(file):

    loadJson = json.dumps(file)
    f = base64.b64encode(loadJson.encode("utf-8")).decode("utf-8")

    return f


def _decode_b64(file, format: Optional[str] = "utf-8"):

    return base64.b64decode(file).decode(format)


def is_default_semantic_model(
    dataset: str, workspace: Optional[str | UUID] = None
) -> bool:
    """
    Identifies whether a semantic model is a default semantic model.

    Parameters
    ----------
    dataset : str
        The name of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    bool
        A True/False value indicating whether the semantic model is a default semantic model.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    dfI = fabric.list_items(workspace=workspace_id)
    filtered_df = dfI.groupby("Display Name").filter(
        lambda x: set(["Warehouse", "SemanticModel"]).issubset(set(x["Type"]))
        or set(["Lakehouse", "SemanticModel"]).issubset(set(x["Type"]))
    )
    default_semantic_models = filtered_df["Display Name"].unique().tolist()

    return dataset in default_semantic_models


def resolve_item_type(item_id: UUID, workspace: Optional[str | UUID] = None) -> str:
    """
    Obtains the item type for a given Fabric Item Id within a Fabric workspace.

    Parameters
    ----------
    item_id : uuid.UUID
        The item/artifact Id.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    str
        The item type for the item Id.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    dfI = fabric.list_items(workspace=workspace_id)
    dfI_filt = dfI[dfI["Id"] == item_id]

    if dfI_filt.empty:
        raise ValueError(
            f"{icons.red_dot} Invalid 'item_id' parameter. The '{item_id}' item was not found in the '{workspace_name}' workspace."
        )
    return dfI_filt["Type"].iloc[0]


def resolve_dataset_from_report(
    report: str | UUID, workspace: Optional[str | UUID] = None
) -> Tuple[UUID, str, UUID, str]:
    """
    Obtains the basic semantic model properties from which the report's data is sourced.

    Parameters
    ----------
    report : str | uuid.UUID
        The name or ID of the Power BI report.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    Tuple[uuid.UUID, str, uuid.UUID, str]
        The semantic model UUID, semantic model name, semantic model workspace UUID, semantic model workspace name
    """

    from sempy_labs.report._generate_report import _get_report

    dfR = _get_report(report=report, workspace=workspace)
    dataset_id = dfR["Dataset Id"].iloc[0]
    dataset_workspace_id = dfR["Dataset Workspace Id"].iloc[0]
    dataset_workspace = fabric.resolve_workspace_name(dataset_workspace_id)
    dataset_name = resolve_dataset_name(
        dataset_id=dataset_id, workspace=dataset_workspace
    )

    return dataset_id, dataset_name, dataset_workspace_id, dataset_workspace


def _add_part(target_dict, path, payload):

    part = {"path": path, "payload": payload, "payloadType": "InlineBase64"}

    target_dict["definition"]["parts"].append(part)


def resolve_workspace_capacity(
    workspace: Optional[str | UUID] = None,
) -> Tuple[UUID, str]:
    """
    Obtains the capacity Id and capacity name for a given workspace.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or UUID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    Tuple[uuid.UUID, str]
        capacity Id; capacity came.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    filter_condition = urllib.parse.quote(workspace_id)
    dfW = fabric.list_workspaces(filter=f"id eq '{filter_condition}'")
    capacity_id = dfW["Capacity Id"].iloc[0]
    dfC = fabric.list_capacities()
    dfC_filt = dfC[dfC["Id"] == capacity_id]
    if len(dfC_filt) == 1:
        capacity_name = dfC_filt["Display Name"].iloc[0]
    else:
        capacity_name = None

    return capacity_id, capacity_name


def get_capacity_id(workspace: Optional[str | UUID] = None) -> UUID:
    """
    Obtains the Capacity Id for a given workspace.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    uuid.UUID
        The capacity Id.
    """

    if workspace is None:
        capacity_id = _get_fabric_context_setting(name="trident.capacity.id")
    else:
        (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
        filter_condition = urllib.parse.quote(workspace_id)
        dfW = fabric.list_workspaces(filter=f"id eq '{filter_condition}'")
        if len(dfW) == 0:
            raise ValueError(f"{icons.red_dot} The '{workspace_name}' does not exist'.")

        capacity_id = dfW["Capacity Id"].iloc[0]

    return capacity_id


def get_capacity_name(workspace: Optional[str | UUID] = None) -> str:
    """
    Obtains the capacity name for a given workspace.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
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
    if dfC_filt.empty:
        raise ValueError(
            f"{icons.red_dot} The '{capacity_id}' capacity Id does not exist."
        )

    return dfC_filt["Display Name"].iloc[0]


def resolve_capacity_name(capacity_id: Optional[UUID] = None) -> str:
    """
    Obtains the capacity name for a given capacity Id.

    Parameters
    ----------
    capacity_id : uuid.UUID, default=None
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

    if dfC_filt.empty:
        raise ValueError(
            f"{icons.red_dot} The '{capacity_id}' capacity Id does not exist."
        )

    return dfC_filt["Display Name"].iloc[0]


def resolve_capacity_id(capacity: Optional[str | UUID] = None, **kwargs) -> UUID:
    """
    Obtains the capacity Id for a given capacity name.

    Parameters
    ----------
    capacity : str | uuid.UUID, default=None
        The capacity name or ID.
        Defaults to None which resolves to the capacity id of the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the capacity name of the workspace of the notebook.

    Returns
    -------
    uuid.UUID
        The capacity Id.
    """

    if "capacity_name" in kwargs:
        capacity = kwargs["capacity_name"]
        print(
            f"{icons.warning} The 'capacity_name' parameter is deprecated. Please use 'capacity' instead."
        )

    if capacity is None:
        return get_capacity_id()
    if _is_valid_uuid(capacity):
        return capacity

    dfC = fabric.list_capacities()
    dfC_filt = dfC[dfC["Display Name"] == capacity]

    if dfC_filt.empty:
        raise ValueError(f"{icons.red_dot} The '{capacity}' capacity does not exist.")

    return dfC_filt["Id"].iloc[0]


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
    return_status_code: bool = False,
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


def resolve_deployment_pipeline_id(deployment_pipeline: str | UUID) -> UUID:
    """
    Obtains the Id for a given deployment pipeline.

    Parameters
    ----------
    deployment_pipeline : str | uuid.UUID
        The deployment pipeline name or ID.

    Returns
    -------
    uuid.UUID
        The deployment pipeline Id.
    """

    from sempy_labs._deployment_pipelines import list_deployment_pipelines

    if _is_valid_uuid(deployment_pipeline):
        return deployment_pipeline
    else:

        dfP = list_deployment_pipelines()
        dfP_filt = dfP[dfP["Deployment Pipeline Name"] == deployment_pipeline]
        if len(dfP_filt) == 0:
            raise ValueError(
                f"{icons.red_dot} The '{deployment_pipeline}' deployment pipeline is not valid."
            )
        return dfP_filt["Deployment Pipeline Id"].iloc[0]


class FabricTokenCredential(TokenCredential):

    def get_token(
        self,
        scopes: str,
        claims: Optional[str] = None,
        tenant_id: Optional[str] = None,
        enable_cae: bool = False,
        **kwargs: any,
    ) -> AccessToken:

        import notebookutils

        token = notebookutils.credentials.getToken(scopes)
        access_token = AccessToken(token, 0)

        return access_token


def _get_adls_client(account_name):

    from azure.storage.filedatalake import DataLakeServiceClient

    account_url = f"https://{account_name}.dfs.core.windows.net"

    service_client = DataLakeServiceClient(
        account_url, credential=FabricTokenCredential()
    )

    return service_client


def resolve_warehouse_id(
    warehouse: str | UUID, workspace: Optional[str | UUID]
) -> UUID:
    """
    Obtains the Id for a given warehouse.

    Parameters
    ----------
    warehouse : str | uuid.UUID
        The warehouse name or ID.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID in which the semantic model resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    uuid.UUID
        The warehouse Id.
    """

    return resolve_item_id(item=warehouse, type="Warehouse", workspace=workspace)


def get_language_codes(languages: str | List[str]):

    if isinstance(languages, str):
        languages = [languages]

    for i, lang in enumerate(languages):
        for k, v in icons.language_map.items():
            if v == lang.capitalize():
                languages[i] = k
                break

    return languages


def _get_azure_token_credentials(
    key_vault_uri: str,
    key_vault_tenant_id: str,
    key_vault_client_id: str,
    key_vault_client_secret: str,
    audience: str = "https://management.azure.com/.default",
) -> Tuple[str, str, dict]:

    import notebookutils
    from azure.identity import ClientSecretCredential

    # "https://analysis.windows.net/powerbi/api/.default"

    tenant_id = notebookutils.credentials.getSecret(key_vault_uri, key_vault_tenant_id)
    client_id = notebookutils.credentials.getSecret(key_vault_uri, key_vault_client_id)
    client_secret = notebookutils.credentials.getSecret(
        key_vault_uri, key_vault_client_secret
    )

    credential = ClientSecretCredential(
        tenant_id=tenant_id, client_id=client_id, client_secret=client_secret
    )

    token = credential.get_token(audience).token

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    return token, credential, headers


def convert_to_alphanumeric_lowercase(input_string):

    # Removes non-alphanumeric characters
    cleaned_string = re.sub(r"[^a-zA-Z0-9]", "", input_string)
    cleaned_string = cleaned_string.lower()

    return cleaned_string


def resolve_environment_id(
    environment: str | UUID, workspace: Optional[str | UUID] = None
) -> UUID:
    """
    Obtains the environment Id for a given environment.

    Parameters
    ----------
    environment: str | uuid.UUID
        Name of the environment.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID in which the semantic model resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    uuid.UUID
        The environment Id.
    """

    return resolve_item_id(item=environment, type="Environment", workspace=workspace)


def _make_clickable(val):

    return f'<a target="_blank" href="{val}">{val}</a>'


def convert_to_friendly_case(text: str) -> str:
    """
    Converts a string of pascal/camel/snake case to business-friendly case.

    Parameters
    ----------
    text : str
        The text to convert.

    Returns
    -------
    str
        Text converted into a business-friendly text.
    """
    if text is not None:
        text = text.replace("_", " ")
        # Insert space before each capital letter, avoiding double spaces
        text = re.sub(r"(?<!\s)(?=[A-Z])", " ", text)
        # Strip leading/trailing whitespace and capitalize the first letter of each word
        text = text.strip().title()

    return text


def resolve_notebook_id(
    notebook: str | UUID, workspace: Optional[str | UUID] = None
) -> UUID:
    """
    Obtains the notebook Id for a given notebook.

    Parameters
    ----------
    notebook: str | uuid.UUID
        Name or ID of the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID in which the semantic model resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    uuid.UUID
        The notebook Id.
    """

    return resolve_item_id(item=notebook, type="Notebook", workspace=workspace)


def generate_guid():

    return str(uuid.uuid4())


def _get_column_aggregate(
    table_name: str,
    column_name: str = "RunId",
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
    function: str = "max",
    default_value: int = 0,
) -> int:

    from pyspark.sql.functions import approx_count_distinct
    from pyspark.sql import functions as F

    function = function.upper()
    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    lakehouse_id = resolve_lakehouse_id(lakehouse, workspace)
    path = create_abfss_path(lakehouse_id, workspace_id, table_name)

    spark = _create_spark_session()
    df = spark.read.format("delta").load(path)

    if function in {"COUNTDISTINCT", "DISTINCTCOUNT"}:
        result = df.select(F.count_distinct(F.col(column_name)))
    elif "APPROX" in function:
        result = df.select(approx_count_distinct(column_name))
    else:
        result = df.selectExpr(f"{function}({column_name})")

    return result.collect()[0][0] or default_value


def _make_list_unique(my_list):

    return list(set(my_list))


def _get_partition_map(
    dataset: str, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:

    partitions = fabric.evaluate_dax(
        dataset=dataset,
        workspace=workspace,
        dax_string="""
    select [ID] AS [PartitionID], [TableID], [Name] AS [PartitionName] from $system.tmschema_partitions
    """,
    )

    tables = fabric.evaluate_dax(
        dataset=dataset,
        workspace=workspace,
        dax_string="""
    select [ID] AS [TableID], [Name] AS [TableName] from $system.tmschema_tables
    """,
    )

    partition_map = pd.merge(partitions, tables, on="TableID", how="left")
    partition_map["PartitionID"] = partition_map["PartitionID"].astype(str)
    partition_counts = partition_map.groupby("TableID")["PartitionID"].transform(
        "count"
    )
    partition_map["Object Name"] = partition_map.apply(
        lambda row: (
            f"'{row['TableName']}'[{row['PartitionName']}]"
            if partition_counts[row.name] > 1
            else row["TableName"]
        ),
        axis=1,
    )
    return partition_map


def _show_chart(spec, title):

    h = f"""
    <!DOCTYPE html>
    <html>
        <head>
            <script src="https://cdn.jsdelivr.net/npm/vega@5"></script>
            <script src="https://cdn.jsdelivr.net/npm/vega-lite@5"></script>
            <script src="https://cdn.jsdelivr.net/npm/vega-embed@6"></script>
            <style>
                table, th, td {{
                border: 10px solid #e7e9eb;
                border-collapse: collapse;
                }}
            </style>
        </head>
        <body>
            <table>
                <tr>
                    <td style="text-align: center;">
                        <h1>{title}</h1>
                    </td>
                </tr>
                <tr>
                    <td>
                        <div id="vis"></div>
                    </td>
                </tr>
            </table>
            <script type="text/javascript">
                var spec = {spec};
                var opt = {{"renderer": "canvas", "actions": false}};
                vegaEmbed("#vis", spec, opt);
            </script>
        </body>
    </html>"""

    display(HTML(h))


def _process_and_display_chart(df, title, widget):

    # Convert time columns to milliseconds
    df["Start"] = df["Start Time"].astype(np.int64) / int(1e6)
    df["End"] = df["End Time"].astype(np.int64) / int(1e6)

    # Calculate the time offset for proper Gantt chart rendering
    Offset = min(df["Start"])
    df["Start"] = df["Start"] - Offset
    df["End"] = df["End"] - Offset

    # Vega-Lite spec for Gantt chart
    spec = (
        """{
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "description": "A simple bar chart with ranged data (aka Gantt Chart).",
        "data": { "values": """
        + df.to_json(orient="records")
        + """ },
        "width": 700,
        "height": 400,
        "mark": "bar",
        "encoding": {
            "y": {
                "field": "Object Name",
                "type": "ordinal",
                "axis": {
                    "labelFontSize": 15,
                    "titleFontSize": 20,
                    "title": "Object"
                }
            },
            "x": {
                "field": "Start",
                "type": "quantitative",
                "title": "milliseconds",
                "axis": {
                    "titleFontSize": 20
                }
            },
            "x2": {"field": "End"},
            "color": {
                "field": "Event Subclass",
                "scale": {
                    "domain": ["Process", "ExecuteSql"],
                    "range": ["#FFC000","#0070C0"]
                },
                "legend": {
                    "labelFontSize": 20,
                    "titleFontSize": 20,
                    "title": "Event Type"
                }
            },
            "tooltip": [
                {"field": "Duration", "type": "quantitative", "format": ","},
                {"field": "Cpu Time", "type": "quantitative", "format": ","},
                {"field": "Event Subclass", "type": "nominal"}
            ]
        }
    }"""
    )

    with widget:
        widget.clear_output(wait=True)
    _show_chart(spec, title=title)


def _convert_data_type(input_data_type: str) -> str:

    data_type_mapping = {
        "string": "String",
        "int": "Int64",
        "tinyint": "Int64",
        "smallint": "Int64",
        "bigint": "Int64",
        "boolean": "Boolean",
        "timestamp": "DateTime",
        "date": "DateTime",
        "double": "Double",
        "float": "Double",
    }

    if "decimal" in input_data_type:
        return "Decimal"
    else:
        return data_type_mapping.get(input_data_type)


def _is_valid_uuid(
    guid: str,
):
    """
    Validates if a string is a valid GUID in version 4

    Parameters
    ----------
    guid : str
        GUID to be validated.

    Returns
    -------
    bool
        Boolean that indicates if the string is a GUID or not.
    """

    try:
        UUID(str(guid), version=4)
        return True
    except ValueError:
        return False


def _get_fabric_context_setting(name: str):

    from synapse.ml.internal_utils.session_utils import get_fabric_context

    return get_fabric_context().get(name)


def get_tenant_id():

    _get_fabric_context_setting(name="trident.tenant.id")


def _base_api(
    request: str,
    client: str = "fabric",
    method: str = "get",
    payload: Optional[str] = None,
    status_codes: Optional[int] = 200,
    uses_pagination: bool = False,
    lro_return_json: bool = False,
    lro_return_status_code: bool = False,
):

    from sempy_labs._authentication import _get_headers

    if (lro_return_json or lro_return_status_code) and status_codes is None:
        status_codes = [200, 202]

    if isinstance(status_codes, int):
        status_codes = [status_codes]

    if client == "fabric":
        c = fabric.FabricRestClient()
    elif client == "fabric_sp":
        c = fabric.FabricRestClient(token_provider=auth.token_provider.get())
    elif client in ["azure", "graph"]:
        pass
    else:
        raise ValueError(f"{icons.red_dot} The '{client}' client is not supported.")

    if client not in ["azure", "graph"]:
        if method == "get":
            response = c.get(request)
        elif method == "delete":
            response = c.delete(request)
        elif method == "post":
            response = c.post(request, json=payload)
        elif method == "patch":
            response = c.patch(request, json=payload)
        elif method == "put":
            response = c.put(request, json=payload)
        else:
            raise NotImplementedError
    else:
        headers = _get_headers(auth.token_provider.get(), audience=client)
        if client == "graph":
            url = f"https://graph.microsoft.com/v1.0/{request}"
        elif client == "azure":
            url = request
        else:
            raise NotImplementedError
        response = requests.request(
            method.upper(),
            url,
            headers=headers,
            json=payload,
        )

    if lro_return_json:
        return lro(c, response, status_codes).json()
    elif lro_return_status_code:
        return lro(c, response, status_codes, return_status_code=True)
    else:
        if response.status_code not in status_codes:
            raise FabricHTTPException(response)
        if uses_pagination:
            responses = pagination(c, response)
            return responses
        else:
            return response


def _create_dataframe(columns: dict) -> pd.DataFrame:

    return pd.DataFrame(columns=list(columns.keys()))


def _update_dataframe_datatypes(dataframe: pd.DataFrame, column_map: dict):
    """
    Updates the datatypes of columns in a pandas dataframe based on a column map.

    Example:
    {
        "Order": "int",
        "Public": "bool",
    }
    """

    for column, data_type in column_map.items():
        if column in dataframe.columns:
            if data_type == "int":
                dataframe[column] = dataframe[column].astype(int)
            elif data_type == "bool":
                dataframe[column] = dataframe[column].astype(bool)
            elif data_type == "float":
                dataframe[column] = dataframe[column].astype(float)
            elif data_type == "datetime":
                dataframe[column] = pd.to_datetime(dataframe[column])
            # This is for a special case in admin.list_reports where datetime itself does not work. Coerce fixes the issue.
            elif data_type == "datetime_coerce":
                dataframe[column] = pd.to_datetime(dataframe[column], errors="coerce")
            # This is for list_synonyms since the weight column is float and can have NaN values.
            elif data_type == "float_fillna":
                dataframe[column] = dataframe[column].fillna(0).astype(float)
            # This is to avoid NaN values in integer columns (for delta analyzer)
            elif data_type == "int_fillna":
                dataframe[column] = dataframe[column].fillna(0).astype(int)
            elif data_type in ["str", "string"]:
                dataframe[column] = dataframe[column].astype(str)
            else:
                raise NotImplementedError


def _print_success(item_name, item_type, workspace_name, action="created"):
    if action == "created":
        print(
            f"{icons.green_dot} The '{item_name}' {item_type} has been successfully created in the '{workspace_name}' workspace."
        )
    elif action == "deleted":
        print(
            f"{icons.green_dot} The '{item_name}' {item_type} has been successfully deleted from the '{workspace_name}' workspace."
        )
    else:
        raise NotImplementedError


def _pure_python_notebook() -> bool:

    from sempy.fabric._environment import _on_jupyter

    return _on_jupyter()


def _create_spark_session():

    if _pure_python_notebook():
        raise ValueError(
            f"{icons.red_dot} This function is only available in a PySpark notebook."
        )

    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def _read_delta_table(path: str):

    spark = _create_spark_session()

    return spark.read.format("delta").load(path)


def _delta_table_row_count(table_name: str) -> int:

    spark = _create_spark_session()

    return spark.table(table_name).count()


def _run_spark_sql_query(query):

    spark = _create_spark_session()

    return spark.sql(query)


def _mount(lakehouse, workspace) -> str:
    """
    Mounts a lakehouse to a notebook if it is not already mounted. Returns the local path to the lakehouse.
    """

    import notebookutils

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace=workspace)
    (lakehouse_name, lakehouse_id) = resolve_lakehouse_name_and_id(
        lakehouse=lakehouse, workspace=workspace
    )

    lake_path = create_abfss_path(lakehouse_id, workspace_id)
    mounts = notebookutils.fs.mounts()
    mount_point = f"/{workspace_name.replace(' ', '')}{lakehouse_name.replace(' ', '')}"
    if not any(i.get("source") == lake_path for i in mounts):
        # Mount lakehouse if not mounted
        notebookutils.fs.mount(lake_path, mount_point)
        print(
            f"{icons.green_dot} Mounted the '{lakehouse_name}' lakehouse within the '{workspace_name}' to the notebook."
        )

    mounts = notebookutils.fs.mounts()
    local_path = next(
        i.get("localPath") for i in mounts if i.get("source") == lake_path
    )

    return local_path
