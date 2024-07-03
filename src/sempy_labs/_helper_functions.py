import sempy.fabric as fabric
import re
import pandas as pd
from pyspark.sql import SparkSession
from typing import Optional, Tuple
from uuid import UUID
import sempy_labs._icons as icons


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

    if workspace is None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    dfP = fabric.list_partitions(dataset=dataset, workspace=workspace)
    dfP_filt = dfP[dfP["Mode"] == "DirectLake"]

    if len(dfP_filt) == 0:
        raise ValueError(
            f"The '{dataset}' semantic model in the '{workspace}' workspace is not in Direct Lake mode."
        )

    dfE = fabric.list_expressions(dataset=dataset, workspace=workspace)
    dfE_filt = dfE[dfE["Name"] == "DatabaseQuery"]
    expr = dfE_filt["Expression"].iloc[0]

    matches = re.findall(r'"([^"]*)"', expr)
    sqlEndpointId = matches[1]

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
