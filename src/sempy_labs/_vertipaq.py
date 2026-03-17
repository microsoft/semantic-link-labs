import pandas as pd
from typing import Optional, Literal
from uuid import UUID
from sempy._utils._log import log
import sempy_labs.semantic_model._vertipaq_analyzer as vertipaq


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

    return vertipaq.vertipaq_analyzer(
        dataset=dataset,
        workspace=workspace,
        export=export,
        read_stats_from_data=read_stats_from_data,
        export_lakehouse=export_lakehouse,
        export_workspace=export_workspace,
        export_schema=export_schema,
    )


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

    return vertipaq.import_vertipaq_analyzer(
        folder_path=folder_path, file_name=file_name
    )
