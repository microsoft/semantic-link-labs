import sempy.fabric as fabric
import numpy as np
import pandas as pd
from typing import Optional, List, Union, Tuple
from uuid import UUID
import sempy_labs._icons as icons
from sempy._utils._log import log
from sempy_labs._helper_functions import retry, resolve_dataset_id


def check_fallback_reason(
    dataset: str, workspace: Optional[str] = None
) -> pd.DataFrame:
    """
    Shows the reason a table in a Direct Lake semantic model would fallback to DirectQuery.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        The tables in the semantic model and their fallback reason.
    """

    workspace = fabric.resolve_workspace_name(workspace)

    dfP = fabric.list_partitions(dataset=dataset, workspace=workspace)
    dfP_filt = dfP[dfP["Mode"] == "DirectLake"]

    if len(dfP_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{dataset}' semantic model is not in Direct Lake. This function is only applicable to Direct Lake semantic models."
        )

    df = fabric.evaluate_dax(
        dataset=dataset,
        workspace=workspace,
        dax_string="""
            SELECT [TableName] AS [Table Name],[FallbackReason] AS [FallbackReasonID]
            FROM $SYSTEM.TMSCHEMA_DELTA_TABLE_METADATA_STORAGES
            """,
    )

    value_mapping = {
        0: "No reason for fallback",
        1: "This table is not framed",
        2: "This object is a view in the lakehouse",
        3: "The table does not exist in the lakehouse",
        4: "Transient error",
        5: "Using OLS will result in fallback to DQ",
        6: "Using RLS will result in fallback to DQ",
    }

    # Create a new column based on the mapping
    df["Fallback Reason Detail"] = np.vectorize(value_mapping.get)(
        df["FallbackReasonID"]
    )

    return df


@log
def generate_direct_lake_semantic_model(
    dataset: str,
    lakehouse_tables: Union[str, List[str]],
    workspace: Optional[str] = None,
    lakehouse: Optional[str] = None,
    lakehouse_workspace: Optional[str] = None,
    overwrite: Optional[bool] = False,
    refresh: Optional[bool] = True,
):
    """
    Dynamically generates a Direct Lake semantic model based on tables in a Fabric lakehouse.

    Parameters
    ----------
    dataset : str
        Name of the semantic model to be created.
    lakehouse_tables : str | List[str]
        The table(s) within the Fabric lakehouse to add to the semantic model. All columns from these tables will be added to the semantic model.
    workspace : str, default=None
        The Fabric workspace name in which the semantic model will reside.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    lakehouse : str, default=None
        The lakehouse which stores the delta tables which will feed the Direct Lake semantic model.
        Defaults to None which resolves to the attached lakehouse.
    lakehouse_workspace : str, default=None
        The Fabric workspace in which the lakehouse resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    overwrite : bool, default=False
        If set to True, overwrites the existing semantic model if it already exists.
    refresh: bool, default=True
        If True, refreshes the newly created semantic model after it is created.

    Returns
    -------
    """

    from sempy_labs.lakehouse import get_lakehouse_tables, get_lakehouse_columns
    from sempy_labs import create_blank_semantic_model, refresh_semantic_model
    from sempy_labs.tom import connect_semantic_model
    from sempy_labs.directlake import get_shared_expression

    if isinstance(lakehouse_tables, str):
        lakehouse_tables = [lakehouse_tables]

    dfLT = get_lakehouse_tables(lakehouse=lakehouse, workspace=lakehouse_workspace)

    # Validate lakehouse tables
    for t in lakehouse_tables:
        if t not in dfLT["Table Name"].values:
            raise ValueError(
                f"{icons.red_dot} The '{t}' table does not exist as a delta table in the '{lakehouse}' within the '{workspace}' workspace."
            )

    dfLC = get_lakehouse_columns(lakehouse=lakehouse, workspace=lakehouse_workspace)
    expr = get_shared_expression(lakehouse=lakehouse, workspace=lakehouse_workspace)
    dfD = fabric.list_datasets(workspace=workspace)
    dfD_filt = dfD[dfD["Dataset Name"] == dataset]
    dfD_filt_len = len(dfD_filt)

    if dfD_filt_len > 0 and overwrite is False:
        raise ValueError(
            f"{icons.red_dot} The '{dataset}' semantic model within the '{workspace}' workspace already exists. Overwrite is set to False so the new semantic model has not been created."
        )
    if dfD_filt_len > 0 and overwrite:
        print(
            f"{icons.warning} Overwriting the existing '{dataset}' semantic model within the '{workspace}' workspace."
        )

    create_blank_semantic_model(dataset=dataset, workspace=workspace)

    @retry(
        sleep_time=1,
        timeout_error_message=f"{icons.red_dot} Function timed out after 1 minute",
    )
    def dyn_connect():
        with connect_semantic_model(
            dataset=dataset, readonly=True, workspace=workspace
        ) as tom:

            tom.model

    dyn_connect()

    expression_name = "DatabaseQuery"
    with connect_semantic_model(
        dataset=dataset, workspace=workspace, readonly=False
    ) as tom:
        if not any(e.Name == expression_name for e in tom.model.Expressions):
            tom.add_expression(name=expression_name, expression=expr)

        for t in lakehouse_tables:
            tom.add_table(name=t)
            tom.add_entity_partition(table_name=t, entity_name=t)
            dfLC_filt = dfLC[dfLC["Table Name"] == t]
            for i, r in dfLC_filt.iterrows():
                lakeCName = r["Column Name"]
                dType = r["Data Type"]
                dt = icons.data_type_mapping.get(dType)
                tom.add_data_column(
                    table_name=t,
                    column_name=lakeCName,
                    source_column=lakeCName,
                    data_type=dt,
                )

    if refresh:
        refresh_semantic_model(dataset=dataset, workspace=workspace)


def get_direct_lake_source(
    dataset: str, workspace: Optional[str] = None
) -> Tuple[str, str, UUID, UUID]:
    """
    Obtains the source information for a direct lake semantic model.

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
    Tuple[str, str, UUID, UUID]
        If the source of the direct lake semantic model is a lakehouse this will return: 'Lakehouse', Lakehouse Name, SQL Endpoint Id, Workspace Id
        If the source of the direct lake semantic model is a warehouse this will return: 'Warehouse', Warehouse Name, Warehouse Id, Workspace Id
        If the semantic model is not a Direct Lake semantic model, it will return None, None, None.
    """

    workspace = fabric.resolve_workspace_name(workspace)
    dataset_id = resolve_dataset_id(dataset, workspace)
    client = fabric.PowerBIRestClient()
    request_body = {
        "artifacts": [
            {
                "objectId": dataset_id,
                "type": "dataset",
            }
        ]
    }
    response = client.post(
        "metadata/relations/upstream?apiVersion=3", json=request_body
    )
    artifacts = response.json().get("artifacts", [])
    sql_id, sql_object_name, sql_workspace_id, artifact_type = None, None, None, None

    for artifact in artifacts:
        object_type = artifact.get("typeName")
        display_name = artifact.get("displayName")
        if object_type in ["Datawarehouse", "Lakewarehouse"]:
            artifact_type = (
                "Warehouse" if object_type == "Datawarehouse" else "Lakehouse"
            )
            sql_id = artifact.get("objectId")
            sql_workspace_id = artifact.get("workspace", {}).get("objectId")
            sql_object_name = display_name
            break

    return artifact_type, sql_object_name, sql_id, sql_workspace_id
