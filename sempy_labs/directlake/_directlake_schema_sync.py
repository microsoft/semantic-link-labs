import sempy
import sempy.fabric as fabric
import pandas as pd
from .lakehouse.GetLakehouseColumns import get_lakehouse_columns
from sempy_labs._helper_functions import (
    format_dax_object_name,
    resolve_lakehouse_name,
    get_direct_lake_sql_endpoint,
)
from typing import Optional
from sempy._utils._log import log


@log
def direct_lake_schema_sync(
    dataset: str,
    workspace: Optional[str] = None,
    add_to_model: Optional[bool] = False,
    lakehouse: Optional[str] = None,
    lakehouse_workspace: Optional[str] = None,
):
    """
    Shows/adds columns which exist in the lakehouse but do not exist in the semantic model (only for tables in the semantic model).

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    add_to_model : bool, default=False
        If set to True, columns which exist in the lakehouse but do not exist in the semantic model are added to the semantic model. No new tables are added.
    lakehouse : str, default=None
        The Fabric lakehouse used by the Direct Lake semantic model.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    lakehouse_workspace : str, default=None
        The Fabric workspace used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    sempy.fabric._client._utils._init_analysis_services()
    import Microsoft.AnalysisServices.Tabular as TOM
    import System

    if workspace == None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    if lakehouse_workspace is None:
        lakehouse_workspace = workspace

    if lakehouse == None:
        lakehouse_id = fabric.get_lakehouse_id()
        lakehouse = resolve_lakehouse_name(lakehouse_id, lakehouse_workspace)

    sqlEndpointId = get_direct_lake_sql_endpoint(dataset, workspace)

    dfI = fabric.list_items(workspace=lakehouse_workspace, type="SQLEndpoint")
    dfI_filt = dfI[(dfI["Id"] == sqlEndpointId)]

    if len(dfI_filt) == 0:
        print(
            f"The SQL Endpoint in the '{dataset}' semantic model in the '{workspace} workspace does not point to the '{lakehouse}' lakehouse in the '{lakehouse_workspace}' workspace as specified."
        )
        return

    dfP = fabric.list_partitions(dataset=dataset, workspace=workspace)
    dfP_filt = dfP[dfP["Source Type"] == "Entity"]
    dfC = fabric.list_columns(dataset=dataset, workspace=workspace)
    dfC_filt = dfC[dfC["Table Name"].isin(dfP_filt["Table Name"].values)]
    dfC_filt = pd.merge(
        dfC_filt, dfP_filt[["Table Name", "Query"]], on="Table Name", how="left"
    )
    dfC_filt["Column Object"] = format_dax_object_name(
        dfC_filt["Query"], dfC_filt["Source"]
    )

    lc = get_lakehouse_columns(lakehouse, lakehouse_workspace)
    lc_filt = lc[lc["Table Name"].isin(dfP_filt["Query"].values)]

    mapping = {
        "string": "String",
        "bigint": "Int64",
        "int": "Int64",
        "smallint": "Int64",
        "boolean": "Boolean",
        "timestamp": "DateTime",
        "date": "DateTime",
        "decimal(38,18)": "Decimal",
        "double": "Double",
    }

    tom_server = fabric.create_tom_server(readonly=False, workspace=workspace)
    m = tom_server.Databases.GetByName(dataset).Model
    for i, r in lc_filt.iterrows():
        lakeTName = r["Table Name"]
        lakeCName = r["Column Name"]
        fullColName = r["Full Column Name"]
        dType = r["Data Type"]

        if fullColName not in dfC_filt["Column Object"].values:
            dfL = dfP_filt[dfP_filt["Query"] == lakeTName]
            tName = dfL["Table Name"].iloc[0]
            if add_to_model:
                col = TOM.DataColumn()
                col.Name = lakeCName
                col.SourceColumn = lakeCName
                dt = mapping.get(dType)
                try:
                    col.DataType = System.Enum.Parse(TOM.DataType, dt)
                except:
                    print(
                        f"ERROR: '{dType}' data type is not mapped properly to the semantic model data types."
                    )
                    return

                m.Tables[tName].Columns.Add(col)
                print(
                    f"The '{lakeCName}' column has been added to the '{tName}' table as a '{dt}' data type within the '{dataset}' semantic model within the '{workspace}' workspace."
                )
            else:
                print(
                    f"The {fullColName} column exists in the lakehouse but not in the '{tName}' table in the '{dataset}' semantic model within the '{workspace}' workspace."
                )
        m.SaveChanges()
