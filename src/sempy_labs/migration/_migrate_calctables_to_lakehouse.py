import sempy
import sempy.fabric as fabric
import pandas as pd
import re
from sempy_labs.lakehouse._get_lakehouse_tables import get_lakehouse_tables
from sempy_labs._helper_functions import (
    resolve_lakehouse_name,
    resolve_lakehouse_id,
    create_abfss_path,
    retry,
)
from sempy_labs.tom import connect_semantic_model
from pyspark.sql import SparkSession
from typing import Optional
from sempy._utils._log import log
import sempy_labs._icons as icons


@log
def migrate_calc_tables_to_lakehouse(
    dataset: str,
    new_dataset: str,
    workspace: Optional[str] = None,
    new_dataset_workspace: Optional[str] = None,
    lakehouse: Optional[str] = None,
    lakehouse_workspace: Optional[str] = None,
):
    """
    Creates delta tables in your lakehouse based on the DAX expression of a calculated table in an import/DirectQuery semantic model.
    The DAX expression encapsulating the calculated table logic is stored in the new Direct Lake semantic model as model annotations.

    Parameters
    ----------
    dataset : str
        Name of the import/DirectQuery semantic model.
    new_dataset : str
        Name of the Direct Lake semantic model.
    workspace : str, default=None
        The Fabric workspace name in which the import/DirectQuery semantic model exists.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    new_dataset_workspace : str
        The Fabric workspace name in which the Direct Lake semantic model will be created.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    lakehouse : str, default=None
        The Fabric lakehouse used by the Direct Lake semantic model.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    lakehouse_workspace : str, default=None
        The Fabric workspace used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    workspace = fabric.resolve_workspace_name(workspace)

    if new_dataset_workspace is None:
        new_dataset_workspace = workspace

    if lakehouse_workspace is None:
        lakehouse_workspace = new_dataset_workspace
        lakehouse_workspace_id = fabric.resolve_workspace_id(lakehouse_workspace)
    else:
        lakehouse_workspace_id = fabric.resolve_workspace_id(lakehouse_workspace)

    if lakehouse is None:
        lakehouse_id = fabric.get_lakehouse_id()
        lakehouse = resolve_lakehouse_name(lakehouse_id, lakehouse_workspace)
    else:
        lakehouse_id = resolve_lakehouse_id(lakehouse, lakehouse_workspace)

    dfP = fabric.list_partitions(dataset=dataset, workspace=workspace)
    dfP_filt = dfP[(dfP["Source Type"] == "Calculated")]
    dfP_filt = dfP_filt[
        ~dfP_filt["Query"].str.contains("NAMEOF")
    ]  # Remove field parameters
    # dfC_CalcColumn = dfC[dfC['Type'] == 'Calculated']
    lakeTables = get_lakehouse_tables(lakehouse, lakehouse_workspace)

    # Do not execute the function if lakehouse tables already exist with the same name
    killFunction = False
    for i, r in dfP_filt.iterrows():
        tName = r["Table Name"]
        dtName = tName.replace(" ", "_")

        if dtName in lakeTables["Table Name"].values:
            print(
                f"{icons.red_dot} The '{tName}' table already exists as '{dtName}' in the '{lakehouse}' lakehouse in the '{workspace}' workspace."
            )
            killFunction = True

    if killFunction:
        return

    spark = SparkSession.builder.getOrCreate()

    if len(dfP_filt) == 0:
        print(
            f"{icons.yellow_dot} The '{dataset}' semantic model in the '{workspace}' workspace has no calculated tables."
        )
        return

    with connect_semantic_model(
        dataset=dataset, workspace=workspace, readonly=True
    ) as tom:
        for t in tom.model.Tables:
            if tom.is_auto_date_table(table_name=t.Name):
                print(
                    f"{icons.yellow_dot} The '{t.Name}' table is an auto-datetime table and is not supported in the Direct Lake migration process. "
                    "Please create a proper Date/Calendar table in your lakehoues and use it in your Direct Lake model."
                )
            else:
                for p in t.Partitions:
                    if str(p.SourceType) == "Calculated":
                        query = p.Source.Expression
                        if "NAMEOF" not in query:  # exclude field parameters
                            daxQuery = ""
                            if query.lower().startswith("calendar") and any(
                                str(c.Type) == "Calculated" for c in t.Columns
                            ):
                                daxQuery = f"ADDCOLUMNS(\n{query},"
                                for c in t.Columns:
                                    if str(c.Type) == "Calculated":
                                        expr = c.Expression
                                        expr = expr.replace(f"'{t.Name}'", "").replace(
                                            f"{t.Name}[Date]", "[Date]"
                                        )
                                        expr = expr.replace(
                                            "[MonthNo]", "MONTH([Date])"
                                        ).replace(
                                            "[QuarterNo]",
                                            "INT((MONTH([Date]) + 2) / 3)",
                                        )
                                        daxQuery = f'{daxQuery}\n"{c.Name}",{expr},'
                                daxQuery = "EVALUATE\n" + daxQuery.rstrip(",") + "\n)"
                            else:
                                daxQuery = f"EVALUATE\n{query}"
                            daxQueryTopN = (
                                daxQuery.replace("EVALUATE\n", "EVALUATE\nTOPN(1,")
                                + ")"
                            )

                            try:
                                df = fabric.evaluate_dax(
                                    dataset=dataset,
                                    dax_string=daxQueryTopN,
                                    workspace=workspace,
                                )

                                for col in df.columns:
                                    pattern = r"\[([^\]]+)\]"

                                    matches = re.findall(pattern, col)
                                    new_column_name = matches[0].replace(" ", "")

                                    df.rename(
                                        columns={col: new_column_name},
                                        inplace=True,
                                    )

                                    try:
                                        dataType = next(
                                            str(c.DataType)
                                            for c in tom.model.Tables[t.Name].Columns
                                            if str(c.Type) == "CalculatedTableColumn"
                                            and c.SourceColumn == col
                                        )
                                    except Exception:
                                        dataType = next(
                                            str(c.DataType)
                                            for c in tom.model.Tables[t.Name].Columns
                                            if str(c.Type) == "Calculated"
                                            and c.Name == new_column_name
                                        )

                                    if dataType == "Int64":
                                        df[new_column_name] = df[
                                            new_column_name
                                        ].astype(int)
                                    elif dataType in ["Decimal", "Double"]:
                                        df[new_column_name] = df[
                                            new_column_name
                                        ].astype(float)
                                    elif dataType == "Boolean":
                                        df[new_column_name] = df[
                                            new_column_name
                                        ].astype(bool)
                                    elif dataType == "DateTime":
                                        df[new_column_name] = pd.to_datetime(
                                            df[new_column_name]
                                        )

                                delta_table_name = t.Name.replace(" ", "_").lower()

                                spark_df = spark.createDataFrame(df)
                                filePath = create_abfss_path(
                                    lakehouse_id=lakehouse_id,
                                    lakehouse_workspace_id=lakehouse_workspace_id,
                                    delta_table_name=delta_table_name,
                                )
                                spark_df.write.mode("overwrite").format("delta").save(
                                    filePath
                                )

                                @retry(
                                    sleep_time=1,
                                    timeout_error_message=f"{icons.red_dot} Function timed out after 1 minute",
                                )
                                def dyn_connect():
                                    with connect_semantic_model(
                                        dataset=new_dataset,
                                        readonly=True,
                                        workspace=new_dataset_workspace,
                                    ) as tom2:

                                        tom2.model

                                dyn_connect()

                                with connect_semantic_model(
                                    dataset=new_dataset,
                                    readonly=False,
                                    workspace=new_dataset_workspace,
                                ) as tom2:
                                    tom2.set_annotation(
                                        object=tom2.model,
                                        name=t.Name,
                                        value=daxQuery,
                                    )

                                print(
                                    f"{icons.green_dot} Calculated table '{t.Name}' has been created as delta table '{delta_table_name.lower()}' "
                                    f"in the '{lakehouse}' lakehouse within the '{lakehouse_workspace}' workspace."
                                )
                            except Exception:
                                print(
                                    f"{icons.red_dot} Failed to create calculated table '{t.Name}' as a delta table in the lakehouse."
                                )


@log
def migrate_field_parameters(
    dataset: str,
    new_dataset: str,
    workspace: Optional[str] = None,
    new_dataset_workspace: Optional[str] = None,
):
    """
    Migrates field parameters from one semantic model to another.

    Parameters
    ----------
    dataset : str
        Name of the import/DirectQuery semantic model.
    new_dataset : str
        Name of the Direct Lake semantic model.
    workspace : str, default=None
        The Fabric workspace name in which the import/DirectQuery semantic model exists.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    new_dataset_workspace : str
        The Fabric workspace name in which the Direct Lake semantic model will be created.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    from sempy_labs import format_dax_object_name

    sempy.fabric._client._utils._init_analysis_services()
    import Microsoft.AnalysisServices.Tabular as TOM

    if workspace is None:
        workspace_id = fabric.get_workspace_id()
        workspace = fabric.resolve_workspace_name(workspace_id)

    if new_dataset_workspace is None:
        new_dataset_workspace = workspace

    dfC = fabric.list_columns(dataset=dataset, workspace=workspace)
    dfC["Column Object"] = format_dax_object_name(dfC["Table Name"], dfC["Column Name"])
    dfP = fabric.list_partitions(dataset=dataset, workspace=workspace)
    dfP_filt = dfP[(dfP["Source Type"] == "Calculated")]
    dfP_filt = dfP_filt[
        dfP_filt["Query"].str.contains("NAMEOF")
    ]  # Only field parameters
    dfC_CalcColumn = dfC[dfC["Type"] == "Calculated"]

    if len(dfP_filt) == 0:
        print(
            f"{icons.green_dot} The '{dataset}' semantic model in the '{workspace}' workspace has no field parameters."
        )
        return

    @retry(
        sleep_time=1,
        timeout_error_message=f"{icons.red_dot} Function timed out after 1 minute",
    )
    def dyn_connect():
        with connect_semantic_model(
            dataset=new_dataset, readonly=True, workspace=new_dataset_workspace
        ) as tom:

            tom.model

    dyn_connect()

    with connect_semantic_model(
        dataset=new_dataset, workspace=new_dataset_workspace, readonly=False
    ) as tom:

        for i, r in dfP_filt.iterrows():
            tName = r["Table Name"]
            query = r["Query"]

            # For field parameters, remove calc columns from the query
            rows = query.strip().split("\n")
            filtered_rows = [
                row
                for row in rows
                if not any(
                    value in row for value in dfC_CalcColumn["Column Object"].values
                )
            ]
            updated_query_string = "\n".join(filtered_rows)

            # Remove extra comma
            lines = updated_query_string.strip().split("\n")
            lines[-2] = lines[-2].rstrip(",")
            expr = "\n".join(lines)

            try:
                par = TOM.Partition()
                par.Name = tName

                parSource = TOM.CalculatedPartitionSource()
                par.Source = parSource
                parSource.Expression = expr

                tbl = TOM.Table()
                tbl.Name = tName
                tbl.Partitions.Add(par)

                columns = ["Value1", "Value2", "Value3"]

                for colName in columns:
                    col = TOM.CalculatedTableColumn()
                    col.Name = colName
                    col.SourceColumn = "[" + colName + "]"
                    col.DataType = TOM.DataType.String

                    tbl.Columns.Add(col)

                tom.model.Tables.Add(tbl)

                ep = TOM.JsonExtendedProperty()
                ep.Name = "ParameterMetadata"
                ep.Value = '{"version":3,"kind":2}'

                rcd = TOM.RelatedColumnDetails()
                gpc = TOM.GroupByColumn()
                gpc.GroupingColumn = tom.model.Tables[tName].Columns["Value2"]
                rcd.GroupByColumns.Add(gpc)

                # Update column properties
                tom.model.Tables[tName].Columns["Value2"].IsHidden = True
                tom.model.Tables[tName].Columns["Value3"].IsHidden = True
                tom.model.Tables[tName].Columns["Value3"].DataType = TOM.DataType.Int64
                tom.model.Tables[tName].Columns["Value1"].SortByColumn = (
                    tom.model.Tables[tName].Columns["Value3"]
                )
                tom.model.Tables[tName].Columns["Value2"].SortByColumn = (
                    tom.model.Tables[tName].Columns["Value3"]
                )
                tom.model.Tables[tName].Columns["Value2"].ExtendedProperties.Add(ep)
                tom.model.Tables[tName].Columns["Value1"].RelatedColumnDetails = rcd

                dfC_filt1 = dfC[
                    (dfC["Table Name"] == tName) & (dfC["Source"] == "[Value1]")
                ]
                col1 = dfC_filt1["Column Name"].iloc[0]
                dfC_filt2 = dfC[
                    (dfC["Table Name"] == tName) & (dfC["Source"] == "[Value2]")
                ]
                col2 = dfC_filt2["Column Name"].iloc[0]
                dfC_filt3 = dfC[
                    (dfC["Table Name"] == tName) & (dfC["Source"] == "[Value3]")
                ]
                col3 = dfC_filt3["Column Name"].iloc[0]

                tom.model.Tables[tName].Columns["Value1"].Name = col1
                tom.model.Tables[tName].Columns["Value2"].Name = col2
                tom.model.Tables[tName].Columns["Value3"].Name = col3

                print(
                    f"{icons.green_dot} The '{tName}' table has been added as a field parameter to the '{new_dataset}' semantic model in the '{new_dataset_workspace}' workspace."
                )
            except Exception:
                print(
                    f"{icons.red_dot} The '{tName}' table has not been added as a field parameter."
                )
