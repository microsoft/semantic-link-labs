import sempy
import sempy.fabric as fabric
import pandas as pd
import re
from sempy_labs.lakehouse._get_lakehouse_tables import get_lakehouse_tables
from sempy_labs._helper_functions import (
    retry,
    generate_guid,
    save_as_delta_table,
    resolve_lakehouse_name_and_id,
    resolve_workspace_name_and_id,
)
from typing import Optional
from sempy._utils._log import log
from sempy_labs._helper_functions import _quiet_if_not_verbose
import sempy_labs._icons as icons
from uuid import UUID


def _uses_sql_endpoint(tom) -> bool:
    """
    Identifies whether a semantic model is a Direct Lake on SQL semantic model (i.e. any of its
    Direct Lake expressions uses the SQL endpoint). Models without a Direct Lake expression
    (e.g. a blank model) are not considered Direct Lake on SQL.
    """

    return any(v[2] for v in tom._get_direct_lake_expressions().values())


@log
def migrate_calc_tables_to_lakehouse(
    dataset: str,
    new_dataset: str,
    workspace: Optional[str | UUID] = None,
    new_dataset_workspace: Optional[str | UUID] = None,
    lakehouse: Optional[str | UUID] = None,
    lakehouse_workspace: Optional[str | UUID] = None,
):
    """
    This function is deprecated for Direct Lake on OneLake semantic models.

    `Calculated tables <https://learn.microsoft.com/fabric/fundamentals/direct-lake-overview#considerations-and-limitations>`_ are supported in
    Direct Lake on OneLake semantic models (preview), so they no longer need to be stored as delta tables in a lakehouse (nor their DAX expressions
    stored as model annotations). Use :func:`sempy_labs.migration.migrate_calc_tables_to_semantic_model` to migrate calculated tables
    to the Direct Lake semantic model as calculated tables. Unless the new semantic model is a Direct Lake on SQL semantic model, this function performs no action.

    Direct Lake on SQL semantic models do not support calculated tables which reference Direct Lake tables. If the new semantic model is a Direct Lake on SQL
    semantic model (i.e. its Direct Lake expression uses the SQL endpoint), this function creates delta tables in your lakehouse based on the DAX expression of each
    calculated table in the import/DirectQuery semantic model and stores the DAX expressions in the new semantic model as model annotations (deprecated).
    Calculated columns within calculated tables are not migrated in this case.

    Parameters
    ----------
    dataset : str
        Name of the import/DirectQuery semantic model.
    new_dataset : str
        Name of the Direct Lake semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name in which the import/DirectQuery semantic model exists.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    new_dataset_workspace : str | uuid.UUID
        The Fabric workspace name in which the Direct Lake semantic model will be created.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse used by the Direct Lake semantic model. Only used for Direct Lake on SQL semantic models.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    lakehouse_workspace : str | uuid.UUID, default=None
        The Fabric workspace used by the lakehouse. Only used for Direct Lake on SQL semantic models.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """
    from sempy_labs.tom import connect_semantic_model

    if dataset == new_dataset:
        raise ValueError(
            f"{icons.red_dot} The 'dataset' and 'new_dataset' parameters are both set to '{dataset}'. These parameters must be set to different values."
        )

    @retry(
        sleep_time=1,
        timeout_error_message=f"{icons.red_dot} Function timed out after 1 minute",
    )
    def dyn_connect():
        with connect_semantic_model(
            dataset=new_dataset, readonly=True, workspace=new_dataset_workspace
        ) as tom:
            return _uses_sql_endpoint(tom)

    if not dyn_connect():
        print(
            f"{icons.warning} The 'migrate_calc_tables_to_lakehouse' function is deprecated and no longer performs any action for Direct Lake on OneLake semantic models. "
            "Calculated tables are supported in Direct Lake on OneLake semantic models (preview) and are migrated as calculated tables by the "
            "'migrate_calc_tables_to_semantic_model' function."
        )
        return

    print(
        f"{icons.warning} The '{new_dataset}' semantic model is a Direct Lake on SQL semantic model, which does not support calculated tables that reference "
        "Direct Lake tables. Calculated tables are therefore stored as delta tables in the lakehouse (deprecated). "
        "Consider using Direct Lake on OneLake, which supports calculated tables."
    )
    _materialize_calc_tables_in_lakehouse(
        dataset=dataset,
        new_dataset=new_dataset,
        workspace=workspace,
        new_dataset_workspace=new_dataset_workspace,
        lakehouse=lakehouse,
        lakehouse_workspace=lakehouse_workspace,
    )


def _materialize_calc_tables_in_lakehouse(
    dataset: str,
    new_dataset: str,
    workspace: Optional[str | UUID] = None,
    new_dataset_workspace: Optional[str | UUID] = None,
    lakehouse: Optional[str | UUID] = None,
    lakehouse_workspace: Optional[str | UUID] = None,
):
    """
    Creates delta tables in the lakehouse based on the DAX expression of each calculated table in an import/DirectQuery semantic model
    and stores the DAX expressions in the new semantic model as model annotations. Used for Direct Lake on SQL semantic models,
    which do not support calculated tables that reference Direct Lake tables.
    """
    from sempy_labs.tom import connect_semantic_model

    if dataset == new_dataset:
        raise ValueError(
            f"{icons.red_dot} The 'dataset' and 'new_dataset' parameters are both set to '{dataset}'. These parameters must be set to different values."
        )

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    new_dataset_workspace_name, new_dataset_workspace_id = (
        resolve_workspace_name_and_id(new_dataset_workspace)
    )
    lakehouse_workspace_id, lakehouse_workspace_name = resolve_workspace_name_and_id(
        lakehouse_workspace
    )
    lakehouse_name, lakehouse_id = resolve_lakehouse_name_and_id(
        lakehouse, lakehouse_workspace
    )

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
                f"{icons.red_dot} The '{tName}' table already exists as '{dtName}' in the '{lakehouse_name}' lakehouse in the '{lakehouse_workspace_name}' workspace."
            )
            killFunction = True

    if killFunction:
        return

    if len(dfP_filt) == 0:
        print(
            f"{icons.yellow_dot} The '{dataset}' semantic model in the '{workspace_name}' workspace has no calculated tables."
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
                            # Only the calculated table's own columns are
                            # materialized; calculated columns within calculated
                            # tables are not migrated.
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

                                save_as_delta_table(
                                    dataframe=df,
                                    delta_table_name=delta_table_name,
                                    lakehouse=lakehouse,
                                    workspace=lakehouse_workspace,
                                    write_mode="overwrite",
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
                                    f"in the '{lakehouse_name}' lakehouse within the '{lakehouse_workspace_name}' workspace."
                                )
                            except Exception as e:
                                print(
                                    f"{icons.red_dot} Failed to create calculated table '{t.Name}' as a delta table in the lakehouse."
                                )
                                print(e)


@_quiet_if_not_verbose
@log
def migrate_field_parameters(
    dataset: str,
    new_dataset: str,
    workspace: Optional[str | UUID] = None,
    new_dataset_workspace: Optional[str | UUID] = None,
):
    """
    Migrates field parameters from one semantic model to another.

    References to calculated columns which do not exist in the new semantic model (i.e. were not migrated) are removed from the field parameters.

    Parameters
    ----------
    dataset : str
        Name of the import/DirectQuery semantic model.
    new_dataset : str
        Name of the Direct Lake semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name in which the import/DirectQuery semantic model exists.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    new_dataset_workspace : str | uuid.UUID, default=None
        The Fabric workspace name in which the Direct Lake semantic model will be created.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    from sempy_labs import format_dax_object_name
    from sempy_labs.tom import connect_semantic_model

    sempy.fabric._client._utils._init_analysis_services()
    import Microsoft.AnalysisServices.Tabular as TOM

    icons.sll_tags.append("DirectLakeMigration")
    fabric.refresh_tom_cache(workspace=workspace)
    new_dataset_workspace_name, new_dataset_workspace_id = (
        resolve_workspace_name_and_id(new_dataset_workspace)
    )

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

        # Calculated columns are supported in Direct Lake (with the 'UserContext'
        # expression context), so only references to calculated columns which
        # were not migrated to the new semantic model are removed.
        new_columns = {
            format_dax_object_name(c.Parent.Name, c.Name) for c in tom.all_columns()
        }
        missing_calc_columns = [
            obj
            for obj in dfC_CalcColumn["Column Object"].values
            if obj not in new_columns
        ]

        for i, r in dfP_filt.iterrows():
            tName = r["Table Name"]
            query = r["Query"]

            # For field parameters, remove non-migrated calc columns from the query
            rows = query.strip().split("\n")
            filtered_rows = [
                row
                for row in rows
                if not any(value in row for value in missing_calc_columns)
            ]
            updated_query_string = "\n".join(filtered_rows)

            # Remove extra comma
            lines = updated_query_string.strip().split("\n")
            lines[-2] = lines[-2].rstrip(",")
            expr = "\n".join(lines)

            try:
                par = TOM.Partition()
                par.Name = tName
                par.Mode = TOM.ModeType.Import

                parSource = TOM.CalculatedPartitionSource()
                par.Source = parSource
                parSource.Expression = expr

                tbl = TOM.Table()
                tbl.Name = tName
                tbl.LineageTag = generate_guid()
                tbl.Partitions.Add(par)

                columns = ["Value1", "Value2", "Value3"]

                for colName in columns:
                    col = TOM.CalculatedTableColumn()
                    col.Name = colName
                    col.SourceColumn = "[" + colName + "]"
                    col.DataType = TOM.DataType.String
                    col.LineageTag = generate_guid()

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
                    f"{icons.green_dot} The '{tName}' table has been added as a field parameter to the '{new_dataset}' semantic model in the '{new_dataset_workspace_name}' workspace."
                )
            except Exception:
                print(
                    f"{icons.red_dot} The '{tName}' table has not been added as a field parameter."
                )
