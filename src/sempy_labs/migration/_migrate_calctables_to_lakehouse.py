import sempy
import sempy.fabric as fabric
from sempy_labs._helper_functions import (
    retry,
    generate_guid,
    resolve_workspace_name_and_id,
)
from typing import Optional
from sempy._utils._log import log
from sempy_labs._helper_functions import _quiet_if_not_verbose
import sempy_labs._icons as icons
from uuid import UUID


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
    This function is deprecated and no longer performs any action.

    `Calculated tables <https://learn.microsoft.com/fabric/fundamentals/direct-lake-overview#considerations-and-limitations>`_ are supported in
    Direct Lake on OneLake semantic models (preview), so they no longer need to be stored as delta tables in a lakehouse (nor their DAX expressions
    stored as model annotations). Use :func:`sempy_labs.migration.migrate_calc_tables_to_semantic_model` to migrate calculated tables
    to the Direct Lake semantic model as calculated tables.

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
        The Fabric lakehouse used by the Direct Lake semantic model.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    lakehouse_workspace : str | uuid.UUID, default=None
        The Fabric workspace used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    print(
        f"{icons.warning} The 'migrate_calc_tables_to_lakehouse' function is deprecated and no longer performs any action. "
        "Calculated tables are supported in Direct Lake on OneLake semantic models (preview) and are migrated as calculated tables by the "
        "'migrate_calc_tables_to_semantic_model' function."
    )


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
