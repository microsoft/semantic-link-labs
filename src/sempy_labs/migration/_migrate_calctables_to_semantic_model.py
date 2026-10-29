import sempy.fabric as fabric
from sempy_labs._helper_functions import (
    format_dax_object_name,
    retry,
    _quiet_if_not_verbose,
)
from sempy_labs.tom import connect_semantic_model
from typing import Optional
from sempy._utils._log import log
import sempy_labs._icons as icons
from uuid import UUID


@_quiet_if_not_verbose
@log
def migrate_calc_tables_to_semantic_model(
    dataset: str,
    new_dataset: str,
    workspace: Optional[str | UUID] = None,
    new_dataset_workspace: Optional[str | UUID] = None,
    lakehouse: Optional[str | UUID] = None,
    lakehouse_workspace: Optional[str | UUID] = None,
):
    """
    Migrates the calculated tables of an import/DirectQuery semantic model to the Direct Lake semantic model as calculated tables.

    `Calculated tables <https://learn.microsoft.com/fabric/fundamentals/direct-lake-overview#considerations-and-limitations>`_ (including those which reference
    Direct Lake tables) are supported in Direct Lake on OneLake semantic models (preview). Each calculated table is migrated as is: its DAX expression,
    its columns and its calculated columns. Calculated tables use the Import storage mode, so their calculated columns keep their original expression context
    and the semantic model must be refreshed for the calculated tables to be calculated.
    Field parameters (see :func:`sempy_labs.migration.migrate_field_parameters`) and auto date/time tables are not migrated by this function.

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
    lakehouse : str | uuid.UUID, default=None
        This parameter is no longer used (calculated tables are no longer stored as lakehouse tables) and is kept for backward compatibility.
    lakehouse_workspace : str | uuid.UUID, default=None
        This parameter is no longer used (calculated tables are no longer stored as lakehouse tables) and is kept for backward compatibility.
    """
    import Microsoft.AnalysisServices.Tabular as TOM

    if dataset == new_dataset:
        raise ValueError(
            f"{icons.red_dot} The 'dataset' and 'new_dataset' parameters are both set to '{dataset}'. These parameters must be set to different values."
        )

    fabric.refresh_tom_cache(workspace=workspace)

    icons.sll_tags.append("DirectLakeMigration")

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
        dataset=dataset, readonly=True, workspace=workspace
    ) as tom_old:
        calc_tables = []
        for t in tom_old.model.Tables:
            expr = next(
                (
                    p.Source.Expression
                    for p in t.Partitions
                    if p.SourceType == TOM.PartitionSourceType.Calculated
                ),
                None,
            )
            # Field parameters are migrated by the 'migrate_field_parameters' function.
            if expr is None or "NAMEOF" in expr:
                continue
            if tom_old.is_auto_date_table(table_name=t.Name):
                print(
                    f"{icons.yellow_dot} The '{t.Name}' table is an auto date/time table and is not supported in the Direct Lake migration process. "
                    "Please create a proper Date/Calendar table and use it in your Direct Lake model."
                )
                continue
            calc_tables.append((t, expr))

        if not calc_tables:
            print(
                f"{icons.green_dot} The '{dataset}' semantic model has no calculated tables."
            )
            return

        with connect_semantic_model(
            dataset=new_dataset, readonly=False, workspace=new_dataset_workspace
        ) as tom:
            for t, expr in calc_tables:
                tName = t.Name
                if any(tbl.Name == tName for tbl in tom.model.Tables):
                    print(
                        f"{icons.warning} The '{tName}' table already exists in the '{new_dataset}' semantic model."
                    )
                    continue

                tom.add_calculated_table(
                    name=tName,
                    expression=expr,
                    description=t.Description or None,
                    data_category=t.DataCategory or None,
                    hidden=t.IsHidden,
                )

                for c in t.Columns:
                    common = dict(
                        table_name=tName,
                        column_name=c.Name,
                        data_type=str(c.DataType),
                        format_string=c.FormatString or None,
                        hidden=c.IsHidden,
                        description=c.Description or None,
                        display_folder=c.DisplayFolder or None,
                        data_category=c.DataCategory or None,
                        key=c.IsKey,
                        summarize_by=str(c.SummarizeBy),
                    )
                    if c.Type == TOM.ColumnType.CalculatedTableColumn:
                        tom.add_calculated_table_column(
                            source_column=c.SourceColumn, **common
                        )
                        new_col = tom.model.Tables[tName].Columns[c.Name]
                        new_col.IsNameInferred = c.IsNameInferred
                        new_col.IsDataTypeInferred = c.IsDataTypeInferred
                    elif c.Type == TOM.ColumnType.Calculated:
                        # Calculated tables use the Import storage mode, so their
                        # calculated columns are migrated as is (they do not
                        # require the 'UserContext' expression context).
                        tom.add_calculated_column(expression=c.Expression, **common)
                    else:
                        continue
                    print(
                        f"{icons.green_dot} The {format_dax_object_name(tName, c.Name)} column has been added."
                    )

                print(
                    f"{icons.green_dot} The '{tName}' calculated table has been added to the '{new_dataset}' semantic model."
                )

        print(
            f"\n{icons.green_dot} All viable calculated tables have been added to the model."
        )
