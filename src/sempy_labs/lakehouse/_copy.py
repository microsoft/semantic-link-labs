from typing import Optional, List
import sempy.fabric as fabric
from sempy_labs.lakehouse import get_lakehouse_tables
from sempy_labs._helper_functions import (
    create_abfss_path,
    _read_delta_table,
    save_as_delta_table,
)
from uuid import UUID
import sempy_labs._icons as icons


def copy_lakehouse_tables(
    source_lakehouse: Optional[str | UUID] = None,
    source_workspace: Optional[str | UUID] = None,
    source_schema: Optional[str] = None,
    target_lakehouse: Optional[str | UUID] = None,
    target_workspace: Optional[str | UUID] = None,
    target_schema: Optional[str] = None,
    tables: Optional[str | List[str]] = None,
    overwrite: bool = False,
):

    if source_schema or target_schema:
        print(
            f"{icons.warning} Schemas are not yet supported. This function currently only supports setting schema parameters to None."
        )
        return

    source_workspace_id = fabric.resolve_workspace_id(source_workspace)
    source_lakehouse_id = fabric.resolve_item_id(
        source_lakehouse, "Lakehouse", source_workspace_id
    )

    target_workspace_id = fabric.resolve_workspace_id(target_workspace)
    target_lakehouse_id = fabric.resolve_item_id(
        target_lakehouse, "Lakehouse", target_workspace_id
    )

    if source_lakehouse_id == target_lakehouse_id:
        raise ValueError(
            f"{icons.red_dot} Source and Target lakehouses must be different."
        )

    if isinstance(tables, str):
        tables = [tables]

    if tables is None:
        tables = get_lakehouse_tables(
            lakehouse=source_lakehouse, workspace=source_workspace
        )["Table Name"].tolist()

    target_tables = get_lakehouse_tables(
        lakehouse=target_lakehouse, workspace=target_workspace
    )["Table Name"].tolist()

    for table in tables:
        if table in target_tables and not overwrite:
            print(
                f"{icons.warning} The '{table}' table already exists in the target lakehouse. Set the 'overwrite' parameter to True if you want to overwrite pre-existing tables in the target lakehouse."
            )
        else:
            source_path = create_abfss_path(
                lakehouse_id=source_lakehouse_id,
                lakehouse_workspace_id=source_workspace_id,
                schema=source_schema,
                delta_table_name=table,
            )
            df = _read_delta_table(source_path)

            # target_path = create_abfss_path(
            #    lakehouse_id=target_lakehouse_id,
            #    lakehouse_workspace_id=target_workspace_id,
            #    schema=target_schema,
            #    delta_table_name=table,
            # )
            save_as_delta_table(
                dataframe=df,
                delta_table_name=table,
                write_mode="overwrite",
                lakehouse=target_lakehouse_id,
                workspace=target_workspace_id,
            )

            # df.write.format("delta").mode("overwrite").save(target_path)
            print(
                f"{icons.green_dot} The '{table}' table has been copied from the '{source_lakehouse}' lakehouse to the '{target_lakehouse}' lakehouse."
            )
