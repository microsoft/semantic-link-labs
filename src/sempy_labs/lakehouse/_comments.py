from typing import Optional
from uuid import UUID
from sempy_labs._helper_functions import (
    _create_spark_session,
    _pure_python_notebook,
    create_abfss_path,
    resolve_workspace_id,
    resolve_lakehouse_name_and_id,
)
from sempy._utils._log import log


def _normalize_comment(x):
    if x is None:
        return None
    if isinstance(x, str) and x.strip() == "":
        return None
    return x


@log
def extract_table_comments(
    table: str,
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
) -> dict:
    """
    Extracts table and column comments from a Delta Lake table.

    Parameters
    ----------
    table : str
        The name of the table, optionally prefixed with the schema (e.g., "schema.table").
    lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    result = {"table": table, "tableDescription": None, "columns": []}
    workspace_id = resolve_workspace_id(workspace)
    (lakehouse_name, lakehouse_id) = resolve_lakehouse_name_and_id(
        lakehouse=lakehouse, workspace=workspace_id
    )

    schema = None
    if "." in table:
        schema, table = table.split(".", 1)

    if _pure_python_notebook():
        from deltalake import DeltaTable

        path = create_abfss_path(
            lakehouse_id=lakehouse_id,
            lakehouse_workspace_id=workspace_id,
            delta_table_name=table.replace(".", "/"),
        )
        dt = DeltaTable(path)

        result["tableDescription"] = dt.metadata().description

        for c in dt.schema().fields:
            result["columns"].append(
                {"columnName": c.name, "description": c.metadata.get("comment")}
            )

    else:
        spark = _create_spark_session()
        if schema is not None:
            rows = spark.sql(
                f"DESCRIBE EXTENDED `{lakehouse_name}`.`{schema}`.`{table}`"
            ).collect()
        else:
            rows = spark.sql(
                f"DESCRIBE EXTENDED `{lakehouse_name}`.`{table}`"
            ).collect()

        for r in rows:
            col = r["col_name"]
            dtype = r["data_type"]

            # ✅ Table comment
            if col and col.lower() == "comment":
                result["tableDescription"] = _normalize_comment(dtype)

            # ❌ skip metadata rows
            elif col is None or col.startswith("#") or col.strip() == "":
                continue

            # ❌ skip partition header
            elif "Partition" in col:
                continue

            # ✅ actual column row
            else:
                result["columns"].append(
                    {
                        "columnName": col,
                        "description": _normalize_comment(
                            r["comment"]
                        ),  # this is where column comment appears (if set)
                    }
                )

    return result
