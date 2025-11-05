import sempy.fabric as fabric
import pandas as pd
from sempy_labs.tom import connect_semantic_model
from sempy_labs._generate_semantic_model import create_blank_semantic_model
from sempy_labs.directlake._generate_shared_expression import generate_shared_expression
from typing import Optional
from sempy._utils._log import log


@log
def model_auto_build(
    dataset: str,
    file_path: str,
    workspace: Optional[str] = None,
    lakehouse: Optional[str] = None,
    lakehouse_workspace: Optional[str] = None,
):
    """
    Dynamically generates a semantic model based on an Excel file template.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    file_path : str
    workspace : str, default=None
        The Fabric workspace name.
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

    if lakehouse_workspace is None:
        lakehouse_workspace = workspace

    sheets = [
        "Model",
        "Tables",
        "Measures",
        "Columns",
        "Roles",
        "Hierarchies",
        "Relationships",
    ]

    create_blank_semantic_model(dataset=dataset, workspace=workspace)

    with connect_semantic_model(
        dataset=dataset, workspace=workspace, readonly=False
    ) as tom:

        # DL Only
        expr = generate_shared_expression(
            item_name=lakehouse, item_type="Lakehouse", workspace=lakehouse_workspace
        )
        tom.add_expression(name="DatbaseQuery", expression=expr)

        for sheet in sheets:
            df = pd.read_excel(file_path, sheet_name=sheet)

            if sheet == "Tables":
                for i, r in df.iterrows():
                    tName = r["Table Name"]
                    desc = r["Description"]
                    dc = r["Data Category"]
                    mode = r["Mode"]
                    hidden = bool(r["Hidden"])

                    tom.add_table(
                        name=tName, description=desc, data_category=dc, hidden=hidden
                    )
                    if mode == "DirectLake":
                        tom.add_entity_partition(table_name=tName, entity_name=tName)
            elif sheet == "Columns":
                for i, r in df.iterrows():
                    tName = r["Table Name"]
                    cName = r["Column Name"]
                    scName = r["Source Column"]
                    dataType = r["Data Type"]
                    hidden = bool(r["Hidden"])
                    key = bool(r["Key"])
                    if dataType == "Integer":
                        dataType = "Int64"
                    desc = r["Description"]

                    tom.add_data_column(
                        table_name=tName,
                        column_name=cName,
                        source_column=scName,
                        data_type=dataType,
                        description=desc,
                        hidden=hidden,
                        key=key,
                    )
            elif sheet == "Measures":
                for i, r in df.iterrows():
                    tName = r["Table Name"]
                    mName = r["Measure Name"]
                    expr = r["Expression"]
                    desc = r["Description"]
                    format = r["Format String"]
                    hidden = bool(r["Hidden"])

                    tom.add_measure(
                        table_name=tName,
                        measure_name=mName,
                        expression=expr,
                        format_string=format,
                        description=desc,
                        hidden=hidden,
                    )
            elif sheet == "Relationships":
                for i, r in df.iterrows():
                    fromTable = r["From Table"]
                    fromColumn = r["From Column"]
                    toTable = r["To Table"]
                    toColumn = r["To Column"]
                    fromCard = r["From Cardinality"]
                    toCard = r["To Cardinality"]

                    tom.add_relationship(
                        from_table=fromTable,
                        from_column=fromColumn,
                        to_table=toTable,
                        to_column=toColumn,
                        from_cardinality=fromCard,
                        to_cardinality=toCard,
                    )
            elif sheet == "Roles":
                print("hi")
            elif sheet == "Hierarchies":
                print("hi")
