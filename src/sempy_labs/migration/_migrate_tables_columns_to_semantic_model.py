import sempy.fabric as fabric
import pandas as pd
from sempy_labs.directlake._generate_shared_expression import generate_shared_expression
from sempy_labs._helper_functions import retry
from sempy_labs.lakehouse._lakehouse import lakehouse_attached
from sempy_labs.tom import connect_semantic_model
from typing import Optional
from sempy._utils._log import log
import sempy_labs._icons as icons
from uuid import UUID


@log
def migrate_tables_columns_to_semantic_model(
    dataset: str,
    new_dataset: str,
    workspace: Optional[str | UUID] = None,
    new_dataset_workspace: Optional[str | UUID] = None,
    lakehouse: Optional[str | UUID] = None,
    lakehouse_workspace: Optional[str | UUID] = None,
):
    """
    Adds tables/columns to the new Direct Lake semantic model based on an import/DirectQuery semantic model.

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

    if dataset == new_dataset:
        raise ValueError(
            f"{icons.red_dot} The 'dataset' and 'new_dataset' parameters are both set to '{dataset}'. These parameters must be set to different values."
        )

    icons.sll_tags.append("DirectLakeMigration")

    # Check that lakehouse is attached to the notebook
    if not lakehouse_attached() and (lakehouse is None and lakehouse_workspace is None):
        raise ValueError(
            f"{icons.red_dot} Lakehouse not attached to notebook and lakehouse/lakehouse_workspace are not specified. Please add your lakehouse to this notebook"
            f" or specify the lakehouse/lakehouse_workspace parameters."
            "To attach a lakehouse to a notebook, go to the the 'Explorer' window to the left, click 'Lakehouses' to add your lakehouse to this notebook"
            "\nLearn more here: https://learn.microsoft.com/fabric/data-engineering/lakehouse-notebook-explore#add-or-remove-a-lakehouse"
        )
    shEx = generate_shared_expression(
        item_name=lakehouse,
        item_type="Lakehouse",
        workspace=lakehouse_workspace,
        use_sql_endpoint=False,
    )

    fabric.refresh_tom_cache(workspace=workspace)
    dfC = fabric.list_columns(dataset=dataset, workspace=workspace)
    dfT = fabric.list_tables(dataset=dataset, workspace=workspace)
    dfT.rename(columns={"Type": "Table Type"}, inplace=True)
    dfC = pd.merge(
        dfC,
        dfT[["Name", "Table Type"]],
        left_on="Table Name",
        right_on="Name",
        how="left",
    )
    dfT_filt = dfT[dfT["Table Type"] == "Table"]
    dfC_filt = dfC[
        (dfC["Table Type"] == "Table")
        & ~(dfC["Column Name"].str.startswith("RowNumber-"))
        & (dfC["Type"] != "Calculated")
    ]

    print(f"{icons.in_progress} Updating '{new_dataset}' based on '{dataset}'...")

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
        dataset=new_dataset, readonly=False, workspace=new_dataset_workspace
    ) as tom:

        # Additional updates
        tom.set_annotation(
            object=tom.model, name="__PBI_TimeIntelligenceEnabled", value="0"
        )
        tom.set_annotation(
            object=tom.model, name="PBI_QueryOrder", value='["DatabaseQuery"]'
        )

        # Begin migration
        if not any(e.Name == "DatabaseQuery" for e in tom.model.Expressions):
            tom.add_expression("DatabaseQuery", expression=shEx)
            print(f"{icons.green_dot} The 'DatabaseQuery' expression has been added.")
            tom.set_annotation(
                object=tom.model.Expressions["DatabaseQuery"],
                name="PBI_IncludeFutureArtifacts",
                value="False",
            )

        for i, r in dfT_filt.iterrows():
            tName = r["Name"]
            tDC = r["Data Category"]
            tHid = bool(r["Hidden"])
            tDesc = r["Description"]
            ent_name = tName  # .replace(" ", "_")
            for char in icons.special_characters:
                ent_name = ent_name.replace(char, "")

            if not any(t.Name == tName for t in tom.model.Tables):
                tom.add_table(
                    name=tName,
                    description=tDesc,
                    data_category=tDC,
                    hidden=tHid,
                )
                tom.add_entity_partition(table_name=tName, entity_name=ent_name)
                print(f"{icons.green_dot} The '{tName}' table has been added.")

        for i, r in dfC_filt.iterrows():
            tName = r["Table Name"]
            cName = r["Column Name"]
            scName = r["Source"].replace(" ", "_")
            cHid = bool(r["Hidden"])
            cDataType = r["Data Type"]
            for char in icons.special_characters:
                scName = scName.replace(char, "")

            if scName.endswith("_"):
                scName = scName[:-1]

            if not any(
                c.Name == cName and c.Parent.Name == tName for c in tom.all_columns()
            ):
                tom.add_data_column(
                    table_name=tName,
                    column_name=cName,
                    source_column=scName,
                    hidden=cHid,
                    data_type=cDataType,
                )
                print(
                    f"{icons.green_dot} The '{tName}'[{cName}] column has been added."
                )

        print(
            f"\n{icons.green_dot} All regular tables and columns have been added to the '{new_dataset}' semantic model."
        )
