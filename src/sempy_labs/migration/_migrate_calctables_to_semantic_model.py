import sempy.fabric as fabric
import re
from sempy_labs.lakehouse._get_lakehouse_tables import get_lakehouse_tables
from sempy_labs._helper_functions import (
    format_dax_object_name,
    retry,
)
from sempy_labs.tom import connect_semantic_model
from typing import Optional
from sempy._utils._log import log
import sempy_labs._icons as icons
from uuid import UUID


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
    Creates new tables in the Direct Lake semantic model based on the lakehouse tables created using the 'migrate_calc_tables_to_lakehouse' function.

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
        The Fabric lakehouse used by the Direct Lake semantic model.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    lakehouse_workspace : str | uuid.UUID, default=None
        The Fabric workspace used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    if dataset == new_dataset:
        raise ValueError(
            f"{icons.red_dot} The 'dataset' and 'new_dataset' parameters are both set to '{dataset}'. These parameters must be set to different values."
        )

    fabric.refresh_tom_cache(workspace=workspace)

    icons.sll_tags.append("DirectLakeMigration")

    # Get calc tables but not field parameters
    dfP = fabric.list_partitions(dataset=dataset, workspace=workspace)
    dfP_filt = dfP[(dfP["Source Type"] == "Calculated")]
    dfP_filt = dfP_filt[~dfP_filt["Query"].str.contains("NAMEOF")]

    dfC = fabric.list_columns(dataset=dataset, workspace=workspace)
    lc = get_lakehouse_tables(lakehouse=lakehouse, workspace=lakehouse_workspace)
    # Get all calc table columns of calc tables not including field parameters
    dfC_filt = dfC[
        (dfC["Table Name"].isin(dfP_filt["Table Name"]))
    ]  # & (dfC['Type'] == 'CalculatedTableColumn')]
    # dfA = fabric.list_annotations(new_dataset, new_dataset_workspace)
    # dfA_filt = dfA[(dfA['Object Type'] == 'Model') & ~ (dfA['Annotation Value'].str.contains('NAMEOF'))]

    if len(dfP_filt) == 0:
        print(
            f"{icons.green_dot} The '{dataset}' semantic model has no calculated tables."
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
        dataset=new_dataset, readonly=False, workspace=new_dataset_workspace
    ) as tom:
        for tName in dfC_filt["Table Name"].unique():
            if tName.lower() in lc["Table Name"].values:
                if not any(t.Name == tName for t in tom.model.Tables):
                    tom.add_table(name=tName)
                    tom.add_entity_partition(
                        table_name=tName,
                        entity_name=tName.replace(" ", "_").lower(),
                    )

            columns_in_table = dfC_filt.loc[
                dfC_filt["Table Name"] == tName, "Column Name"
            ].unique()

            for cName in columns_in_table:
                scName = dfC.loc[
                    (dfC["Table Name"] == tName) & (dfC["Column Name"] == cName),
                    "Source",
                ].iloc[0]
                cDataType = dfC.loc[
                    (dfC["Table Name"] == tName) & (dfC["Column Name"] == cName),
                    "Data Type",
                ].iloc[0]
                # cType = dfC.loc[
                #    (dfC["Table Name"] == tName)
                #    & (dfC["Column Name"] == cName),
                #    "Type",
                # ].iloc[0]

                # av = tom.get_annotation_value(object = tom.model, name = tName)

                # if cType == 'CalculatedTableColumn':
                # lakeColumn = scName.replace(' ','_')
                # elif cType == 'Calculated':
                pattern = r"\[([^]]+)\]"

                matches = re.findall(pattern, scName)
                lakeColumn = matches[0].replace(" ", "")
                if not any(
                    c.Name == cName and c.Parent.Name == tName
                    for c in tom.all_columns()
                ):
                    tom.add_data_column(
                        table_name=tName,
                        column_name=cName,
                        source_column=lakeColumn,
                        data_type=cDataType,
                    )
                    print(
                        f"{icons.green_dot} The {format_dax_object_name(tName,cName)} column has been added."
                    )

        print(
            f"\n{icons.green_dot} All viable calculated tables have been added to the model."
        )
