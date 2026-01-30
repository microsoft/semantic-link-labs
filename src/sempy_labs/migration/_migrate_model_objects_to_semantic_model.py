import sempy
import sempy.fabric as fabric
import re
from sempy_labs._helper_functions import (
    create_relationship_name,
    retry,
    format_dax_object_name,
)
from sempy_labs.tom import connect_semantic_model
from typing import Optional
from sempy._utils._log import log
import sempy_labs._icons as icons
from uuid import UUID


@log
def migrate_model_objects_to_semantic_model(
    dataset: str,
    new_dataset: str,
    workspace: Optional[str | UUID] = None,
    new_dataset_workspace: Optional[str | UUID] = None,
):
    """
    Adds the rest of the model objects (besides tables/columns) and their properties to a Direct Lake semantic model based on an import/DirectQuery semantic model.

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

    sempy.fabric._client._utils._init_analysis_services()
    import Microsoft.AnalysisServices.Tabular as TOM
    import System

    if dataset == new_dataset:
        raise ValueError(
            f"{icons.red_dot} The 'dataset' and 'new_dataset' parameters are both set to '{dataset}'. These parameters must be set to different values."
        )

    fabric.refresh_tom_cache(workspace=workspace)
    icons.sll_tags.append("DirectLakeMigration")

    dfT = fabric.list_tables(dataset=dataset, workspace=workspace)
    dfC = fabric.list_columns(dataset=dataset, workspace=workspace)
    dfM = fabric.list_measures(dataset=dataset, workspace=workspace)
    dfRole = fabric.get_roles(dataset=dataset, workspace=workspace)
    dfRLS = fabric.get_row_level_security_permissions(
        dataset=dataset, workspace=workspace
    )
    dfCI = fabric.list_calculation_items(dataset=dataset, workspace=workspace)
    dfP = fabric.list_perspectives(dataset=dataset, workspace=workspace)
    dfTranslation = fabric.list_translations(dataset=dataset, workspace=workspace)
    dfH = fabric.list_hierarchies(dataset=dataset, workspace=workspace)
    dfPar = fabric.list_partitions(dataset=dataset, workspace=workspace)

    dfP_cc = dfPar[(dfPar["Source Type"] == "Calculated")]
    dfP_fp = dfP_cc[dfP_cc["Query"].str.contains("NAMEOF")]

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

    @retry(
        sleep_time=1,
        timeout_error_message=f"{icons.red_dot} Function timed out after 1 minute",
    )
    def dyn_connect2():
        with connect_semantic_model(
            dataset=dataset, readonly=True, workspace=workspace
        ) as tom:

            tom.model

    dyn_connect2()

    with connect_semantic_model(
        dataset=new_dataset, readonly=False, workspace=new_dataset_workspace
    ) as tom:

        isDirectLake = tom.is_direct_lake()

        print(f"\n{icons.in_progress} Updating table properties...")
        for t in tom.model.Tables:
            t.IsHidden = bool(dfT.loc[dfT["Name"] == t.Name, "Hidden"].iloc[0])
            t.Description = dfT.loc[dfT["Name"] == t.Name, "Description"].iloc[0]
            t.DataCategory = dfT.loc[dfT["Name"] == t.Name, "Data Category"].iloc[0]

            print(
                f"{icons.green_dot} The '{t.Name}' table's properties have been updated."
            )

        print(f"\n{icons.in_progress} Updating column properties...")
        for t in tom.model.Tables:
            if (
                t.Name not in dfP_fp["Table Name"].values
            ):  # do not include field parameters
                dfT_filtered = dfT[dfT["Name"] == t.Name]
                tType = dfT_filtered["Type"].iloc[0]
                for c in t.Columns:
                    if not c.Name.startswith("RowNumber-"):
                        dfC_filt = dfC[
                            (dfC["Table Name"] == t.Name)
                            & (dfC["Column Name"] == c.Name)
                        ]
                        cName = dfC_filt["Column Name"].iloc[0]
                        c.Name = cName
                        # if tType == "Table":
                        #    c.SourceColumn = cName.replace(" ", "_")
                        c.IsHidden = bool(dfC_filt["Hidden"].iloc[0])
                        c.DataType = System.Enum.Parse(
                            TOM.DataType, dfC_filt["Data Type"].iloc[0]
                        )
                        c.DisplayFolder = dfC_filt["Display Folder"].iloc[0]
                        c.FormatString = dfC_filt["Format String"].iloc[0]
                        c.SummarizeBy = System.Enum.Parse(
                            TOM.AggregateFunction,
                            dfC_filt["Summarize By"].iloc[0],
                        )
                        c.DataCategory = dfC_filt["Data Category"].iloc[0]
                        c.IsKey = bool(dfC_filt["Key"].iloc[0])
                        sbc = dfC_filt["Sort By Column"].iloc[0]

                        if sbc is not None:
                            if any(
                                o.Name == sbc and o.Parent.Name == c.Parent.Name
                                for o in tom.all_columns()
                            ):
                                c.SortByColumn = tom.model.Tables[t.Name].Columns[sbc]
                            else:
                                print(
                                    f"{icons.red_dot} Failed to create '{sbc}' as a Sort By Column for the '{c.Name}' in the '{t.Name}' table."
                                )
                        print(
                            f"{icons.green_dot} The '{t.Name}'[{c.Name}] column's properties have been updated."
                        )

        print(f"\n{icons.in_progress} Creating hierarchies...")
        dfH_grouped = (
            dfH.groupby(
                [
                    "Table Name",
                    "Hierarchy Name",
                    "Hierarchy Hidden",
                    "Hierarchy Description",
                ]
            )
            .agg({"Level Name": list, "Column Name": list})
            .reset_index()
        )

        for i, r in dfH_grouped.iterrows():
            tName = r["Table Name"]
            hName = r["Hierarchy Name"]
            hDesc = r["Hierarchy Description"]
            hHid = bool(r["Hierarchy Hidden"])
            cols = r["Column Name"]
            lvls = r["Level Name"]

            missing_columns = []
            for col in cols:
                if not any(
                    c.Name == col
                    for t in tom.model.Tables
                    if t.Name == tName
                    for c in t.Columns
                ):
                    missing_columns.append(col)

            if any(
                t.Name == tName and h.Name == hName
                for t in tom.model.Tables
                for h in t.Hierarchies
            ):
                print(
                    f"{icons.warning} The '{hName}' hierarchy within the '{tName}' table already exists."
                )
            elif len(missing_columns) > 0:
                print(
                    f"{icons.red_dot} The '{hName}' hierarchy within the '{tName}' table cannot be created as the {missing_columns} column)s) do not exist."
                )
            else:
                tom.add_hierarchy(
                    table_name=tName,
                    hierarchy_name=hName,
                    hierarchy_description=hDesc,
                    hierarchy_hidden=hHid,
                    columns=cols,
                    levels=lvls,
                )
                print(f"{icons.green_dot} The '{hName}' hierarchy has been added.")

        print(f"\n{icons.in_progress} Creating measures...")
        for i, r in dfM.iterrows():
            tName = r["Table Name"]
            mName = r["Measure Name"]
            mExpr = r["Measure Expression"]
            mHidden = bool(r["Measure Hidden"])
            mDF = r["Measure Display Folder"]
            mDesc = r["Measure Description"]
            mFS = r["Format String"]

            if not any(m.Name == mName for m in tom.all_measures()):
                tom.add_measure(
                    table_name=tName,
                    measure_name=mName,
                    expression=mExpr,
                    hidden=mHidden,
                    display_folder=mDF,
                    description=mDesc,
                    format_string=mFS,
                )
                print(f"{icons.green_dot} The '{mName}' measure has been added.")
        print(f"\n{icons.in_progress} Creating calculation groups...")
        for cgName in dfCI["Calculation Group Name"].unique():

            isHidden = bool(
                dfCI.loc[(dfCI["Calculation Group Name"] == cgName), "Hidden"].iloc[0]
            )
            prec = int(
                dfCI.loc[(dfCI["Calculation Group Name"] == cgName), "Precedence"].iloc[
                    0
                ]
            )
            desc = dfCI.loc[
                (dfCI["Calculation Group Name"] == cgName), "Description"
            ].iloc[0]

            if not any(t.Name == cgName for t in tom.model.Tables):
                tom.add_calculation_group(
                    name=cgName,
                    description=desc,
                    precedence=prec,
                    hidden=isHidden,
                )
                print(
                    f"{icons.green_dot} The '{cgName}' calculation group has been added."
                )
                tom.model.DiscourageImplicitMeasures = True

                # print(
                #    f"\n{icons.in_progress} Updating calculation group column names..."
                # )
                dfC_filt = dfC[(dfC["Table Name"] == cgName) & (dfC["Hidden"] == False)]
                colName = dfC_filt["Column Name"].iloc[0]
                tom.model.Tables[cgName].Columns["Name"].Name = colName

            calcItems = dfCI.loc[
                dfCI["Calculation Group Name"] == cgName,
                "Calculation Item Name",
            ].unique()

            print(f"\n{icons.in_progress} Creating calculation items...")
            for calcItem in calcItems:
                ordinal = int(
                    dfCI.loc[
                        (dfCI["Calculation Group Name"] == cgName)
                        & (dfCI["Calculation Item Name"] == calcItem),
                        "Ordinal",
                    ].iloc[0]
                )
                expr = dfCI.loc[
                    (dfCI["Calculation Group Name"] == cgName)
                    & (dfCI["Calculation Item Name"] == calcItem),
                    "Expression",
                ].iloc[0]
                fse = dfCI.loc[
                    (dfCI["Calculation Group Name"] == cgName)
                    & (dfCI["Calculation Item Name"] == calcItem),
                    "Format String Expression",
                ].iloc[0]

                if not any(
                    ci.CalculationGroup.Parent.Name == cgName and ci.Name == calcItem
                    for ci in tom.all_calculation_items()
                ):
                    tom.add_calculation_item(
                        table_name=cgName,
                        calculation_item_name=calcItem,
                        expression=expr,
                        format_string_expression=fse,
                        ordinal=ordinal,
                    )
                    print(
                        f"{icons.green_dot} The '{calcItem}' has been added to the '{cgName}' calculation group."
                    )

        print(f"\n{icons.in_progress} Creating relationships...")
        with connect_semantic_model(
            dataset=dataset, readonly=True, workspace=workspace
        ) as tom_old:

            for r in tom_old.model.Relationships:
                relName = create_relationship_name(
                    r.FromTable.Name, r.FromColumn.Name, r.ToTable.Name, r.ToColumn.Name
                )

                # Relationship already exists
                if any(
                    rel.FromTable.Name == r.FromTable.Name
                    and rel.FromColumn.Name == r.FromColumn.Name
                    and rel.ToTable.Name == r.ToTable.Name
                    and rel.ToColumn.Name == r.ToColumn.Name
                    for rel in tom.model.Relationships
                ):
                    print(
                        f"{icons.warning} The {relName} relationship was not created as it already exists in the '{new_dataset}' semantic model within the '{new_dataset_workspace}' workspace."
                    )

                # Direct Lake with incompatible column data types
                elif isDirectLake and r.FromColumn.DataType != r.ToColumn.DataType:
                    print(
                        f"{icons.warning} The {relName} relationship was not created as Direct Lake does not support relationships based on columns with different data types."
                    )
                # Columns do not exist in the new semantic model
                elif not any(
                    c.Name == r.FromColumn.Name and c.Parent.Name == r.FromTable.Name
                    for c in tom.all_columns()
                ) or not any(
                    c.Name == r.ToColumn.Name and c.Parent.Name == r.ToTable.Name
                    for c in tom.all_columns()
                ):
                    # Direct lake and based on calculated column
                    if isDirectLake and (
                        any(
                            c.Name == r.FromColumn.Name
                            and c.Parent.Name == r.FromTable.Name
                            for c in tom_old.all_calculated_columns()
                        )
                        or any(
                            c.Name == r.ToColumn.Name
                            and c.Parent.Name == r.ToTable.Name
                            for c in tom_old.all_calculated_columns()
                        )
                    ):
                        print(
                            f"{icons.red_dot} The {relName} relationship was not created as the necssary column(s) do not exist. This is due to Direct Lake not supporting calculated columns."
                        )
                    elif not any(
                        c.Name == r.FromColumn.Name
                        and c.Parent.Name == r.FromTable.Name
                        for c in tom.all_columns()
                    ):
                        print(
                            f"{icons.red_dot} The {relName} relationship cannot be created because the {format_dax_object_name(r.FromTable.Name, r.FromColumn.Name)} column is not available in the '{new_dataset}' semantic model within the '{new_dataset_workspace}' workspace."
                        )
                    elif not any(
                        c.Name == r.ToColumn.Name and c.Parent.Name == r.ToTable.Name
                        for c in tom.all_columns()
                    ):
                        print(
                            f"{icons.red_dot} The {relName} relationship cannot be created because the {format_dax_object_name(r.ToTable.Name, r.ToColumn.Name)} column is not available in the '{new_dataset}' semantic model within the '{new_dataset_workspace}' workspace."
                        )
                else:
                    tom.add_relationship(
                        from_table=r.FromTable.Name,
                        from_column=r.FromColumn.Name,
                        to_table=r.ToTable.Name,
                        to_column=r.ToColumn.Name,
                        from_cardinality=str(r.FromCardinality),
                        to_cardinality=str(r.ToCardinality),
                        cross_filtering_behavior=str(r.CrossFilteringBehavior),
                        security_filtering_behavior=str(r.SecurityFilteringBehavior),
                        rely_on_referential_integrity=r.RelyOnReferentialIntegrity,
                        is_active=r.IsActive,
                    )
                    print(
                        f"{icons.green_dot} The {relName} relationship has been added."
                    )

        print(f"\n{icons.in_progress} Creating roles...")
        for index, row in dfRole.iterrows():
            roleName = row["Role"]
            roleDesc = row["Description"]
            modPerm = row["Model Permission"]

            if not any(r.Name == roleName for r in tom.model.Roles):
                tom.add_role(
                    role_name=roleName,
                    model_permission=modPerm,
                    description=roleDesc,
                )
                print(f"{icons.green_dot} The '{roleName}' role has been added.")

        print(f"\n{icons.in_progress} Creating row level security...")
        for index, row in dfRLS.iterrows():
            roleName = row["Role"]
            tName = row["Table"]
            expr = row["Filter Expression"]

            if any(t.Name == tName for t in tom.model.Tables):
                tom.set_rls(
                    role_name=roleName, table_name=tName, filter_expression=expr
                )
                print(
                    f"{icons.green_dot} Row level security for the '{tName}' table within the '{roleName}' role has been set."
                )
            else:
                print(
                    f"{icons.red_dot} Row level security for the '{tName}' table within the '{roleName}' role was not set."
                )

        print(f"\n{icons.in_progress} Creating perspectives...")
        for pName in dfP["Perspective Name"].unique():

            if not any(p.Name == pName for p in tom.model.Perspectives):
                tom.add_perspective(perspective_name=pName)
                print(f"{icons.green_dot} The '{pName}' perspective has been added.")

        print(f"\n{icons.in_progress} Adding objects to perspectives...")
        for index, row in dfP.iterrows():
            pName = row["Perspective Name"]
            tName = row["Table Name"]
            oName = row["Object Name"]
            oType = row["Object Type"]
            tType = dfT.loc[(dfT["Name"] == tName), "Type"].iloc[0]

            try:
                if oType == "Table":
                    tom.add_to_perspective(
                        object=tom.model.Tables[tName], perspective_name=pName
                    )
                elif oType == "Column":
                    tom.add_to_perspective(
                        object=tom.model.Tables[tName].Columns[oName],
                        perspective_name=pName,
                    )
                elif oType == "Measure":
                    tom.add_to_perspective(
                        object=tom.model.Tables[tName].Measures[oName],
                        perspective_name=pName,
                    )
                elif oType == "Hierarchy":
                    tom.add_to_perspective(
                        object=tom.model.Tables[tName].Hierarchies[oName],
                        perspective_name=pName,
                    )
            except Exception:
                pass

        print(f"\n{icons.in_progress} Creating translation languages...")
        for trName in dfTranslation["Culture Name"].unique():
            if not any(c.Name == trName for c in tom.model.Cultures):
                tom.add_translation(trName)
                print(
                    f"{icons.green_dot} The '{trName}' translation language has been added."
                )

        print(f"\n{icons.in_progress} Creating translation values...")
        for index, row in dfTranslation.iterrows():
            trName = row["Culture Name"]
            tName = row["Table Name"]
            oName = row["Object Name"]
            oType = row["Object Type"]
            translation = row["Translation"]
            prop = row["Property"]

            if prop == "Caption":
                prop = "Name"
            elif prop == "DisplayFolder":
                prop = "Display Folder"

            try:
                if oType == "Table":
                    tom.set_translation(
                        object=tom.model.Tables[tName],
                        language=trName,
                        property=prop,
                        value=translation,
                    )
                elif oType == "Column":
                    tom.set_translation(
                        object=tom.model.Tables[tName].Columns[oName],
                        language=trName,
                        property=prop,
                        value=translation,
                    )
                elif oType == "Measure":
                    tom.set_translation(
                        object=tom.model.Tables[tName].Measures[oName],
                        language=trName,
                        property=prop,
                        value=translation,
                    )
                elif oType == "Hierarchy":
                    tom.set_translation(
                        object=tom.model.Tables[tName].Hierarchies[oName],
                        language=trName,
                        property=prop,
                        value=translation,
                    )
                elif oType == "Level":

                    pattern = r"\[([^]]+)\]"
                    matches = re.findall(pattern, oName)
                    lName = matches[0]

                    pattern = r"'([^']+)'"
                    matches = re.findall(pattern, oName)
                    hName = matches[0]
                    tom.set_translation(
                        object=tom.model.Tables[tName].Hierarchies[hName].Levels[lName],
                        language=trName,
                        property=prop,
                        value=translation,
                    )
            except Exception:
                pass

        print(
            f"\n{icons.green_dot} Migration of objects from '{dataset}' -> '{new_dataset}' is complete."
        )
