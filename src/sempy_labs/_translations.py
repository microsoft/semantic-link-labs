import pandas as pd
from typing import List, Optional, Union
from sempy._utils._log import log
import sempy_labs._icons as icons


@log
def translate_semantic_model(
    dataset: str,
    languages: Union[str, List[str]],
    exclude_characters: Optional[str] = None,
    workspace: Optional[str] = None,
):
    """
    Translates names, descriptions, display folders for all objects in a semantic model.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    languages : str, List[str]
        The language code(s) in which to translate the semantic model.
    exclude_characters : str
        A string specifying characters which will be replaced by a space in the translation text when sent to the translation service.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------

    """

    from synapse.ml.services import Translate
    from pyspark.sql.functions import col, flatten
    from pyspark.sql import SparkSession
    from sempy_labs.tom import connect_semantic_model

    if isinstance(languages, str):
        languages = [languages]

    dfPrep = pd.DataFrame(
        columns=["Object Type", "Name", "Description", "Display Folder"]
    )

    with connect_semantic_model(
        dataset=dataset, readonly=False, workspace=workspace
    ) as tom:

        if exclude_characters is None:
            for o in tom.model.Tables:
                new_data = {
                    "Object Type": "Table",
                    "Name": o.Name,
                    "TName": o.Name,
                    "Description": o.Description,
                    "TDescription": o.Description,
                    "Display Folder": None,
                    "TDisplay Folder": None,
                }
                dfPrep = pd.concat(
                    [dfPrep, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )
            for o in tom.all_columns():
                new_data = {
                    "Object Type": "Column",
                    "Name": o.Name,
                    "TName": o.Name,
                    "Description": o.Description,
                    "TDescription": o.Description,
                    "Display Folder": o.DisplayFolder,
                    "TDisplay Folder": o.DisplayFolder,
                }
                dfPrep = pd.concat(
                    [dfPrep, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )
            for o in tom.all_measures():
                new_data = {
                    "Object Type": "Measure",
                    "Name": o.Name,
                    "TName": o.Name,
                    "Description": o.Description,
                    "TDescription": o.Description,
                    "Display Folder": o.DisplayFolder,
                    "TDisplay Folder": o.DisplayFolder,
                }
                dfPrep = pd.concat(
                    [dfPrep, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )
            for o in tom.all_hierarchies():
                new_data = {
                    "Object Type": "Hierarchy",
                    "Name": o.Name,
                    "TName": o.Name,
                    "Description": o.Description,
                    "TDescription": o.Description,
                    "Display Folder": o.DisplayFolder,
                    "TDisplay Folder": o.DisplayFolder,
                }
                dfPrep = pd.concat(
                    [dfPrep, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )

            for o in tom.all_levels():
                new_data = {
                    "Object Type": "Level",
                    "Name": o.Name,
                    "TName": o.Name,
                    "Description": o.Description,
                    "TDescription": o.Description,
                    "Display Folder": None,
                    "TDisplay Folder": None,
                }
                dfPrep = pd.concat(
                    [dfPrep, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )
        else:
            for o in tom.model.Tables:
                oName = o.Name
                oDescription = o.Description
                for s in exclude_characters:
                    oName = oName.replace(s, " ")
                    oDescription = oDescription.replace(s, " ")
                new_data = {
                    "Object Type": "Table",
                    "Name": o.Name,
                    "TName": oName,
                    "Description": o.Description,
                    "TDescription": oDescription,
                    "Display Folder": None,
                    "TDisplay Folder": None,
                }
                dfPrep = pd.concat(
                    [dfPrep, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )
            for o in tom.all_columns():
                oName = o.Name
                oDescription = o.Description
                oDisplayFolder = o.DisplayFolder
                for s in exclude_characters:
                    oName = oName.replace(s, " ")
                    oDescription = oDescription.replace(s, " ")
                    oDisplayFolder = oDisplayFolder.replace(s, " ")
                new_data = {
                    "Object Type": "Column",
                    "Name": o.Name,
                    "TName": oName,
                    "Description": o.Description,
                    "TDescription": oDescription,
                    "Display Folder": o.DisplayFolder,
                    "TDisplay Folder": oDisplayFolder,
                }
                dfPrep = pd.concat(
                    [dfPrep, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )
            for o in tom.all_measures():
                oName = o.Name
                oDescription = o.Description
                oDisplayFolder = o.DisplayFolder
                for s in exclude_characters:
                    oName = oName.replace(s, " ")
                    oDescription = oDescription.replace(s, " ")
                    oDisplayFolder = oDisplayFolder.replace(s, " ")
                new_data = {
                    "Object Type": "Measure",
                    "Name": o.Name,
                    "TName": oName,
                    "Description": o.Description,
                    "TDescription": oDescription,
                    "Display Folder": o.DisplayFolder,
                    "TDisplay Folder": oDisplayFolder,
                }
                dfPrep = pd.concat(
                    [dfPrep, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )
            for o in tom.all_hierarchies():
                oName = o.Name
                oDescription = o.Description
                oDisplayFolder = o.DisplayFolder
                for s in exclude_characters:
                    oName = oName.replace(s, " ")
                    oDescription = oDescription.replace(s, " ")
                    oDisplayFolder = oDisplayFolder.replace(s, " ")
                new_data = {
                    "Object Type": "Hierarchy",
                    "Name": o.Name,
                    "TName": oName,
                    "Description": o.Description,
                    "TDescription": oDescription,
                    "Display Folder": o.DisplayFolder,
                    "TDisplay Folder": oDisplayFolder,
                }
                dfPrep = pd.concat(
                    [dfPrep, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )
            for o in tom.all_levels():
                oName = o.Name
                oDescription = o.Description
                for s in exclude_characters:
                    oName = oName.replace(s, " ")
                    oDescription = oDescription.replace(s, " ")
                new_data = {
                    "Object Type": "Level",
                    "Name": o.Name,
                    "TName": oName,
                    "Description": o.Description,
                    "TDescription": oDescription,
                    "Display Folder": None,
                    "TDisplay Folder": None,
                }
                dfPrep = pd.concat(
                    [dfPrep, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )

        spark = SparkSession.builder.getOrCreate()
        df = spark.createDataFrame(dfPrep)

        columns = ["Name", "Description", "Display Folder"]

        for clm in columns:
            columnToTranslate = f"T{clm}"
            translate = (
                Translate()
                .setTextCol(columnToTranslate)
                .setToLanguage(languages)
                .setOutputCol("translation")
                .setConcurrency(5)
            )

            transDF = (
                translate.transform(df)
                .withColumn("translation", flatten(col("translation.translations")))
                .withColumn("translation", col("translation.text"))
                .select("Object Type", clm, columnToTranslate, "translation")
            )

            df_panda = transDF.toPandas()

            for lang in languages:
                i = languages.index(lang)
                tom.add_translation(language=lang)
                print(f"{icons.in_progress} Translating {clm.lower()}s into the '{lang}' language...")

                for t in tom.model.Tables:
                    if t.IsHidden is False:
                        if clm == "Name":
                            df_filt = df_panda[
                                (df_panda["Object Type"] == "Table")
                                & (df_panda["Name"] == t.Name)
                            ]
                            if len(df_filt) == 1:
                                tr = df_filt["translation"].str[i].iloc[0]
                                tom.set_translation(
                                    object=t, language=lang, property="Name", value=tr
                                )
                        elif clm == "Description" and len(t.Description) > 0:
                            df_filt = df_panda[
                                (df_panda["Object Type"] == "Table")
                                & (df_panda["Description"] == t.Description)
                            ]
                            if len(df_filt) == 1:
                                tr = df_filt["translation"].str[i].iloc[0]
                                tom.set_translation(
                                    object=t,
                                    language=lang,
                                    property="Description",
                                    value=tr,
                                )
                        for c in t.Columns:
                            if c.IsHidden is False:
                                if clm == "Name":
                                    df_filt = df_panda[
                                        (df_panda["Object Type"] == "Column")
                                        & (df_panda["Name"] == c.Name)
                                    ]
                                    if len(df_filt) == 1:
                                        tr = df_filt["translation"].str[i].iloc[0]
                                        tom.set_translation(
                                            object=c,
                                            language=lang,
                                            property="Name",
                                            value=tr,
                                        )
                                elif clm == "Description" and len(c.Description) > 0:
                                    df_filt = df_panda[
                                        (df_panda["Object Type"] == "Column")
                                        & (df_panda["Description"] == c.Description)
                                    ]
                                    if len(df_filt) == 1:
                                        tr = df_filt["translation"].str[i].iloc[0]
                                        tom.set_translation(
                                            object=c,
                                            language=lang,
                                            property="Description",
                                            value=tr,
                                        )
                                elif (
                                    clm == "Display Folder"
                                    and len(c.DisplayFolder) > 0
                                ):
                                    df_filt = df_panda[
                                        (df_panda["Object Type"] == "Column")
                                        & (df_panda["Display Folder"] == c.DisplayFolder)
                                    ]
                                    if len(df_filt) == 1:
                                        tr = df_filt["translation"].str[i].iloc[0]
                                        tom.set_translation(
                                            object=c,
                                            language=lang,
                                            property="Display Folder",
                                            value=tr,
                                        )
                        for h in t.Hierarchies:
                            if h.IsHidden is False:
                                if clm == "Name":
                                    df_filt = df_panda[
                                        (df_panda["Object Type"] == "Hierarchy")
                                        & (df_panda["Name"] == h.Name)
                                    ]
                                    if len(df_filt) == 1:
                                        tr = df_filt["translation"].str[i].iloc[0]
                                        tom.set_translation(
                                            object=h,
                                            language=lang,
                                            property="Name",
                                            value=tr,
                                        )
                                elif clm == "Description" and len(h.Description) > 0:
                                    df_filt = df_panda[
                                        (df_panda["Object Type"] == "Hierarchy")
                                        & (df_panda["Description"] == h.Description)
                                    ]
                                    if len(df_filt) == 1:
                                        tr = df_filt["translation"].str[i].iloc[0]
                                        tom.set_translation(
                                            object=h,
                                            language=lang,
                                            property="Description",
                                            value=tr,
                                        )
                                elif (
                                    clm == "Display Folder"
                                    and len(h.DisplayFolder) > 0
                                ):
                                    df_filt = df_panda[
                                        (df_panda["Object Type"] == "Hierarchy")
                                        & (df_panda["Display Folder"] == h.DisplayFolder)
                                    ]
                                    if len(df_filt) == 1:
                                        tr = df_filt["translation"].str[i].iloc[0]
                                        tom.set_translation(
                                            object=h,
                                            language=lang,
                                            property="Display Folder",
                                            value=tr,
                                        )
                                for lev in h.Levels:
                                    if clm == "Name":
                                        df_filt = df_panda[
                                            (df_panda["Object Type"] == "Level")
                                            & (df_panda["Name"] == lev.Name)
                                        ]
                                        if len(df_filt) == 1:
                                            tr = df_filt["translation"].str[i].iloc[0]
                                            tom.set_translation(
                                                object=lev,
                                                language=lang,
                                                property="Name",
                                                value=tr,
                                            )
                                    elif clm == "Description" and len(lev.Description) > 0:
                                        df_filt = df_panda[
                                            (df_panda["Object Type"] == "Level")
                                            & (df_panda["Description"] == lev.Description)
                                        ]
                                        if len(df_filt) == 1:
                                            tr = df_filt["translation"].str[i].iloc[0]
                                            tom.set_translation(
                                                object=lev,
                                                language=lang,
                                                property="Description",
                                                value=tr,
                                            )
                    for ms in t.Measures:
                        if ms.IsHidden is False:
                            if clm == "Name":
                                df_filt = df_panda[
                                    (df_panda["Object Type"] == "Measure")
                                    & (df_panda["Name"] == ms.Name)
                                ]
                                if len(df_filt) == 1:
                                    tr = df_filt["translation"].str[i].iloc[0]
                                    tom.set_translation(
                                        object=ms,
                                        language=lang,
                                        property="Name",
                                        value=tr,
                                    )
                            elif clm == "Description" and len(ms.Description) > 0:
                                df_filt = df_panda[
                                    (df_panda["Object Type"] == "Measure")
                                    & (df_panda["Description"] == ms.Description)
                                ]
                                if len(df_filt) == 1:
                                    tr = df_filt["translation"].str[i].iloc[0]
                                    tom.set_translation(
                                        object=ms,
                                        language=lang,
                                        property="Description",
                                        value=tr,
                                    )
                            elif (
                                clm == "Display Folder" and len(ms.DisplayFolder) > 0
                            ):
                                df_filt = df_panda[
                                    (df_panda["Object Type"] == "Measure")
                                    & (df_panda["Display Folder"] == ms.DisplayFolder)
                                ]
                                if len(df_filt) == 1:
                                    tr = df_filt["translation"].str[i].iloc[0]
                                    tom.set_translation(
                                        object=ms,
                                        language=lang,
                                        property="Display Folder",
                                        value=tr,
                                    )
