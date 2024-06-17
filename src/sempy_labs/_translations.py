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
    from .tom import connect_semantic_model

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
            print(f"{icons.in_progress} Translating {clm}s...")

            for lang in languages:
                i = languages.index(lang)
                tom.add_translation(language=lang)
                print(f"{icons.in_progress} Translating into the '{lang}' language...")

                for t in tom.model.Tables:
                    if t.IsHidden == False:
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
                                print(
                                    f"{icons.green_dot} Translation '{tr}' set for the '{lang}' language on the '{t.Name}' table."
                                )
                        elif clm == "Description" and t.Description is not None:
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
                            if c.IsHidden == False:
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
                                        print(
                                            f"{icons.green_dot} Translation '{tr}' set on the '{c.Name}' column within the {t.Name}' table."
                                        )
                                elif clm == "Description" and c.Description is not None:
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
                                    and c.DisplayFolder is not None
                                ):
                                    df_filt = df_panda[
                                        (df_panda["Object Type"] == "Column")
                                        & (df_panda["Display Folder"] == c.Description)
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
                            if h.IsHidden == False:
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
                                elif clm == "Description" and h.Description is not None:
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
                                    and h.DisplayFolder is not None
                                ):
                                    df_filt = df_panda[
                                        (df_panda["Object Type"] == "Hierarchy")
                                        & (df_panda["Display Folder"] == h.Description)
                                    ]
                                    if len(df_filt) == 1:
                                        tr = df_filt["translation"].str[i].iloc[0]
                                        tom.set_translation(
                                            object=h,
                                            language=lang,
                                            property="Display Folder",
                                            value=tr,
                                        )
                    for ms in t.Measures:
                        if ms.IsHidden == False:
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
                                    print(
                                        f"{icons.green_dot} Translation '{tr}' set on the '{ms.Name}' column within the {t.Name}' table."
                                    )
                            elif clm == "Description" and ms.Description is not None:
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
                                clm == "Display Folder" and ms.DisplayFolder is not None
                            ):
                                df_filt = df_panda[
                                    (df_panda["Object Type"] == "Measure")
                                    & (df_panda["Display Folder"] == ms.Description)
                                ]
                                if len(df_filt) == 1:
                                    tr = df_filt["translation"].str[i].iloc[0]
                                    tom.set_translation(
                                        object=ms,
                                        language=lang,
                                        property="Display Folder",
                                        value=tr,
                                    )
