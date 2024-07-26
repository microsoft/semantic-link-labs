import sempy
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
) -> pd.DataFrame:
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
    pandas.DataFrame
        Shows a pandas dataframe which displays all of the translations in the semantic model.

    """

    from synapse.ml.services import Translate
    from pyspark.sql.functions import col, flatten
    from pyspark.sql import SparkSession
    from sempy_labs.tom import connect_semantic_model

    def clean_text(text, exclude_chars):
        if exclude_chars:
            for char in exclude_chars:
                text = text.replace(char, " ")
        return text

    if isinstance(languages, str):
        languages = [languages]

    df_prep = pd.DataFrame(
        columns=["Object Type", "Name", "Description", "Display Folder"]
    )

    with connect_semantic_model(
        dataset=dataset, readonly=False, workspace=workspace
    ) as tom:

        for o in tom.model.Tables:
            oName = clean_text(o.Name, exclude_characters)
            oDescription = clean_text(o.Description, exclude_characters)
            new_data = {
                "Object Type": "Table",
                "Name": o.Name,
                "TName": oName,
                "Description": o.Description,
                "TDescription": oDescription,
                "Display Folder": None,
                "TDisplay Folder": None,
            }
            df_prep = pd.concat(
                [df_prep, pd.DataFrame(new_data, index=[0])], ignore_index=True
            )
        for o in tom.all_columns():
            oName = clean_text(o.Name, exclude_characters)
            oDescription = clean_text(o.Description, exclude_characters)
            oDisplayFolder = clean_text(o.DisplayFolder, exclude_characters)
            new_data = {
                "Object Type": "Column",
                "Name": o.Name,
                "TName": oName,
                "Description": o.Description,
                "TDescription": oDescription,
                "Display Folder": o.DisplayFolder,
                "TDisplay Folder": oDisplayFolder,
            }
            df_prep = pd.concat(
                [df_prep, pd.DataFrame(new_data, index=[0])], ignore_index=True
            )
        for o in tom.all_measures():
            oName = clean_text(o.Name, exclude_characters)
            oDescription = clean_text(o.Description, exclude_characters)
            oDisplayFolder = clean_text(o.DisplayFolder, exclude_characters)
            new_data = {
                "Object Type": "Measure",
                "Name": o.Name,
                "TName": oName,
                "Description": o.Description,
                "TDescription": oDescription,
                "Display Folder": o.DisplayFolder,
                "TDisplay Folder": oDisplayFolder,
            }
            df_prep = pd.concat(
                [df_prep, pd.DataFrame(new_data, index=[0])], ignore_index=True
            )
        for o in tom.all_hierarchies():
            oName = clean_text(o.Name, exclude_characters)
            oDescription = clean_text(o.Description, exclude_characters)
            oDisplayFolder = clean_text(o.DisplayFolder, exclude_characters)
            new_data = {
                "Object Type": "Hierarchy",
                "Name": o.Name,
                "TName": oName,
                "Description": o.Description,
                "TDescription": oDescription,
                "Display Folder": o.DisplayFolder,
                "TDisplay Folder": oDisplayFolder,
            }
            df_prep = pd.concat(
                [df_prep, pd.DataFrame(new_data, index=[0])], ignore_index=True
            )
        for o in tom.all_levels():
            oName = clean_text(o.Name, exclude_characters)
            oDescription = clean_text(o.Description, exclude_characters)
            new_data = {
                "Object Type": "Level",
                "Name": o.Name,
                "TName": oName,
                "Description": o.Description,
                "TDescription": oDescription,
                "Display Folder": None,
                "TDisplay Folder": None,
            }
            df_prep = pd.concat(
                [df_prep, pd.DataFrame(new_data, index=[0])], ignore_index=True
            )

        spark = SparkSession.builder.getOrCreate()
        df = spark.createDataFrame(df_prep)

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

            def set_translation_if_exists(
                obj, obj_type, property_name, property_value, df, lang, index
            ):
                if property_name in df.columns and len(property_value) > 0:
                    df_filt = df[
                        (df["Object Type"] == obj_type)
                        & (df[property_name] == property_value)
                    ]
                    if len(df_filt) == 1:
                        translation = df_filt["translation"].str[index].iloc[0]
                        tom.set_translation(
                            object=obj,
                            language=lang,
                            property=property_name,
                            value=translation,
                        )

            for lang in languages:
                i = languages.index(lang)
                tom.add_translation(language=lang)
                print(
                    f"{icons.in_progress} Translating {clm.lower()}s into the '{lang}' language..."
                )

                for t in tom.model.Tables:
                    if t.IsHidden is False:
                        if clm == "Name":
                            set_translation_if_exists(
                                t, "Table", "Name", t.Name, df_panda, lang, i
                            )
                        elif clm == "Description":
                            set_translation_if_exists(
                                t,
                                "Table",
                                "Description",
                                t.Description,
                                df_panda,
                                lang,
                                i,
                            )
                        for c in t.Columns:
                            if c.IsHidden is False:
                                if clm == "Name":
                                    set_translation_if_exists(
                                        c, "Column", "Name", c.Name, df_panda, lang, i
                                    )
                                elif clm == "Description":
                                    set_translation_if_exists(
                                        c,
                                        "Column",
                                        "Description",
                                        c.Description,
                                        df_panda,
                                        lang,
                                        i,
                                    )
                                elif clm == "Display Folder":
                                    set_translation_if_exists(
                                        c,
                                        "Column",
                                        "Display Folder",
                                        c.DisplayFolder,
                                        df_panda,
                                        lang,
                                        i,
                                    )
                        for h in t.Hierarchies:
                            if h.IsHidden is False:
                                if clm == "Name":
                                    set_translation_if_exists(
                                        h,
                                        "Hierarchy",
                                        "Name",
                                        h.Name,
                                        df_panda,
                                        lang,
                                        i,
                                    )
                                elif clm == "Description":
                                    set_translation_if_exists(
                                        h,
                                        "Hierarchy",
                                        "Description",
                                        h.Description,
                                        df_panda,
                                        lang,
                                        i,
                                    )
                                elif clm == "Display Folder":
                                    set_translation_if_exists(
                                        h,
                                        "Hierarchy",
                                        "Display Folder",
                                        h.DisplayFolder,
                                        df_panda,
                                        lang,
                                        i,
                                    )
                                for lev in h.Levels:
                                    if clm == "Name":
                                        set_translation_if_exists(
                                            lev,
                                            "Level",
                                            "Name",
                                            lev.Name,
                                            df_panda,
                                            lang,
                                            i,
                                        )
                                    elif clm == "Description":
                                        set_translation_if_exists(
                                            lev,
                                            "Level",
                                            "Description",
                                            lev.Description,
                                            df_panda,
                                            lang,
                                            i,
                                        )
                    for ms in t.Measures:
                        if ms.IsHidden is False:
                            if clm == "Name":
                                set_translation_if_exists(
                                    ms, "Measure", "Name", ms.Name, df_panda, lang, i
                                )
                            elif clm == "Description":
                                set_translation_if_exists(
                                    ms,
                                    "Measure",
                                    "Description",
                                    ms.Description,
                                    df_panda,
                                    lang,
                                    i,
                                )
                            elif clm == "Display Folder":
                                set_translation_if_exists(
                                    ms,
                                    "Measure",
                                    "Display Folder",
                                    ms.DisplayFolder,
                                    df_panda,
                                    lang,
                                    i,
                                )
    result = pd.DataFrame(
        columns=[
            "Language",
            "Object Type",
            "Table Name",
            "Object Name",
            "Translated Object Name",
            "Description",
            "Translated Description",
            "Display Folder",
            "Translated Display Folder",
        ]
    )
    with connect_semantic_model(
        dataset=dataset, readonly=True, workspace=workspace
    ) as tom:

        sempy.fabric._client._utils._init_analysis_services()
        import Microsoft.AnalysisServices.Tabular as TOM

        for c in tom.model.Cultures:
            for tr in c.ObjectTranslations:
                oType = str(tr.Object.ObjectType)
                oName = tr.Object.Name
                tValue = tr.Value
                prop = str(tr.Property)

                if tr.Object.ObjectType == TOM.ObjectType.Table:
                    desc = tom.model.Tables[oName].Description
                    new_data = {
                        "Language": c.Name,
                        "Table Name": oName,
                        "Object Name": oName,
                        "Object Type": oType,
                        "Description": desc,
                    }
                    result = pd.concat(
                        [result, pd.DataFrame(new_data, index=[0])], ignore_index=True
                    )
                    condition = (
                        (result["Language"] == c.Name)
                        & (result["Table Name"] == oName)
                        & (result["Object Name"] == oName)
                        & (result["Object Type"] == oType)
                    )
                elif tr.Object.ObjectType == TOM.ObjectType.Level:
                    hierarchyName = tr.Object.Parent.Name
                    tName = tr.Object.Parent.Parent.Name
                    levelName = "'" + hierarchyName + "'[" + oName + "]"
                    desc = (
                        tom.model.Tables[tName]
                        .Hierarchies[hierarchyName]
                        .Levels[oName]
                        .Description
                    )
                    new_data = {
                        "Language": c.Name,
                        "Table Name": tName,
                        "Object Name": levelName,
                        "Object Type": oType,
                        "Description": desc,
                    }
                    result = pd.concat(
                        [result, pd.DataFrame(new_data, index=[0])], ignore_index=True
                    )
                    condition = (
                        (result["Language"] == c.Name)
                        & (result["Table Name"] == tName)
                        & (result["Object Name"] == levelName)
                        & (result["Object Type"] == oType)
                    )
                elif tr.Object.ObjectType == TOM.ObjectType.Column:
                    tName = tr.Object.Table.Name
                    desc = tom.model.Tables[tName].Columns[oName].Description
                    display_folder = (
                        tom.model.Tables[tName].Columns[oName].DisplayFolder
                    )
                    new_data = {
                        "Language": c.Name,
                        "Table Name": tName,
                        "Object Name": oName,
                        "Object Type": oType,
                        "Description": desc,
                        "Display Folder": display_folder,
                    }
                    result = pd.concat(
                        [result, pd.DataFrame(new_data, index=[0])], ignore_index=True
                    )
                    condition = (
                        (result["Language"] == c.Name)
                        & (result["Table Name"] == tName)
                        & (result["Object Name"] == oName)
                        & (result["Object Type"] == oType)
                    )
                elif tr.Object.ObjectType == TOM.ObjectType.Measure:
                    tName = tr.Object.Table.Name
                    desc = tom.model.Tables[tName].Measures[oName].Description
                    display_folder = (
                        tom.model.Tables[tName].Measures[oName].DisplayFolder
                    )
                    new_data = {
                        "Language": c.Name,
                        "Table Name": tName,
                        "Object Name": oName,
                        "Object Type": oType,
                        "Description": desc,
                        "Display Folder": display_folder,
                    }
                    result = pd.concat(
                        [result, pd.DataFrame(new_data, index=[0])], ignore_index=True
                    )
                    condition = (
                        (result["Language"] == c.Name)
                        & (result["Table Name"] == tName)
                        & (result["Object Name"] == oName)
                        & (result["Object Type"] == oType)
                    )
                elif tr.Object.ObjectType == TOM.ObjectType.Hierarchy:
                    tName = tr.Object.Table.Name
                    desc = tom.model.Tables[tName].Hierarchies[oName].Description
                    display_folder = (
                        tom.model.Tables[tName].Hierarchies[oName].DisplayFolder
                    )
                    new_data = {
                        "Language": c.Name,
                        "Table Name": tName,
                        "Object Name": oName,
                        "Object Type": oType,
                        "Description": desc,
                        "Display Folder": display_folder,
                    }
                    result = pd.concat(
                        [result, pd.DataFrame(new_data, index=[0])], ignore_index=True
                    )
                    condition = (
                        (result["Language"] == c.Name)
                        & (result["Table Name"] == tName)
                        & (result["Object Name"] == oName)
                        & (result["Object Type"] == oType)
                    )

                if prop == "Caption":
                    result.loc[condition, "Translated Object Name"] = tValue
                elif prop == "Description":
                    result.loc[condition, "Translated Description"] = tValue
                else:
                    result.loc[condition, "Translated Display Folder"] = tValue
        result.fillna("", inplace=True)

    return result
