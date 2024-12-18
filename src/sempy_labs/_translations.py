import sempy
import pandas as pd
from typing import List, Optional, Union
from sempy._utils._log import log
import sempy_labs._icons as icons
from sempy_labs._helper_functions import (
    get_language_codes,
)
from uuid import UUID


@log
def translate_semantic_model(
    dataset: str | UUID,
    languages: Union[str, List[str]],
    exclude_characters: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Translates names, descriptions, display folders for all objects in a semantic model.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    languages : str, List[str]
        The language code(s) in which to translate the semantic model.
    exclude_characters : str
        A string specifying characters which will be replaced by a space in the translation text when sent to the translation service.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
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

    icons.sll_tags.append("TranslateSemanticModel")

    def _clean_text(text, exclude_chars):
        if exclude_chars:
            for char in exclude_chars:
                text = text.replace(char, " ")
        return text

    if isinstance(languages, str):
        languages = [languages]

    languages = get_language_codes(languages)

    df_prep = pd.DataFrame(
        columns=["Object Type", "Name", "Description", "Display Folder"]
    )

    final_df = pd.DataFrame(columns=["Value", "Translation"])

    with connect_semantic_model(
        dataset=dataset, readonly=False, workspace=workspace
    ) as tom:

        for o in tom.model.Tables:
            oName = _clean_text(o.Name, exclude_characters)
            oDescription = _clean_text(o.Description, exclude_characters)
            new_data = {
                "Name": o.Name,
                "TName": oName,
                "Object Type": "Table",
                "Description": o.Description,
                "TDescription": oDescription,
                "Display Folder": None,
                "TDisplay Folder": None,
            }
            df_prep = pd.concat(
                [df_prep, pd.DataFrame(new_data, index=[0])], ignore_index=True
            )
        for o in tom.all_columns():
            oName = _clean_text(o.Name, exclude_characters)
            oDescription = _clean_text(o.Description, exclude_characters)
            oDisplayFolder = _clean_text(o.DisplayFolder, exclude_characters)
            new_data = {
                "Name": o.Name,
                "TName": oName,
                "Object Type": "Column",
                "Description": o.Description,
                "TDescription": oDescription,
                "Display Folder": o.DisplayFolder,
                "TDisplay Folder": oDisplayFolder,
            }
            df_prep = pd.concat(
                [df_prep, pd.DataFrame(new_data, index=[0])], ignore_index=True
            )
        for o in tom.all_measures():
            oName = _clean_text(o.Name, exclude_characters)
            oDescription = _clean_text(o.Description, exclude_characters)
            oDisplayFolder = _clean_text(o.DisplayFolder, exclude_characters)
            new_data = {
                "Name": o.Name,
                "TName": oName,
                "Object Type": "Measure",
                "Description": o.Description,
                "TDescription": oDescription,
                "Display Folder": o.DisplayFolder,
                "TDisplay Folder": oDisplayFolder,
            }
            df_prep = pd.concat(
                [df_prep, pd.DataFrame(new_data, index=[0])], ignore_index=True
            )
        for o in tom.all_hierarchies():
            oName = _clean_text(o.Name, exclude_characters)
            oDescription = _clean_text(o.Description, exclude_characters)
            oDisplayFolder = _clean_text(o.DisplayFolder, exclude_characters)
            new_data = {
                "Name": o.Name,
                "TName": oName,
                "Object Type": "Hierarchy",
                "Description": o.Description,
                "TDescription": oDescription,
                "Display Folder": o.DisplayFolder,
                "TDisplay Folder": oDisplayFolder,
            }
            df_prep = pd.concat(
                [df_prep, pd.DataFrame(new_data, index=[0])], ignore_index=True
            )
        for o in tom.all_levels():
            oName = _clean_text(o.Name, exclude_characters)
            oDescription = _clean_text(o.Description, exclude_characters)
            new_data = {
                "Name": o.Name,
                "TName": oName,
                "Object Type": "Level",
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
            df_panda = df_panda[~df_panda[clm].isin([None, ""])][[clm, "translation"]]

            df_panda = df_panda.rename(columns={clm: "value"})
            final_df = pd.concat([final_df, df_panda], ignore_index=True)

        def set_translation_if_exists(object, language, property, index):

            if property == "Name":
                trans = object.Name
            elif property == "Description":
                trans = object.Description
            elif property == "Display Folder":
                trans = object.DisplayFolder

            df_filt = final_df[final_df["value"] == trans]
            if not df_filt.empty:
                translation_value = df_filt["translation"].str[index].iloc[0]
                tom.set_translation(
                    object=object,
                    language=language,
                    property=property,
                    value=translation_value,
                )

        for language in languages:
            index = languages.index(language)
            tom.add_translation(language=language)
            print(
                f"{icons.in_progress} Translating {clm.lower()}s into the '{language}' language..."
            )

            for t in tom.model.Tables:
                set_translation_if_exists(
                    object=t, language=language, property="Name", index=index
                )
                set_translation_if_exists(
                    object=t, language=language, property="Description", index=index
                )
            for c in tom.all_columns():
                set_translation_if_exists(
                    object=c, language=language, property="Name", index=index
                )
                set_translation_if_exists(
                    object=c, language=language, property="Description", index=index
                )
                set_translation_if_exists(
                    object=c, language=language, property="Display Folder", index=index
                )
            for c in tom.all_measures():
                set_translation_if_exists(
                    object=c, language=language, property="Name", index=index
                )
                set_translation_if_exists(
                    object=c, language=language, property="Description", index=index
                )
                set_translation_if_exists(
                    object=c, language=language, property="Display Folder", index=index
                )
            for c in tom.all_hierarchies():
                set_translation_if_exists(
                    object=c, language=language, property="Name", index=index
                )
                set_translation_if_exists(
                    object=c, language=language, property="Description", index=index
                )
                set_translation_if_exists(
                    object=c, language=language, property="Display Folder", index=index
                )
            for c in tom.all_levels():
                set_translation_if_exists(
                    object=c, language=language, property="Name", index=index
                )
                set_translation_if_exists(
                    object=c, language=language, property="Description", index=index
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
