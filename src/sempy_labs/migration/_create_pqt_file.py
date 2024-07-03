import sempy.fabric as fabric
import json
import os
import shutil
import xml.etree.ElementTree as ET
from sempy_labs._list_functions import list_tables
from sempy_labs.lakehouse._lakehouse import lakehouse_attached
from sempy._utils._log import log
from typing import Optional
import sempy_labs._icons as icons


@log
def create_pqt_file(
    dataset: str,
    workspace: Optional[str] = None,
    file_name: Optional[str] = "PowerQueryTemplate",
):
    """
    Dynamically generates a `Power Query Template <https://learn.microsoft.com/power-query/power-query-template>`_ file based on the semantic model. The .pqt file is
     saved within the Files section of your lakehouse.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    file_name : str, default='PowerQueryTemplate'
        The name of the Power Query Template file to be generated.
    """

    lakeAttach = lakehouse_attached()

    if lakeAttach is False:
        raise ValueError(
            f"{icons.red_dot} In order to run the 'create_pqt_file' function, a lakehouse must be attached to the notebook. Please attach a lakehouse to this notebook."
        )

    workspace = fabric.resolve_workspace_name(workspace)

    folderPath = "/lakehouse/default/Files"
    subFolderPath = os.path.join(folderPath, "pqtnewfolder")
    os.makedirs(subFolderPath, exist_ok=True)

    dfP = fabric.list_partitions(dataset=dataset, workspace=workspace)
    dfT = list_tables(dataset, workspace)
    dfE = fabric.list_expressions(dataset=dataset, workspace=workspace)

    # Check if M-partitions are used
    if any(dfP["Source Type"] == "M"):

        class QueryMetadata:
            def __init__(
                self,
                QueryName,
                QueryGroupId=None,
                LastKnownIsParameter=None,
                LastKnownResultTypeName=None,
                LoadEnabled=True,
                IsHidden=False,
            ):
                self.QueryName = QueryName
                self.QueryGroupId = QueryGroupId
                self.LastKnownIsParameter = LastKnownIsParameter
                self.LastKnownResultTypeName = LastKnownResultTypeName
                self.LoadEnabled = LoadEnabled
                self.IsHidden = IsHidden

        class RootObject:
            def __init__(
                self, DocumentLocale, EngineVersion, QueriesMetadata, QueryGroups=None
            ):
                if QueryGroups is None:
                    QueryGroups = []
                self.DocumentLocale = DocumentLocale
                self.EngineVersion = EngineVersion
                self.QueriesMetadata = QueriesMetadata
                self.QueryGroups = QueryGroups

        # STEP 1: Create MashupDocument.pq
        mdfileName = "MashupDocument.pq"
        mdFilePath = os.path.join(subFolderPath, mdfileName)
        sb = "section Section1;"
        for table_name in dfP["Table Name"].unique():
            tName = '#"' + table_name + '"'
            sourceExpression = dfT.loc[
                (dfT["Name"] == table_name), "Source Expression"
            ].iloc[0]
            refreshPolicy = dfT.loc[(dfT["Name"] == table_name), "Refresh Policy"].iloc[
                0
            ]
            sourceType = dfP.loc[(dfP["Table Name"] == table_name), "Source Type"].iloc[
                0
            ]

            if sourceType == "M" or refreshPolicy:
                sb = sb + "\n" + "shared " + tName + " = "

            partitions_in_table = dfP.loc[
                dfP["Table Name"] == table_name, "Partition Name"
            ].unique()

            i = 1
            for partition_name in partitions_in_table:
                pSourceType = dfP.loc[
                    (dfP["Table Name"] == table_name)
                    & (dfP["Partition Name"] == partition_name),
                    "Source Type",
                ].iloc[0]
                pQuery = dfP.loc[
                    (dfP["Table Name"] == table_name)
                    & (dfP["Partition Name"] == partition_name),
                    "Query",
                ].iloc[0]

                if pQuery is not None:
                    pQueryNoSpaces = (
                        pQuery.replace(" ", "")
                        .replace("\n", "")
                        .replace("\t", "")
                        .replace("\r", "")
                    )
                    if pQueryNoSpaces.startswith('letSource=""'):
                        pQuery = 'let\n\tSource = ""\nin\n\tSource'

                if pSourceType == "M" and i == 1:
                    sb = sb + pQuery + ";"
                elif refreshPolicy and i == 1:
                    sb = sb + sourceExpression + ";"
                i += 1

        for index, row in dfE.iterrows():
            expr = row["Expression"]
            eName = row["Name"]
            eName = '#"' + eName + '"'
            sb = sb + "\n" + "shared " + eName + " = " + expr + ";"

        with open(mdFilePath, "w") as file:
            file.write(sb)

        # STEP 2: Create the MashupMetadata.json file
        mmfileName = "MashupMetadata.json"
        mmFilePath = os.path.join(subFolderPath, mmfileName)
        queryMetadata = []

        for tName in dfP["Table Name"].unique():
            sourceType = dfP.loc[(dfP["Table Name"] == tName), "Source Type"].iloc[0]
            refreshPolicy = dfT.loc[(dfT["Name"] == tName), "Refresh Policy"].iloc[0]
            if sourceType == "M" or refreshPolicy:
                queryMetadata.append(
                    QueryMetadata(tName, None, None, None, True, False)
                )

        for i, r in dfE.iterrows():
            eName = r["Name"]
            eKind = r["Kind"]
            if eKind == "M":
                queryMetadata.append(
                    QueryMetadata(eName, None, None, None, True, False)
                )
            else:
                queryMetadata.append(
                    QueryMetadata(eName, None, None, None, False, False)
                )

        rootObject = RootObject("en-US", "2.126.453.0", queryMetadata)

        def obj_to_dict(obj):
            if isinstance(obj, list):
                return [obj_to_dict(e) for e in obj]
            elif hasattr(obj, "__dict__"):
                return {k: obj_to_dict(v) for k, v in obj.__dict__.items()}
            else:
                return obj

        jsonContent = json.dumps(obj_to_dict(rootObject), indent=4)

        with open(mmFilePath, "w") as json_file:
            json_file.write(jsonContent)

        # STEP 3: Create Metadata.json file
        mFileName = "Metadata.json"
        mFilePath = os.path.join(subFolderPath, mFileName)
        metaData = {"Name": "fileName", "Description": "", "Version": "1.0.0.0"}
        jsonContent = json.dumps(metaData, indent=4)

        with open(mFilePath, "w") as json_file:
            json_file.write(jsonContent)

        # STEP 4: Create [Content_Types].xml file:
        ns = "http://schemas.openxmlformats.org/package/2006/content-types"
        ET.register_namespace("", ns)
        types = ET.Element("{%s}Types" % ns)
        # default1 = ET.SubElement(
        #    types,
        #    "{%s}Default" % ns,
        #    {"Extension": "json", "ContentType": "application/json"},
        # )
        # default2 = ET.SubElement(
        #    types,
        #    "{%s}Default" % ns,
        #    {"Extension": "pq", "ContentType": "application/x-ms-m"},
        # )
        xmlDocument = ET.ElementTree(types)
        xmlFileName = "[Content_Types].xml"
        xmlFilePath = os.path.join(subFolderPath, xmlFileName)
        xmlDocument.write(
            xmlFilePath, xml_declaration=True, encoding="utf-8", method="xml"
        )

        # STEP 5: Zip up the 4 files
        zipFileName = file_name + ".zip"
        zipFilePath = os.path.join(folderPath, zipFileName)
        shutil.make_archive(zipFilePath[:-4], "zip", subFolderPath)

        # STEP 6: Convert the zip file back into a .pqt file
        newExt = ".pqt"
        directory = os.path.dirname(zipFilePath)
        fileNameWithoutExtension = os.path.splitext(os.path.basename(zipFilePath))[0]
        newFilePath = os.path.join(directory, fileNameWithoutExtension + newExt)
        shutil.move(zipFilePath, newFilePath)

        # STEP 7: Delete subFolder directory which is no longer needed
        shutil.rmtree(subFolderPath, ignore_errors=True)

        print(
            f"{icons.green_dot} '{file_name}.pqt' has been created based on the '{dataset}' semantic model in the '{workspace}' workspace within the Files section of your lakehouse."
        )

    else:
        print(
            f"{icons.yellow_dot} The '{dataset}' semantic model in the '{workspace}' workspace does not use Power Query so a Power Query Template file cannot be generated."
        )
