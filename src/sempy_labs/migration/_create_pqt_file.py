import sempy
import json
import os
import shutil
from sempy_labs.lakehouse._lakehouse import lakehouse_attached
from sempy._utils._log import log
from typing import Optional
import sempy_labs._icons as icons
from uuid import UUID
from sempy_labs._helper_functions import (
    resolve_dataset_name_and_id,
    resolve_workspace_name_and_id,
)


@log
def create_pqt_file(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    file_name: str = "PowerQueryTemplate",
):
    """
    Dynamically generates a `Power Query Template <https://learn.microsoft.com/power-query/power-query-template>`_ file based on the semantic model. The .pqt file is
    saved within the Files section of your lakehouse.

    Dataflows Gen2 has a `limit of 50 tables <https://learn.microsoft.com/power-query/power-query-online-limits>`_. If there are more than 50 tables, this will save multiple Power Query Template
    files (with each file having a max of 50 tables).

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    file_name : str, default='PowerQueryTemplate'
        The name of the Power Query Template file to be generated.
    """

    sempy.fabric._client._utils._init_analysis_services()
    import Microsoft.AnalysisServices.Tabular as TOM
    from sempy_labs.tom import connect_semantic_model

    if not lakehouse_attached():
        raise ValueError(
            f"{icons.red_dot} In order to run the 'create_pqt_file' function, a lakehouse must be attached to the notebook. Please attach a lakehouse to this notebook."
        )

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    folderPath = "/lakehouse/default/Files"
    subFolderPath = os.path.join(folderPath, "pqtnewfolder")

    with connect_semantic_model(
        dataset=dataset_id, workspace=workspace_id, readonly=True
    ) as tom:
        if not any(
            p.SourceType == TOM.PartitionSourceType.M for p in tom.all_partitions()
        ) and not any(t.RefreshPolicy for t in tom.model.Tables):
            print(
                f"{icons.info} The '{dataset_name}' semantic model within the '{workspace_name}' workspace has no Power Query logic."
            )
            return

        table_map = {}
        expr_map = {}

        for t in tom.model.Tables:
            table_name = t.Name
            for char in icons.special_characters:
                table_name = table_name.replace(char, "")
            if t.RefreshPolicy:
                table_map[table_name] = t.RefreshPolicy.SourceExpression
            elif any(p.SourceType == TOM.PartitionSourceType.M for p in t.Partitions):
                part_name = next(
                    p.Name
                    for p in t.Partitions
                    if p.SourceType == TOM.PartitionSourceType.M
                )
                expr = t.Partitions[part_name].Source.Expression
                table_map[table_name] = expr

        for e in tom.model.Expressions:
            expr_map[e.Name] = [str(e.Kind), e.Expression]

        expressions = tom.model.Expressions.Count

        # Dataflows Gen2 max table limit is 50.
        max_length = 50 - expressions
        table_chunks = [
            dict(list(table_map.items())[i : i + max_length])
            for i in range(0, len(table_map), max_length)
        ]

        def create_pqt(table_map: dict, expr_map: dict, file_name: str):

            os.makedirs(subFolderPath, exist_ok=True)

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
                    self,
                    DocumentLocale,
                    EngineVersion,
                    QueriesMetadata,
                    QueryGroups=None,
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
            for t_name, query in table_map.items():
                sb = f'{sb}\nshared #"{t_name}" = '
                if query is not None:
                    pQueryNoSpaces = (
                        query.replace(" ", "")
                        .replace("\n", "")
                        .replace("\t", "")
                        .replace("\r", "")
                    )
                    if pQueryNoSpaces.startswith('letSource=""'):
                        query = 'let\n\tSource = ""\nin\n\tSource'
                sb = f"{sb}{query};"

            for e_name, kind_expr in expr_map.items():
                expr = kind_expr[1]
                sb = f'{sb}\nshared #"{e_name}" = {expr};'

            with open(mdFilePath, "w") as file:
                file.write(sb)

            # STEP 2: Create the MashupMetadata.json file
            mmfileName = "MashupMetadata.json"
            mmFilePath = os.path.join(subFolderPath, mmfileName)
            queryMetadata = []

            for t_name, query in table_map.items():
                queryMetadata.append(
                    QueryMetadata(t_name, None, None, None, True, False)
                )
            for e_name, kind_expr in expr_map.items():
                e_kind = kind_expr[0]
                if e_kind == "M":
                    queryMetadata.append(
                        QueryMetadata(e_name, None, None, None, True, False)
                    )
                else:
                    queryMetadata.append(
                        QueryMetadata(e_name, None, None, None, False, False)
                    )

            rootObject = RootObject(
                "en-US", "2.132.328.0", queryMetadata
            )  # "2.126.453.0"

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
            metaData = {"Name": f"{file_name}", "Description": "", "Version": "1.0.0.0"}
            jsonContent = json.dumps(metaData, indent=4)

            with open(mFilePath, "w") as json_file:
                json_file.write(jsonContent)

            # STEP 4: Create [Content_Types].xml file:
            xml_content = """<?xml version="1.0" encoding="utf-8"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="json" ContentType="application/json" /><Default Extension="pq" ContentType="application/x-ms-m" /></Types>"""
            xmlFileName = "[Content_Types].xml"
            xmlFilePath = os.path.join(subFolderPath, xmlFileName)
            with open(xmlFilePath, "w", encoding="utf-8") as file:
                file.write(xml_content)

            # STEP 5: Zip up the 4 files
            zipFileName = f"{file_name}.zip"
            zipFilePath = os.path.join(folderPath, zipFileName)
            shutil.make_archive(zipFilePath[:-4], "zip", subFolderPath)

            # STEP 6: Convert the zip file back into a .pqt file
            newExt = ".pqt"
            directory = os.path.dirname(zipFilePath)
            fileNameWithoutExtension = os.path.splitext(os.path.basename(zipFilePath))[
                0
            ]
            newFilePath = os.path.join(directory, fileNameWithoutExtension + newExt)
            shutil.move(zipFilePath, newFilePath)

            # STEP 7: Delete subFolder directory which is no longer needed
            shutil.rmtree(subFolderPath, ignore_errors=True)

            print(
                f"{icons.green_dot} '{file_name}.pqt' has been created based on the '{dataset_name}' semantic model in the '{workspace_name}' workspace within the Files section of your lakehouse."
            )

        a = 0
        for t_map in table_chunks:
            if a > 0:
                save_file_name = f"{file_name}_{a}"
            else:
                save_file_name = file_name
            a += 1
            create_pqt(t_map, expr_map, file_name=save_file_name)
