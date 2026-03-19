import sempy
from sempy_labs.tom import connect_semantic_model
from uuid import UUID
from typing import Optional
import sempy_labs._icons as icons
from sempy._utils._log import log
from sempy_labs._helper_functions import (
    _conv_b64,
)


@log
def create_dataflow_definition_from_semantic_model(
    dataset: str | UUID, workspace: Optional[str | UUID] = None
) -> dict:

    sempy.fabric._client._utils._init_analysis_services()
    import Microsoft.AnalysisServices.Tabular as TOM

    queries_metadata = []
    mashup = "section Section1;"
    with connect_semantic_model(
        dataset=dataset, workspace=workspace, readonly=True
    ) as tom:

        for t in tom.model.Tables:
            p = next(p for p in t.Partitions)
            if p.SourceType == TOM.PartitionSourceType.M:
                queries_metadata.append(
                    {
                        "queryId": None,
                        "queryName": t.Name,
                        "queryGroupId": None,
                        "isHidden": False,
                        "loadEnabled": True,
                    }
                )

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

        for t_name, query in table_map.items():
            mashup += f'\nshared #"{t_name}" = '
            if query is not None:
                pQueryNoSpaces = (
                    query.replace(" ", "")
                    .replace("\n", "")
                    .replace("\t", "")
                    .replace("\r", "")
                )
                if pQueryNoSpaces.startswith('letSource=""'):
                    query = 'let\n\tSource = ""\nin\n\tSource'
            mashup += f"{query};"

        for e_name, kind_expr in expr_map.items():
            expr = kind_expr[1]
            mashup += f'\nshared #"{e_name}" = {expr};'

    metadata_content = {
        "formatVersion": "202502",
        "computeEngineSettings": {"allowFastCopy": True, "maxConcurrency": 1},
        "name": "SampleMetadata",
        "queryGroups": [],
        "documentLocale": "en-US",
        "queriesMetadata": queries_metadata,
        "fastCombine": False,
        "allowNativeQueries": True,
        "skipAutomaticTypeAndHeaderDetection": False,
    }

    definition = {
        "parts": [
            {
                "path": "queryMetadata.json",
                "payload": _conv_b64(metadata_content),
                "payloadType": "InlineBase64",
            },
            {
                "path": "mashup.pq",
                "payload": _conv_b64(mashup),
                "payloadType": "InlineBase64",
            },
        ]
    }

    return definition
