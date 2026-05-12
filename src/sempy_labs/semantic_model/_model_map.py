model_map = {
    "model": {
        "name": "",
        "description": "",
        "tables": [
            {
                "tableName": "Segment",
                "description": "This is a segment table.",
                "sourceName": "database.schema.table",
                "sourceItemId": "",  # Map to a fabric item
                "sourceWorkspaceId": "",  # Map to a fabric workspace
                "columns": [
                    {
                        "name": "Summary Segment",
                        "sourceColumn": "Segment",
                        "sourceDataType": "VARCHAR",
                        "pbiDataType": "String",
                        "sourceFormat": "",
                        "pbiFormat": "General",
                        "description": "This is a description.",
                        "expression": "",
                        "synonyms": [],
                        "fullDAXObjectName": "'Segment'[Summary Segment]",
                        "isCalculated": False,
                        "isKey": False,
                        "isHidden": False,
                        "columnReferences": [],
                    }
                ],
                "measures": [
                    {
                        "name": "",
                        "sourceExpression": "",
                        "daxExpression": "",
                        "sourceFormat": "",
                        "pbiFormat": "",
                        "description": "",
                        "synonyms": [],
                    }
                ],
            }
        ],
        "relationships": [
            {
                "name": "",
                "fromTable": "",
                "fromColumn": "",
                "toTable": "",
                "toColumn": "",
                "fromCardinality": "Many",
                "toCardinality": "One",
            }
        ],
    }
}


source_map = [
    {
        "sourceName": "database.schema.table",
        "sourceItem": "",
        "sourceItemType": "",
        "sourceWorkspace": "",
    }
]
