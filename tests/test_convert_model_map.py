import pytest

from sempy_labs.semantic_model._convert_model_map import convert_model_map_to_bim


@pytest.fixture
def model_map():
    return {
        "model": {
            "name": "Retail",
            "description": "Retail model.",
            "tables": [
                {
                    "tableName": "Sales",
                    "description": "Sales table.",
                    "sourceName": "db.dbo.sales",
                    "sourceItemId": "11111111-1111-1111-1111-111111111111",
                    "sourceWorkspaceId": "22222222-2222-2222-2222-222222222222",
                    "columns": [
                        {
                            "name": "Sales Amount",
                            "sourceColumn": "SALES_AMOUNT",
                            "sourceDataType": "NUMBER(10,2)",
                            "pbiDataType": "Decimal",
                            "sourceFormat": "#,##0.00",
                            "pbiFormat": None,
                            "description": "",
                            "expression": "",
                            "synonyms": ["revenue", "sales"],
                            "isCalculated": False,
                            "isKey": False,
                            "isHidden": False,
                        },
                        {
                            "name": "Net Amount",
                            "sourceColumn": "",
                            "sourceDataType": "NUMBER",
                            "pbiDataType": "Decimal",
                            "sourceFormat": None,
                            "pbiFormat": None,
                            "description": "",
                            "expression": "sales_amount - discount",
                            "isCalculated": True,
                            "isKey": False,
                            "isHidden": False,
                        },
                        {
                            "name": "Discount",
                            "sourceColumn": "DISCOUNT",
                            "sourceDataType": "NUMBER",
                            "pbiDataType": "Decimal",
                            "sourceFormat": None,
                            "pbiFormat": None,
                            "description": "",
                            "expression": "",
                            "isCalculated": False,
                            "isKey": False,
                            "isHidden": False,
                        },
                        {
                            "name": "Segment Key",
                            "sourceColumn": "SEGMENT_ID",
                            "sourceDataType": "INT",
                            "pbiDataType": "Int64",
                            "sourceFormat": None,
                            "pbiFormat": None,
                            "description": "",
                            "expression": "",
                            "isCalculated": False,
                            "isKey": False,
                            "isHidden": False,
                        },
                        {
                            "name": "Payload",
                            "sourceColumn": "PAYLOAD",
                            "sourceDataType": "BINARY",
                            "pbiDataType": "Binary",
                            "sourceFormat": None,
                            "pbiFormat": None,
                            "description": "",
                            "expression": "",
                            "isCalculated": False,
                            "isKey": False,
                            "isHidden": False,
                        },
                    ],
                    "measures": [
                        {
                            "name": "Total Sales",
                            "sourceExpression": "SUM(sales.sales_amount)",
                            "daxExpression": "SUM('Sales'[Sales Amount])",
                            "sourceFormat": "#,##0",
                            "pbiFormat": "#,##0",
                            "description": "Total sales.",
                            "synonyms": ["total revenue"],
                        }
                    ],
                },
                {
                    "tableName": "Segment",
                    "description": "",
                    "sourceName": "db.dbo.segment",
                    "sourceItemId": "11111111-1111-1111-1111-111111111111",
                    "sourceWorkspaceId": "22222222-2222-2222-2222-222222222222",
                    "columns": [
                        {
                            "name": "Segment Id",
                            "sourceColumn": "SEGMENT_ID",
                            "sourceDataType": "INT",
                            "pbiDataType": "Int64",
                            "sourceFormat": None,
                            "pbiFormat": None,
                            "description": "",
                            "expression": "",
                            "isCalculated": False,
                            "isKey": True,
                            "isHidden": True,
                        }
                    ],
                    "measures": [],
                },
            ],
            "relationships": [
                {
                    "name": None,
                    "fromTable": "Sales",
                    "fromColumn": "SEGMENT_ID",
                    "toTable": "Segment",
                    "toColumn": "SEGMENT_ID",
                    "fromCardinality": "Many",
                    "toCardinality": "One",
                }
            ],
        }
    }


def test_bim_structure_and_direct_lake_expression(model_map):
    bim = convert_model_map_to_bim(model_map)

    assert bim["name"] == "Retail"
    assert bim["compatibilityLevel"] == 1702
    assert bim["model"]["description"] == "Retail model."

    # Tables sharing the same source item share a single expression.
    expressions = bim["model"]["expressions"]
    assert len(expressions) == 1
    assert expressions[0]["kind"] == "m"
    assert (
        'AzureStorage.DataLake("https://onelake.dfs.fabric.microsoft.com/'
        '22222222-2222-2222-2222-222222222222/11111111-1111-1111-1111-111111111111")'
        in expressions[0]["expression"]
    )


def test_direct_lake_partitions(model_map):
    bim = convert_model_map_to_bim(model_map)

    partition = bim["model"]["tables"][0]["partitions"][0]
    assert partition["mode"] == "directLake"
    assert partition["source"] == {
        "type": "entity",
        "entityName": "sales",
        "expressionSource": "DirectLake",
        "schemaName": "dbo",
    }


def test_column_annotations_and_data_types(model_map):
    bim = convert_model_map_to_bim(model_map)

    columns = {c["name"]: c for c in bim["model"]["tables"][0]["columns"]}

    # Binary columns are not supported in Direct Lake.
    assert "Payload" not in columns

    sales_amount = columns["Sales Amount"]
    assert sales_amount["dataType"] == "decimal"
    assert sales_amount["sourceColumn"] == "SALES_AMOUNT"
    assert sales_amount["summarizeBy"] == "sum"
    assert sales_amount["annotations"] == [
        {"name": "SourceDataType", "value": "NUMBER(10,2)"},
        {"name": "SourceFormat", "value": "#,##0.00"},
        {"name": "Synonyms", "value": "revenue, sales"},
    ]

    # Columns with an expression become calculated columns and retain their
    # source expression as an annotation.
    net_amount = columns["Net Amount"]
    assert net_amount["type"] == "calculated"
    assert "sourceColumn" not in net_amount
    assert net_amount["isDataTypeInferred"] is False
    assert net_amount["expression"] == "'Sales'[Sales Amount] - 'Sales'[Discount]"
    assert {
        "name": "SourceExpression",
        "value": "sales_amount - discount",
    } in net_amount["annotations"]

    key_column = bim["model"]["tables"][1]["columns"][0]
    assert key_column["isKey"] is True
    assert key_column["isHidden"] is True
    assert key_column["summarizeBy"] == "count"


def test_measure_annotations(model_map):
    bim = convert_model_map_to_bim(model_map)

    measure = bim["model"]["tables"][0]["measures"][0]
    assert measure["expression"] == "SUM('Sales'[Sales Amount])"
    assert measure["formatString"] == "#,##0"
    assert measure["annotations"] == [
        {"name": "SourceExpression", "value": "SUM(sales.sales_amount)"},
        {"name": "SourceFormat", "value": "#,##0"},
        {"name": "Synonyms", "value": "total revenue"},
    ]


def test_relationships_resolve_source_columns(model_map):
    bim = convert_model_map_to_bim(model_map)

    relationship = bim["model"]["relationships"][0]
    assert relationship["fromTable"] == "Sales"
    assert relationship["fromColumn"] == "Segment Key"
    assert relationship["toTable"] == "Segment"
    assert relationship["toColumn"] == "Segment Id"
    assert relationship["fromCardinality"] == "many"
    assert relationship["toCardinality"] == "one"


def test_missing_source_item_raises(model_map):
    model_map["model"]["tables"][0]["sourceItemId"] = None

    with pytest.raises(ValueError, match="sourceWorkspaceId"):
        convert_model_map_to_bim(model_map)


def test_missing_model_key_raises():
    with pytest.raises(ValueError, match="must contain a 'model' key"):
        convert_model_map_to_bim({})
