import requests


vis_type_mapping = {
    "barChart": "Bar chart",
    "columnChart": "Column chart",
    "clusteredBarChart": "Clustered bar chart",
    "clusteredColumnChart": "Clustered column chart",
    "hundredPercentStackedBarChart": "100% Stacked bar chart",
    "hundredPercentStackedColumnChart": "100% Stacked column chart",
    "lineChart": "Line chart",
    "areaChart": "Area chart",
    "stackedAreaChart": "Stacked area chart",
    "lineStackedColumnComboChart": "Line and stacked column chart",
    "lineClusteredColumnComboChart": "Line and clustered column chart",
    "ribbonChart": "Ribbon chart",
    "waterfallChart": "Waterfall chart",
    "funnel": "Funnel chart",
    "scatterChart": "Scatter chart",
    "pieChart": "Pie chart",
    "donutChart": "Donut chart",
    "treemap": "Treemap",
    "map": "Map",
    "filledMap": "Filled map",
    "shapeMap": "Shape map",
    "azureMap": "Azure map",
    "gauge": "Gauge",
    "card": "Card",
    "multiRowCard": "Multi-row card",
    "kpi": "KPI",
    "slicer": "Slicer",
    "tableEx": "Table",
    "pivotTable": "Matrix",
    "scriptVisual": "R script visual",
    "pythonVisual": "Python visual",
    "keyDriversVisual": "Key influencers",
    "decompositionTreeVisual": "Decomposition tree",
    "qnaVisual": "Q&A",
    "aiNarratives": "Narrative",
    "scorecard": "Metrics (Preview)",
    "rdlVisual": "Paginated report",
    "cardVisual": "Card (new)",
    "advancedSlicerVisual": "Slicer (new)",
    "actionButton": "Button",
    "bookmarkNavigator": "Bookmark navigator",
    "image": "Image",
    "textbox": "Textbox",
    "pageNavigator": "Page navigator",
    "shape": "Shape",
    "Group": "Group",
}

page_type_mapping = {
    (320, 240): "Tooltip",
    (816, 1056): "Letter",
    (960, 720): "4:3",
    (1280, 720): "16:9",
}

page_types = ["Tooltip", "Letter", "4:3", "16:9"]


def populate_custom_visual_display_names():

    url = "https://catalogapi.azure.com/offers?api-version=2018-08-01-beta&storefront=appsource&$filter=offerType+eq+%27PowerBIVisuals%27"

    def fetch_all_pages(start_url):
        combined_json = {}
        current_url = start_url

        while current_url:
            # Send GET request to the current page URL
            response = requests.get(current_url)

            if response.status_code == 200:
                data = response.json()
                # Merge the current page JSON into the combined JSON
                for key, value in data.items():
                    if key not in combined_json:
                        combined_json[key] = value
                    else:
                        # If the key already exists and is a list, extend it
                        if isinstance(value, list):
                            combined_json[key].extend(value)
                        # For other types (non-lists), update the value
                        else:
                            combined_json[key] = value

                # Get the next page link if it exists
                current_url = data.get("nextPageLink")
            else:
                print(f"Error fetching page: {response.status_code}")
                break

        return combined_json

    cvJson = fetch_all_pages(url)

    for i in cvJson.get("items", []):
        vizId = i.get("powerBIVisualId")
        displayName = i.get("displayName")
        vis_type_mapping[vizId] = displayName


def find_entity_property_pairs(data, result=None, keys_path=None):

    if result is None:
        result = {}
    if keys_path is None:
        keys_path = []

    if isinstance(data, dict):
        expression = data.get("Expression", {})
        source_ref = (
            expression.get("SourceRef", {}) if isinstance(expression, dict) else {}
        )

        if (
            isinstance(source_ref, dict)
            and "Entity" in source_ref
            and "Property" in data
        ):
            entity = source_ref.get("Entity", "")
            property_value = data.get("Property", "")

            object_type = (
                keys_path[-1].replace("HierarchyLevel", "Hierarchy")
                if keys_path
                else "Unknown"
            )
            result[property_value] = (entity, object_type)
            if keys_path:
                keys_path.pop()

        # Recursively search the rest of the dictionary
        for key, value in data.items():
            keys_path.append(key)
            find_entity_property_pairs(value, result, keys_path)

    elif isinstance(data, list):
        for item in data:
            find_entity_property_pairs(item, result, keys_path)

    return result


def _get_agg_type_mapping() -> dict:
    """
    This function extracts a mapping dictionary like this:
    {
        "0": "Sum",
        "1": "Average",
        "2": "Distinct count",
    }
    """

    schema_url = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/semanticQuery/1.2.0/schema.json"
    response = requests.get(schema_url)
    schema = response.json()
    aggtypes_schema = schema.get("definitions", {}).get("QueryAggregateFunction", {})

    agg_type_map = {}
    agg_type_map = {
        a.get("const"): a.get("description")
        for a in aggtypes_schema.get("anyOf", [])
        if "const" in a and "description" in a
    }
    agg_type_map["-1"] = "Unknown"

    return agg_type_map


def _get_expression(expr_json, agg_type_map):

    expr_type = list(expr_json.keys())[0]
    if expr_type == "Literal":
        expr = expr_json.get("Literal", {}).get("Value")[1:-1]
    elif expr_type == "Aggregation":
        entity = (
            expr_json.get("Aggregation", {})
            .get("Expression", {})
            .get("Column", {})
            .get("Expression", {})
            .get("SourceRef", {})
            .get("Entity", "Unknown")
        )
        column = (
            expr_json.get("Aggregation", {})
            .get("Expression", {})
            .get("Column", {})
            .get("Property", "Unknown")
        )
        function_id = expr_json.get("Aggregation", {}).get("Function", "-1")
        function = agg_type_map.get(function_id)
        expr = f"{function}('{entity}'[{column}])"
    elif expr_type == "Measure":
        measure = expr_json.get("Measure", {}).get("Property", "Unknown")
        expr = f"[{measure}]"
    else:
        expr = "Unknown"

    return expr
