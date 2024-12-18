import sempy.fabric as fabric
from typing import Tuple, Optional
import sempy_labs._icons as icons
import re
import base64
import json
import requests
from uuid import UUID
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
)


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


def get_web_url(report: str, workspace: Optional[str | UUID] = None):

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    dfR = fabric.list_reports(workspace=workspace_id)
    dfR_filt = dfR[dfR["Name"] == report]

    if len(dfR_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{report}' report does not exist within the '{workspace_name}' workspace."
        )
    web_url = dfR_filt["Web Url"].iloc[0]

    return web_url


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


def resolve_page_name(self, page_name: str) -> Tuple[str, str, str]:

    dfP = self.list_pages()
    if any(r["Page Name"] == page_name for _, r in dfP.iterrows()):
        valid_page_name = page_name
        dfP_filt = dfP[dfP["Page Name"] == page_name]
        valid_display_name = dfP_filt["Page Display Name"].iloc[0]
        file_path = dfP_filt["File Path"].iloc[0]
    elif any(r["Page Display Name"] == page_name for _, r in dfP.iterrows()):
        valid_display_name = page_name
        dfP_filt = dfP[dfP["Page Display Name"] == page_name]
        valid_page_name = dfP_filt["Page Name"].iloc[0]
        file_path = dfP_filt["File Path"].iloc[0]
    else:
        raise ValueError(
            f"{icons.red_dot} Invalid page name. The '{page_name}' page does not exist in the '{self._report}' report within the '{self._workspace}' workspace."
        )

    return valid_page_name, valid_display_name, file_path


def visual_page_mapping(self) -> Tuple[dict, dict]:

    page_mapping = {}
    visual_mapping = {}
    rd = self.rdef
    for _, r in rd.iterrows():
        file_path = r["path"]
        payload = r["payload"]
        if file_path.endswith("/page.json"):
            pattern_page = r"/pages/(.*?)/page.json"
            page_name = re.search(pattern_page, file_path).group(1)
            obj_file = base64.b64decode(payload).decode("utf-8")
            obj_json = json.loads(obj_file)
            page_id = obj_json.get("name")
            page_display = obj_json.get("displayName")
            page_mapping[page_name] = (page_id, page_display)
    for _, r in rd.iterrows():
        file_path = r["path"]
        payload = r["payload"]
        if file_path.endswith("/visual.json"):
            pattern_page = r"/pages/(.*?)/visuals/"
            page_name = re.search(pattern_page, file_path).group(1)
            visual_mapping[file_path] = (
                page_mapping.get(page_name)[0],
                page_mapping.get(page_name)[1],
            )

    return page_mapping, visual_mapping


def resolve_visual_name(
    self, page_name: str, visual_name: str
) -> Tuple[str, str, str, str]:
    """
    Obtains the page name, page display name, and the file path for a given page in a report.

    Parameters
    ----------
    page_name : str
        The name of the page of the report - either the page name (GUID) or the page display name.
    visual_name : str
        The name of the visual of the report.

    Returns
    -------
    Tuple[str, str, str, str] Page name, page display name, visual name, file path from the report definition.

    """

    dfV = self.list_visuals()
    if any(
        (r["Page Name"] == page_name) & (r["Visual Name"] == visual_name)
        for _, r in dfV.iterrows()
    ):
        valid_page_name = page_name
        dfV_filt = dfV[
            (dfV["Page Name"] == page_name) & (dfV["Visual Name"] == visual_name)
        ]
        file_path = dfV_filt["File Path"].iloc[0]
        valid_display_name = dfV_filt["Page Display Name"].iloc[0]
    elif any(
        (r["Page Display Name"] == page_name) & (r["Visual Name"] == visual_name)
        for _, r in dfV.iterrows()
    ):
        valid_display_name = page_name
        dfV_filt = dfV[
            (dfV["Page Display Name"] == page_name)
            & (dfV["Visual Name"] == visual_name)
        ]
        file_path = dfV_filt["File Path"].iloc[0]
        valid_page_name = dfV_filt["Page Name"].iloc[0]
    else:
        raise ValueError(
            f"{icons.red_dot} Invalid page/visual name. The '{visual_name}' visual on the '{page_name}' page does not exist in the '{self._report}' report within the '{self._workspace}' workspace."
        )

    return valid_page_name, valid_display_name, visual_name, file_path


def find_entity_property_pairs(data, result=None, keys_path=None):

    if result is None:
        result = {}
    if keys_path is None:
        keys_path = []

    if isinstance(data, dict):
        if (
            "Entity" in data.get("Expression", {}).get("SourceRef", {})
            and "Property" in data
        ):
            entity = data.get("Expression", {}).get("SourceRef", {}).get("Entity", {})
            property_value = data.get("Property")
            object_type = keys_path[-1].replace("HierarchyLevel", "Hierarchy")
            result[property_value] = (entity, object_type)
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
