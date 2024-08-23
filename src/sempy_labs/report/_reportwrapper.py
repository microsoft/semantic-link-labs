import sempy.fabric as fabric
from sempy_labs._helper_functions import (
    resolve_report_id,
    format_dax_object_name,
    resolve_dataset_from_report,
    _conv_b64,
    _extract_json,
    _add_part,
)
import requests
from typing import Optional, List, Tuple
import pandas as pd
import re
import json
import base64
import time
from sempy._utils._log import log
import sempy_labs._icons as icons
from sempy.fabric.exceptions import FabricHTTPException
from sempy_labs._list_functions import list_reports_using_semantic_model
from sempy_labs._model_dependencies import get_measure_dependencies
from sempy_labs.tom import connect_semantic_model

_vis_type_mapping = {
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


def list_semantic_model_report_objects(
    dataset: str, workspace: Optional[str] = None, extended: Optional[bool] = False
) -> pd.DataFrame:

    dfRO = pd.DataFrame(
        columns=[
            "Report Name",
            "Report Workspace Name",
            "Table Name",
            "Object Name",
            "Object Type",
            "Report Source",
            "Report Source Object",
        ]
    )

    # Collect all reports which use the semantic model
    dfR = list_reports_using_semantic_model(dataset=dataset, workspace=workspace)
    if len(dfR) > 0:
        for i, r in dfR.iterrows():
            report_name = r["Report Name"]
            report_workspace = r["Report Workspace Name"]

            rpt = ReportWrapper(
                report=report_name, workspace=report_workspace, readonly=True
            )
            # Collect all semantic model objects used in the report
            dfRSO = rpt.list_semantic_model_objects()
            dfRSO["Report Name"] = report_name
            dfRSO["Report Workspace Name"] = report_workspace
            colName = "Report Name"
            dfRSO.insert(0, colName, dfRSO.pop(colName))
            colName = "Report Workspace Name"
            dfRSO.insert(1, colName, dfRSO.pop(colName))

            dfRO = pd.concat([dfRO, dfRSO], ignore_index=True)

    # Collect all semantic model objects
    if extended:
        with connect_semantic_model(
            dataset=dataset, readonly=True, workspace=workspace
        ) as tom:
            for index, row in dfRO.iterrows():
                object_type = row["Object Type"]
                if object_type == "Measure":
                    dfRO.at[index, "Valid Object"] = any(
                        o.Name == row["Object Name"] for o in tom.all_measures()
                    )
                elif object_type == "Column":
                    dfRO.at[index, "Valid Object"] = any(
                        format_dax_object_name(c.Parent.Name, c.Name)
                        == format_dax_object_name(row["Table Name"], row["Object Name"])
                        for c in tom.all_columns()
                    )
                elif object_type == "Hierarchy":
                    dfRO.at[index, "Valid Object"] = any(
                        format_dax_object_name(h.Parent.Name, h.Name)
                        == format_dax_object_name(row["Table Name"], row["Object Name"])
                        for h in tom.all_hierarchies()
                    )

    return dfRO


class ReportWrapper:

    _report: str
    _workspace: str
    _readonly: bool

    @log
    def __init__(
        self,
        report: str,
        workspace: Optional[str] = None,
        readonly: Optional[bool] = True,
    ):

        from sempy_labs.report import get_report_definition

        self._report = report
        self._workspace = workspace
        self._workspace_id = fabric.resolve_workspace_id(workspace)
        self._report_id = resolve_report_id(report, workspace)
        self._readonly = readonly
        self.rdef = get_report_definition(
            report=self._report, workspace=self._workspace
        )

        if len(self.rdef[self.rdef["path"] == "definition/report.json"]) == 0:
            raise ValueError(
                f"{icons.red_dot} The ReportWrapper function requires the report to be in the PBIR format."
                "See here for details: https://powerbi.microsoft.com/blog/power-bi-enhanced-report-format-pbir-in-power-bi-desktop-developer-mode-preview/"
            )

    # Helper functions
    def __populate_custom_visual_display_names(self):

        url1 = "https://catalogapi.azure.com/offers?api-version=2018-08-01-beta&storefront=appsource&$filter=offerType+eq+%27PowerBIVisuals%27"
        url2 = f"{url1}&$skiptoken=W3sidG9rZW4iOiIrUklEOn4yVk53QUxkRkVIeEJhUUFBQUFCQUNBPT0jUlQ6MSNUUkM6MTk3I0lTVjoyI0lFTzo2NTU2NyNRQ0Y6OCIsInJhbmdlIjp7Im1pbiI6IjA1QzFFNzBCM0IzOTUwIiwibWF4IjoiMDVDMUU3QUIxN0U3QTYifX1d"
        url3 = f"{url1}&$skiptoken=W3sidG9rZW4iOiIrUklEOn4yVk53QUxkRkVId1pyQVVBQUFBQURBPT0jUlQ6MiNUUkM6NDg3I0lTVjoyI0lFTzo2NTU2NyNRQ0Y6OCIsInJhbmdlIjp7Im1pbiI6IjA1QzFFNzBCM0IzOTUwIiwibWF4IjoiMDVDMUU3QUIxN0U3QTYifX1d"

        my_list = [url1, url2, url3]

        for url in my_list:
            response = requests.get(url)
            cvJson = response.json()

            for i in cvJson.get("items", []):
                vizId = i.get("powerBIVisualId")
                displayName = i.get("displayName")
                _vis_type_mapping[vizId] = displayName

    def _get_web_url(self):

        dfR = fabric.list_reports(workspace=self._workspace)
        dfR_filt = dfR[dfR["Name"] == self._report]
        web_url = dfR_filt["Web Url"].iloc[0]

        return web_url

    def update_report(self, request_body: dict):

        client = fabric.FabricRestClient()
        response = client.post(
            f"/v1/workspaces/{self._workspace_id}/reports/{self._report_id}/updateDefinition",
            json=request_body,
        )

        if response.status_code not in [200, 202]:
            raise FabricHTTPException(response)
        if response.status_code == 202:
            operationId = response.headers["x-ms-operation-id"]
            response = client.get(f"/v1/operations/{operationId}")
            response_body = json.loads(response.content)
            while response_body["status"] not in ["Succeeded", "Failed"]:
                time.sleep(1)
                response = client.get(f"/v1/operations/{operationId}")
                response_body = json.loads(response.content)
            if response_body["status"] != "Succeeded":
                raise FabricHTTPException(response)

    def resolve_page_name(self, page_name: str) -> Tuple[str, str, str]:
        """
        Obtains the page name, page display name, and the file path for a given page in a report.

        Parameters
        ----------
        page_name : str
            The name of the page of the report - either the page name (GUID) or the page display name.

        Returns
        -------
        Tuple[str, str, str] Page name, page display name, file path from the report definition.

        """

        dfP = self.list_pages()
        if any(r["Page Name"] == page_name for i, r in dfP.iterrows()):
            valid_page_name = page_name
            dfP_filt = dfP[dfP["Page Name"] == page_name]
            valid_display_name = dfP_filt["Page Display Name"].iloc[0]
            file_path = dfP_filt["File Path"].iloc[0]
        elif any(r["Page Display Name"] == page_name for i, r in dfP.iterrows()):
            valid_display_name = page_name
            dfP_filt = dfP[dfP["Page Display Name"] == page_name]
            valid_page_name = dfP_filt["Page Name"].iloc[0]
            file_path = dfP_filt["File Path"].iloc[0]
        else:
            raise ValueError(
                f"Invalid page name. The '{page_name}' page does not exist in the '{self._report}' report within the '{self._workspace}' workspace."
            )

        return valid_page_name, valid_display_name, file_path

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
            for i, r in dfV.iterrows()
        ):
            valid_page_name = page_name
            dfV_filt = dfV[
                (dfV["Page Name"] == page_name) & (dfV["Visual Name"] == visual_name)
            ]
            file_path = dfV_filt["File Path"].iloc[0]
            valid_display_name = dfV_filt["Page Display Name"].iloc[0]
        elif any(
            (r["Page Display Name"] == page_name) & (r["Visual Name"] == visual_name)
            for i, r in dfV.iterrows()
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

    def __visual_page_mapping(self) -> Tuple[dict, dict]:

        page_mapping = {}
        visual_mapping = {}
        rd = self.rdef
        for i, r in rd.iterrows():
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
        for i, r in rd.iterrows():
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

    def get_theme(self, theme_type: Optional[str] = "baseTheme"):

        theme_types = ["baseTheme", "customTheme"]
        theme_type = theme_type.lower()

        if "custom" in theme_type:
            theme_type = "customTheme"
        elif "base" in theme_type:
            theme_type = "baseTheme"
        if theme_type not in theme_types:
            raise ValueError(
                f"{icons.red_dot} Invalid theme type. Valid options: {theme_types}."
            )

        rptdef = self.rdef[self.rdef["path"] == "definition/report.json"]
        rptJson = _extract_json(rptdef)
        theme_collection = rptJson.get("themeCollection", {})
        if theme_type not in theme_collection:
            raise ValueError(
                f"{icons.red_dot} The {self._report} report within the '{self._workspace} workspace has no custom theme."
            )
        ct = theme_collection.get(theme_type)
        theme_name = ct["name"]
        theme_location = ct["type"]
        theme_file_path = f"StaticResources/{theme_location}/{theme_name}"
        if theme_type == "baseTheme":
            theme_file_path = (
                f"StaticResources/{theme_location}/BaseThemes/{theme_name}"
            )
        if not theme_file_path.endswith(".json"):
            theme_file_path = f"{theme_file_path}.json"

        theme_df = self.rdef[self.rdef["path"] == theme_file_path]
        theme_json = _extract_json(theme_df)

        return theme_json

    # List functions
    def list_custom_visuals(self, extended: Optional[bool] = False) -> pd.DataFrame:
        """
        Shows a list of all custom visuals used in the report.

        Parameters
        ----------
        extended : bool, default=False
            If True, adds extra columns to the resulting dataframe.

        Returns
        -------
        pandas.DataFrame
            A pandas dataframe containing a list of all the custom visuals used in the report.
        """

        self.__populate_custom_visual_display_names()

        df = pd.DataFrame(columns=["Custom Visual Name", "Custom Visual Display Name"])
        rd = self.rdef
        rd_filt = rd[rd["path"] == "definition/report.json"]
        rptJson = _extract_json(rd_filt)
        df["Custom Visual Name"] = rptJson.get("publicCustomVisuals")
        df["Custom Visual Display Name"] = df["Custom Visual Name"].apply(
            lambda x: _vis_type_mapping.get(x, x)
        )

        if extended:
            df["Used in Report"] = df["Custom Visual Name"].isin(
                self.list_visuals()["Type"]
            )

        return df

    def list_report_filters(self, extended: Optional[bool] = False) -> pd.DataFrame:
        """
        Shows a list of all report filters used in the report.

        Parameters
        ----------
        extended : bool, default=False
            If set to True, adds a column 'Valid' identifying whether the object is a valid object within the semantic model used by the report.

        Returns
        -------
        pandas.DataFrame
            A pandas dataframe containing a list of all the report filters used in the report.
        """

        rd = self.rdef
        rd_filt = rd[rd["path"] == "definition/report.json"]

        df = pd.DataFrame(
            columns=[
                "Filter Name",
                "Type",
                "Table Name",
                "Object Name",
                "Object Type",
                "Hidden",
                "Locked",
                "How Created",
                "Used",
            ]
        )

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
                    entity = (
                        data.get("Expression", {})
                        .get("SourceRef", {})
                        .get("Entity", {})
                    )
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

        if len(rd_filt) == 1:
            rpt_json = _extract_json(rd_filt)
            if "filterConfig" in rpt_json:
                for flt in rpt_json.get("filterConfig", {}).get("filters", {}):
                    filter_name = flt.get("name")
                    how_created = flt.get("howCreated")
                    locked = flt.get("isLockedInViewMode", False)
                    hidden = flt.get("isHiddenInViewMode", False)
                    filter_type = flt.get("type", "Basic")
                    filter_used = True if "Where" in flt.get("filter", {}) else False

                    entity_property_pairs = find_entity_property_pairs(flt)

                    for object_name, properties in entity_property_pairs.items():
                        new_data = {
                            "Filter Name": filter_name,
                            "Type": filter_type,
                            "Table Name": properties[0],
                            "Object Name": object_name,
                            "Object Type": properties[1],
                            "Hidden": hidden,
                            "Locked": locked,
                            "How Created": how_created,
                            "Used": filter_used,
                        }

                        df = pd.concat(
                            [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                        )

        bool_cols = ["Hidden", "Locked", "Used"]
        df[bool_cols] = df[bool_cols].astype(bool)

        if extended:
            dataset_id, dataset_name, dataset_workspace_id, dataset_workspace = (
                resolve_dataset_from_report(
                    report=self._report, workspace=self._workspace
                )
            )

            with connect_semantic_model(
                dataset=dataset_name, readonly=True, workspace=dataset_workspace
            ) as tom:
                for index, row in df.iterrows():
                    obj_type = row["Object Type"]
                    if obj_type == "Measure":
                        df.at[index, "Valid Object"] = any(
                            o.Name == row["Object Name"] for o in tom.all_measures()
                        )
                    elif obj_type == "Column":
                        df.at[index, "Valid Object"] = any(
                            format_dax_object_name(c.Parent.Name, c.Name)
                            == format_dax_object_name(
                                row["Table Name"], row["Object Name"]
                            )
                            for c in tom.all_columns()
                        )
                    elif obj_type == "Hierarchy":
                        df.at[index, "Valid Object"] = any(
                            format_dax_object_name(h.Parent.Name, h.Name)
                            == format_dax_object_name(
                                row["Table Name"], row["Object Name"]
                            )
                            for h in tom.all_hierarchies()
                        )

        return df

    def list_page_filters(self, extended: Optional[bool] = False) -> pd.DataFrame:
        """
        Shows a list of all page filters used in the report.

        Parameters
        ----------
        extended : bool, default=False
            If set to True, adds more columns showing additional properties.

        Returns
        -------
        pandas.DataFrame
            A pandas dataframe containing a list of all the page filters used in the report.
        """

        rd = self.rdef
        df = pd.DataFrame(
            columns=[
                "Page Name",
                "Page Display Name",
                "Filter Name",
                "Type",
                "Table Name",
                "Object Name",
                "Object Type",
                "Hidden",
                "Locked",
                "How Created",
                "Used",
            ]
        )

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
                    entity = (
                        data.get("Expression", {})
                        .get("SourceRef", {})
                        .get("Entity", {})
                    )
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

        for i, r in rd.iterrows():
            path = r["path"]
            payload = r["payload"]
            if path.endswith("/page.json"):
                obj_file = base64.b64decode(payload).decode("utf-8")
                obj_json = json.loads(obj_file)
                page_id = obj_json.get("name")
                page_display = obj_json.get("displayName")

                if "filterConfig" in obj_json:
                    for flt in obj_json.get("filterConfig", {}).get("filters", {}):
                        filter_name = flt.get("name")
                        how_created = flt.get("howCreated")
                        locked = flt.get("isLockedInViewMode", False)
                        hidden = flt.get("isHiddenInViewMode", False)
                        filter_type = flt.get("type", "Basic")
                        filter_used = (
                            True if "Where" in flt.get("filter", {}) else False
                        )

                        entity_property_pairs = find_entity_property_pairs(flt)

                        for object_name, properties in entity_property_pairs.items():
                            new_data = {
                                "Page Name": page_id,
                                "Page Display Name": page_display,
                                "Filter Name": filter_name,
                                "Type": filter_type,
                                "Table Name": properties[0],
                                "Object Name": object_name,
                                "Object Type": properties[1],
                                "Hidden": hidden,
                                "Locked": locked,
                                "How Created": how_created,
                                "Used": filter_used,
                            }

                            df = pd.concat(
                                [df, pd.DataFrame(new_data, index=[0])],
                                ignore_index=True,
                            )

        bool_cols = ["Hidden", "Locked", "Used"]
        df[bool_cols] = df[bool_cols].astype(bool)

        if extended:
            dataset_id, dataset_name, dataset_workspace_id, dataset_workspace = (
                resolve_dataset_from_report(
                    report=self._report, workspace=self._workspace
                )
            )

            with connect_semantic_model(
                dataset=dataset_name, readonly=True, workspace=dataset_workspace
            ) as tom:
                for index, row in df.iterrows():
                    obj_type = row["Object Type"]
                    if obj_type == "Measure":
                        df.at[index, "Valid Object"] = any(
                            o.Name == row["Object Name"] for o in tom.all_measures()
                        )
                    elif obj_type == "Column":
                        df.at[index, "Valid Object"] = any(
                            format_dax_object_name(c.Parent.Name, c.Name)
                            == format_dax_object_name(
                                row["Table Name"], row["Object Name"]
                            )
                            for c in tom.all_columns()
                        )
                    elif obj_type == "Hierarchy":
                        df.at[index, "Valid Object"] = any(
                            format_dax_object_name(h.Parent.Name, h.Name)
                            == format_dax_object_name(
                                row["Table Name"], row["Object Name"]
                            )
                            for h in tom.all_hierarchies()
                        )

            web_url = self._get_web_url()
            df["Web Url"] = web_url + "/" + df["Page Name"]

        return df

    def list_visual_filters(self, extended: Optional[bool] = False) -> pd.DataFrame:
        """
        Shows a list of all visual filters used in the report.

        Parameters
        ----------
        extended : bool, default=False
            If set to True, adds a column 'Valid' identifying whether the object is a valid object within the semantic model used by the report.

        Returns
        -------
        pandas.DataFrame
            A pandas dataframe containing a list of all the visual filters used in the report.
        """

        rd = self.rdef
        df = pd.DataFrame(
            columns=[
                "Page Name",
                "Page Display Name",
                "Visual Name",
                "Filter Name",
                "Type",
                "Table Name",
                "Object Name",
                "Object Type",
                "Hidden",
                "Locked",
                "How Created",
                "Used",
            ]
        )
        page_mapping, visual_mapping = self.__visual_page_mapping()

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
                    entity = (
                        data.get("Expression", {})
                        .get("SourceRef", {})
                        .get("Entity", {})
                    )
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

        for i, r in rd.iterrows():
            path = r["path"]
            payload = r["payload"]
            if path.endswith("/visual.json"):
                obj_file = base64.b64decode(payload).decode("utf-8")
                obj_json = json.loads(obj_file)
                page_id = visual_mapping.get(path)[0]
                page_display = visual_mapping.get(path)[1]
                visual_name = obj_json.get("name")

                if "filterConfig" in obj_json:
                    for flt in obj_json.get("filterConfig", {}).get("filters", {}):
                        filter_name = flt.get("name")
                        how_created = flt.get("howCreated")
                        locked = flt.get("isLockedInViewMode", False)
                        hidden = flt.get("isHiddenInViewMode", False)
                        filter_type = flt.get("type", "Basic")
                        filter_used = (
                            True if "Where" in flt.get("filter", {}) else False
                        )

                        entity_property_pairs = find_entity_property_pairs(flt)

                        for object_name, properties in entity_property_pairs.items():
                            new_data = {
                                "Page Name": page_id,
                                "Page Display Name": page_display,
                                "Visual Name": visual_name,
                                "Filter Name": filter_name,
                                "Type": filter_type,
                                "Table Name": properties[0],
                                "Object Name": object_name,
                                "Object Type": properties[1],
                                "Hidden": hidden,
                                "Locked": locked,
                                "How Created": how_created,
                                "Used": filter_used,
                            }

                            df = pd.concat(
                                [df, pd.DataFrame(new_data, index=[0])],
                                ignore_index=True,
                            )

        bool_cols = ["Hidden", "Locked", "Used"]
        df[bool_cols] = df[bool_cols].astype(bool)

        if extended:
            dataset_id, dataset_name, dataset_workspace_id, dataset_workspace = (
                resolve_dataset_from_report(
                    report=self._report, workspace=self._workspace
                )
            )

            with connect_semantic_model(
                dataset=dataset_name, readonly=True, workspace=dataset_workspace
            ) as tom:
                for index, row in df.iterrows():
                    obj_type = row["Object Type"]
                    if obj_type == "Measure":
                        df.at[index, "Valid Object"] = any(
                            o.Name == row["Object Name"] for o in tom.all_measures()
                        )
                    elif obj_type == "Column":
                        df.at[index, "Valid Object"] = any(
                            format_dax_object_name(c.Parent.Name, c.Name)
                            == format_dax_object_name(
                                row["Table Name"], row["Object Name"]
                            )
                            for c in tom.all_columns()
                        )
                    elif obj_type == "Hierarchy":
                        df.at[index, "Valid Object"] = any(
                            format_dax_object_name(h.Parent.Name, h.Name)
                            == format_dax_object_name(
                                row["Table Name"], row["Object Name"]
                            )
                            for h in tom.all_hierarchies()
                        )

        return df

    def list_visual_interactions(self) -> pd.DataFrame:
        """
        Shows a list of all modified `visual interactions <https://learn.microsoft.com/power-bi/create-reports/service-reports-visual-interactions?tabs=powerbi-desktop>`_ used in the report.

        Parameters
        ----------

        Returns
        -------
        pandas.DataFrame
            A pandas dataframe containing a list of all modified visual interactions used in the report.
        """

        rd = self.rdef
        df = pd.DataFrame(
            columns=[
                "Page Name",
                "Page Display Name",
                "Source Visual Name",
                "Target Visual Name",
                "Type",
            ]
        )

        for i, r in rd.iterrows():
            file_path = r["path"]
            payload = r["payload"]
            if file_path.endswith("/page.json"):
                obj_file = base64.b64decode(payload).decode("utf-8")
                obj_json = json.loads(obj_file)
                page_name = obj_json.get("name")
                page_display = obj_json.get("displayName")

                for vizInt in obj_json.get("visualInteractions", []):
                    sourceVisual = vizInt.get("source")
                    targetVisual = vizInt.get("target")
                    vizIntType = vizInt.get("type")

                    new_data = {
                        "Page Name": page_name,
                        "Page Display Name": page_display,
                        "Source Visual Name": sourceVisual,
                        "Target Visual Name": targetVisual,
                        "Type": vizIntType,
                    }
                    df = pd.concat(
                        [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                    )

        return df

    def list_pages(self, extended: Optional[bool] = False) -> pd.DataFrame:
        """
        Shows a list of all pages in the report.

        Parameters
        ----------
        extended : bool, default=False
            Adds columns showing additional properties.

        Returns
        -------
        pandas.DataFrame
            A pandas dataframe containing a list of all pages in the report.
        """

        rd = self.rdef
        df = pd.DataFrame(
            columns=[
                "File Path",
                "Page Name",
                "Page Display Name",
                "Hidden",
                "Active",
                "Width",
                "Height",
                "Display Option",
                "Type",
                "Alignment",
                "Drillthrough Target Page",
                "Visual Count",
                "Data Visual Count",
                "Visible Visual Count",
                "Page Filter Count",
            ]
        )
        page_type_mapping = {
            (320, 240): "Tooltip",
            (816, 1056): "Letter",
            (960, 720): "4:3",
            (1280, 720): "16:9",
        }

        dfV = self.list_visuals()

        for i, r in rd.iterrows():
            file_path = r["path"]
            payload = r["payload"]
            if file_path.endswith("/page.json"):
                pageFile = base64.b64decode(payload).decode("utf-8")
                page_prefix = file_path[0:-9]
                pageJson = json.loads(pageFile)
                page_name = pageJson.get("name")
                height = pageJson.get("height")
                width = pageJson.get("width")
                alignment = pageJson.get("objects", {}).get("displayArea", [])
                if alignment:
                    alignment_properties = (
                        alignment[0]
                        .get("properties", {})
                        .get("verticalAlignment", {})
                        .get("expr", {})
                        .get("Literal", {})
                        .get("Value", None)
                    )
                    if alignment_properties:
                        alignment_value = alignment_properties[1:-1]
                    else:
                        alignment_value = "Top"
                else:
                    alignment_value = "Top"
                drill_through = False
                filterConfig = pageJson.get("filterConfig")
                if filterConfig:
                    for filt in filterConfig.get("filters", []):
                        if filt.get("howCreated", {}) == "Drillthrough":
                            drill_through = True
                visual_count = len(
                    rd[
                        rd["path"].str.endswith("/visual.json")
                        & (rd["path"].str.startswith(page_prefix))
                    ]
                )
                data_visual_count = len(
                    dfV[(dfV["Page Name"] == page_name) & (dfV["Data Visual"])]
                )
                visible_visual_count = len(
                    dfV[(dfV["Page Name"] == page_name) & (dfV["Hidden"] == False)]
                )
                page_filter_count = len(
                    pageJson.get("filterConfig", {}).get("filters", [])
                )

                new_data = {
                    "File Path": file_path,
                    "Page Name": page_name,
                    "Page Display Name": pageJson.get("displayName"),
                    "Display Option": pageJson.get("displayOption"),
                    "Height": height,
                    "Width": width,
                    "Hidden": (
                        True
                        if pageJson.get("visibility") == "HiddenInViewMode"
                        else False
                    ),
                    "Active": False,
                    "Type": page_type_mapping.get((width, height), "Custom"),
                    "Alignment": alignment_value,
                    "Drillthrough Target Page": drill_through,
                    "Visual Count": visual_count,
                    "Data Visual Count": data_visual_count,
                    "Visible Visual Count": visible_visual_count,
                    "Page Filter Count": page_filter_count,
                }
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )
            elif file_path == "definition/pages/pages.json":
                pageFile = base64.b64decode(payload).decode("utf-8")
                pageJson = json.loads(pageFile)
                activePage = pageJson["activePageName"]

        df.loc[df["Page Name"] == activePage, "Active"] = True

        int_cols = [
            "Width",
            "Height",
            "Page Filter Count",
            "Visual Count",
            "Visible Visual Count",
            "Data Visual Count",
        ]
        df[int_cols] = df[int_cols].astype(int)

        bool_cols = ["Hidden", "Active", "Drillthrough Target Page"]
        df[bool_cols] = df[bool_cols].astype(bool)

        if extended:
            web_url = self._get_web_url()

            df["Web Url"] = web_url + "/" + df["Page Name"]

        return df

    def list_visuals(self, extended: Optional[bool] = False) -> pd.DataFrame:
        """
        Shows a list of all visuals in the report.

        Parameters
        ----------
        extended : bool, default=False
            If True, adds a column showing the Visual Object Count (number of measures/columns/hierarchies used in the visual).
            Defaults to False.

        Returns
        -------
        pandas.DataFrame
            A pandas dataframe containing a list of all visuals in the report.
        """

        rd = self.rdef
        df = pd.DataFrame(
            columns=[
                "File Path",
                "Page Name",
                "Page Display Name",
                "Visual Name",
                "Type",
                "Display Type",
                "X",
                "Y",
                "Z",
                "Width",
                "Height",
                "Tab Order",
                "Hidden",
                "Title",
                "SubTitle",
                "Custom Visual",
                "Alt Text",
                "Show Items With No Data",
                "Divider",
                "Slicer Type",
                "Row SubTotals",
                "Column SubTotals",
                "Data Visual",
                "Has Sparkline",
                "Visual Filter Count",
            ]
        )

        rd_filt = rd[rd["path"] == "definition/report.json"]
        payload = rd_filt["payload"].iloc[0]
        rptJson = _extract_json(rd_filt)
        custom_visuals = rptJson.get("publicCustomVisuals", [])
        page_mapping, visual_mapping = self.__visual_page_mapping()
        self.__populate_custom_visual_display_names()

        def contains_key(data, keys_to_check):
            if isinstance(data, dict):
                for key, value in data.items():
                    if key in keys_to_check:
                        return True
                    if contains_key(value, keys_to_check):
                        return True
            elif isinstance(data, list):
                for item in data:
                    if contains_key(item, keys_to_check):
                        return True
            return False

        for i, r in rd.iterrows():
            file_path = r["path"]
            payload = r["payload"]
            if file_path.endswith("/visual.json"):
                visual_file = base64.b64decode(payload).decode("utf-8")
                visual_json = json.loads(visual_file)
                page_id = visual_mapping.get(file_path)[0]
                page_display = visual_mapping.get(file_path)[1]
                pos = visual_json.get("position")
                visual_type = visual_json.get("visual", {}).get("visualType", "Group")
                visual_type_display = _vis_type_mapping.get(visual_type, visual_type)
                cst_value, rst_value, slicer_type = False, False, "N/A"
                visual_filter_count = len(
                    visual_json.get("filterConfig", {}).get("filters", [])
                )

                title = (
                    visual_json.get("visual", {})
                    .get("visualContainerObjects", {})
                    .get("title", [{}])[0]
                    .get("properties", {})
                    .get("text", {})
                    .get("expr", {})
                    .get("Literal", {})
                    .get("Value", "")[1:-1]
                )
                sub_title = (
                    visual_json.get("visual", {})
                    .get("visualContainerObjects", {})
                    .get("subTitle", [{}])[0]
                    .get("properties", {})
                    .get("text", {})
                    .get("expr", {})
                    .get("Literal", {})
                    .get("Value", "")[1:-1]
                )
                alt_text = (
                    visual_json.get("visual", {})
                    .get("visualContainerObjects", {})
                    .get("general", [{}])[0]
                    .get("properties", {})
                    .get("altText", {})
                    .get("expr", {})
                    .get("Literal", {})
                    .get("Value", "")[1:-1]
                )
                show_all_data = (
                    visual_json.get("visual", {})
                    .get("query", {})
                    .get("queryState", {})
                    .get("Values", {})
                    .get("showAll", False)
                )
                if show_all_data is not False:
                    show_all_data = True

                divider = (
                    visual_json.get("visual", {})
                    .get("visualContainerObjects", {})
                    .get("divider", [{}])[0]
                    .get("properties", {})
                    .get("show", {})
                    .get("expr", {})
                    .get("Literal", {})
                    .get("Value", "")
                )

                if visual_type == "pivotTable":
                    subt = (
                        visual_json.get("visual", {})
                        .get("objects", {})
                        .get("subTotals", [{}])[0]
                        .get("properties", {})
                    )
                    cst = (
                        subt.get("columnSubtotals", {})
                        .get("expr", {})
                        .get("Literal", {})
                        .get("Value")
                    )
                    rst = (
                        subt.get("rowSubtotals", {})
                        .get("expr", {})
                        .get("Literal", {})
                        .get("Value")
                    )
                    cst_value = False if cst == "false" else True
                    rst_value = False if rst == "false" else True

                elif visual_type == "slicer":
                    slicer_type = (
                        visual_json.get("visual", {})
                        .get("objects", {})
                        .get("data", {})[0]
                        .get("properties", {})
                        .get("mode", {})
                        .get("expr", {})
                        .get("Literal", {})
                        .get("Value", "")[1:-1]
                    )

                is_data_visual = contains_key(
                    visual_json,
                    [
                        "Aggregation",
                        "Column",
                        "Measure",
                        "HierarchyLevel",
                        "NativeVisualCalculation",
                    ],
                )
                has_sparkline = contains_key(visual_json, ["SparklineData"])

                new_data = {
                    "File Path": file_path,
                    "Page Name": page_id,
                    "Page Display Name": page_display,
                    "Visual Name": visual_json.get("name"),
                    "X": pos.get("x"),
                    "Y": pos.get("y"),
                    "Z": pos.get("z"),
                    "Width": pos.get("width"),
                    "Height": pos.get("height"),
                    "Tab Order": pos.get("tabOrder"),
                    "Hidden": visual_json.get("isHidden", False),
                    "Type": visual_type,
                    "Display Type": visual_type_display,
                    "Title": title,
                    "SubTitle": sub_title,
                    "Custom Visual": visual_type in custom_visuals,
                    "Alt Text": alt_text,
                    "Show Items With No Data": show_all_data,
                    "Divider": divider,
                    "Row SubTotals": rst_value,
                    "Column SubTotals": cst_value,
                    "Slicer Type": slicer_type,
                    "Data Visual": is_data_visual,
                    "Has Sparkline": has_sparkline,
                    "Visual Filter Count": visual_filter_count,
                }

                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )

        bool_cols = [
            "Hidden",
            "Show Items With No Data",
            "Custom Visual",
            "Data Visual",
            "Has Sparkline",
            "Row SubTotals",
            "Column SubTotals",
        ]
        float_cols = ["X", "Y", "Width", "Height"]
        int_cols = ["Z", "Visual Filter Count"]
        df[bool_cols] = df[bool_cols].astype(bool)
        df[int_cols] = df[int_cols].astype(int)
        df[float_cols] = df[float_cols].astype(float)

        if extended:
            grouped_df = (
                self.list_visual_objects()
                .groupby(["Page Name", "Visual Name"])
                .size()
                .reset_index(name="Visual Object Count")
            )

            df = pd.merge(
                df,
                grouped_df,
                left_on=["Page Name", "Visual Name"],
                right_on=["Page Name", "Visual Name"],
                how="left",
            )
            df["Visual Object Count"] = df["Visual Object Count"].fillna(0).astype(int)

        return df

    def list_visual_objects(self, extended: Optional[bool] = False) -> pd.DataFrame:
        """
        Shows a list of all semantic model objects used in each visual in the report.

        Parameters
        ----------
        extended : bool, default=False
            If set to True, adds a column 'Valid' identifying whether the object is a valid object within the semantic model used by the report.

        Returns
        -------
        pandas.DataFrame
            A pandas dataframe containing a list of all semantic model objects used in each visual in the report.
        """

        rd = self.rdef
        page_mapping, visual_mapping = self.__visual_page_mapping()
        df = pd.DataFrame(columns=["Page Name", "Page Display Name", "Visual Name"])

        def contains_key(data, keys_to_check):
            if isinstance(data, dict):
                for key, value in data.items():
                    if key in keys_to_check:
                        return True
                    if contains_key(value, keys_to_check):
                        return True
            elif isinstance(data, list):
                for item in data:
                    if contains_key(item, keys_to_check):
                        return True
            return False

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
                    entity = (
                        data.get("Expression", {})
                        .get("SourceRef", {})
                        .get("Entity", {})
                    )
                    property_value = data.get("Property", {})
                    object_type = keys_path[-1].replace("HierarchyLevel", "Hierarchy")
                    is_agg = keys_path[-3] == "Aggregation"
                    is_viz_calc = keys_path[-3] == "NativeVisualCalculation"
                    is_sparkline = keys_path[-3] == "SparklineData"
                    result[property_value] = (
                        entity,
                        object_type,
                        is_agg,
                        is_viz_calc,
                        is_sparkline,
                    )
                    keys_path.pop()

                # Recursively search the rest of the dictionary
                for key, value in data.items():
                    keys_path.append(key)
                    find_entity_property_pairs(value, result, keys_path)

            elif isinstance(data, list):
                for item in data:
                    find_entity_property_pairs(item, result, keys_path)

            return result

        for i, r in rd.iterrows():
            file_path = r["path"]
            payload = r["payload"]
            if file_path.endswith("/visual.json"):
                visual_file = base64.b64decode(payload).decode("utf-8")
                visual_json = json.loads(visual_file)
                page_id = visual_mapping.get(file_path)[0]
                page_display = visual_mapping.get(file_path)[1]

                entity_property_pairs = find_entity_property_pairs(visual_json)

                for object_name, properties in entity_property_pairs.items():
                    new_data = {
                        "Page Name": page_id,
                        "Page Display Name": page_display,
                        "Visual Name": visual_json.get("name"),
                        "Table Name": properties[0],
                        "Object Name": object_name,
                        "Object Type": properties[1],
                        "Implicit Measure": properties[2],
                        "Sparkline": properties[4],
                        "Visual Calc": properties[3],
                    }

                    df = pd.concat(
                        [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                    )

        if extended:
            dataset_id, dataset_name, dataset_workspace_id, dataset_workspace = (
                resolve_dataset_from_report(
                    report=self._report, workspace=self._workspace
                )
            )

            with connect_semantic_model(
                dataset=dataset_name, readonly=True, workspace=dataset_workspace
            ) as tom:
                for index, row in df.iterrows():
                    obj_type = row["Object Type"]
                    if obj_type == "Measure":
                        df.at[index, "Valid Object"] = any(
                            o.Name == row["Object Name"] for o in tom.all_measures()
                        )
                    elif obj_type == "Column":
                        df.at[index, "Valid Object"] = any(
                            format_dax_object_name(c.Parent.Name, c.Name)
                            == format_dax_object_name(
                                row["Table Name"], row["Object Name"]
                            )
                            for c in tom.all_columns()
                        )
                    elif obj_type == "Hierarchy":
                        df.at[index, "Valid Object"] = any(
                            format_dax_object_name(h.Parent.Name, h.Name)
                            == format_dax_object_name(
                                row["Table Name"], row["Object Name"]
                            )
                            for h in tom.all_hierarchies()
                        )

        return df

    def list_semantic_model_objects(
        self, extended: Optional[bool] = False
    ) -> pd.DataFrame:
        """
        Shows a list of all semantic model objects (measures, columns, hierarchies) that are used in the report and where the objects
        were used (i.e. visual, report filter, page filter, visual filter).

        Parameters
        ----------
        extended : bool, default=False
            If set to True, adds a column 'Valid' identifying whether the object is a valid object within the semantic model used by the report.

        Returns
        -------
        pandas.DataFrame
            A pandas dataframe showing the semantic model objects used in the report.
        """

        df = pd.DataFrame(
            columns=[
                "Table Name",
                "Object Name",
                "Object Type",
                "Report Source",
                "Report Source Object",
            ]
        )

        rf = self.list_report_filters()
        pf = self.list_page_filters()
        vf = self.list_visual_filters()
        vo = self.list_visual_objects()

        rf_subset = rf[["Table Name", "Object Name", "Object Type"]].copy()
        rf_subset["Report Source"] = "Report Filter"
        rf_subset["Report Source Object"] = self._report

        pf_subset = pf[
            ["Table Name", "Object Name", "Object Type", "Page Display Name"]
        ].copy()
        pf_subset["Report Source"] = "Page Filter"
        pf_subset["Report Source Object"] = pf_subset["Page Display Name"]
        pf_subset.drop(columns=["Page Display Name"], inplace=True)

        vf_subset = vf[
            [
                "Table Name",
                "Object Name",
                "Object Type",
                "Page Display Name",
                "Visual Name",
            ]
        ].copy()
        vf_subset["Report Source"] = "Visual Filter"
        vf_subset["Report Source Object"] = format_dax_object_name(
            vf_subset["Page Display Name"], vf_subset["Visual Name"]
        )
        vf_subset.drop(columns=["Page Display Name", "Visual Name"], inplace=True)

        vo_subset = vo[
            [
                "Table Name",
                "Object Name",
                "Object Type",
                "Page Display Name",
                "Visual Name",
            ]
        ].copy()
        vo_subset["Report Source"] = "Visual"
        vo_subset["Report Source Object"] = format_dax_object_name(
            vo_subset["Page Display Name"], vo_subset["Visual Name"]
        )
        vo_subset.drop(columns=["Page Display Name", "Visual Name"], inplace=True)

        df = pd.concat(
            [df, rf_subset, pf_subset, vf_subset, vo_subset], ignore_index=True
        )

        if extended:
            dataset_id, dataset_name, dataset_workspace_id, dataset_workspace = (
                resolve_dataset_from_report(
                    report=self._report, workspace=self._workspace
                )
            )

            def check_validity(tom, row):
                object_validators = {
                    "Measure": lambda: any(
                        o.Name == row["Object Name"] for o in tom.all_measures()
                    ),
                    "Column": lambda: any(
                        format_dax_object_name(c.Parent.Name, c.Name)
                        == format_dax_object_name(row["Table Name"], row["Object Name"])
                        for c in tom.all_columns()
                    ),
                    "Hierarchy": lambda: any(
                        format_dax_object_name(h.Parent.Name, h.Name)
                        == format_dax_object_name(row["Table Name"], row["Object Name"])
                        for h in tom.all_hierarchies()
                    ),
                }
                return object_validators.get(row["Object Type"], lambda: False)()

            with connect_semantic_model(
                dataset=dataset_name, readonly=True, workspace=dataset_workspace
            ) as tom:
                df["Valid"] = df.apply(lambda row: check_validity(tom, row), axis=1)

        return df

    def list_all_semantic_model_objects(self):

        # Includes dependencies

        df = (
            self.list_semantic_model_objects()[
                ["Table Name", "Object Name", "Object Type"]
            ]
            .drop_duplicates()
            .reset_index(drop=True)
        )
        dataset_id, dataset_name, dataset_workspace_id, dataset_workspace = (
            resolve_dataset_from_report(report=self._report, workspace=self._workspace)
        )
        dep = get_measure_dependencies(
            dataset=dataset_name, workspace=dataset_workspace
        )
        rpt_measures = df[df["Object Type"] == "Measure"]["Object Name"].values
        new_rows = dep[dep["Object Name"].isin(rpt_measures)][
            ["Referenced Table", "Referenced Object", "Referenced Object Type"]
        ]
        new_rows.columns = ["Table Name", "Object Name", "Object Type"]
        result_df = (
            pd.concat([df, new_rows], ignore_index=True)
            .drop_duplicates()
            .reset_index(drop=True)
        )

        result_df["Dataset Name"] = dataset_name
        result_df["Dataset Workspace Name"] = dataset_workspace
        colName = "Dataset Name"
        result_df.insert(0, colName, result_df.pop(colName))
        colName = "Dataset Workspace Name"
        result_df.insert(1, colName, result_df.pop(colName))

        return result_df

    def list_bookmarks(self) -> pd.DataFrame:
        """
        Shows a list of all bookmarks in the report.

        Parameters
        ----------

        Returns
        -------
        pandas.DataFrame
            A pandas dataframe containing a list of all bookmarks in the report.
        """

        rd = self.rdef
        df = pd.DataFrame(
            columns=[
                "File Path",
                "Bookmark Name",
                "Bookmark Display Name",
                "Page Name",
                "Page Display Name",
                "Visual Name",
                "Visual Hidden",
            ]
        )

        for i, r in rd.iterrows():
            path = r["path"]
            payload = r["payload"]
            if path.endswith(".bookmark.json"):
                obj_file = base64.b64decode(payload).decode("utf-8")
                obj_json = json.loads(obj_file)

                bookmark_name = obj_json.get("name")
                bookmark_display = obj_json.get("displayName")
                rpt_page_id = obj_json.get("explorationState", {}).get("activeSection")
                page_id, page_display, file_path = self.resolve_page_name(
                    page_name=rpt_page_id
                )

                for rptPg in obj_json.get("explorationState", {}).get("sections", {}):
                    for visual_name in (
                        obj_json.get("explorationState", {})
                        .get("sections", {})
                        .get(rptPg, {})
                        .get("visualContainers", {})
                    ):
                        if (
                            obj_json.get("explorationState", {})
                            .get("sections", {})
                            .get(rptPg, {})
                            .get("visualContainers", {})
                            .get(visual_name, {})
                            .get("singleVisual", {})
                            .get("display", {})
                            .get("mode", {})
                            == "hidden"
                        ):
                            visual_hidden = True
                        else:
                            visual_hidden = False

                        new_data = {
                            "File Path": path,
                            "Bookmark Name": bookmark_name,
                            "Bookmark Display Name": bookmark_display,
                            "Page Name": page_id,
                            "Page Display Name": page_display,
                            "Visual Name": visual_name,
                            "Visual Hidden": visual_hidden,
                        }
                        df = pd.concat(
                            [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                        )

        return df

    def list_report_level_measures(self) -> pd.DataFrame:
        """
        Shows a list of all `report-level measures <https://learn.microsoft.com/power-bi/transform-model/desktop-measures#report-level-measures>`_ in the report.

        Parameters
        ----------

        Returns
        -------
        pandas.DataFrame
            A pandas dataframe containing a list of all report-level measures in the report.
        """

        df = pd.DataFrame(
            columns=[
                "Measure Name",
                "Table Name",
                "Expression",
                "Data Type",
                "Format String",
            ]
        )
        rd = self.rdef
        rd_filt = rd[rd["path"] == "definition/reportExtensions.json"]

        if len(rd_filt) == 1:
            payload = rd_filt["payload"].iloc[0]
            obj_file = base64.b64decode(payload).decode("utf-8")
            obj_json = json.loads(obj_file)

            for e in obj_json.get("entities", []):
                table_name = e.get("name")
                for m in e.get("measures", []):
                    measure_name = m.get("name")
                    expr = m.get("expression")
                    data_type = m.get("dataType")
                    format_string = m.get("formatString")

                    new_data = {
                        "Measure Name": measure_name,
                        "Table Name": table_name,
                        "Expression": expr,
                        "Data Type": data_type,
                        "Format String": format_string,
                    }
                    df = pd.concat(
                        [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                    )

        return df

    # Automation functions
    def set_active_page(self, page_name: str):
        """
        Sets the active page (first page displayed when opening a report) for a report.

        Parameters
        ----------
        page_name : str
            The page name or page display name of the report.

        Returns
        -------
        """

        pages_file = "definition/pages/pages.json"
        request_body = {"definition": {"parts": []}}

        rd = self.rdef
        page_id, page_display, file_path = self.resolve_page_name(page_name=page_name)
        for i, r in rd.iterrows():
            path = r["path"]
            file_payload = r["payload"]
            if path != pages_file:
                _add_part(request_body, path, file_payload)

        pagePath = rd[rd["path"] == pages_file]
        payload = pagePath["payload"].iloc[0]
        pageFile = base64.b64decode(payload).decode("utf-8")
        pageJson = json.loads(pageFile)
        pageJson["activePageName"] = page_id
        file_payload = _conv_b64(pageJson)

        _add_part(request_body, pages_file, file_payload)

        if not self._readonly:
            self.update_report(request_body=request_body)
            print(
                f"{icons.green_dot} The '{page_name}' page has been set as the active page in the '{self._report}' report within the '{self._workspace}' workspace."
            )

    def set_page_type(self, page_name: str, page_type: str):

        page_types = ["Tooltip", "Letter", "4:3", "16:9"]

        if page_type not in page_types:
            raise ValueError(
                f"{icons.red_dot} Invalid page type. Valid options: {page_types}."
            )

        page_type_mapping = {
            (320, 240): "Tooltip",
            (816, 1056): "Letter",
            (960, 720): "4:3",
            (1280, 720): "16:9",
        }

        request_body = {"definition": {"parts": []}}

        letter_key = next(
            (key for key, value in page_type_mapping.items() if value == page_type),
            None,
        )
        if letter_key:
            width, height = letter_key
        else:
            raise ValueError(
                "Invalid page_type parameter. Valid options: ['Tooltip', 'Letter', '4:3', '16:9']."
            )

        rd = self.rdef
        page_id, page_display, file_path = self.resolve_page_name(page_name=page_name)
        rd_filt = rd[rd["path"] == file_path]
        payload = rd_filt["payload"].iloc[0]
        pageFile = base64.b64decode(payload).decode("utf-8")
        pageJson = json.loads(pageFile)
        pageJson["width"] = width
        pageJson["height"] = height

        file_payload = _conv_b64(pageJson)
        _add_part(request_body, file_path, file_payload)

        for i, r in rd.iterrows():
            if r["path"] != file_path:
                _add_part(request_body, r["path"], r["payload"])

        if not self._readonly:
            self.update_report(request_body=request_body)
            print(
                f"The '{page_display}' page has been updated to the '{page_type}' page type."
            )

    def remove_unnecessary_custom_visuals(self):
        """
        Removes any custom visuals within the report that are not used in the report.

        Parameters
        ----------

        Returns
        -------
        """

        dfCV = self.list_custom_visuals()
        dfV = self.list_visuals()
        rd = self.rdef
        cv_remove = []
        cv_remove_display = []
        request_body = {"definition": {"parts": []}}

        for i, r in dfCV.iterrows():
            cv = r["Custom Visual Name"]
            cv_display = r["Custom Visual Display Name"]
            dfV_filt = dfV[dfV["Type"] == cv]
            if len(dfV_filt) == 0:
                cv_remove.append(cv)  # Add to the list for removal
                cv_remove_display.append(cv_display)
        if len(cv_remove) == 0:
            print(
                f"{icons.green_dot} There are no unnecessary custom visuals in the '{self._report}' report within the '{self._workspace}' workspace."
            )
            return

        for i, r in rd.iterrows():
            file_path = r["path"]
            payload = r["payload"]
            if file_path == "definition/report.json":
                rpt_file = base64.b64decode(payload).decode("utf-8")
                rpt_json = json.loads(rpt_file)
                rpt_json["publicCustomVisuals"] = [
                    item
                    for item in rpt_json["publicCustomVisuals"]
                    if item not in cv_remove
                ]

                payload = _conv_b64(rpt_json)

            _add_part(request_body, file_path, payload)

        if not self._readonly:
            self.update_report(request_body=request_body)
            print(
                f"{icons.green_dot} The {cv_remove_display} custom visuals have been removed from the '{self._report}' report within the '{self._workspace}' workspace."
            )

    def migrate_report_level_measures(self, measures: Optional[str | List[str]] = None):
        """
        Moves all report-level measures from the report to the semantic model on which the report is based.

        Parameters
        ----------
        measures : str | List[str], default=None
            A measure or list of measures to move to the semantic model.
            Defaults to None which resolves to moving all report-level measures to the semantic model.

        Returns
        -------
        """

        rlm = self.list_report_level_measures()
        if len(rlm) == 0:
            print(
                f"{icons.green_dot} The '{self._report}' report within the '{self._workspace}' workspace has no report-level measures."
            )
            return

        dataset_id, dataset_name, dataset_workspace_id, dataset_workspace = (
            resolve_dataset_from_report(report=self._report, workspace=self._workspace)
        )

        if isinstance(measures, str):
            measures = [measures]

        request_body = {"definition": {"parts": []}}
        rpt_file = "definition/reportExtensions.json"

        rd = self.rdef
        rd_filt = rd[rd["path"] == rpt_file]
        payload = rd_filt["payload"].iloc[0]
        extFile = base64.b64decode(payload).decode("utf-8")
        extJson = json.loads(extFile)

        mCount = 0
        with connect_semantic_model(
            dataset=dataset_name, readonly=False, workspace=dataset_workspace
        ) as tom:
            for i, r in rlm.iterrows():
                tableName = r["Table Name"]
                mName = r["Measure Name"]
                mExpr = r["Expression"]
                # mDataType = r["Data Type"]
                mformatString = r["Format String"]
                # Add measures to the model
                if mName in measures or measures is None:
                    tom.add_measure(
                        table_name=tableName,
                        measure_name=mName,
                        expression=mExpr,
                        format_string=mformatString,
                    )
                    tom.set_annotation(
                        object=tom.model.Tables[tableName].Measures[mName],
                        name="semanticlinklabs",
                        value="reportlevelmeasure",
                    )
                mCount += 1
            # Remove measures from the json
            if measures is not None and len(measures) < mCount:
                for e in extJson["entities"]:
                    e["measures"] = [
                        measure
                        for measure in e["measures"]
                        if measure["name"] not in measures
                    ]
                extJson["entities"] = [
                    entity for entity in extJson["entities"] if entity["measures"]
                ]
                file_payload = _conv_b64(extJson)
                _add_part(request_body, rpt_file, file_payload)

        # Add unchanged payloads
        for i, r in rd.iterrows():
            path = r["path"]
            payload = r["payload"]
            if path != rpt_file:
                _add_part(request_body, path, payload)

        if not self._readonly:
            self.update_report(request_body=request_body)
            print(
                f"{icons.green_dot} The report-level measures have been migrated to the '{dataset_name}' semantic model within the '{dataset_workspace}' workspace."
            )

    def set_theme(self, theme_file_path: str):
        """
        Sets a custom theme for a report based on a theme .json file.

        Parameters
        ----------
        theme_file_path : str
            The file path of the theme.json file. This can either be from a Fabric lakehouse or from the web.
            Examples:
                file_path = '/lakehouse/default/Files/CY23SU09.json'
                file_path = 'https://raw.githubusercontent.com/PowerBiDevCamp/FabricUserApiDemo/main/FabricUserApiDemo/DefinitionTemplates/Shared/Reports/StaticResources/SharedResources/BaseThemes/CY23SU08.json'

        Returns
        -------
        """

        import requests

        report_path = "definition/report.json"
        theme_version = "5.5.4"
        request_body = {"definition": {"parts": []}}

        if not theme_file_path.endswith(".json"):
            raise ValueError(
                f"{icons.red_dot} The theme file path must be a .json file."
            )
        elif theme_file_path.startswith("https://"):
            response = requests.get(theme_file_path)
            json_file = response.json()
        elif theme_file_path.startswith("/lakehouse"):
            with open(theme_file_path, "r", encoding="utf-8-sig") as file:
                json_file = json.load(file)
        else:
            ValueError(f"{icons.red_dot} Incorrect theme file path value.")

        theme_name = json_file["name"]
        theme_name_full = f"{theme_name}.json"

        # Add theme.json file to request_body
        file_payload = _conv_b64(json_file)
        filePath = f"StaticResources/RegisteredResources/{theme_name_full}"

        _add_part(request_body, filePath, file_payload)

        new_theme = {
            "name": theme_name_full,
            "path": theme_name_full,
            "type": "CustomTheme",
        }

        rd = self.rdef
        for i, r in rd.iterrows():
            path = r["path"]
            payload = r["payload"]
            if path != report_path:
                _add_part(request_body, path, payload)
            # Update the report.json file
            else:
                rptFile = base64.b64decode(payload).decode("utf-8")
                rptJson = json.loads(rptFile)
                resource_type = "RegisteredResources"

                # Add to theme collection
                if "customTheme" not in rptJson["themeCollection"]:
                    rptJson["themeCollection"]["customTheme"] = {
                        "name": theme_name_full,
                        "reportVersionAtImport": theme_version,
                        "type": resource_type,
                    }
                else:
                    rptJson["themeCollection"]["customTheme"]["name"] = theme_name_full
                    rptJson["themeCollection"]["customTheme"]["type"] = resource_type

                for package in rptJson["resourcePackages"]:
                    package["items"] = [
                        item
                        for item in package["items"]
                        if item["type"] != "CustomTheme"
                    ]

                if not any(
                    package["name"] == resource_type
                    for package in rptJson["resourcePackages"]
                ):
                    new_registered_resources = {
                        "name": resource_type,
                        "type": resource_type,
                        "items": [new_theme],
                    }
                    rptJson["resourcePackages"].append(new_registered_resources)
                else:
                    names = []
                    for rp in rptJson["resourcePackages"][1]["items"]:
                        names.append(rp["name"])
                    if theme_name_full not in names:
                        rptJson["resourcePackages"][1]["items"].append(new_theme)

                file_payload = _conv_b64(rptJson)
                _add_part(request_body, path, file_payload)

        if not self._readonly:
            self.update_report(request_body=request_body)
            print(
                f"{icons.green_dot} The '{theme_name}' theme has been set as the theme for the '{self._report}' report within the '{self._workspace}' workspace."
            )

    def set_page_visibility(self, page_name: str, hidden: bool):
        """
        Sets whether a report page is visible or hidden.

        Parameters
        ----------
        page_name : str
            The page name or page display name of the report.
        hidden : bool
            If set to True, hides the report page.
            If set to False, makes the report page visible.

        Returns
        -------
        """

        rd = self.rdef
        page_id, page_display, file_path = self.resolve_page_name(page_name=page_name)
        visibility = "visible" if hidden is False else "hidden"

        request_body = {"definition": {"parts": []}}

        for i, r in rd.iterrows():
            path = r["path"]
            payload = r["payload"]
            if path == file_path:
                obj_file = base64.b64decode(payload).decode("utf-8")
                obj_json = json.loads(obj_file)
                if hidden:
                    obj_json["visibility"] = "HiddenInViewMode"
                else:
                    if "visibility" in obj_json:
                        del obj_json["visibility"]
                _add_part(request_body, path, _conv_b64(obj_json))
            else:
                _add_part(request_body, path, payload)

        if not self._readonly:
            self.update_report(request_body=request_body)
            print(
                f"{icons.green_dot} The '{page_name}' page has been set to {visibility}."
            )

    def hide_tooltip_drillthrough_pages(self):
        """
        Hides all tooltip pages and drillthrough pages in a report.

        Parameters
        ----------

        Returns
        -------
        """

        dfP = self.list_pages()
        dfP_filt = dfP[
            (dfP["Type"] == "Tooltip") | (dfP["Drillthrough Target Page"] == True)
        ]

        if len(dfP_filt) == 0:
            print(
                f"{icons.green_dot} There are no Tooltip or Drillthrough pages in the '{self._report}' report within the '{self._workspace}' workspace."
            )
            return

        for i, r in dfP_filt.iterrows():
            page_name = r["Page Name"]
            self.set_page_visibility(page_name=page_name, hidden=True)

    def disable_show_items_with_no_data(self):
        """
        Disables the `show items with no data <https://learn.microsoft.com/power-bi/create-reports/desktop-show-items-no-data>`_ property in all visuals within the report.

        Parameters
        ----------

        Returns
        -------
        """

        request_body = {"definition": {"parts": []}}

        def delete_key_in_json(obj, key_to_delete):
            if isinstance(obj, dict):
                if key_to_delete in obj:
                    del obj[key_to_delete]
                for key, value in obj.items():
                    delete_key_in_json(value, key_to_delete)
            elif isinstance(obj, list):
                for item in obj:
                    delete_key_in_json(item, key_to_delete)

        rd = self.rdef
        for i, r in rd.iterrows():
            file_path = r["path"]
            payload = r["payload"]
            if file_path.endswith("/visual.json"):
                # pattern_page = r"/pages/(.*?)/visuals/"
                # pattern_visual = r"/visuals/(.*?)/visual.json"
                # page_name = re.search(pattern_page, file_path).group(1)
                # visual_name = re.search(pattern_visual, file_path).group(1)
                # page_id, page_display = resolve_page_name(report=report, page_name=page_name, workspace=workspace)
                objFile = base64.b64decode(payload).decode("utf-8")
                objJson = json.loads(objFile)
                delete_key_in_json(objJson, "showAll")
                # print(f"Show items with no data has been disabled for the '{visual_name}' visual on the '{page_display} page in the '{report}' report within the '{workspace}' workspace.")
                _add_part(request_body, file_path, _conv_b64(objJson))
            else:
                _add_part(request_body, file_path, payload)

        if not self._readonly:
            self.update_report(request_body=request_body)
            print(
                f"{icons.green_dot} Show items with data has been disabled for all visuals in the '{self._report}' report within the '{self._workspace}' workspace."
            )

    def __get_annotation_value(self, object_name: str, object_type: str, name: str):

        object_types = ["Visual", "Page", "Report"]
        object_type = object_type.capitalize()
        if object_type not in object_types:
            raise ValueError(
                f"{icons.red_dot} Invalid object type. Valid options: {object_types}."
            )

        rd = self.rdef

        if object_type == "Report":
            rd_filt = rd[rd["path"] == "definition/report.json"]
            obj_json = _extract_json(rd_filt)
        elif object_type == "Page":
            page_id, page_display, page_file = self.resolve_page_name(
                page_name=object_name
            )
            rd_filt = rd[rd["path"] == page_file]
            payload = rd_filt["payload"].iloc[0]
            obj_file = base64.b64decode(payload).decode("utf-8")
            obj_json = json.loads(obj_file)
        elif object_type == "Visual":
            pattern = r"'([^']+)'\[([^]]+)\]"
            match = re.search(pattern, object_name)
            if match:
                p_name = match.group(1)
                v_name = match.group(2)
            else:
                raise ValueError(
                    "Invalid page/visual name within the 'object_name' parameter. Valid format: 'Page 1'[f8dvo24PdJ39fp6]"
                )
            valid_page_name, valid_display_name, visual_name, file_path = (
                self.resolve_visual_name(page_name=p_name, visual_name=v_name)
            )
            rd_filt = rd[rd["path"] == file_path]
            payload = rd_filt["payload"].iloc[0]
            obj_file = base64.b64decode(payload).decode("utf-8")
            obj_json = json.loads(obj_file)

        value = obj_json.get("annotations", {}).get(name, "")

        return value

    def __remove_annotation(self, object_name: str, object_type: str, name: str):

        object_types = ["Visual", "Page", "Report"]
        object_type = object_type.capitalize()
        if object_type not in object_types:
            raise ValueError(
                f"{icons.red_dot} Invalid object type. Valid options: {object_types}."
            )

    def __set_annotation(
        self, object_name: str, object_type: str, name: str, value: str
    ):

        object_types = ["Visual", "Page", "Report"]
        object_type = object_type.capitalize()
        if object_type not in object_types:
            raise ValueError(
                f"{icons.red_dot} Invalid object type. Valid options: {object_types}."
            )

        request_body = {"definition": {"parts": []}}
        new_annotation = {"name": name, "value": value}

        # Creates the annotation if it does not exist. Updates the annotation value if the annotation already exists
        def update_annotation(payload):
            objFile = base64.b64decode(payload).decode("utf-8")
            objJson = json.loads(objFile)
            if "annotations" not in objJson:
                objJson["annotations"] = [new_annotation]
            else:
                names = []
                for ann in objJson["annotations"]:
                    names.append(ann["name"])
                if name not in names:
                    objJson["annotations"].append(new_annotation)
                else:
                    for ann in objJson["annotations"]:
                        if ann["name"] == name:
                            ann["value"] = value
            return objJson

        # Validate page and visual names
        if object_type == "Page":
            page_id, page_display, file_path = self.resolve_page_name(
                page_name=object_name
            )
        elif object_type == "Visual":
            pattern = r"'(.*?)'|\[(.*?)\]"
            matches = re.findall(pattern, object_name)
            page_name = matches[0][0]
            visual_id = matches[1][1]
            page_id, page_display, file_path = self.resolve_page_name(
                page_name=page_name
            )

        rd = self.rdef
        for i, r in rd.iterrows():
            path = r["path"]
            payload = r["payload"]
            if object_type == "Report" and path == "definition/report.json":
                a = update_annotation(payload=payload)
                _add_part(request_body, path, _conv_b64(a))
            elif (
                object_type == "Page"
                and path == f"definition/pages/{page_id}/page.json"
            ):
                a = update_annotation(payload=payload)
                _add_part(request_body, path, _conv_b64(a))
            elif (
                object_type == "Visual"
                and path
                == f"definition/pages/{page_id}/visuals/{visual_id}/visual.json"
            ):
                a = update_annotation(payload=payload)
                _add_part(request_body, path, _conv_b64(a))
            else:
                _add_part(request_body, path, payload)

        if not self._readonly:
            self.update_report(request_body=request_body)
            if object_type == "Report":
                print(
                    f"{icons.green_dot} The '{name}' annotation has been set on the report with the '{value}' value."
                )
            elif object_type == "Page":
                print(
                    f"{icons.green_dot} The '{name}' annotation has been set on the '{object_name}' page with the '{value}' value."
                )
            elif object_type == "Visual":
                print(
                    f"{icons.green_dot} The '{name}' annotation has been set on the '{visual_id}' visual on the '{page_display}' page with the '{value}' value."
                )

    def __adjust_settings(
        self, setting_type: str, setting_name: str, setting_value: bool
    ):  # Meta function

        valid_setting_types = ["settings", "slowDataSourceSettings"]
        valid_settings = [
            "isPersistentUserStateDisabled",
            "hideVisualContainerHeader",
            "defaultFilterActionIsDataFilter",
            "useStylableVisualContainerHeader",
            "useDefaultAggregateDisplayName",
            "useEnhancedTooltips",
            "allowChangeFilterTypes",
            "disableFilterPaneSearch",
            "useCrossReportDrillthrough",
        ]
        valid_slow_settings = [
            "isCrossHighlightingDisabled",
            "isSlicerSelectionsButtonEnabled",
        ]

        if setting_type not in valid_setting_types:
            raise ValueError(
                f"Invalid setting_type. Valid options: {valid_setting_types}."
            )
        if setting_type == "settings" and setting_name not in valid_settings:
            raise ValueError(
                f"The '{setting_name}' is not a valid setting. Valid options: {valid_settings}."
            )
        if (
            setting_type == "slowDataSourceSettings"
            and setting_name not in valid_slow_settings
        ):
            raise ValueError(
                f"The '{setting_name}' is not a valid setting. Valid options: {valid_slow_settings}."
            )

        request_body = {"definition": {"parts": []}}

        rd = self.rdef
        for i, r in rd.iterrows():
            path = r["path"]
            payload = r["payload"]
            if path == "definition/report.json":
                obj_file = base64.b64decode(payload).decode("utf-8")
                obj_json = json.loads(obj_file)
                if setting_value is False:
                    if setting_name in obj_json.get(setting_type, {}):
                        del obj_json[setting_type][setting_name]
                else:
                    if setting_name not in obj_json.get(setting_type, {}):
                        obj_json[setting_type][setting_name] = True

                _add_part(request_body, path, _conv_b64(obj_json))
            else:
                _add_part(request_body, path, payload)

        if not self._readonly:
            upd = self.update_report(request_body=request_body)
            if upd == 200:
                print(f"{icons.green_dot}")
            else:
                print(f"{icons.red_dot}")

    def __persist_filters(self, value: Optional[bool] = False):
        """
        Don't allow end user to save filters on this file in the Power BI service.
        """

        self.adjust_settings(
            setting_type="settings",
            setting_name="isPersistentUserStateDisabled",
            setting_value=value,
        )

    def __hide_visual_header(self, value: Optional[bool] = False):
        """
        Hide the visual header in reading view.
        """

        self.adjust_settings(
            setting_type="settings",
            setting_name="hideVisualContainerHeader",
            setting_value=value,
        )

    def __default_cross_filtering(self, value: Optional[bool] = False):
        """
        Change the default visual interaction from cross highlighting to cross filtering.
        """

        self.adjust_settings(
            setting_type="settings",
            setting_name="defaultFilterActionIsDataFilter",
            setting_value=value,
        )

    def __modern_visual_header(self, value: Optional[bool] = True):
        """
        Use the modern visual header with updated styling options.
        """

        self.adjust_settings(
            setting_type="settings",
            setting_name="useStylableVisualContainerHeader",
            setting_value=value,
        )

    def __show_default_summarization_type(self, value: Optional[bool] = True):
        """
        For aggregated fields, always show the default summarization type.
        """

        self.adjust_settings(
            setting_type="settings",
            setting_name="useDefaultAggregateDisplayName",
            setting_value=value,
        )

    def __modern_visual_tooltips(self, value: Optional[bool] = True):
        """
        Use modern visual tooltips with drill actions and updated styling.
        """

        self.adjust_settings(
            setting_type="settings",
            setting_name="useEnhancedTooltips",
            setting_value=value,
        )

    def __user_can_change_filter_types(self, value: Optional[bool] = True):
        """
        Allow users to change filter types.
        """

        self.adjust_settings(
            setting_type="settings",
            setting_name="allowChangeFilterTypes",
            setting_value=value,
        )

    def __disable_search_filter_pane(self, value: Optional[bool] = False):
        """
        Enable search for the filter pane.
        """

        self.adjust_settings(
            setting_type="settings",
            setting_name="disableFilterPaneSearch",
            setting_value=value,
        )

    def __enable_cross_repor_drillthrough(self, value: Optional[bool] = False):
        """
        Allow visuals in this report to use drillthrough targets from other reports.
        """

        self.adjust_settings(
            setting_type="settings",
            setting_name="useCrossReportDrillthrough",
            setting_value=value,
        )

    def __disable_default_cross_highlighting(self, value: Optional[bool] = False):
        """
        Disable cross highlighting/filtering by default.
        """

        self.adjust_settings(
            setting_type="slowDataSourceSettings",
            setting_name="isCrossHighlightingDisabled",
            setting_value=value,
        )

    def __add_slicer_apply_button(self, value: Optional[bool] = False):
        """
        Add an Apply button to each individual slicer (not recommended).
        """

        self.adjust_settings(
            setting_type="slowDataSourceSettings",
            setting_name="isSlicerSelectionsButtonEnabled",
            setting_value=value,
        )

    def close(self):
        if not self._readonly and self._report is not None:
            print("saving...")

            self._report = None
