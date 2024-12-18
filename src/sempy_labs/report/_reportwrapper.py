import sempy.fabric as fabric
from sempy_labs._helper_functions import (
    resolve_report_id,
    format_dax_object_name,
    resolve_dataset_from_report,
    _conv_b64,
    _extract_json,
    _add_part,
    lro,
    _decode_b64,
    resolve_workspace_name_and_id,
)
from typing import Optional, List
import pandas as pd
import json
import base64
from uuid import UUID
from sempy._utils._log import log
import sempy_labs._icons as icons
import sempy_labs.report._report_helper as helper
from sempy_labs._model_dependencies import get_measure_dependencies
from jsonpath_ng.ext import parse
import warnings


class ReportWrapper:
    """
    Connects to a Power BI report and retrieves its definition.

    The ReportWrapper and all functions which depend on it require the report to be in the `PBIR <https://powerbi.microsoft.com/blog/power-bi-enhanced-report-format-pbir-in-power-bi-desktop-developer-mode-preview>`_ format.

    Parameters
    ----------
    report : str
        The name of the report.
    workspace : str | uuid.UUID
        The name or ID of the workspace in which the report resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe containing the report metadata definition files.
    """

    _report: str
    _workspace: str

    @log
    def __init__(
        self,
        report: str,
        workspace: Optional[str | UUID] = None,
    ):
        """
        Connects to a Power BI report and retrieves its definition.

        The ReportWrapper and all functions which depend on it require the report to be in the `PBIR <https://powerbi.microsoft.com/blog/power-bi-enhanced-report-format-pbir-in-power-bi-desktop-developer-mode-preview>`_ format.

        Parameters
        ----------
        report : str
            The name of the report.
        workspace : str | UUID
            The name or ID of the workspace in which the report resides.
            Defaults to None which resolves to the workspace of the attached lakehouse
            or if no lakehouse attached, resolves to the workspace of the notebook.

        Returns
        -------
        pandas.DataFrame
            A pandas dataframe containing the report metadata definition files.
        """

        from sempy_labs.report import get_report_definition

        warnings.simplefilter(action="ignore", category=FutureWarning)

        self._report = report
        (self._workspace_name, self._workspace_id) = resolve_workspace_name_and_id(
            workspace
        )
        self._report_id = resolve_report_id(report, self._workspace_id)
        self.rdef = get_report_definition(
            report=self._report, workspace=self._workspace_id
        )

        if len(self.rdef[self.rdef["path"] == "definition/report.json"]) == 0:
            raise ValueError(
                f"{icons.red_dot} The ReportWrapper function requires the report to be in the PBIR format."
                "See here for details: https://powerbi.microsoft.com/blog/power-bi-enhanced-report-format-pbir-in-power-bi-desktop-developer-mode-preview/"
            )

    # Helper functions
    def _add_extended(self, dataframe):

        from sempy_labs.tom import connect_semantic_model

        dataset_id, dataset_name, dataset_workspace_id, dataset_workspace_name = (
            resolve_dataset_from_report(
                report=self._report, workspace=self._workspace_id
            )
        )

        with connect_semantic_model(
            dataset=dataset_id, readonly=True, workspace=dataset_workspace_id
        ) as tom:
            for index, row in dataframe.iterrows():
                obj_type = row["Object Type"]
                if obj_type == "Measure":
                    dataframe.at[index, "Valid Semantic Model Object"] = any(
                        o.Name == row["Object Name"] for o in tom.all_measures()
                    )
                elif obj_type == "Column":
                    dataframe.at[index, "Valid Semantic Model Object"] = any(
                        format_dax_object_name(c.Parent.Name, c.Name)
                        == format_dax_object_name(row["Table Name"], row["Object Name"])
                        for c in tom.all_columns()
                    )
                elif obj_type == "Hierarchy":
                    dataframe.at[index, "Valid Semantic Model Object"] = any(
                        format_dax_object_name(h.Parent.Name, h.Name)
                        == format_dax_object_name(row["Table Name"], row["Object Name"])
                        for h in tom.all_hierarchies()
                    )
        return dataframe

    def _update_single_file(self, file_name: str, new_payload):
        """
        Updates a single file within the PBIR structure
        """

        request_body = {"definition": {"parts": []}}
        for _, r in self.rdef.iterrows():
            path = r["path"]
            payload = r["payload"]
            if path == file_name:
                _add_part(request_body, path=path, payload=new_payload)
            else:
                _add_part(request_body, path=path, payload=payload)

        self.update_report(request_body)

    def update_report(self, request_body: dict):

        client = fabric.FabricRestClient()
        response = client.post(
            f"/v1/workspaces/{self._workspace_id}/reports/{self._report_id}/updateDefinition",
            json=request_body,
        )

        lro(client, response, return_status_code=True)

    def resolve_page_name(self, page_display_name: str) -> UUID:
        """
        Obtains the page name, page display name, and the file path for a given page in a report.

        Parameters
        ----------
        page_display_name : str
            The display name of the page of the report.

        Returns
        -------
        UUID
            The page name.
        """

        x, y, z = helper.resolve_page_name(self, page_display_name)

        return x

    def resolve_page_display_name(self, page_name: UUID) -> str:
        """
        Obtains the page dispaly name.

        Parameters
        ----------
        page_name : UUID
            The name of the page of the report.

        Returns
        -------
        str
            The page display name.
        """

        x, y, z = helper.resolve_page_name(self, page_name=page_name)

        return y

    def get_theme(self, theme_type: str = "baseTheme") -> dict:
        """
        Obtains the theme file of the report.

        Parameters
        ----------
        theme_type : str, default="baseTheme"
            The theme type. Options: "baseTheme", "customTheme".

        Returns
        -------
        dict
            The theme.json file
        """

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
                f"{icons.red_dot} The {self._report} report within the '{self._workspace_name} workspace has no custom theme."
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
    def list_custom_visuals(self) -> pd.DataFrame:
        """
        Shows a list of all custom visuals used in the report.

        Returns
        -------
        pandas.DataFrame
            A pandas dataframe containing a list of all the custom visuals used in the report.
        """

        helper.populate_custom_visual_display_names()

        df = pd.DataFrame(columns=["Custom Visual Name", "Custom Visual Display Name"])
        rd = self.rdef
        rd_filt = rd[rd["path"] == "definition/report.json"]
        rptJson = _extract_json(rd_filt)
        df["Custom Visual Name"] = rptJson.get("publicCustomVisuals")
        df["Custom Visual Display Name"] = df["Custom Visual Name"].apply(
            lambda x: helper.vis_type_mapping.get(x, x)
        )

        df["Used in Report"] = df["Custom Visual Name"].isin(
            self.list_visuals()["Type"]
        )

        bool_cols = ["Used in Report"]
        df[bool_cols] = df[bool_cols].astype(bool)

        return df

    def list_report_filters(self, extended: bool = False) -> pd.DataFrame:
        """
        Shows a list of all report filters used in the report.

        Parameters
        ----------
        extended : bool, default=False
            If True, adds an extra column called 'Valid Semantic Model Object' which identifies whether the semantic model object used
            in the report exists in the semantic model which feeds data to the report.

        Returns
        -------
        pandas.DataFrame
            A pandas dataframe containing a list of all the report filters used in the report.
        """

        rd_filt = self.rdef[self.rdef["path"] == "definition/report.json"]

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

                    entity_property_pairs = helper.find_entity_property_pairs(flt)

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
            df = self._add_extended(dataframe=df)

        return df

    def list_page_filters(self, extended: bool = False) -> pd.DataFrame:
        """
        Shows a list of all page filters used in the report.

        Parameters
        ----------
        extended : bool, default=False
            If True, adds an extra column called 'Valid Semantic Model Object' which identifies whether the semantic model object used
            in the report exists in the semantic model which feeds data to the report.

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

        for _, r in rd.iterrows():
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

                        entity_property_pairs = helper.find_entity_property_pairs(flt)

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

        df["Page URL"] = df["Page Name"].apply(
            lambda page_name: f"{helper.get_web_url(report=self._report, workspace=self._workspace_id)}/{page_name}"
        )

        bool_cols = ["Hidden", "Locked", "Used"]
        df[bool_cols] = df[bool_cols].astype(bool)

        if extended:
            df = self._add_extended(dataframe=df)

        return df
        # return df.style.format({"Page URL": _make_clickable})

    def list_visual_filters(self, extended: bool = False) -> pd.DataFrame:
        """
        Shows a list of all visual filters used in the report.

        Parameters
        ----------
        extended : bool, default=False
            If True, adds an extra column called 'Valid Semantic Model Object' which identifies whether the semantic model object used
            in the report exists in the semantic model which feeds data to the report.

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
        page_mapping, visual_mapping = helper.visual_page_mapping(self)

        for _, r in rd.iterrows():
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

                        entity_property_pairs = helper.find_entity_property_pairs(flt)

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
            df = self._add_extended(dataframe=df)

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

        for _, r in rd.iterrows():
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

    def list_pages(self) -> pd.DataFrame:
        """
        Shows a list of all pages in the report.

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

        dfV = self.list_visuals()

        page_rows = rd[rd["path"].str.endswith("/page.json")]
        pages_row = rd[rd["path"] == "definition/pages/pages.json"]

        for _, r in page_rows.iterrows():
            file_path = r["path"]
            payload = r["payload"]

            pageFile = base64.b64decode(payload).decode("utf-8")
            page_prefix = file_path[0:-9]
            pageJson = json.loads(pageFile)
            page_name = pageJson.get("name")
            height = pageJson.get("height")
            width = pageJson.get("width")

            # Alignment
            matches = parse(
                "$.objects.displayArea[0].properties.verticalAlignment.expr.Literal.Value"
            ).find(pageJson)
            alignment_value = (
                matches[0].value[1:-1] if matches and matches[0].value else "Top"
            )

            # Drillthrough
            matches = parse("$.filterConfig.filters[*].howCreated").find(pageJson)
            how_created_values = [match.value for match in matches]
            drill_through = any(value == "Drillthrough" for value in how_created_values)
            # matches = parse("$.filterConfig.filters[*]").find(pageJson)
            # drill_through = any(
            #    filt.get("howCreated") == "Drillthrough"
            #    for filt in (match.value for match in matches)
            # )

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

            # Page Filter Count
            matches = parse("$.filterConfig.filters").find(pageJson)
            page_filter_count = (
                len(matches[0].value) if matches and matches[0].value else 0
            )

            # Hidden
            matches = parse("$.visibility").find(pageJson)
            is_hidden = any(match.value == "HiddenInViewMode" for match in matches)

            new_data = {
                "File Path": file_path,
                "Page Name": page_name,
                "Page Display Name": pageJson.get("displayName"),
                "Display Option": pageJson.get("displayOption"),
                "Height": height,
                "Width": width,
                "Hidden": is_hidden,
                "Active": False,
                "Type": helper.page_type_mapping.get((width, height), "Custom"),
                "Alignment": alignment_value,
                "Drillthrough Target Page": drill_through,
                "Visual Count": visual_count,
                "Data Visual Count": data_visual_count,
                "Visible Visual Count": visible_visual_count,
                "Page Filter Count": page_filter_count,
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

        page_payload = pages_row["payload"].iloc[0]
        pageFile = base64.b64decode(page_payload).decode("utf-8")
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

        df["Page URL"] = df["Page Name"].apply(
            lambda page_name: f"{helper.get_web_url(report=self._report, workspace=self._workspace_id)}/{page_name}"
        )

        return df
        # return df.style.format({"Page URL": _make_clickable})

    def list_visuals(self) -> pd.DataFrame:
        """
        Shows a list of all visuals in the report.

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
                "Data Limit",
            ]
        )

        rd_filt = rd[rd["path"] == "definition/report.json"]
        payload = rd_filt["payload"].iloc[0]
        rptJson = _extract_json(rd_filt)
        custom_visuals = rptJson.get("publicCustomVisuals", [])
        page_mapping, visual_mapping = helper.visual_page_mapping(self)
        helper.populate_custom_visual_display_names()
        agg_type_map = helper._get_agg_type_mapping()

        def contains_key(data, keys_to_check):
            matches = parse("$..*").find(data)

            all_keys = set()
            for match in matches:
                if isinstance(match.value, dict):
                    all_keys.update(match.value.keys())
                elif isinstance(match.value, list):
                    for item in match.value:
                        if isinstance(item, dict):
                            all_keys.update(item.keys())

            return any(key in all_keys for key in keys_to_check)

        for _, r in rd.iterrows():
            file_path = r["path"]
            payload = r["payload"]
            if file_path.endswith("/visual.json"):
                visual_file = base64.b64decode(payload).decode("utf-8")
                visual_json = json.loads(visual_file)
                page_id = visual_mapping.get(file_path)[0]
                page_display = visual_mapping.get(file_path)[1]
                pos = visual_json.get("position")

                # Visual Type
                matches = parse("$.visual.visualType").find(visual_json)
                visual_type = matches[0].value if matches else "Group"

                visual_type_display = helper.vis_type_mapping.get(
                    visual_type, visual_type
                )
                cst_value, rst_value, slicer_type = False, False, "N/A"

                # Visual Filter Count
                matches = parse("$.filterConfig.filters[*]").find(visual_json)
                visual_filter_count = len(matches)

                # Data Limit
                matches = parse(
                    '$.filterConfig.filters[?(@.type == "VisualTopN")].filter.Where[*].Condition.VisualTopN.ItemCount'
                ).find(visual_json)
                data_limit = matches[0].value if matches else 0

                # Title
                matches = parse(
                    "$.visual.visualContainerObjects.title[0].properties.text.expr"
                ).find(visual_json)
                # title = matches[0].value[1:-1] if matches else ""
                title = (
                    helper._get_expression(matches[0].value, agg_type_map)
                    if matches
                    else ""
                )

                # SubTitle
                matches = parse(
                    "$.visual.visualContainerObjects.subTitle[0].properties.text.expr"
                ).find(visual_json)
                # sub_title = matches[0].value[1:-1] if matches else ""
                sub_title = (
                    helper._get_expression(matches[0].value, agg_type_map)
                    if matches
                    else ""
                )

                # Alt Text
                matches = parse(
                    "$.visual.visualContainerObjects.general[0].properties.altText.expr"
                ).find(visual_json)
                # alt_text = matches[0].value[1:-1] if matches else ""
                alt_text = (
                    helper._get_expression(matches[0].value, agg_type_map)
                    if matches
                    else ""
                )

                # Show items with no data
                def find_show_all_with_jsonpath(obj):
                    matches = parse("$..showAll").find(obj)
                    return any(match.value is True for match in matches)

                show_all_data = find_show_all_with_jsonpath(visual_json)

                # Divider
                matches = parse(
                    "$.visual.visualContainerObjects.divider[0].properties.show.expr.Literal.Value"
                ).find(visual_json)
                divider = matches[0] if matches else ""

                # Row/Column Subtotals
                if visual_type == "pivotTable":
                    cst_matches = parse(
                        "$.visual.objects.subTotals[0].properties.columnSubtotals.expr.Literal.Value"
                    ).find(visual_json)
                    rst_matches = parse(
                        "$.visual.objects.subTotals[0].properties.rowSubtotals.expr.Literal.Value"
                    ).find(visual_json)

                    if cst_matches:
                        cst_value = False if cst_matches[0].value == "false" else True

                    if rst_matches:
                        rst_value = False if rst_matches[0].value == "false" else True

                # Slicer Type
                if visual_type == "slicer":
                    matches = parse(
                        "$.visual.objects.data[0].properties.mode.expr.Literal.Value"
                    ).find(visual_json)
                    slicer_type = matches[0].value[1:-1] if matches else "N/A"

                # Data Visual
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

                # Sparkline
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
                    "Data Limit": data_limit,
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

        float_cols = ["X", "Y", "Width", "Height"]
        int_cols = ["Z", "Visual Filter Count", "Data Limit", "Visual Object Count"]
        df[bool_cols] = df[bool_cols].astype(bool)
        df[int_cols] = df[int_cols].astype(int)
        df[float_cols] = df[float_cols].astype(float)

        return df

    def list_visual_objects(self, extended: bool = False) -> pd.DataFrame:
        """
        Shows a list of all semantic model objects used in each visual in the report.

        Parameters
        ----------
        extended : bool, default=False
            If True, adds an extra column called 'Valid Semantic Model Object' which identifies whether the semantic model object used
            in the report exists in the semantic model which feeds data to the report.

        Returns
        -------
        pandas.DataFrame
            A pandas dataframe containing a list of all semantic model objects used in each visual in the report.
        """

        rd = self.rdef
        page_mapping, visual_mapping = helper.visual_page_mapping(self)
        df = pd.DataFrame(
            columns=[
                "Page Name",
                "Page Display Name",
                "Visual Name",
                "Table Name",
                "Object Name",
                "Object Type",
                "Implicit Measure",
                "Sparkline",
                "Visual Calc",
                "Format",
                "Object Display Name",
            ]
        )

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

        for _, r in rd.iterrows():
            file_path = r["path"]
            payload = r["payload"]
            if file_path.endswith("/visual.json"):
                visual_file = base64.b64decode(payload).decode("utf-8")
                visual_json = json.loads(visual_file)
                page_id = visual_mapping.get(file_path)[0]
                page_display = visual_mapping.get(file_path)[1]

                entity_property_pairs = find_entity_property_pairs(visual_json)
                query_state = (
                    visual_json.get("visual", {}).get("query", {}).get("queryState", {})
                )

                format_mapping = {}
                obj_display_mapping = {}
                for a, p in query_state.items():
                    for proj in p.get("projections", []):
                        query_ref = proj.get("queryRef")
                        fmt = proj.get("format")
                        obj_display_name = proj.get("displayName")
                        if fmt is not None:
                            format_mapping[query_ref] = fmt
                        obj_display_mapping[query_ref] = obj_display_name

                for object_name, properties in entity_property_pairs.items():
                    table_name = properties[0]
                    obj_full = f"{table_name}.{object_name}"
                    is_agg = properties[2]
                    format_value = format_mapping.get(obj_full)
                    obj_display = obj_display_mapping.get(obj_full)

                    if is_agg:
                        for k, v in format_mapping.items():
                            if obj_full in k:
                                format_value = v
                    new_data = {
                        "Page Name": page_id,
                        "Page Display Name": page_display,
                        "Visual Name": visual_json.get("name"),
                        "Table Name": table_name,
                        "Object Name": object_name,
                        "Object Type": properties[1],
                        "Implicit Measure": is_agg,
                        "Sparkline": properties[4],
                        "Visual Calc": properties[3],
                        "Format": format_value,
                        "Object Display Name": obj_display,
                    }

                    df = pd.concat(
                        [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                    )

        if extended:
            df = self._add_extended(dataframe=df)

        bool_cols = ["Implicit Measure", "Sparkline", "Visual Calc"]
        df[bool_cols] = df[bool_cols].astype(bool)

        return df

    def list_semantic_model_objects(self, extended: bool = False) -> pd.DataFrame:
        """
        Shows a list of all semantic model objects (measures, columns, hierarchies) that are used in the report and where the objects
        were used (i.e. visual, report filter, page filter, visual filter).

        Parameters
        ----------
        extended : bool, default=False
            If True, adds an extra column called 'Valid Semantic Model Object' which identifies whether the semantic model object used
            in the report exists in the semantic model which feeds data to the report.

        Returns
        -------
        pandas.DataFrame
            A pandas dataframe showing the semantic model objects used in the report.
        """

        from sempy_labs.tom import connect_semantic_model

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
            dataset_id, dataset_name, dataset_workspace_id, dataset_workspace_name = (
                resolve_dataset_from_report(
                    report=self._report, workspace=self._workspace_id
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
                dataset=dataset_id, readonly=True, workspace=dataset_workspace_id
            ) as tom:
                df["Valid Semantic Model Object"] = df.apply(
                    lambda row: check_validity(tom, row), axis=1
                )

        return df

    def _list_all_semantic_model_objects(self):

        # Includes dependencies

        df = (
            self.list_semantic_model_objects()[
                ["Table Name", "Object Name", "Object Type"]
            ]
            .drop_duplicates()
            .reset_index(drop=True)
        )
        dataset_id, dataset_name, dataset_workspace_id, dataset_workspace_name = (
            resolve_dataset_from_report(
                report=self._report, workspace=self._workspace_id
            )
        )
        dep = get_measure_dependencies(
            dataset=dataset_id, workspace=dataset_workspace_id
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
        result_df["Dataset Workspace Name"] = dataset_workspace_name
        colName = "Dataset Name"
        result_df.insert(0, colName, result_df.pop(colName))
        colName = "Dataset Workspace Name"
        result_df.insert(1, colName, result_df.pop(colName))

        return result_df

    def list_bookmarks(self) -> pd.DataFrame:
        """
        Shows a list of all bookmarks in the report.

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

        bookmark_rows = rd[rd["path"].str.endswith(".bookmark.json")]

        for _, r in bookmark_rows.iterrows():
            path = r["path"]
            payload = r["payload"]

            obj_file = base64.b64decode(payload).decode("utf-8")
            obj_json = json.loads(obj_file)

            bookmark_name = obj_json.get("name")
            bookmark_display = obj_json.get("displayName")
            rpt_page_id = obj_json.get("explorationState", {}).get("activeSection")
            page_id, page_display, file_path = helper.resolve_page_name(
                self, page_name=rpt_page_id
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

        bool_cols = ["Visual Hidden"]
        df[bool_cols] = df[bool_cols].astype(bool)

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

    def _list_annotations(self) -> pd.DataFrame:
        """
        Shows a list of annotations in the report.

        Returns
        -------
        pandas.DataFrame
            A pandas dataframe showing a list of report, page and visual annotations in the report.
        """

        df = pd.DataFrame(
            columns=["Type", "Object Name", "Annotation Name", "Annotation Value"]
        )

        page_mapping, visual_mapping = helper.visual_page_mapping(self)
        for _, r in self.rdef.iterrows():
            payload = r["payload"]
            path = r["path"]
            if path == "definition/report.json":
                file = _decode_b64(payload)
                json_file = json.loads(file)
                if "annotations" in json_file:
                    for ann in json_file["annotations"]:
                        new_data = {
                            "Type": "Report",
                            "Object Name": self._report,
                            "Annotation Name": ann.get("name"),
                            "Annotation Value": ann.get("value"),
                        }
                        df = pd.concat(
                            [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                        )
            elif path.endswith("/page.json"):
                file = _decode_b64(payload)
                json_file = json.loads(file)
                if "annotations" in json_file:
                    for ann in json_file["annotations"]:
                        new_data = {
                            "Type": "Page",
                            "Object Name": json_file.get("displayName"),
                            "Annotation Name": ann.get("name"),
                            "Annotation Value": ann.get("value"),
                        }
                        df = pd.concat(
                            [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                        )
            elif path.endswith("/visual.json"):
                file = _decode_b64(payload)
                json_file = json.loads(file)
                page_display = visual_mapping.get(path)[1]
                visual_name = json_file.get("name")
                if "annotations" in json_file:
                    for ann in json_file["annotations"]:
                        new_data = {
                            "Type": "Visual",
                            "Object Name": f"'{page_display}'[{visual_name}]",
                            "Annotation Name": ann.get("name"),
                            "Annotation Value": ann.get("value"),
                        }
                        df = pd.concat(
                            [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                        )

        return df

    # Automation functions
    def set_theme(self, theme_file_path: str):
        """
        Sets a custom theme for a report based on a theme .json file.

        Parameters
        ----------
        theme_file_path : str
            The file path of the theme.json file. This can either be from a Fabric lakehouse or from the web.
            Example for lakehouse: file_path = '/lakehouse/default/Files/CY23SU09.json'
            Example for web url: file_path = 'https://raw.githubusercontent.com/PowerBiDevCamp/FabricUserApiDemo/main/FabricUserApiDemo/DefinitionTemplates/Shared/Reports/StaticResources/SharedResources/BaseThemes/CY23SU08.json'
        """

        import requests

        report_path = "definition/report.json"
        theme_version = "5.5.4"
        request_body = {"definition": {"parts": []}}

        if not theme_file_path.endswith(".json"):
            raise ValueError(
                f"{icons.red_dot} The '{theme_file_path}' theme file path must be a .json file."
            )
        elif theme_file_path.startswith("https://"):
            response = requests.get(theme_file_path)
            json_file = response.json()
        elif theme_file_path.startswith("/lakehouse"):
            with open(theme_file_path, "r", encoding="utf-8-sig") as file:
                json_file = json.load(file)
        else:
            ValueError(
                f"{icons.red_dot} Incorrect theme file path value '{theme_file_path}'."
            )

        theme_name = json_file["name"]
        theme_name_full = f"{theme_name}.json"
        rd = self.rdef

        # Add theme.json file to request_body
        file_payload = _conv_b64(json_file)
        filePath = f"StaticResources/RegisteredResources/{theme_name_full}"

        _add_part(request_body, filePath, file_payload)

        new_theme = {
            "name": theme_name_full,
            "path": theme_name_full,
            "type": "CustomTheme",
        }

        for _, r in rd.iterrows():
            path = r["path"]
            payload = r["payload"]
            if path == filePath:
                pass
            elif path != report_path:
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
                    names = [
                        rp["name"] for rp in rptJson["resourcePackages"][1]["items"]
                    ]

                    if theme_name_full not in names:
                        rptJson["resourcePackages"][1]["items"].append(new_theme)

                file_payload = _conv_b64(rptJson)
                _add_part(request_body, path, file_payload)

        self.update_report(request_body=request_body)
        print(
            f"{icons.green_dot} The '{theme_name}' theme has been set as the theme for the '{self._report}' report within the '{self._workspace_name}' workspace."
        )

    def set_active_page(self, page_name: str):
        """
        Sets the active page (first page displayed when opening a report) for a report.

        Parameters
        ----------
        page_name : str
            The page name or page display name of the report.
        """

        pages_file = "definition/pages/pages.json"
        page_id, page_display_name, file_path = helper.resolve_page_name(
            self, page_name=page_name
        )

        pagePath = self.rdef[self.rdef["path"] == pages_file]
        payload = pagePath["payload"].iloc[0]
        page_file = _decode_b64(payload)
        json_file = json.loads(page_file)
        json_file["activePageName"] = page_id
        file_payload = _conv_b64(json_file)

        self._update_single_file(file_name=pages_file, new_payload=file_payload)

        print(
            f"{icons.green_dot} The '{page_display_name}' page has been set as the active page in the '{self._report}' report within the '{self._workspace_name}' workspace."
        )

    def set_page_type(self, page_name: str, page_type: str):
        """
        Changes the page type of a report page.

        Parameters
        ----------
        page_name : str
            Name or display name of the report page.
        page_type : str
            The page type. Valid page types: 'Tooltip', 'Letter', '4:3', '16:9'.
        """

        if page_type not in helper.page_types:
            raise ValueError(
                f"{icons.red_dot} Invalid page type. Valid options: {helper.page_types}."
            )

        letter_key = next(
            (
                key
                for key, value in helper.page_type_mapping.items()
                if value == page_type
            ),
            None,
        )
        if letter_key:
            width, height = letter_key
        else:
            raise ValueError(
                f"{icons.red_dot} Invalid page_type parameter. Valid options: ['Tooltip', 'Letter', '4:3', '16:9']."
            )

        page_id, page_display_name, file_path = helper.resolve_page_name(
            self, page_name=page_name
        )
        rd_filt = self.rdef[self.rdef["path"] == file_path]
        payload = rd_filt["payload"].iloc[0]
        page_file = _decode_b64(payload)
        json_file = json.loads(page_file)
        json_file["width"] = width
        json_file["height"] = height
        file_payload = _conv_b64(json_file)

        self._update_single_file(file_name=file_path, new_payload=file_payload)

        print(
            f"{icons.green_dot} The '{page_display_name}' page has been updated to the '{page_type}' page type."
        )

    def remove_unnecessary_custom_visuals(self):
        """
        Removes any custom visuals within the report that are not used in the report.
        """

        dfCV = self.list_custom_visuals()
        dfV = self.list_visuals()
        rd = self.rdef
        cv_remove = []
        cv_remove_display = []
        request_body = {"definition": {"parts": []}}

        for _, r in dfCV.iterrows():
            cv = r["Custom Visual Name"]
            cv_display = r["Custom Visual Display Name"]
            dfV_filt = dfV[dfV["Type"] == cv]
            if len(dfV_filt) == 0:
                cv_remove.append(cv)  # Add to the list for removal
                cv_remove_display.append(cv_display)
        if len(cv_remove) == 0:
            print(
                f"{icons.green_dot} There are no unnecessary custom visuals in the '{self._report}' report within the '{self._workspace_name}' workspace."
            )
            return

        for _, r in rd.iterrows():
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

        self.update_report(request_body=request_body)
        print(
            f"{icons.green_dot} The {cv_remove_display} custom visuals have been removed from the '{self._report}' report within the '{self._workspace_name}' workspace."
        )

    def migrate_report_level_measures(self, measures: Optional[str | List[str]] = None):
        """
        Moves all report-level measures from the report to the semantic model on which the report is based.

        Parameters
        ----------
        measures : str | List[str], default=None
            A measure or list of measures to move to the semantic model.
            Defaults to None which resolves to moving all report-level measures to the semantic model.
        """

        from sempy_labs.tom import connect_semantic_model

        rlm = self.list_report_level_measures()
        if len(rlm) == 0:
            print(
                f"{icons.green_dot} The '{self._report}' report within the '{self._workspace_name}' workspace has no report-level measures."
            )
            return

        dataset_id, dataset_name, dataset_workspace_id, dataset_workspace_name = (
            resolve_dataset_from_report(
                report=self._report, workspace=self._workspace_id
            )
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
            dataset=dataset_id, readonly=False, workspace=dataset_workspace_id
        ) as tom:
            for _, r in rlm.iterrows():
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
        for _, r in rd.iterrows():
            path = r["path"]
            payload = r["payload"]
            if path != rpt_file:
                _add_part(request_body, path, payload)

        self.update_report(request_body=request_body)
        print(
            f"{icons.green_dot} The report-level measures have been migrated to the '{dataset_name}' semantic model within the '{dataset_workspace_name}' workspace."
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
        """

        page_id, page_display_name, file_path = helper.resolve_page_name(
            self, page_name=page_name
        )
        visibility = "visible" if hidden is False else "hidden"

        rd_filt = self.rdef[self.rdef["path"] == file_path]
        payload = rd_filt["payload"].iloc[0]
        obj_file = _decode_b64(payload)
        obj_json = json.loads(obj_file)
        if hidden:
            obj_json["visibility"] = "HiddenInViewMode"
        else:
            if "visibility" in obj_json:
                del obj_json["visibility"]
        new_payload = _conv_b64(obj_json)

        self._update_single_file(file_name=file_path, new_payload=new_payload)

        print(
            f"{icons.green_dot} The '{page_display_name}' page has been set to {visibility}."
        )

    def hide_tooltip_drillthrough_pages(self):
        """
        Hides all tooltip pages and drillthrough pages in a report.
        """

        dfP = self.list_pages()
        dfP_filt = dfP[
            (dfP["Type"] == "Tooltip") | (dfP["Drillthrough Target Page"] == True)
        ]

        if len(dfP_filt) == 0:
            print(
                f"{icons.green_dot} There are no Tooltip or Drillthrough pages in the '{self._report}' report within the '{self._workspace_name}' workspace."
            )
            return

        for _, r in dfP_filt.iterrows():
            page_name = r["Page Name"]
            self.set_page_visibility(page_name=page_name, hidden=True)

    def disable_show_items_with_no_data(self):
        """
        Disables the `show items with no data <https://learn.microsoft.com/power-bi/create-reports/desktop-show-items-no-data>`_ property in all visuals within the report.
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
        for _, r in rd.iterrows():
            file_path = r["path"]
            payload = r["payload"]
            if file_path.endswith("/visual.json"):
                objFile = base64.b64decode(payload).decode("utf-8")
                objJson = json.loads(objFile)
                delete_key_in_json(objJson, "showAll")
                _add_part(request_body, file_path, _conv_b64(objJson))
            else:
                _add_part(request_body, file_path, payload)

        self.update_report(request_body=request_body)
        print(
            f"{icons.green_dot} Show items with data has been disabled for all visuals in the '{self._report}' report within the '{self._workspace_name}' workspace."
        )

    # Set Annotations
    def __set_annotation(self, json_file: dict, name: str, value: str) -> dict:

        if "annotations" in json_file:
            if any(
                annotation["name"] == name for annotation in json_file["annotations"]
            ):
                for annotation in json_file["annotations"]:
                    if annotation["name"] == name:
                        annotation["value"] = value
                        break
            else:
                json_file["annotations"].append({"name": name, "value": value})
        else:
            json_file["annotations"] = []
            json_file["annotations"].append({"name": name, "value": value})

        return json_file

    def _set_annotation(
        self,
        annotation_name: str,
        annotation_value: str,
        page_name: Optional[str] = None,
        visual_name: Optional[str] = None,
    ):
        """
        Sets an annotation on the report/page/visual. If the annotation already exists, the annotation value is updated.
        In order to set a report annotation, leave page_name=None, visual_name=None.
        In order to set a page annotation, leave visual_annotation=None.
        In order to set a visual annotation, set all parameters.

        Parameters
        ----------
        annotation_name : str
            Name of the annotation.
        annotation_value : str
            Value of the annotation.
        page_name : str, default=None
            The page name or page display name.
            Set this annotation when setting an annotation on a page or visual.
        visual_name : str, default=None
            The visual name.
            Set this property when setting an annotation on a visual.
        """

        if page_name is None and visual_name is None:
            file_path = "definition/report.json"
        elif page_name is not None and visual_name is None:
            page_id, page_display, file_path = helper.resolve_page_name(
                self, page_name=page_name
            )
        elif page_name is not None and visual_name is not None:
            page_name, page_display_name, visual_name, file_path = (
                helper.resolve_visual_name(
                    self, page_name=page_name, visual_name=visual_name
                )
            )
        else:
            raise ValueError(
                f"{icons.red_dot} Invalid parameters. If specifying a visual_name you must specify the page_name."
            )

        payload = self.rdef[self.rdef["path"] == file_path]["payload"].iloc[0]
        file = _decode_b64(payload)
        json_file = json.loads(file)

        new_file = self.__set_annotation(
            json_file, name=annotation_name, value=annotation_value
        )
        new_payload = _conv_b64(new_file)

        self._update_single_file(file_name=file_path, new_payload=new_payload)

    # Remove Annotations
    def __remove_annotation(self, json_file: dict, name: str) -> dict:

        if "annotations" in json_file:
            json_file["annotations"] = [
                annotation
                for annotation in json_file["annotations"]
                if annotation["name"] != name
            ]

        return json_file

    def _remove_annotation(
        self,
        annotation_name: str,
        page_name: Optional[str] = None,
        visual_name: Optional[str] = None,
    ):
        """
        Removes an annotation on the report/page/visual.
        In order to remove a report annotation, leave page_name=None, visual_name=None.
        In order to remove a page annotation, leave visual_annotation=None.
        In order to remove a visual annotation, set all parameters.

        Parameters
        ----------
        annotation_name : str
            Name of the annotation.
        page_name : str, default=None
            The page name or page display name.
            Set this annotation when setting an annotation on a page or visual.
        visual_name : str, default=None
            The visual name.
            Set this property when setting an annotation on a visual.
        """

        if page_name is None and visual_name is None:
            file_path = "definition/report.json"
        elif page_name is not None and visual_name is None:
            page_id, page_display, file_path = helper.resolve_page_name(
                self, page_name=page_name
            )
        elif page_name is not None and visual_name is not None:
            page_name, page_display_name, visual_name, file_path = (
                helper.resolve_visual_name(
                    self, page_name=page_name, visual_name=visual_name
                )
            )
        else:
            raise ValueError(
                f"{icons.red_dot} Invalid parameters. If specifying a visual_name you must specify the page_name."
            )

        payload = self.rdef[self.rdef["path"] == file_path]["payload"].iloc[0]
        file = _decode_b64(payload)
        json_file = json.loads(file)

        new_file = self.__remove_annotation(json_file, name=annotation_name)
        new_payload = _conv_b64(new_file)

        self._update_single_file(file_name=file_path, new_payload=new_payload)

    # Get Annotation Value
    def __get_annotation_value(self, json_file: dict, name: str) -> str:

        if "annotations" in json_file:
            for ann in json_file["annotations"]:
                if ann.get("name") == name:
                    return ann.get("value")

    def _get_annotation_value(
        self,
        annotation_name: str,
        page_name: Optional[str] = None,
        visual_name: Optional[str] = None,
    ) -> str:
        """
        Retrieves the annotation value of an annotation on the report/page/visual.
        In order to retrieve a report annotation value, leave page_name=None, visual_name=None.
        In order to retrieve a page annotation value, leave visual_annotation=None.
        In order to retrieve a visual annotation value, set all parameters.

        Parameters
        ----------
        annotation_name : str
            Name of the annotation.
        page_name : str, default=None
            The page name or page display name.
            Set this annotation when setting an annotation on a page or visual.
        visual_name : str, default=None
            The visual name.
            Set this property when setting an annotation on a visual.

        Returns
        -------
        str
            The annotation value.
        """

        if page_name is None and visual_name is None:
            file_path = "definition/report.json"
        elif page_name is not None and visual_name is None:
            page_id, page_display, file_path = helper.resolve_page_name(
                self, page_name=page_name
            )
        elif page_name is not None and visual_name is not None:
            page_name, page_display_name, visual_name, file_path = (
                helper.resolve_visual_name(
                    self, page_name=page_name, visual_name=visual_name
                )
            )
        else:
            raise ValueError(
                f"{icons.red_dot} Invalid parameters. If specifying a visual_name you must specify the page_name."
            )

        payload = self.rdef[self.rdef["path"] == file_path]["payload"].iloc[0]
        file = _decode_b64(payload)
        json_file = json.loads(file)

        return self.__get_annotation_value(json_file, name=annotation_name)

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
        for _, r in rd.iterrows():
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

    def __enable_cross_report_drillthrough(self, value: Optional[bool] = False):
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

    # def close(self):
    # if not self._readonly and self._report is not None:
    #    print("saving...")

    #    self._report = None
