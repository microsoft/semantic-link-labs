from typing import Optional, Tuple
from contextlib import contextmanager
from sempy._utils._log import log
from uuid import UUID
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_item_name_and_id,
    _base_api,
    _conv_b64,
    _decode_b64,
    _create_dataframe,
    _update_dataframe_datatypes,
    format_dax_object_name,
    resolve_dataset_from_report,
)
import json
import sempy_labs._icons as icons
import copy
import pandas as pd
from jsonpath_ng.ext import parse
import sempy_labs.report._report_helper as helper
from sempy_labs._model_dependencies import get_measure_dependencies
import requests
import re
import base64
from io import BytesIO
import zipfile


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

    _report_name: str
    _report_id: str
    _workspace_name: str
    _workspace_id: str
    _readonly: bool

    @log
    def __init__(
        self,
        report: str | UUID,
        workspace: Optional[str | UUID] = None,
        readonly: bool = True,
    ):
        (self._workspace_name, self._workspace_id) = resolve_workspace_name_and_id(
            workspace
        )
        (self._report_name, self._report_id) = resolve_item_name_and_id(
            item=report, type="Report", workspace=self._workspace_id
        )
        self._readonly = readonly

        result = _base_api(
            request=f"/v1/workspaces/{self._workspace_id}/items/{self._report_id}/getDefinition",
            method="post",
            status_codes=None,
            lro_return_json=True,
        )

        # def is_zip_file(data: bytes) -> bool:
        #    return data.startswith(b"PK\x03\x04")

        self._report_definition = {"parts": []}
        for parts in result.get("definition", {}).get("parts", []):
            path = parts.get("path")
            payload = parts.get("payload")

            # decoded_bytes = base64.b64decode(payload)
            # decoded_payload = json.loads(_decode_b64(payload))
            try:
                decoded_payload = json.loads(base64.b64decode(payload).decode("utf-8"))
            except:
                decoded_payload = payload

            # if is_zip_file(decoded_bytes):
            #    merged_payload = {}
            #    with zipfile.ZipFile(BytesIO(decoded_bytes)) as zip_file:
            #        for filename in zip_file.namelist():
            #            if filename.endswith(".json"):
            #                with zip_file.open(filename) as f:
            #                    content = f.read()
            #                    part_data = json.loads(content.decode("utf-8"))

            #                    if isinstance(part_data, dict):
            #                        merged_payload.update(part_data)
            #                    else:
            #                        # For non-dict top-level json (rare), store under filename
            #                        merged_payload[filename] = part_data

            #    self._report_definition["parts"].append(
            #        {"path": path, "payload": merged_payload}
            #   )
            # else:
            #    decoded_payload = json.loads(decoded_bytes.decode("utf-8"))
            self._report_definition["parts"].append(
                {"path": path, "payload": decoded_payload}
            )
        self._current_report_definition = copy.deepcopy(self._report_definition)

    def get(self, file_path: str) -> dict:
        """
        Get the json content of the specified report definition file.

        Parameters
        ----------
        file_path : str
            The path of the report definition file. For example: "definition/pages/pages.json".

        Returns
        -------
        dict
            The json content of the specified report definition file.
        """
        for part in self._report_definition.get("parts"):
            if part.get("path") == file_path:
                return part.get("payload")

        raise ValueError(f"File {file_path} not found in report definition.")

    def add(self, file_path: str, payload: dict):

        if not isinstance(payload, dict):
            raise ValueError("Payload must be a dictionary.")

        existing_paths = [
            part.get("path") for part in self._report_definition.get("parts")
        ]
        if file_path in existing_paths:
            raise ValueError(
                f"{icons.red_dot} Cannot add the '{file_path}' file as this file path already exists in the report definition."
            )

        self._report_definition["parts"].append({"path": file_path, "payload": payload})

    def remove(self, file_path: str):

        for part in self._report_definition.get("parts"):
            if part.get("path") == file_path:
                self._report_definition["parts"].remove(part)
                # print(f"The file '{file_path}' has been removed from report definition.")
                return

        raise ValueError(f"File {file_path} not found in report definition.")

    def update(self, file_path: str, payload: dict):
        if not isinstance(payload, dict):
            raise ValueError("Payload must be a dictionary.")

        for part in self._report_definition.get("parts"):
            if part.get("path") == file_path:
                part["payload"] = payload
                # print(f"The file '{file_path}' has been updated in report definition.")
                return

        raise ValueError(f"File {file_path} not found in report definition.")

    # def all_files(self):

    #    yield self._report_definition.get('parts')

    def save_changes(self):

        if self._readonly:
            print(
                f"{icons.red_dot} The connection is read-only. Set 'readonly' to False to save changes."
            )
        else:
            # Convert the report definition to base64
            new_report_definition = copy.deepcopy(self._report_definition)

            for part in new_report_definition.get("parts", []):
                if isinstance(part.get("payload"), dict):
                    part["payload"] = _conv_b64(
                        part["payload"]
                    )  # Do I need to zip some of these?

            # Combine report and non-report definitions
            payload = {"definition": {"parts": new_report_definition.get("parts")}}

            # Update item definition
            _base_api(
                request=f"/v1/workspaces/{self._workspace_id}/reports/{self._report_id}/updateDefinition",
                method="post",
                payload=payload,
                lro_return_status_code=True,
                status_codes=None,
            )
            print(
                f"{icons.green_dot} The report definition has been updated successfully."
            )

    def list_custom_visuals(self) -> pd.DataFrame:
        """
        Shows a list of all custom visuals used in the report.

        Returns
        -------
        pandas.DataFrame
            A pandas dataframe containing a list of all the custom visuals used in the report.
        """

        helper.populate_custom_visual_display_names()

        columns = {
            "Custom Visual Name": "str",
            "Custom Visual Display Name": "str",
            "Used in Report": "bool",
        }

        df = _create_dataframe(columns=columns)

        report_file = self.get(file_path="definition/report.json")

        df["Custom Visual Name"] = report_file.get("publicCustomVisuals")
        df["Custom Visual Display Name"] = df["Custom Visual Name"].apply(
            lambda x: helper.vis_type_mapping.get(x, x)
        )

        df["Used in Report"] = df["Custom Visual Name"].isin(
            self.list_visuals()["Type"]
        )

        _update_dataframe_datatypes(dataframe=df, column_map=columns)

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

        report_file = self.get(file_path="definition/report.json")

        columns = {
            "Filter Name": "str",
            "Type": "str",
            "Table Name": "str",
            "Object Name": "str",
            "Object Type": "str",
            "Hidden": "bool",
            "Locked": "bool",
            "How Created": "str",
            "Used": "bool",
        }
        df = _create_dataframe(columns=columns)

        dfs = []

        if "filterConfig" in report_file:
            for flt in report_file.get("filterConfig", {}).get("filters", {}):
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

                    dfs.append(pd.DataFrame(new_data, index=[0]))

        if dfs:
            df = pd.concat(dfs, ignore_index=True)

        _update_dataframe_datatypes(dataframe=df, column_map=columns)

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

        columns = {
            "Page Name": "str",
            "Page Display Name": "str",
            "Filter Name": "str",
            "Type": "str",
            "Table Name": "str",
            "Object Name": "str",
            "Object Type": "str",
            "Hidden": "bool",
            "Locked": "bool",
            "How Created": "str",
            "Used": "bool",
        }
        df = _create_dataframe(columns=columns)

        pages = [
            p
            for p in self._report_definition.get("parts")
            if p.get("path").endswith("/page.json")
        ]

        dfs = []
        for p in pages:
            payload = p.get("payload")
            page_id = payload.get("name")
            page_display = payload.get("displayName")

            if "filterConfig" in payload:
                for flt in payload.get("filterConfig", {}).get("filters", {}):
                    filter_name = flt.get("name")
                    how_created = flt.get("howCreated")
                    locked = flt.get("isLockedInViewMode", False)
                    hidden = flt.get("isHiddenInViewMode", False)
                    filter_type = flt.get("type", "Basic")
                    filter_used = True if "Where" in flt.get("filter", {}) else False

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
                            "Page URL": f"{helper.get_web_url(report=self._report_name, workspace=self._workspace_id)}/{page_id}",
                        }

                        dfs.append(pd.DataFrame(new_data, index=[0]))

        if dfs:
            df = pd.concat(dfs, ignore_index=True)

        _update_dataframe_datatypes(dataframe=df, column_map=columns)

        if extended:
            df = self._add_extended(dataframe=df)

        return df

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

        columns = {
            "Page Name": "str",
            "Page Display Name": "str",
            "Visual Name": "str",
            "Filter Name": "str",
            "Type": "str",
            "Table Name": "str",
            "Object Name": "str",
            "Object Type": "str",
            "Hidden": "bool",
            "Locked": "bool",
            "How Created": "str",
            "Used": "bool",
        }
        df = _create_dataframe(columns=columns)

        page_mapping, visual_mapping = self.visual_page_mapping()

        visuals = [
            o
            for o in self._report_definition.get("parts")
            if o.get("path").endswith("/visual.json")
        ]

        dfs = []
        for v in visuals:
            path = v.get("path")
            payload = v.get("payload")
            page_id = visual_mapping.get(path)[0]
            page_display = visual_mapping.get(path)[1]
            visual_name = payload.get("name")

            if "filterConfig" in payload:
                for flt in payload.get("filterConfig", {}).get("filters", {}):
                    filter_name = flt.get("name")
                    how_created = flt.get("howCreated")
                    locked = flt.get("isLockedInViewMode", False)
                    hidden = flt.get("isHiddenInViewMode", False)
                    filter_type = flt.get("type", "Basic")
                    filter_used = True if "Where" in flt.get("filter", {}) else False

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

                        dfs.append(pd.DataFrame(new_data, index=[0]))

        if dfs:
            df = pd.concat(dfs, ignore_index=True)

        _update_dataframe_datatypes(dataframe=df, column_map=columns)

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

        columns = {
            "Page Name": "str",
            "Page Display Name": "str",
            "Source Visual Name": "str",
            "Target Visual Name": "str",
            "Type": "str",
        }
        df = _create_dataframe(columns=columns)

        pages = [
            o
            for o in self._report_definition.get("parts")
            if o.get("path").endswith("/page.json")
        ]

        dfs = []
        for p in pages:
            payload = p.get("payload")
            page_name = payload.get("name")
            page_display = payload.get("displayName")

            for vizInt in payload.get("visualInteractions", []):
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
                dfs.append(pd.DataFrame(new_data, index=[0]))

        if dfs:
            df = pd.concat(dfs, ignore_index=True)

        return df

    def list_pages(self) -> pd.DataFrame:
        """
        List all pages in the report.

        Returns
        -------
        list
            A list of page names.
        """

        columns = {
            "File Path": "str",
            "Page Name": "str",
            "Page Display Name": "str",
            "Hidden": "bool",
            "Active": "bool",
            "Width": "int",
            "Height": "int",
            "Display Option": "str",
            "Type": "str",
            "Alignment": "str",
            "Drillthrough Target Page": "bool",
            "Visual Count": "int",
            "Data Visual Count": "int",
            "Visible Visual Count": "int",
            "Page Filter Count": "int",
            "Page URL": "str",
        }
        df = _create_dataframe(columns=columns)

        pages = [
            o
            for o in self._report_definition.get("parts")
            if o.get("path").endswith("/page.json")
        ]
        page = self.get(file_path="definition/pages/pages.json")
        active_page = page.get("activePageName")

        dfV = self.list_visuals()

        dfs = []
        for p in pages:
            file_path = p.get("path")
            page_prefix = file_path[0:-9]
            payload = p.get("payload")
            page_name = payload.get("name")
            height = payload.get("height")
            width = payload.get("width")

            # Alignment
            matches = parse(
                "$.objects.displayArea[0].properties.verticalAlignment.expr.Literal.Value"
            ).find(payload)
            alignment_value = (
                matches[0].value[1:-1] if matches and matches[0].value else "Top"
            )

            # Drillthrough
            matches = parse("$.filterConfig.filters[*].howCreated").find(payload)
            how_created_values = [match.value for match in matches]
            drill_through = any(value == "Drillthrough" for value in how_created_values)

            visual_count = len(
                [
                    v
                    for v in self._report_definition.get("parts")
                    if v.get("path").endswith("/visual.json")
                    and v.get("path").startswith(page_prefix)
                ]
            )

            data_visual_count = len(
                dfV[(dfV["Page Name"] == page_name) & (dfV["Data Visual"])]
            )
            visible_visual_count = len(
                dfV[(dfV["Page Name"] == page_name) & (dfV["Hidden"] == False)]
            )

            # Page Filter Count
            matches = parse("$.filterConfig.filters").find(payload)
            page_filter_count = (
                len(matches[0].value) if matches and matches[0].value else 0
            )

            # Hidden
            matches = parse("$.visibility").find(payload)
            is_hidden = any(match.value == "HiddenInViewMode" for match in matches)

            new_data = {
                "File Path": file_path,
                "Page Name": page_name,
                "Page Display Name": payload.get("displayName"),
                "Display Option": payload.get("displayOption"),
                "Height": height,
                "Width": width,
                "Hidden": is_hidden,
                "Active": True if page_name == active_page else False,
                "Type": helper.page_type_mapping.get((width, height), "Custom"),
                "Alignment": alignment_value,
                "Drillthrough Target Page": drill_through,
                "Visual Count": visual_count,
                "Data Visual Count": data_visual_count,
                "Visible Visual Count": visible_visual_count,
                "Page Filter Count": page_filter_count,
                "Page URL": f"{helper.get_web_url(report=self._report_name, workspace=self._workspace_id)}/{page_name}",
            }
            dfs.append(pd.DataFrame(new_data, index=[0]))

        if dfs:
            df = pd.concat(dfs, ignore_index=True)

        _update_dataframe_datatypes(dataframe=df, column_map=columns)

        return df

    def list_visuals(self) -> pd.DataFrame:
        """
        Shows a list of all visuals in the report.

        Returns
        -------
        pandas.DataFrame
            A pandas dataframe containing a list of all visuals in the report.
        """

        columns = {
            "File Path": "str",
            "Page Name": "str",
            "Page Display Name": "str",
            "Visual Name": "str",
            "Type": "str",
            "Display Type": "str",
            "X": "float",
            "Y": "float",
            "Z": "int",
            "Width": "float",
            "Height": "float",
            "Tab Order": "str",
            "Hidden": "bool",
            "Title": "str",
            "SubTitle": "str",
            "Custom Visual": "bool",
            "Alt Text": "str",
            "Show Items With No Data": "bool",
            "Divider": "str",
            "Slicer Type": "str",
            "Row SubTotals": "bool",
            "Column SubTotals": "bool",
            "Data Visual": "bool",
            "Has Sparkline": "bool",
            "Visual Filter Count": "int",
            "Data Limit": "int",
        }
        df = _create_dataframe(columns=columns)

        report_file = self.get(file_path="definition/report.json")
        custom_visuals = report_file.get("publicCustomVisuals", [])
        page_mapping, visual_mapping = self.visual_page_mapping()
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

        visuals = [
            o
            for o in self._report_definition.get("parts")
            if o.get("path").endswith("/visual.json")
        ]

        dfs = []

        for v in visuals:
            path = v.get("path")
            payload = v.get("payload")
            page_id = visual_mapping.get(path)[0]
            page_display = visual_mapping.get(path)[1]
            pos = payload.get("position")

            # Visual Type
            matches = parse("$.visual.visualType").find(payload)
            visual_type = matches[0].value if matches else "Group"

            visual_type_display = helper.vis_type_mapping.get(visual_type, visual_type)
            cst_value, rst_value, slicer_type = False, False, "N/A"

            # Visual Filter Count
            matches = parse("$.filterConfig.filters[*]").find(payload)
            visual_filter_count = len(matches)

            # Data Limit
            matches = parse(
                '$.filterConfig.filters[?(@.type == "VisualTopN")].filter.Where[*].Condition.VisualTopN.ItemCount'
            ).find(payload)
            data_limit = matches[0].value if matches else 0

            # Title
            matches = parse(
                "$.visual.visualContainerObjects.title[0].properties.text.expr"
            ).find(payload)
            # title = matches[0].value[1:-1] if matches else ""
            title = (
                helper._get_expression(matches[0].value, agg_type_map)
                if matches
                else ""
            )

            # SubTitle
            matches = parse(
                "$.visual.visualContainerObjects.subTitle[0].properties.text.expr"
            ).find(payload)
            # sub_title = matches[0].value[1:-1] if matches else ""
            sub_title = (
                helper._get_expression(matches[0].value, agg_type_map)
                if matches
                else ""
            )

            # Alt Text
            matches = parse(
                "$.visual.visualContainerObjects.general[0].properties.altText.expr"
            ).find(payload)
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

            show_all_data = find_show_all_with_jsonpath(payload)

            # Divider
            matches = parse(
                "$.visual.visualContainerObjects.divider[0].properties.show.expr.Literal.Value"
            ).find(payload)
            divider = matches[0] if matches else ""

            # Row/Column Subtotals
            if visual_type == "pivotTable":
                cst_matches = parse(
                    "$.visual.objects.subTotals[0].properties.columnSubtotals.expr.Literal.Value"
                ).find(payload)
                rst_matches = parse(
                    "$.visual.objects.subTotals[0].properties.rowSubtotals.expr.Literal.Value"
                ).find(payload)

                if cst_matches:
                    cst_value = False if cst_matches[0].value == "false" else True

                if rst_matches:
                    rst_value = False if rst_matches[0].value == "false" else True

            # Slicer Type
            if visual_type == "slicer":
                matches = parse(
                    "$.visual.objects.data[0].properties.mode.expr.Literal.Value"
                ).find(payload)
                slicer_type = matches[0].value[1:-1] if matches else "N/A"

            # Data Visual
            is_data_visual = contains_key(
                payload,
                [
                    "Aggregation",
                    "Column",
                    "Measure",
                    "HierarchyLevel",
                    "NativeVisualCalculation",
                ],
            )

            # Sparkline
            has_sparkline = contains_key(payload, ["SparklineData"])

            new_data = {
                "File Path": path,
                "Page Name": page_id,
                "Page Display Name": page_display,
                "Visual Name": payload.get("name"),
                "X": pos.get("x"),
                "Y": pos.get("y"),
                "Z": pos.get("z"),
                "Width": pos.get("width"),
                "Height": pos.get("height"),
                "Tab Order": pos.get("tabOrder"),
                "Hidden": payload.get("isHidden", False),
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
            dfs.append(pd.DataFrame(new_data, index=[0]))

        if dfs:
            df = pd.concat(dfs, ignore_index=True)

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

        _update_dataframe_datatypes(dataframe=df, column_map=columns)

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

        (page_mapping, visual_mapping) = self.visual_page_mapping()

        columns = {
            "Page Name": "str",
            "Page Display Name": "str",
            "Visual Name": "str",
            "Table Name": "str",
            "Object Name": "str",
            "Object Type": "str",
            "Implicit Measure": "bool",
            "Sparkline": "bool",
            "Visual Calc": "bool",
            "Format": "str",
            "Object Display Name": "str",
        }
        df = _create_dataframe(columns=columns)

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
                expression = data.get("Expression", {})
                source_ref = (
                    expression.get("SourceRef", {})
                    if isinstance(expression, dict)
                    else {}
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

        visuals = [
            o
            for o in self._report_definition.get("parts")
            if o.get("path").endswith("/visual.json")
        ]

        dfs = []
        for v in visuals:
            path = v.get("path")
            payload = v.get("payload")
            page_id = visual_mapping.get(path)[0]
            page_display = visual_mapping.get(path)[1]

            entity_property_pairs = find_entity_property_pairs(payload)
            query_state = (
                payload.get("visual", {}).get("query", {}).get("queryState", {})
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
                    "Visual Name": payload.get("name"),
                    "Table Name": table_name,
                    "Object Name": object_name,
                    "Object Type": properties[1],
                    "Implicit Measure": is_agg,
                    "Sparkline": properties[4],
                    "Visual Calc": properties[3],
                    "Format": format_value,
                    "Object Display Name": obj_display,
                }

                dfs.append(pd.DataFrame(new_data, index=[0]))

        if dfs:
            df = pd.concat(dfs, ignore_index=True)

        if extended:
            df = self._add_extended(dataframe=df)

        _update_dataframe_datatypes(dataframe=df, column_map=columns)

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

        columns = {
            "Table Name": "str",
            "Object Name": "str",
            "Object Type": "str",
            "Report Source": "str",
            "Report Source Object": "str",
        }

        df = _create_dataframe(columns=columns)
        rf = self.list_report_filters()
        pf = self.list_page_filters()
        vf = self.list_visual_filters()
        vo = self.list_visual_objects()

        rf_subset = rf[["Table Name", "Object Name", "Object Type"]].copy()
        rf_subset["Report Source"] = "Report Filter"
        rf_subset["Report Source Object"] = self._report_name

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
            (dataset_id, dataset_name, dataset_workspace_id, dataset_workspace_name) = (
                resolve_dataset_from_report(
                    report=self._report_id, workspace=self._workspace_id
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
                report=self._report_id, workspace=self._workspace_id
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

        columns = {
            "File Path": "str",
            "Bookmark Name": "str",
            "Bookmark Display Name": "str",
            "Page Name": "str",
            "Page Display Name": "str",
            "Visual Name": "str",
            "Visual Hidden": "bool",
        }
        df = _create_dataframe(columns=columns)

        bookmarks = [
            o
            for o in self._report_definition.get("parts")
            if o.get("path").endswith("/bookmark.json")
        ]

        dfs = []

        for b in bookmarks:
            path = b.get("path")
            payload = b.get("payload")

            bookmark_name = payload.get("name")
            bookmark_display = payload.get("displayName")
            rpt_page_id = payload.get("explorationState", {}).get("activeSection")
            page_id, page_display, file_path = helper.resolve_page_name(
                self, page_name=rpt_page_id
            )

            for rptPg in payload.get("explorationState", {}).get("sections", {}):
                for visual_name in (
                    payload.get("explorationState", {})
                    .get("sections", {})
                    .get(rptPg, {})
                    .get("visualContainers", {})
                ):
                    if (
                        payload.get("explorationState", {})
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
                    dfs.append(pd.DataFrame(new_data, index=[0]))

        if dfs:
            df = pd.concat(dfs, ignore_index=True)

        _update_dataframe_datatypes(dataframe=df, column_map=columns)

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

        columns = {
            "Measure Name": "str",
            "Table Name": "str",
            "Expression": "str",
            "Data Type": "str",
            "Format String": "str",
        }

        df = _create_dataframe(columns=columns)

        report_file = self.get(file_path="definition/reportExtensions.json")

        dfs = []
        if report_file:
            payload = report_file.get("payload")
            for e in payload.get("entities", []):
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
                    dfs.append(pd.DataFrame(new_data, index=[0]))

        if dfs:
            df = pd.concat(dfs, ignore_index=True)

        return df

    def _add_extended(self, dataframe):

        from sempy_labs.tom import connect_semantic_model

        (dataset_id, dataset_name, dataset_workspace_id, dataset_workspace_name) = (
            resolve_dataset_from_report(
                report=self._report_id, workspace=self._workspace_id
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

        report_file = self.get(file_path="definition/report.json")
        theme_collection = report_file.get("themeCollection", {})
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

        return self.get(file_path=theme_file_path)

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

        theme_version = "5.5.4"

        # Open file
        if not theme_file_path.endswith(".json"):
            raise ValueError(
                f"{icons.red_dot} The '{theme_file_path}' theme file path must be a .json file."
            )
        elif theme_file_path.startswith("https://"):
            response = requests.get(theme_file_path)
            theme_file = response.json()
        elif theme_file_path.startswith("/lakehouse"):
            with open(theme_file_path, "r", encoding="utf-8-sig") as file:
                theme_file = json.load(file)
        else:
            ValueError(
                f"{icons.red_dot} Incorrect theme file path value '{theme_file_path}'."
            )

        theme_name = theme_file.get("name")
        theme_name_full = f"{theme_name}.json"

        # Add theme.json file to request_body
        self.add(
            file_path=f"StaticResources/RegisteredResources/{theme_name_full}",
            payload=theme_file,
        )

        new_theme = {
            "name": theme_name_full,
            "path": theme_name_full,
            "type": "CustomTheme",
        }

        # Update the report.json file
        report_file_path = "definition/report.json"
        report_file = self.get(file_path=report_file_path)
        resource_type = "RegisteredResources"

        # Add to theme collection
        if "customTheme" not in report_file["themeCollection"]:
            report_file["themeCollection"]["customTheme"] = {
                "name": theme_name_full,
                "reportVersionAtImport": theme_version,
                "type": resource_type,
            }
        else:
            report_file["themeCollection"]["customTheme"]["name"] = theme_name_full
            report_file["themeCollection"]["customTheme"]["type"] = resource_type

        for package in report_file["resourcePackages"]:
            package["items"] = [
                item for item in package["items"] if item["type"] != "CustomTheme"
            ]

        if not any(
            package["name"] == resource_type
            for package in report_file["resourcePackages"]
        ):
            new_registered_resources = {
                "name": resource_type,
                "type": resource_type,
                "items": [new_theme],
            }
            report_file["resourcePackages"].append(new_registered_resources)
        else:
            names = [rp["name"] for rp in report_file["resourcePackages"][1]["items"]]

            if theme_name_full not in names:
                report_file["resourcePackages"][1]["items"].append(new_theme)

        self.update(file_path=report_file_path, payload=report_file)
        print(
            f"{icons.green_dot} The '{theme_name}' theme has been set as the theme for the '{self._report_name}' report within the '{self._workspace_name}' workspace."
        )

    def visual_page_mapping(self) -> Tuple[dict, dict]:

        page_mapping = {}
        visual_mapping = {}

        pages = [
            o
            for o in self._report_definition.get("parts")
            if o.get("path").endswith("/page.json")
        ]
        visuals = [
            o
            for o in self._report_definition.get("parts")
            if o.get("path").endswith("/visual.json")
        ]
        for p in pages:
            path = p.get("path")
            payload = p.get("payload")
            pattern_page = r"/pages/(.*?)/page.json"
            page_name = re.search(pattern_page, path).group(1)
            page_id = payload.get("name")
            page_display = payload.get("displayName")
            page_mapping[page_name] = (page_id, page_display)

        for v in visuals:
            path = v.get("path")
            payload = v.get("payload")
            pattern_page = r"/pages/(.*?)/visuals/"
            page_name = re.search(pattern_page, path).group(1)
            visual_mapping[path] = (
                page_mapping.get(page_name)[0],
                page_mapping.get(page_name)[1],
            )

        return page_mapping, visual_mapping

    def close(self):

        # Save the changes to the service if the connection is read/write and the report definition has changed
        if (
            not self._readonly
            and self._report_definition != self._current_report_definition
        ):
            self.save_changes()

            # self._report_definition = None


@log
@contextmanager
def connect_report(
    report: str | UUID,
    readonly: bool = True,
    workspace: Optional[str | UUID] = None,
):
    """
    Connects to the report.

    Parameters
    ----------
    report : str | uuid.UUID
        Name or ID of the report.
    readonly: bool, default=True
        Whether the connection is read-only or read/write. Setting this to False enables read/write which saves the changes made back to the server.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID. Also supports Azure Analysis Services (Service Principal Authentication required).
        If connecting to Azure Analysis Services, enter the workspace parameter in the following format: 'asazure://<region>.asazure.windows.net/<server_name>'.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    typing.Iterator[TOMWrapper]
        A connection to the semantic model's Tabular Object Model.
    """

    rw = ReportWrapper(
        report=report,
        workspace=workspace,
        readonly=readonly,
    )
    try:
        yield rw
    finally:
        rw.close()
