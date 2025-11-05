from typing import Optional, Tuple, List, Literal
from contextlib import contextmanager
from sempy._utils._log import log
from uuid import UUID
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_item_name_and_id,
    _base_api,
    _create_dataframe,
    _update_dataframe_datatypes,
    format_dax_object_name,
    resolve_dataset_from_report,
    generate_number_guid,
    decode_payload,
    is_base64,
    generate_hex,
    get_jsonpath_value,
    set_json_value,
    remove_json_value,
    get_tenant_id,
)
from sempy_labs._dictionary_diffs import (
    diff_parts,
)
import json
import sempy_labs._icons as icons
import copy
import pandas as pd
from jsonpath_ng.ext import parse
import sempy_labs.report._report_helper as helper
from .._model_dependencies import get_measure_dependencies
import requests
import re
import base64
from pathlib import Path
from urllib.parse import urlparse
import os
import fnmatch


class ReportWrapper:
    """
    Connects to a Power BI report and retrieves its definition.

    The ReportWrapper and all functions which depend on it require the report to be in the `PBIR <https://powerbi.microsoft.com/blog/power-bi-enhanced-report-format-pbir-in-power-bi-desktop-developer-mode-preview>`_ format.

    Parameters
    ----------
    report : str | uuid.UUID
        The name or ID of the report.
    workspace : str | uuid.UUID
        The name or ID of the workspace in which the report resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    readonly: bool, default=True
        Whether the connection is read-only or read/write. Setting this to False enables read/write which saves the changes made back to the server.
    show_diffs: bool, default=True
        Whether to show the differences between the current report definition in the service and the new report definition.

    Returns
    -------
    None
        A connection to the report is established and the report definition is retrieved.
    """

    _report_name: str
    _report_id: str
    _workspace_name: str
    _workspace_id: str
    _readonly: bool
    _report_file_path = "definition/report.json"
    _pages_file_path = "definition/pages/pages.json"
    _report_extensions_path = "definition/reportExtensions.json"

    # Visuals
    _title_path = (
        "$.visual.visualContainerObjects.title[*].properties.text.expr.Literal.Value"
    )
    _subtitle_path = (
        "$.visual.visualContainerObjects.subTitle[*].properties.text.expr.Literal.Value"
    )
    _visual_x_path = "$.position.x"
    _visual_y_path = "$.position.y"

    @log
    def __init__(
        self,
        report: str | UUID,
        workspace: Optional[str | UUID] = None,
        readonly: bool = True,
        show_diffs: bool = True,
    ):
        (self._workspace_name, self._workspace_id) = resolve_workspace_name_and_id(
            workspace
        )
        (self._report_name, self._report_id) = resolve_item_name_and_id(
            item=report, type="Report", workspace=self._workspace_id
        )
        self._readonly = readonly
        self._show_diffs = show_diffs

        result = _base_api(
            request=f"/v1/workspaces/{self._workspace_id}/items/{self._report_id}/getDefinition",
            method="post",
            status_codes=None,
            lro_return_json=True,
        )

        # def is_zip_file(data: bytes) -> bool:
        #    return data.startswith(b"PK\x03\x04")

        # Check that the report is in the PBIR format
        parts = result.get("definition", {}).get("parts", [])
        if self._report_file_path not in [p.get("path") for p in parts]:
            self.format = "PBIR-Legacy"
        else:
            self.format = "PBIR"
        self._report_definition = {"parts": []}
        for part in parts:
            path = part.get("path")
            payload = part.get("payload")

            # decoded_bytes = base64.b64decode(payload)
            # decoded_payload = json.loads(_decode_b64(payload))
            # try:
            #    decoded_payload = json.loads(base64.b64decode(payload).decode("utf-8"))
            # except Exception:
            #    decoded_payload = base64.b64decode(payload)
            decoded_payload = decode_payload(payload)

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

        # self.report = self.Report(self)

        helper.populate_custom_visual_display_names()

    def _ensure_pbir(self):

        if self.format != "PBIR":
            raise NotImplementedError(
                f"{icons.red_dot} This ReportWrapper function requires the report to be in the PBIR format."
                "See here for details: https://powerbi.microsoft.com/blog/power-bi-enhanced-report-format-pbir-in-power-bi-desktop-developer-mode-preview/"
            )

    # Basic functions
    def get(
        self,
        file_path: str,
        json_path: Optional[str] = None,
        verbose: bool = True,
    ) -> dict | List[Tuple[str, dict]]:
        """
        Get the json content of the specified report definition file.

        Parameters
        ----------
        file_path : str
            The path of the report definition file. For example: "definition/pages/pages.json". You may also use wildcards. For example: "definition/pages/*/page.json".
        json_path : str, default=None
            The json path to the specific part of the file to be retrieved. If None, the entire file content is returned.
        verbose : bool, default=True
            If True, prints messages about the retrieval process. If False, suppresses these messages.

        Returns
        -------
        dict | List[Tuple[str, dict]]
            The json content of the specified report definition file.
        """

        parts = self._report_definition.get("parts")

        # Find matching parts
        if "*" in file_path:
            results = []
            matching_parts = [
                (part.get("path"), part.get("payload"))
                for part in parts
                if fnmatch.fnmatch(part.get("path"), file_path)
            ]

            if not matching_parts:
                if verbose:
                    print(
                        f"{icons.red_dot} No files match the wildcard path '{file_path}'."
                    )
                return results

            results = []
            for path, payload in matching_parts:
                if not json_path:
                    results.append((path, payload))
                elif not isinstance(payload, dict):
                    raise ValueError(
                        f"{icons.red_dot} The payload of the file '{path}' is not a dictionary."
                    )
                else:
                    jsonpath_expr = parse(json_path)
                    matches = jsonpath_expr.find(payload)
                    if matches:
                        results.append((path, matches[0].value))
                    # else:
                    #    raise ValueError(
                    #        f"{icons.red_dot} No match found for '{json_path}' in '{path}'."
                    #    )
            if not results and verbose:
                print(
                    f"{icons.red_dot} No match found for '{json_path}' in any of the files matching the wildcard path '{file_path}'."
                )
            return results

        # Exact path match
        for part in parts:
            if part.get("path") == file_path:
                payload = part.get("payload")
                if not json_path:
                    return payload
                elif not isinstance(payload, dict):
                    raise ValueError(
                        f"{icons.red_dot} The payload of the file '{file_path}' is not a dictionary."
                    )
                else:
                    jsonpath_expr = parse(json_path)
                    matches = jsonpath_expr.find(payload)
                    if matches:
                        return matches[0].value
                    elif verbose:
                        print(f"{icons.red_dot} No match found for '{json_path}'.")

        if verbose:
            print(f"{icons.red_dot} File '{file_path}' not found in report definition.")

    def add(self, file_path: str, payload: dict | bytes):
        """
        Add a new file to the report definition.

        Parameters
        ----------
        file_path : str
            The path of the file to be added. For example: "definition/pages/pages.json".
        payload : dict | bytes
            The json content of the file to be added. This can be a dictionary or a base64 encoded string.
        """

        decoded_payload = decode_payload(payload)

        if file_path in self.list_paths().get("Path").values:
            raise ValueError(
                f"{icons.red_dot} Cannot add the '{file_path}' file as this file path already exists in the report definition."
            )

        self._report_definition["parts"].append(
            {"path": file_path, "payload": decoded_payload}
        )

    def remove(self, file_path: str, json_path: Optional[str] = None, verbose=True):
        """
        Removes a file from the report definition.

        Parameters
        ----------
        file_path : str
            The path of the file to be removed. For example: "definition/pages/fjdis323484/page.json".
        json_path : str, default=None
            The json path to the specific part of the file to be removed. If None, the entire file is removed. Wildcards are supported (i.e. "definition/pages/*/page.json").
        verbose : bool, default=True
            If True, prints messages about the removal process. If False, suppresses these messages.
        """

        parts = self._report_definition.get("parts")
        matching_parts = []

        if "*" in file_path:
            matching_parts = [
                part for part in parts if fnmatch.fnmatch(part.get("path"), file_path)
            ]
        else:
            matching_parts = [part for part in parts if part.get("path") == file_path]

        if not matching_parts:
            raise ValueError(
                f"{icons.red_dot} No file(s) found for path '{file_path}'."
            )

        for part in matching_parts:
            path = part.get("path")
            payload = part.get("payload")

            if not json_path:
                self._report_definition["parts"].remove(part)
                if verbose:
                    print(
                        f"{icons.green_dot} The file '{path}' has been removed from the report definition."
                    )
            else:
                remove_json_value(
                    path=path, payload=payload, json_path=json_path, verbose=verbose
                )

    def update(self, file_path: str, payload: dict | bytes):
        """
        Updates the payload of a file in the report definition.

        Parameters
        ----------
        file_path : str
            The path of the file to be updated. For example: "definition/pages/pages.json".
        payload : dict | bytes
            The new json content of the file to be updated. This can be a dictionary or a base64 encoded string.
        """

        decoded_payload = decode_payload(payload)

        for part in self._report_definition.get("parts"):
            if part.get("path") == file_path:
                part["payload"] = decoded_payload
                # if not self._readonly:
                #    print(
                #        f"The file '{file_path}' has been updated in the report definition."
                #    )
                return

        raise ValueError(
            f"The '{file_path}' file was not found in the report definition."
        )

    def set_json(self, file_path: str, json_path: str, json_value: str | dict | List):
        """
        Sets the JSON value of a file in the report definition. If the json_path does not exist, it will be created.

        Parameters
        ----------
        file_path : str
            The file path of the JSON file to be updated. For example: "definition/pages/ReportSection1/visuals/a1d8f99b81dcc2d59035/visual.json". Also supports wildcards.
        json_path : str
            The JSON path to the value to be updated or created. This must be a valid JSONPath expression.
            Examples:
                "$.objects.outspace"
                "$.hi.def[*].vv"
        json_value : str | dict | List
            The new value to be set at the specified JSON path. This can be a string, dictionary, or list.
        """

        files = self.get(file_path=file_path)

        if isinstance(files, dict):
            files = [(file_path, files)]

        for file in files:
            path = file[0]
            payload = file[1]
            new_payload = set_json_value(
                payload=payload, json_path=json_path, json_value=json_value
            )

            self.update(file_path=path, payload=new_payload)

    def list_paths(self) -> pd.DataFrame:
        """
        List all file paths in the report definition.

        Returns
        -------
        pandas.DataFrame
            A pandas dataframe containing a list of all paths in the report definition.
        """

        existing_paths = [
            part.get("path") for part in self._report_definition.get("parts")
        ]
        return pd.DataFrame(existing_paths, columns=["Path"])

    def __all_pages(self):

        self._ensure_pbir()

        return [
            o
            for o in self._report_definition.get("parts")
            if o.get("path").endswith("/page.json")
        ]

    def __all_visuals(self):

        self._ensure_pbir()

        return [
            o
            for o in self._report_definition.get("parts")
            if o.get("path").endswith("/visual.json")
        ]

    # Helper functions
    def __resolve_page_list(self, page: Optional[str | List[str]] = None) -> List[str]:

        if isinstance(page, str):
            page = [page]

        # Resolve page list
        return (
            [self.resolve_page_name(p) for p in page]
            if page
            else [
                p["payload"]["name"]
                for p in self.__all_pages()
                if "payload" in p and "name" in p["payload"]
            ]
        )

    def _get_url(
        self, page_name: Optional[str] = None, visual_name: Optional[str] = None
    ) -> str:
        """
        Gets the URL of the report. If specified, gets the URL of the specified page.

        Parameters
        ----------
        page_name : str, default=None
            The name of the page. If None, gets the URL of the report.
            If specified, gets the URL of the specified page.

        Returns
        -------
        str
            The URL of the report or the specified page.
        """

        url = f"https://app.powerbi.com/groups/{self._workspace_id}/reports/{self._report_id}"

        if page_name:
            if page_name in [page["payload"]["name"] for page in self.__all_pages()]:
                pass
            else:
                page_name = self.resolve_page_name(page_name)
            url += f"/{page_name}"

            if visual_name:
                tenant_id = get_tenant_id()
                url += f"?ctid={tenant_id}&pbi_source=shareVisual&visual={visual_name}"

        return url

    def __resolve_page_name_and_display_name_file_path(
        self, page: str
    ) -> Tuple[str, str, str]:

        self._ensure_pbir()
        page_map = {
            p["path"]: [p["payload"]["name"], p["payload"]["displayName"]]
            for p in self._report_definition.get("parts", [])
            if p.get("path", "").endswith("/page.json") and "payload" in p
        }

        # Build lookup: page_id → (path, display_name)
        id_lookup = {v[0]: (k, v[1]) for k, v in page_map.items()}

        # Build lookup: display_name → (path, page_id)
        name_lookup = {v[1]: (k, v[0]) for k, v in page_map.items()}

        if page in id_lookup:
            path, display_name = id_lookup[page]
            return path, page, display_name
        elif page in name_lookup:
            path, page_id = name_lookup[page]
            return path, page_id, page
        else:
            raise ValueError(
                f"{icons.red_dot} Invalid page display name. The '{page}' page does not exist in the '{self._report_name}' report within the '{self._workspace_name}' workspace."
            )

    def _resolve_page_name_and_display_name(self, page: str) -> Tuple[str, str]:
        """
        Obtains the page name, page display name for a given page in a report.

        Parameters
        ----------
        page : str
            The page name or display name.

        Returns
        -------
        Tuple[str, str]
            The page name and display name.
        """

        (_, page_id, page_name) = self.__resolve_page_name_and_display_name_file_path(
            page
        )

        return (page_id, page_name)

    def resolve_page_name(self, page_display_name: str) -> str:
        """
        Obtains the page name, page display name, and the file path for a given page in a report.

        Parameters
        ----------
        page_display_name : str
            The display name of the page of the report.

        Returns
        -------
        str
            The page name.
        """

        (path, page_id, page_name) = (
            self.__resolve_page_name_and_display_name_file_path(page_display_name)
        )
        return page_id

    def resolve_page_display_name(self, page_name: str) -> str:
        """
        Obtains the page dispaly name.

        Parameters
        ----------
        page_name : str
            The name of the page of the report.

        Returns
        -------
        str
            The page display name.
        """

        (path, page_id, page_name) = (
            self.__resolve_page_name_and_display_name_file_path(page_name)
        )
        return page_name

    def __add_to_registered_resources(self, name: str, path: str, type: str):

        type = type.capitalize()

        report_file = self.get(file_path=self._report_file_path)
        rp_names = [rp.get("name") for rp in report_file.get("resourcePackages")]

        new_item = {"name": name, "path": path, "type": type}
        if "RegisteredResources" not in rp_names:
            res = {
                "name": "RegisteredResources",
                "type": "RegisteredResources",
                "items": [new_item],
            }
            report_file.get("resourcePackages").append(res)
        else:
            for rp in report_file.get("resourcePackages"):
                if rp.get("name") == "RegisteredResources":
                    for item in rp.get("items"):
                        item_name = item.get("name")
                        item_type = item.get("type")
                        item_path = item.get("path")
                        if (
                            item_name == name
                            and item_type == type
                            and item_path == path
                        ):
                            print(
                                f"{icons.info} The '{item_name}' {type.lower()} already exists in the report definition."
                            )
                            raise ValueError()

                    # Add the new item to the existing RegisteredResources
                    rp["items"].append(new_item)

        self.update(file_path=self._report_file_path, payload=report_file)

    def _add_extended(self, dataframe):

        from sempy_labs.tom import connect_semantic_model

        dataset_id, dataset_name, dataset_workspace_id, dataset_workspace_name = (
            resolve_dataset_from_report(
                report=self._report_id, workspace=self._workspace_id
            )
        )

        report_level_measures = list(
            self.list_report_level_measures()["Measure Name"].values
        )
        with connect_semantic_model(
            dataset=dataset_id, readonly=True, workspace=dataset_workspace_id
        ) as tom:
            measure_names = {m.Name for m in tom.all_measures()}
            measure_names.update(report_level_measures)
            column_names = {
                format_dax_object_name(c.Parent.Name, c.Name) for c in tom.all_columns()
            }
            hierarchy_names = {
                format_dax_object_name(h.Parent.Name, h.Name)
                for h in tom.all_hierarchies()
            }

        # Vectorized checks
        def is_valid(row):
            obj_type = row["Object Type"]
            obj_name = row["Object Name"]
            if obj_type == "Measure":
                return obj_name in measure_names
            elif obj_type == "Column":
                return (
                    format_dax_object_name(row["Table Name"], obj_name) in column_names
                )
            elif obj_type == "Hierarchy":
                return (
                    format_dax_object_name(row["Table Name"], obj_name)
                    in hierarchy_names
                )
            return False

        dataframe["Valid Semantic Model Object"] = dataframe.apply(is_valid, axis=1)
        return dataframe

    def _visual_page_mapping(self) -> dict:
        self._ensure_pbir()

        page_mapping = {}
        visual_mapping = {}

        for p in self.__all_pages():
            path = p.get("path")
            payload = p.get("payload")
            pattern_page = r"/pages/(.*?)/page.json"
            page_name = re.search(pattern_page, path).group(1)
            page_id = payload.get("name")
            page_display = payload.get("displayName")
            page_mapping[page_name] = (page_id, page_display)

        for v in self.__all_visuals():
            path = v.get("path")
            payload = v.get("payload")
            pattern_page = r"/pages/(.*?)/visuals/"
            page_name = re.search(pattern_page, path).group(1)
            visual_mapping[path] = (
                page_mapping.get(page_name)[0],
                page_mapping.get(page_name)[1],
            )

        return visual_mapping

    # List functions
    def list_custom_visuals(self) -> pd.DataFrame:
        """
        Shows a list of all custom visuals used in the report.

        Returns
        -------
        pandas.DataFrame
            A pandas dataframe containing a list of all the custom visuals used in the report.
        """
        self._ensure_pbir()

        columns = {
            "Custom Visual Name": "str",
            "Custom Visual Display Name": "str",
            "Is Public": "bool",
            "Used in Report": "bool",
        }

        df = _create_dataframe(columns=columns)

        visuals = []
        rp = self.get(
            file_path=self._report_file_path,
            json_path="$.resourcePackages",
            verbose=False,
        )

        if rp:
            visuals += [
                {"Custom Visual Name": item.get("name"), "Is Public": False}
                for item in rp
                if item.get("type") == "CustomVisual"
            ]

        # Load public custom visuals
        public_custom_visuals = (
            self.get(
                file_path=self._report_file_path,
                json_path="$.publicCustomVisuals",
                verbose=False,
            )
            or []
        )

        visuals += [
            {
                "Custom Visual Name": (
                    item.get("name") if isinstance(item, dict) else item
                ),
                "Is Public": True,
            }
            for item in public_custom_visuals
        ]

        if visuals:
            df = pd.DataFrame(visuals, columns=list(columns.keys()))

            # df["Custom Visual Name"] = report_file.get("publicCustomVisuals")
            df["Custom Visual Display Name"] = df["Custom Visual Name"].apply(
                lambda x: helper.vis_type_mapping.get(x, x)
            )

            visual_types = set()
            for v in self.__all_visuals():
                payload = v.get("payload", {})
                visual = payload.get("visual", {})
                visual_type = visual.get("visualType")
                if visual_type:
                    visual_types.add(visual_type)

            df["Used in Report"] = df["Custom Visual Name"].isin(visual_types)

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

        self._ensure_pbir()

        report_file = self.get(file_path=self._report_file_path)

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

        rows = []

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
                    rows.append(
                        {
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
                    )

        if rows:
            df = pd.DataFrame(rows, columns=list(columns.keys()))
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
        self._ensure_pbir()

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

        rows = []
        for p in self.__all_pages():
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
                        rows.append(
                            {
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
                                "Page URL": self._get_url(page_name=page_id),
                            }
                        )

        if rows:
            df = pd.DataFrame(rows, columns=list(columns.keys()))
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
        self._ensure_pbir()

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

        visual_mapping = self._visual_page_mapping()

        rows = []
        for v in self.__all_visuals():
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
                        rows.append(
                            {
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
                        )

        if rows:
            df = pd.DataFrame(rows, columns=list(columns.keys()))
            _update_dataframe_datatypes(dataframe=df, column_map=columns)

        if extended:
            df = self._add_extended(dataframe=df)

        return df

    def list_visual_interactions(self) -> pd.DataFrame:
        """
        Shows a list of all modified `visual interactions <https://learn.microsoft.com/power-bi/create-reports/service-reports-visual-interactions?tabs=powerbi-desktop>`_ used in the report.

        Returns
        -------
        pandas.DataFrame
            A pandas dataframe containing a list of all modified visual interactions used in the report.
        """
        self._ensure_pbir()

        columns = {
            "Page Name": "str",
            "Page Display Name": "str",
            "Source Visual Name": "str",
            "Target Visual Name": "str",
            "Type": "str",
        }
        df = _create_dataframe(columns=columns)

        rows = []
        for p in self.__all_pages():
            payload = p.get("payload")
            page_name = payload.get("name")
            page_display = payload.get("displayName")

            for vizInt in payload.get("visualInteractions", []):
                sourceVisual = vizInt.get("source")
                targetVisual = vizInt.get("target")
                vizIntType = vizInt.get("type")

                rows.append(
                    {
                        "Page Name": page_name,
                        "Page Display Name": page_display,
                        "Source Visual Name": sourceVisual,
                        "Target Visual Name": targetVisual,
                        "Type": vizIntType,
                    }
                )

        if rows:
            df = pd.DataFrame(rows, columns=list(columns.keys()))

        return df

    def list_visual_calculations(self) -> pd.DataFrame:
        """
        Shows a list of all `visual calculations <https://learn.microsoft.com/power-bi/transform-model/desktop-visual-calculations-overview>`_.

        Returns
        -------
        pandas.DataFrame
            A pandas dataframe containing a list of all visual calculations within the report.
        """

        self._ensure_pbir()

        columns = {
            "Page Display Name": "str",
            "Visual Name": "str",
            "Name": "str",
            "Language": "str",
            "Expression": "str",
        }

        df = _create_dataframe(columns=columns)
        visual_mapping = self._visual_page_mapping()

        rows = []
        for v in self.__all_visuals():
            path = v.get("path")
            payload = v.get("payload")
            page_name = visual_mapping.get(path)[0]
            page_display_name = visual_mapping.get(path)[1]
            visual_name = payload.get("name")
            matches = parse("$..field.NativeVisualCalculation").find(payload)
            if matches:
                for match in matches:
                    m = match.value
                    rows.append(
                        {
                            "Page Display Name": page_display_name,
                            "Page Name": page_name,
                            "Visual Name": visual_name,
                            "Name": m.get("Name"),
                            "Language": m.get("Language"),
                            "Expression": m.get("Expression"),
                        }
                    )

        if rows:
            df = pd.DataFrame(rows, columns=list(columns.keys()))

        return df

    def list_pages(self) -> pd.DataFrame:
        """
        Shows a list of all pages in the report.

        Returns
        -------
        pandas.DataFrame
            A pandas dataframe containing a list of all pages in the report.
        """
        self._ensure_pbir()

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

        page = self.get(file_path=self._pages_file_path)
        active_page = page.get("activePageName")

        dfV = self.list_visuals()

        rows = []
        for p in self.__all_pages():
            file_path = p.get("path")
            page_prefix = file_path[0:-9]
            payload = p.get("payload")
            page_name = payload.get("name")
            height = payload.get("height")
            width = payload.get("width")

            # Alignment
            alignment_value = get_jsonpath_value(
                data=payload,
                path="$.objects.displayArea[*].properties.verticalAlignment.expr.Literal.Value",
                default="Top",
                remove_quotes=True,
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
            page_filter_count = len(
                get_jsonpath_value(
                    data=payload, path="$.filterConfig.filters", default=[]
                )
            )

            # Hidden
            matches = parse("$.visibility").find(payload)
            is_hidden = any(match.value == "HiddenInViewMode" for match in matches)

            rows.append(
                {
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
                    "Page URL": self._get_url(page_name=page_name),
                }
            )

        if rows:
            df = pd.DataFrame(rows, columns=list(columns.keys()))
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
        self._ensure_pbir()

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
            "URL": "str",
        }
        df = _create_dataframe(columns=columns)

        report_file = self.get(file_path=self._report_file_path)
        custom_visuals = report_file.get("publicCustomVisuals", [])
        visual_mapping = self._visual_page_mapping()
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

        rows = []
        for v in self.__all_visuals():
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
            title = (
                helper._get_expression(matches[0].value, agg_type_map)
                if matches
                else ""
            )

            # SubTitle
            matches = parse(
                "$.visual.visualContainerObjects.subTitle[0].properties.text.expr"
            ).find(payload)
            sub_title = (
                helper._get_expression(matches[0].value, agg_type_map)
                if matches
                else ""
            )

            # Alt Text
            matches = parse(
                "$.visual.visualContainerObjects.general[0].properties.altText.expr"
            ).find(payload)
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
            visual_name = payload.get("name")

            rows.append(
                {
                    "File Path": path,
                    "Page Name": page_id,
                    "Page Display Name": page_display,
                    "Visual Name": visual_name,
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
                    "URL": self._get_url(page_name=page_id, visual_name=visual_name),
                }
            )

        if rows:
            df = pd.DataFrame(rows, columns=list(columns.keys()))

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
        self._ensure_pbir()

        visual_mapping = self._visual_page_mapping()

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
                    is_agg = len(keys_path) > 2 and keys_path[-3] == "Aggregation"
                    is_viz_calc = (
                        len(keys_path) > 2
                        and keys_path[-3] == "NativeVisualCalculation"
                    )
                    is_sparkline = (
                        len(keys_path) > 2 and keys_path[-3] == "SparklineData"
                    )

                    result[property_value] = (
                        entity,
                        object_type,
                        is_agg,
                        is_viz_calc,
                        is_sparkline,
                    )

                # Recursively search the rest of the dictionary
                for key, value in data.items():
                    find_entity_property_pairs(value, result, keys_path + [key])

            elif isinstance(data, list):
                for item in data:
                    find_entity_property_pairs(item, result, keys_path)

            return result

        rows = []
        for v in self.__all_visuals():
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
                rows.append(
                    {
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
                )

        if rows:
            df = pd.DataFrame(rows, columns=list(columns.keys()))
            _update_dataframe_datatypes(dataframe=df, column_map=columns)

        if extended:
            df = self._add_extended(dataframe=df)

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
        self._ensure_pbir()

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
        self._ensure_pbir()

        columns = {
            "File Path": "str",
            "Bookmark Name": "str",
            "Bookmark Display Name": "str",
            "Page Name": "str",
            "Page Display Name": "str",
            "Visual Name": "str",
            "Visual Hidden": "bool",
            "Suppress Data": "bool",
            "Current Page Selected": "bool",
            "Apply Visual Display State": "bool",
            "Apply To All Visuals": "bool",
        }
        df = _create_dataframe(columns=columns)

        bookmarks = [
            o
            for o in self._report_definition.get("parts")
            if o.get("path").endswith(".bookmark.json")
        ]

        rows = []
        for b in bookmarks:
            path = b.get("path")
            payload = b.get("payload")

            bookmark_name = payload.get("name")
            bookmark_display = payload.get("displayName")
            rpt_page_id = payload.get("explorationState", {}).get("activeSection")
            suppress_data = payload.get("options", {}).get("suppressData", False)
            suppress_active_section = payload.get("options", {}).get(
                "suppressActiveSection", False
            )
            suppress_display = payload.get("options", {}).get("suppressDisplay", False)
            apply_only_to_target_visuals = payload.get("options", {}).get(
                "applyOnlyToTargetVisuals", False
            )
            (page_id, page_display) = self._resolve_page_name_and_display_name(
                rpt_page_id
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

                    rows.append(
                        {
                            "File Path": path,
                            "Bookmark Name": bookmark_name,
                            "Bookmark Display Name": bookmark_display,
                            "Page Name": page_id,
                            "Page Display Name": page_display,
                            "Visual Name": visual_name,
                            "Visual Hidden": visual_hidden,
                            "Suppress Data": suppress_data,
                            "Current Page Selected": not suppress_active_section,
                            "Apply Visual Display State": not suppress_display,
                            "Apply To All Visuals": not apply_only_to_target_visuals,
                        }
                    )

        if rows:
            df = pd.DataFrame(rows, columns=list(columns.keys()))
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

        self._ensure_pbir()

        columns = {
            "Measure Name": "str",
            "Table Name": "str",
            "Expression": "str",
            "Data Type": "str",
            "Format String": "str",
            "Data Category": "str",
        }

        df = _create_dataframe(columns=columns)

        # If no report extensions path, return empty DataFrame
        if self._report_extensions_path not in self.list_paths()["Path"].values:
            return df

        report_file = self.get(file_path=self._report_extensions_path)

        rows = []
        for e in report_file.get("entities", []):
            table_name = e.get("name")
            for m in e.get("measures", []):
                measure_name = m.get("name")
                expr = m.get("expression")
                data_type = m.get("dataType")
                format_string = m.get("formatString")
                data_category = m.get("dataCategory")

                rows.append(
                    {
                        "Measure Name": measure_name,
                        "Table Name": table_name,
                        "Expression": expr,
                        "Data Type": data_type,
                        "Format String": format_string,
                        "Data Category": data_category,
                    }
                )

        if rows:
            df = pd.DataFrame(rows, columns=list(columns.keys()))

        return df

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

        self._ensure_pbir()

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

        report_file = self.get(file_path=self._report_file_path)
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

    # Action functions
    def set_theme(
        self, theme_file_path: Optional[str] = None, theme_json: Optional[dict] = None
    ):
        """
        Sets a custom theme for a report based on a theme .json file.

        Parameters
        ----------
        theme_file_path : str, default=None
            The file path of the theme.json file. This can either be from a Fabric lakehouse or from the web.
            Example for lakehouse: file_path = '/lakehouse/default/Files/CY23SU09.json'
            Example for web url: file_path = 'https://raw.githubusercontent.com/PowerBiDevCamp/FabricUserApiDemo/main/FabricUserApiDemo/DefinitionTemplates/Shared/Reports/StaticResources/SharedResources/BaseThemes/CY23SU08.json'
        theme_json : dict, default=None
            The theme file in .json format. Must specify either the theme_file_path or the theme_json.
        """

        if theme_file_path and theme_json:
            raise ValueError(
                f"{icons.red_dot} Please specify either the 'theme_file_path' or the 'theme_json' parameter, not both."
            )
        if not theme_file_path and not theme_json:
            raise ValueError(
                f"{icons.red_dot} Please specify either the 'theme_file_path' or the 'theme_json' parameter."
            )

        self._ensure_pbir()

        # Extract theme_json from theme_file_path
        if theme_file_path:
            # Open file
            if not theme_file_path.endswith(".json"):
                raise ValueError(
                    f"{icons.red_dot} The '{theme_file_path}' theme file path must be a .json file."
                )
            elif theme_file_path.startswith("https://"):
                response = requests.get(theme_file_path)
                theme_json = response.json()
            elif theme_file_path.startswith("/lakehouse") or theme_file_path.startswith(
                "/synfs/"
            ):
                with open(theme_file_path, "r", encoding="utf-8-sig") as file:
                    theme_json = json.load(file)
            else:
                ValueError(
                    f"{icons.red_dot} Incorrect theme file path value '{theme_file_path}'."
                )

        theme_name = theme_json.get("name")
        theme_name_full = f"{theme_name}.json"

        # Add theme.json file
        try:
            self.add(
                file_path=f"StaticResources/RegisteredResources/{theme_name_full}",
                payload=theme_json,
            )
        except Exception:
            self.update(
                file_path=f"StaticResources/RegisteredResources/{theme_name_full}",
                payload=theme_json,
            )

        rpt_version_at_import = self.get(
            file_path=self._report_file_path,
            json_path="$.themeCollection.baseTheme.reportVersionAtImport",
        )

        custom_theme = {
            "name": theme_name_full,
            "reportVersionAtImport": rpt_version_at_import,
            "type": "RegisteredResources",
        }

        self.set_json(
            file_path=self._report_file_path,
            json_path="$.themeCollection.customTheme",
            json_value=custom_theme,
        )

        # Update
        report_file = self.get(
            file_path=self._report_file_path, json_path="$.resourcePackages"
        )
        new_item = {
            "name": theme_name_full,
            "path": theme_name_full,
            "type": "CustomTheme",
        }
        # Find or create RegisteredResources
        registered = next(
            (res for res in report_file if res["name"] == "RegisteredResources"), None
        )

        if not registered:
            registered = {
                "name": "RegisteredResources",
                "type": "RegisteredResources",
                "items": [new_item],
            }
            report_file.append(registered)
        else:
            # Check for duplicate by 'name'
            if all(item["name"] != new_item["name"] for item in registered["items"]):
                registered["items"].append(new_item)

        self.set_json(
            file_path=self._report_file_path,
            json_path="$.resourcePackages",
            json_value=report_file,
        )

        if not self._readonly:
            print(
                f"{icons.green_dot} The '{theme_name}' theme has been set as the theme for the '{self._report_name}' report within the '{self._workspace_name}' workspace."
            )

    def set_active_page(self, page_name: str):
        """
        Sets the active page (first page displayed when opening a report) for a report.

        Parameters
        ----------
        page_name : str
            The page name or page display name of the report.
        """
        self._ensure_pbir()

        (page_id, page_display_name) = self._resolve_page_name_and_display_name(
            page_name
        )
        self.set_json(
            file_path=self._pages_file_path,
            json_path="$.activePageName",
            json_value=page_id,
        )

        if not self._readonly:
            print(
                f"{icons.green_dot} The '{page_display_name}' page has been set as the active page in the '{self._report_name}' report within the '{self._workspace_name}' workspace."
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
        self._ensure_pbir()

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

        (file_path, page_id, page_display_name) = (
            self.__resolve_page_name_and_display_name_file_path(page_name)
        )

        self.set_json(file_path=file_path, json_path="$.width", json_value=width)
        self.set_json(file_path=file_path, json_path="$.height", json_value=height)

        if not self._readonly:
            print(
                f"{icons.green_dot} The '{page_display_name}' page has been updated to the '{page_type}' page type."
            )

    # def set_page_vertical_alignment(self, page: str, vertical_alignment: Literal["Top", "Middle"] = "Top"):

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
        self._ensure_pbir()
        (file_path, page_id, page_display_name) = (
            self.__resolve_page_name_and_display_name_file_path(page_name)
        )

        if hidden:
            self.set_json(
                file_path=file_path,
                json_path="$.visibility",
                json_value="HiddenInViewMode",
            )
        else:
            self.remove(file_path=file_path, json_path="$.visibility", verbose=False)

        visibility = "visible" if hidden is False else "hidden"

        if not self._readonly:
            print(
                f"{icons.green_dot} The '{page_display_name}' page has been set to '{visibility}' in the '{self._report_name}' report within the '{self._workspace_name}' workspace."
            )

    def hide_tooltip_drillthrough_pages(self):
        """
        Hides all tooltip pages and drillthrough pages in a report.
        """

        dfP = self.list_pages()
        dfP_filt = dfP[
            (dfP["Type"] == "Tooltip") | (dfP["Drillthrough Target Page"] == True)
        ]

        if dfP_filt.empty:
            print(
                f"{icons.green_dot} There are no Tooltip or Drillthrough pages in the '{self._report_name}' report within the '{self._workspace_name}' workspace."
            )
            return

        for _, r in dfP_filt.iterrows():
            page_name = r["Page Name"]
            self.set_page_visibility(page_name=page_name, hidden=True)

    def disable_show_items_with_no_data(self):
        """
        Disables the `show items with no data <https://learn.microsoft.com/power-bi/create-reports/desktop-show-items-no-data>`_ property in all visuals within the report.
        """

        self.remove(
            file_path="definition/pages/*/visual.json",
            json_path="$..showAll",
            verbose=False,
        )

        if not self._readonly:
            print(
                f"{icons.green_dot} Show items with data has been disabled for all visuals in the '{self._report_name}' report within the '{self._workspace_name}' workspace."
            )

    def remove_unnecessary_custom_visuals(self):
        """
        Removes any custom visuals within the report that are not used in the report.
        """

        dfCV = self.list_custom_visuals()
        df = dfCV[dfCV["Used in Report"] == False]

        if not df.empty:
            cv_remove = df["Custom Visual Name"].values
            cv_remove_display = df["Custom Visual Display Name"].values
        else:
            print(
                f"{icons.red_dot} There are no unnecessary custom visuals in the '{self._report_name}' report within the '{self._workspace_name}' workspace."
            )
            return

        json_path = "$.publicCustomVisuals"
        custom_visuals = self.get(file_path=self._report_file_path, json_path=json_path)
        updated_custom_visuals = [
            item for item in custom_visuals if item not in cv_remove
        ]
        self.set_json(
            file_path=self._report_file_path,
            json_path=json_path,
            json_value=updated_custom_visuals,
        )

        if not self._readonly:
            print(
                f"{icons.green_dot} The {cv_remove_display} custom visuals have been removed from the '{self._report_name}' report within the '{self._workspace_name}' workspace."
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
        self._ensure_pbir()

        from sempy_labs.tom import connect_semantic_model

        rlm = self.list_report_level_measures()
        if rlm.empty:
            print(
                f"{icons.info} The '{self._report_name}' report within the '{self._workspace_name}' workspace has no report-level measures."
            )
            return

        dataset_id, dataset_name, dataset_workspace_id, dataset_workspace_name = (
            resolve_dataset_from_report(
                report=self._report_id, workspace=self._workspace_id
            )
        )

        if isinstance(measures, str):
            measures = [measures]

        entities = self.get(
            file_path=self._report_extensions_path, json_path="$.entities"
        )
        with connect_semantic_model(
            dataset=dataset_id, readonly=self._readonly, workspace=dataset_workspace_id
        ) as tom:
            existing_measures = [m.Name for m in tom.all_measures()]
            # Add measure to semantic model
            for _, r in rlm.iterrows():
                table_name = r["Table Name"]
                measure_name = r["Measure Name"]
                expr = r["Expression"]
                # mDataType = r["Data Type"]
                format_string = r["Format String"]
                # Add measures to the model
                if (
                    measures is None or measure_name in measures
                ) and measure_name not in existing_measures:
                    tom.add_measure(
                        table_name=table_name,
                        measure_name=measure_name,
                        expression=expr,
                        format_string=format_string,
                    )
                    tom.set_annotation(
                        object=tom.model.Tables[table_name].Measures[measure_name],
                        name="semanticlinklabs",
                        value="reportlevelmeasure",
                    )

                    for entity in entities:
                        if entity.get("name") == table_name:
                            entity["measures"] = [
                                m
                                for m in entity.get("measures", [])
                                if m.get("name") != measure_name
                            ]
                    entities = [e for e in entities if e.get("measures")]
                    self.set_json(
                        file_path=self._report_extensions_path,
                        json_path="$.entities",
                        json_value=entities,
                    )
            if not entities:
                self.remove(
                    file_path=self._report_extensions_path,
                    verbose=False,
                )

        if not self._readonly:
            print(
                f"{icons.green_dot} The report-level measures have been migrated to the '{dataset_name}' semantic model within the '{dataset_workspace_name}' workspace."
            )

    # In progress...
    def _list_annotations(self) -> pd.DataFrame:
        """
        Shows a list of annotations in the report.

        Returns
        -------
        pandas.DataFrame
            A pandas dataframe showing a list of report, page and visual annotations in the report.
        """

        columns = {
            "Type": "str",
            "Object Name": "str",
            "Annotation Name": "str",
            "Annotation Value": "str",
        }
        df = _create_dataframe(columns=columns)

        visual_mapping = self._visual_page_mapping()
        report_file = self.get(file_path="definition/report.json")

        rows = []
        if "annotations" in report_file:
            for ann in report_file["annotations"]:
                rows.append(
                    {
                        "Type": "Report",
                        "Object Name": self._report_name,
                        "Annotation Name": ann.get("name"),
                        "Annotation Value": ann.get("value"),
                    }
                )

        for p in self.__all_pages():
            path = p.get("path")
            payload = p.get("payload")
            page_name = payload.get("displayName")
            if "annotations" in payload:
                for ann in payload["annotations"]:
                    rows.append(
                        {
                            "Type": "Page",
                            "Object Name": page_name,
                            "Annotation Name": ann.get("name"),
                            "Annotation Value": ann.get("value"),
                        }
                    )

        for v in self.__all_visuals():
            path = v.get("path")
            payload = v.get("payload")
            page_display = visual_mapping.get(path)[1]
            visual_name = payload.get("name")
            if "annotations" in payload:
                for ann in payload["annotations"]:
                    rows.append(
                        {
                            "Type": "Visual",
                            "Object Name": f"'{page_display}'[{visual_name}]",
                            "Annotation Name": ann.get("name"),
                            "Annotation Value": ann.get("value"),
                        }
                    )

        if rows:
            df = pd.DataFrame(rows, columns=list(columns.keys()))

        return df

    def _add_image(self, image_path: str, resource_name: Optional[str] = None) -> str:
        """
        Add an image to the report definition. The image will be added to the StaticResources/RegisteredResources folder in the report definition. If the image_name already exists as a file in the report definition it will be updated.

        Parameters
        ----------
        image_path : str
            The path of the image file to be added. For example: "./builtin/MyImage.png".
        resource_name : str, default=None
            The name of the image file to be added. For example: "MyImage.png". If not specified, the name will be derived from the image path and a unique ID will be appended to it.

        Returns
        -------
        str
            The name of the image file added to the report definition.
        """
        self._ensure_pbir()

        id = generate_number_guid()

        if image_path.startswith("http://") or image_path.startswith("https://"):
            response = requests.get(image_path)
            response.raise_for_status()
            image_bytes = response.content
            # Extract the suffix (extension) from the URL path
            suffix = Path(urlparse(image_path).path).suffix
        else:
            with open(image_path, "rb") as image_file:
                image_bytes = image_file.read()
            suffix = Path(image_path).suffix

        payload = base64.b64encode(image_bytes).decode("utf-8")
        if resource_name is None:
            resource_name = os.path.splitext(os.path.basename(image_path))[0]
            file_name = f"{resource_name}{id}{suffix}"
        else:
            file_name = resource_name
        file_path = f"StaticResources/RegisteredResources/{file_name}"

        # Add StaticResources/RegisteredResources file. If the file already exists, update it.
        try:
            self.get(file_path=file_path)
            self.update(file_path=file_path, payload=payload)
        except Exception:
            self.add(
                file_path=file_path,
                payload=payload,
            )

        # Add to report.json file
        self.__add_to_registered_resources(
            name=file_name,
            path=file_name,
            type="Image",
        )

        return file_name

    def _remove_wallpaper(self, page: Optional[str | List[str]] = None):
        """
        Remove the wallpaper image from a page.

        Parameters
        ----------
        page : str | List[str], default=None
            The name or display name of the page(s) from which the wallpaper image will be removed.
            If None, removes from all pages.
        """
        self._ensure_pbir()

        if isinstance(page, str):
            page = [page]

        page_list = []
        if page:
            for p in page:
                page_id = self.resolve_page_name(p)
                page_list.append(page_id)
        else:
            page_list = [
                p.get("payload", {}).get("name")
                for p in self.__all_pages()
                if p.get("payload") and "name" in p["payload"]
            ]

        for p in self.__all_pages():
            path = p.get("path")
            payload = p.get("payload")
            page_name = payload.get("name")
            page_display_name = payload.get("displayName")
            if page_name in page_list:
                self.remove(file_path=path, json_path="$.objects.outspace")
                print(
                    f"{icons.green_dot} The wallpaper has been removed from the '{page_display_name}' page."
                )

    def _set_wallpaper_color(
        self,
        color_value: str,
        page: Optional[str | List[str]] = None,
        transparency: int = 0,
        theme_color_percent: float = 0.0,
    ):
        """
        Set the wallpaper color of a page (or pages).

        Parameters
        ----------
        color_value : str
            The color value to be set. This can be a hex color code (e.g., "#FF5733") or an integer based on the theme color.
        page : str | List[str], default=None
            The name or display name of the page(s) to which the wallpaper color will be applied.
            If None, applies to all pages.
        transparency : int, default=0
            The transparency level of the wallpaper color. Valid values are between 0 and 100.
        theme_color_percent : float, default=0.0
            The percentage of the theme color to be applied. Valid values are between -0.6 and 0.6.
        """
        self._ensure_pbir()

        if transparency < 0 or transparency > 100:
            raise ValueError(f"{icons.red_dot} Transparency must be between 0 and 100.")

        if theme_color_percent < -0.6 or theme_color_percent > 0.6:
            raise ValueError(
                f"{icons.red_dot} Theme color percentage must be between -0.6 and 0.6."
            )

        page_list = self.__resolve_page_list(page)

        # Define the color dictionary based on color_value type
        if isinstance(color_value, int):
            color_expr = {
                "ThemeDataColor": {
                    "ColorId": color_value,
                    "Percent": theme_color_percent,
                }
            }
        elif isinstance(color_value, str) and color_value.startswith("#"):
            color_expr = {"Literal": {"Value": f"'{color_value}'"}}
        else:
            raise NotImplementedError(
                f"{icons.red_dot} The color value '{color_value}' is not supported. Please provide a hex color code or an integer based on the color theme."
            )

        color_dict = ({"solid": {"color": {"expr": color_expr}}},)
        transparency_dict = {"expr": {"Literal": {"Value": f"{transparency}D"}}}

        for p in self.__all_pages():
            path = p.get("path")
            payload = p.get("payload", {})
            page_name = payload.get("name")

            if page_name in page_list:
                self.set_json(
                    file_path=path,
                    json_path="$.objects.outspace[*].properties.color",
                    json_value=color_dict,
                )
                self.set_json(
                    file_path=path,
                    json_path="$.objects.outspace[*].properties.transparency",
                    json_value=transparency_dict,
                )

    def _set_wallpaper_image(
        self,
        image_path: str,
        page: Optional[str | List[str]] = None,
        transparency: int = 0,
        image_fit: Literal["Normal", "Fit", "Fill"] = "Normal",
    ):
        """
        Add an image as the wallpaper of a page.

        Parameters
        ----------
        image_path : str
            The path of the image file to be added. For example: "./builtin/MyImage.png".
        page : str | List[str], default=None
            The name or display name of the page(s) to which the wallpaper image will be applied.
            If None, applies to all pages.
        transparency : int, default=0
            The transparency level of the wallpaper image. Valid values are between 0 and 100.
        image_fit : str, default="Normal"
            The fit type of the wallpaper image. Valid options: "Normal", "Fit", "Fill".
        """
        self._ensure_pbir()

        image_fits = ["Normal", "Fit", "Fill"]
        image_fit = image_fit.capitalize()
        if image_fit not in image_fits:
            raise ValueError(
                f"{icons.red_dot} Invalid image fit. Valid options: {image_fits}."
            )
        if transparency < 0 or transparency > 100:
            raise ValueError(f"{icons.red_dot} Transparency must be between 0 and 100.")

        page_list = self.__resolve_page_list(page)

        image_name = os.path.splitext(os.path.basename(image_path))[0]
        image_file_path = self._add_image(image_path=image_path, image_name=image_name)

        image_dict = {
            "image": {
                "name": {"expr": {"Literal": {"Value": f"'{image_file_path}'"}}},
                "url": {
                    "expr": {
                        "ResourcePackageItem": {
                            "PackageName": "RegisteredResources",
                            "PackageType": 1,
                            "ItemName": image_file_path,
                        }
                    }
                },
                "scaling": {"expr": {"Literal": {"Value": f"'{image_fit}'"}}},
            }
        }
        transparency_dict = {"expr": {"Literal": {"Value": f"{transparency}D"}}}

        for p in self.__all_pages():
            path = p.get("path")
            payload = p.get("payload")
            page_name = payload.get("name")
            if page_name in page_list:
                self.set_json(
                    file_path=path,
                    json_path="$.objects.outspace[*].properties.image",
                    json_value=image_dict,
                )
                self.set_json(
                    file_path=path,
                    json_path="$.objects.outspace[*].properties.transparency",
                    json_value=transparency_dict,
                )

    def _add_blank_page(
        self,
        name: str,
        width: int = 1280,
        height: int = 720,
        display_option: str = "FitToPage",
    ):
        self._ensure_pbir()

        page_id = generate_hex()
        payload = {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/1.4.0/schema.json",
            "name": page_id,
            "displayName": name,
            "displayOption": display_option,
            "height": height,
            "width": width,
        }
        self.add(file_path=f"definition/pages/{page_id}/page.json", payload=payload)

        # Add the page to the pages.json file
        pages_file = self.get(file_path=self._pages_file_path)
        pages_file["pageOrder"].append(page_id)

    def _add_page(self, payload: dict | bytes, generate_id: bool = True):
        """
        Add a new page to the report.

        Parameters
        ----------
        payload : dict | bytes
            The json content of the page to be added. This can be a dictionary or a base64 encoded string.
        generate_id : bool, default=True
            Whether to generate a new page ID. If False, the page ID will be taken from the payload.
        """
        self._ensure_pbir()

        page_file = decode_payload(payload)
        page_file_copy = copy.deepcopy(page_file)

        if generate_id:
            # Generate a new page ID and update the page file accordingly
            page_id = generate_hex()
            page_file_copy["name"] = page_id
        else:
            page_id = page_file_copy.get("name")

        self.add(
            file_path=f"definition/pages/{page_id}/page.json", payload=page_file_copy
        )

    def _add_visual(self, page: str, payload: dict | bytes, generate_id: bool = True):
        """
        Add a new visual to a page in the report.

        Parameters
        ----------
        page : str
            The name or display name of the page to which the visual will be added.
        payload : dict | bytes
            The json content of the visual to be added. This can be a dictionary or a base64 encoded string.
        generate_id : bool, default=True
            Whether to generate a new visual ID. If False, the visual ID will be taken from the payload.
        """
        self._ensure_pbir()

        visual_file = decode_payload(payload)
        visual_file_copy = copy.deepcopy(visual_file)

        if generate_id:
            # Generate a new visual ID and update the visual file accordingly
            visual_id = generate_hex()
            visual_file_copy["name"] = visual_id
        else:
            visual_id = visual_file_copy.get("name")
        (page_file_path, page_id, page_name) = (
            self.__resolve_page_name_and_display_name_file_path(page)
        )
        visual_file_path = helper.generate_visual_file_path(page_file_path, visual_id)

        self.add(file_path=visual_file_path, payload=visual_file_copy)

    def _add_new_visual(
        self,
        page: str,
        type: str,
        x: int,
        y: int,
        height: int = 720,
        width: int = 1280,
    ):
        self._ensure_pbir()

        type = helper.resolve_visual_type(type)
        visual_id = generate_hex()
        (page_file_path, page_id, page_name) = (
            self.__resolve_page_name_and_display_name_file_path(page)
        )
        visual_file_path = helper.generate_visual_file_path(page_file_path, visual_id)

        payload = {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.0.0/schema.json",
            "name": visual_id,
            "position": {
                "x": x,
                "y": y,
                "z": 0,
                "height": height,
                "width": width,
                "tabOrder": 0,
            },
            "visual": {"visualType": type, "drillFilterOtherVisuals": True},
        }

        self.add(file_path=visual_file_path, payload=payload)

    def _update_to_theme_colors(self, mapping: dict[str, tuple[int, float]]):
        """
        Updates the report definition to use theme colors instead of hex colors.

        Parameters
        ----------
        mapping : dict[str, tuple[int, float]
            A dictionary mapping color names to their corresponding theme color IDs.
            Example: {"#FF0000": (1, 0), "#00FF00": (2, 0)}
            The first value in the tuple is the theme color ID and the second value is the percentage (a value between -0.6 and 0.6).
        """
        self._ensure_pbir()

        # Ensure theme color mapping is in the correct format (with Percent value)
        mapping = {k: (v, 0) if isinstance(v, int) else v for k, v in mapping.items()}

        out_of_range = {
            color: value
            for color, value in mapping.items()
            if len(value) > 1 and not (-0.6 <= value[1] <= 0.6)
        }

        if out_of_range:
            print(
                f"{icons.red_dot} The following mapping entries have Percent values out of range [-0.6, 0.6]:"
            )
            for color, val in out_of_range.items():
                print(f"  {color}: Percent = {val[1]}")
            raise ValueError(
                f"{icons.red_dot} The Percent values must be between -0.6 and 0.6."
            )

        json_path = "$..color.expr.Literal.Value"
        jsonpath_expr = parse(json_path)

        for part in [
            part
            for part in self._report_definition.get("parts")
            if part.get("path").endswith(".json")
        ]:
            file_path = part.get("path")
            payload = part.get("payload")
            matches = jsonpath_expr.find(payload)
            if matches:
                for match in matches:
                    color_string = match.value.strip("'")
                    if color_string in mapping:
                        color_data = mapping[color_string]
                        if isinstance(color_data, int):
                            color_data = [color_data, 0]

                        # Get reference to parent of 'Value' (i.e. 'Literal')
                        # literal_dict = match.context.value
                        # Get reference to parent of 'Literal' (i.e. 'expr')
                        expr_dict = match.context.context.value

                        # Replace the 'expr' with new structure
                        expr_dict.clear()
                        expr_dict["ThemeDataColor"] = {
                            "ColorId": color_data[0],
                            "Percent": color_data[1],
                        }

            self.update(file_path=file_path, payload=payload)

    def _rename_fields(self, mapping: dict):
        """
        Renames fields in the report definition based on the provided rename mapping.

        Parameters
        ----------
        mapping : dict
            A dictionary containing the mapping of old field names to new field names.
            Example:

            {
                "columns": {
                    ("TableName", "OldColumnName1"): "NewColumnName1",
                    ("TableName", "OldColumnName2"): "NewColumnName2",
                },
                "measures": {
                    ("TableName", "OldMeasureName1"): "NewMeasureName1",
                    ("TableName", "OldMeasureName2"): "NewMeasureName2",
                }
            }
        """
        self._ensure_pbir()

        selector_mapping = {
            key: {
                variant: f"{table}.{new_col}"
                for (table, col), new_col in value.items()
                for variant in (
                    f"({table}.{col})",
                    f"[{table}.{col}]",
                )
            }
            for key, value in mapping.items()
        }

        for part in [
            part
            for part in self._report_definition.get("parts")
            if part.get("path").endswith(".json")
        ]:
            file_path = part.get("path")
            payload = part.get("payload")

            # Paths for columns, measures, and expressions
            col_expr_path = parse("$..Column")
            meas_expr_path = parse("$..Measure")
            entity_ref_path = parse("$..Expression.SourceRef.Entity")
            query_ref_path = parse("$..queryRef")
            native_query_ref_path = parse("$..nativeQueryRef")
            filter_expr_path = parse("$..filterConfig.filters[*].filter.From")
            source_ref_path = parse("$..Expression.SourceRef.Source")
            metadata_ref_path = parse("$..selector.metadata")

            # Populate table alias map
            alias_map = {}
            for match in filter_expr_path.find(payload):
                alias_list = match.value
                for alias in alias_list:
                    alias_name = alias.get("Name")
                    alias_entity = alias.get("Entity")
                    alias_map[alias_name] = alias_entity

            # Rename selector.metadata objects
            for match in metadata_ref_path.find(payload):
                obj = match.value

                # Check both measures and columns
                for category in ["measures", "columns"]:
                    for i, value in selector_mapping.get(category).items():
                        if i in obj:
                            prefix = i[0]
                            if prefix == "[":
                                new_value = obj.replace(i, f"[{value}]")
                            else:
                                new_value = obj.replace(i, f"({value})")
                            match.context.value["metadata"] = new_value
                            break

            # Rename Column Properties
            for match in col_expr_path.find(payload):
                col_obj = match.value
                parent = match.context.value

                # Extract table name from SourceRef
                source_matches = entity_ref_path.find(parent)
                if source_matches:
                    table = source_matches[0].value
                else:
                    alias = source_ref_path.find(parent)
                    table = alias_map.get(alias[0].value)

                if not table:
                    continue  # skip if can't resolve table

                old_name = col_obj.get("Property")
                if (table, old_name) in mapping.get("columns", {}):
                    col_obj["Property"] = mapping["columns"][(table, old_name)]

            # Rename Measure Properties
            for match in meas_expr_path.find(payload):
                meas_obj = match.value
                parent = match.context.value

                source_matches = entity_ref_path.find(parent)
                if source_matches:
                    table = source_matches[0].value
                else:
                    alias = source_ref_path.find(parent)
                    table = alias_map.get(alias[0].value)

                if not table:
                    continue  # skip if can't resolve table

                old_name = meas_obj.get("Property")
                if (table, old_name) in mapping.get("measures", {}):
                    meas_obj["Property"] = mapping["measures"][(table, old_name)]

            # Update queryRef and nativeQueryRef
            def update_refs(path_expr):
                for match in path_expr.find(payload):
                    ref_key = match.path.fields[0]
                    ref_value = match.value
                    parent = match.context.value

                    for (tbl, old_name), new_name in mapping.get("columns", {}).items():
                        pattern = rf"\b{re.escape(tbl)}\.{re.escape(old_name)}\b"
                        if re.search(pattern, ref_value):
                            if ref_key == "queryRef":
                                ref_value = re.sub(
                                    pattern, f"{tbl}.{new_name}", ref_value
                                )
                            elif ref_key == "nativeQueryRef":
                                agg_match = re.match(
                                    rf"(?i)([a-z]+)\s*\(\s*{re.escape(tbl)}\.{re.escape(old_name)}\s*\)",
                                    ref_value,
                                )
                                if agg_match:
                                    func = agg_match.group(1).capitalize()
                                    ref_value = f"{func} of {new_name}"
                                else:
                                    ref_value = ref_value.replace(old_name, new_name)
                            parent[ref_key] = ref_value

                    for (tbl, old_name), new_name in mapping.get(
                        "measures", {}
                    ).items():
                        pattern = rf"\b{re.escape(tbl)}\.{re.escape(old_name)}\b"
                        if re.search(pattern, ref_value):
                            if ref_key == "queryRef":
                                ref_value = re.sub(
                                    pattern, f"{tbl}.{new_name}", ref_value
                                )
                            elif ref_key == "nativeQueryRef":
                                agg_match = re.match(
                                    rf"(?i)([a-z]+)\s*\(\s*{re.escape(tbl)}\.{re.escape(old_name)}\s*\)",
                                    ref_value,
                                )
                                if agg_match:
                                    func = agg_match.group(1).capitalize()
                                    ref_value = f"{func} of {new_name}"
                                else:
                                    ref_value = ref_value.replace(old_name, new_name)
                            parent[ref_key] = ref_value

            update_refs(query_ref_path)
            update_refs(native_query_ref_path)

            self.update(file_path=file_path, payload=payload)

    def _list_color_codes(self) -> List[str]:
        """
        Shows a list of all the hex color codes used in the report.

        Returns
        -------
        list[str]
            A list of hex color codes used in the report.
        """
        self._ensure_pbir()

        file = self.get("*.json", json_path="$..color.expr.Literal.Value")

        return [x[1].strip("'") for x in file]

    def __update_visual_image(self, file_path: str, image_path: str):
        """
        Update the image of a visual in the report definition. Only supported for 'image' visual types.

        Parameters
        ----------
        file_path : str
            The file path of the visual to be updated. For example: "definition/pages/ReportSection1/visuals/a1d8f99b81dcc2d59035/visual.json".
        image_path : str
            The name of the image file to be added. For example: "MyImage".
        """

        if image_path not in self.list_paths().get("Path").values:
            raise ValueError(
                f"Image path '{image_path}' not found in the report definition."
            )
        if not image_path.startswith("StaticResources/RegisteredResources/"):
            raise ValueError(
                f"Image path must start with 'StaticResources/RegisteredResources/'. Provided: {image_path}"
            )

        image_name = image_path.split("RegisteredResources/")[1]

        if not file_path.endswith("/visual.json"):
            raise ValueError(
                f"File path must end with '/visual.json'. Provided: {file_path}"
            )

        file = self.get(file_path=file_path)
        if file.get("visual").get("visualType") != "image":
            raise ValueError("This function is only valid for image visuals.")
        file.get("visual").get("objects").get("general")[0].get("properties").get(
            "imageUrl"
        ).get("expr").get("ResourcePackageItem")["ItemName"] == image_name

    def save_changes(self):

        if self._readonly:
            print(
                f"{icons.warning} The connection is read-only. Set 'readonly' to False to save changes."
            )
        else:
            # Convert the report definition to base64
            if self._current_report_definition == self._report_definition:
                print(f"{icons.info} No changes were made to the report definition.")
                return
            new_report_definition = copy.deepcopy(self._report_definition)

            for part in new_report_definition.get("parts"):
                part["payloadType"] = "InlineBase64"
                path = part.get("path")
                payload = part.get("payload")
                if isinstance(payload, dict):
                    converted_json = json.dumps(part["payload"])
                    part["payload"] = base64.b64encode(
                        converted_json.encode("utf-8")
                    ).decode("utf-8")
                elif isinstance(payload, bytes):
                    part["payload"] = base64.b64encode(part["payload"]).decode("utf-8")
                elif is_base64(payload):
                    part["payload"] = payload
                else:
                    raise NotImplementedError(
                        f"{icons.red_dot} Unsupported payload type: {type(payload)} for the '{path}' file."
                    )

            # Generate payload for the updateDefinition API
            new_payload = {"definition": {"parts": new_report_definition.get("parts")}}

            # Update item definition
            _base_api(
                request=f"/v1/workspaces/{self._workspace_id}/reports/{self._report_id}/updateDefinition",
                method="post",
                payload=new_payload,
                lro_return_status_code=True,
                status_codes=None,
            )
            print(
                f"{icons.green_dot} The report definition has been updated successfully."
            )

    def close(self):

        if self._show_diffs and (
            self._current_report_definition != self._report_definition
        ):
            diff_parts(
                self._current_report_definition.get("parts"),
                self._report_definition.get("parts"),
            )
        # Save the changes to the service if the connection is read/write
        if not self._readonly:
            self.save_changes()


@log
@contextmanager
def connect_report(
    report: str | UUID,
    workspace: Optional[str | UUID] = None,
    readonly: bool = True,
    show_diffs: bool = True,
):
    """
    Connects to the report.

    Parameters
    ----------
    report : str | uuid.UUID
        Name or ID of the report.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    readonly: bool, default=True
        Whether the connection is read-only or read/write. Setting this to False enables read/write which saves the changes made back to the server.
    show_diffs: bool, default=True
        Whether to show the differences between the current report definition in the service and the new report definition.

    Returns
    -------
    typing.Iterator[ReportWrapper]
        A connection to the report's metadata.
    """

    rw = ReportWrapper(
        report=report,
        workspace=workspace,
        readonly=readonly,
        show_diffs=show_diffs,
    )
    try:
        yield rw
    finally:
        rw.close()
