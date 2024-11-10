import sempy.fabric as fabric
from typing import Optional, List
from sempy.fabric.exceptions import FabricHTTPException
import pandas as pd
import time
import sempy_labs._icons as icons


def scan_workspaces(
    dataset_expressions: bool = False,
    dataset_schema: bool = False,
    data_source_details: bool = False,
    artifact_users: bool = False,
    lineage: bool = False,
    workspace: Optional[str | List[str]] = None,
) -> dict:
    """
    Scans a workspace or set of workspace for detailed metadata.

    Parameters
    ----------
    dataset_expressions : bool, default=False
        Whether to return data source details.
    dataset_schema : bool, defualt=False
        Whether to return dataset schema (tables, columns and measures). If you set this parameter to true, you must fully enable metadata scanning in order for data to be returned.
    data_source_details : bool, default=False
        Whether to return dataset expressions (DAX and Mashup queries). If you set this parameter to true, you must fully enable metadata scanning in order for data to be returned.
    artifact_users : bool, default=False
        Whether to return user details for a Power BI item (such as a report or a dashboard).
    lineage : bool, default=False
        Whether to return lineage info (upstream dataflows, tiles, data source IDs).
    workspace : str | List[str], default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    dict
        The .json output showing the metadata for the workspace(s) and their items.
    """

    # https://learn.microsoft.com/en-us/rest/api/power-bi/admin/workspace-info-post-workspace-info

    if workspace is None:
        workspace = fabric.resolve_workspace_name(workspace)

    if isinstance(workspace, str):
        workspace = [workspace]

    workspace_list = []

    if len(workspace_list) > 100:
        raise ValueError(f"{icons.red_dot} There is a limit of 100 workspaces.")

    for w in workspace:
        workspace_list.append(fabric.resolve_workspace_id(w))

    client = fabric.PowerBIRestClient()
    request_body = {"workspaces": workspace_list}

    url = f"/v1.0/myorg/admin/workspaces/getInfo?lineage={lineage}&datasourceDetails={data_source_details}&datasetSchema={dataset_schema}&datasetExpressions={dataset_expressions}&getArtifactUsers={artifact_users}"
    response = client.post(url, json=request_body)

    if response.status_code != 202:
        raise FabricHTTPException(response)
    scan_id = response.json().get("id")
    scan_status = response.json().get("status")
    while scan_status not in ["Succeeded", "Failed"]:
        time.sleep(1)
        response = client.get(f"/v1.0/myorg/admin/workspaces/scanStatus/{scan_id}")
        scan_status = response.json().get("status")
    if scan_status == "Failed":
        raise FabricHTTPException(response)
    response = client.get(f"/v1.0/myorg/admin/workspaces/scanResult/{scan_id}")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    return response.json()


class ScannerWrapper:

    def __init__(
        self,
        data_source_details: Optional[bool] = False,
        dataset_schema: Optional[bool] = False,
        dataset_expressions: Optional[bool] = False,
        lineage: Optional[bool] = False,
        artifact_users: Optional[bool] = False,
        workspace: Optional[str | List[str]] = None,
    ):

        self._data_source_details = data_source_details
        self._dataset_schema = dataset_schema
        self._dataset_expressions = dataset_expressions
        self._lineage = lineage
        self._artifact_users = artifact_users
        self._workspace = workspace

        self.output = scan_workspaces(
            data_source_details=self._data_source_details,
            dataset_schema=self._dataset_schema,
            dataset_expressions=self._dataset_expressions,
            lineage=self._lineage,
            artifact_users=self._artifact_users,
            workspace=self._workspace,
        )

    def list_workspaces(self) -> pd.DataFrame:

        df = pd.DataFrame(
            columns=[
                "Workspace Name",
                "Workspace Id",
                "State",
                "Is On Dedicated Capacity",
                "Capacity Id",
                "Default Dataset Storage Format",
            ]
        )

        for w in self.output.get("workspaces", []):
            new_data = {
                "Workspace Name": w.get("name"),
                "Workspace Id": w.get("id"),
                "State": w.get("state"),
                "Is On Dedicated Capacity": w.get("isOnDedicatedCapacity"),
                "Capacity Id": w.get("capacityId"),
                "Default Dataset Storage Format": w.get("defaultDatasetStorageFormat"),
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

        bool_cols = ["Is On Dedicated Capacity"]
        df[bool_cols] = df[bool_cols].astype(bool)

        return df

    def list_dashboards(self) -> pd.DataFrame:

        df = pd.DataFrame(
            columns=[
                "Workspace Name",
                "Workspace Id",
                "Dashboard Name",
                "Dashboard Id",
                "Description",
                "Is Read Only",
                "Data Classification",
                "App Id",
                "Sensitivity Label Id",
            ]
        )

        for w in self.output.get("workspaces", []):
            for obj in w.get("dashboards", []):
                new_data = {
                    "Workspace Name": w.get("name"),
                    "Workspace Id": w.get("id"),
                    "Dashboard Name": obj.get("displayName"),
                    "Dashboard Id": obj.get("id"),
                    "Description": obj.get("description"),
                    "Is Read Only": obj.get("isReadOnly"),
                    "Data Classification": obj.get("dataClassification"),
                    "App Id": obj.get("appId"),
                    "Sensitivity Label Id": obj.get("sensitivityLabel", {}).get(
                        "labelId"
                    ),
                }
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )

        bool_cols = ["Is Read Only"]
        df[bool_cols] = df[bool_cols].astype(bool)

        return df

    def list_dataflows(self) -> pd.DataFrame:

        df = pd.DataFrame(
            columns=[
                "Workspace Name",
                "Workspace Id",
                "Dataflow Name",
                "Dataflow Id",
                "Description",
                "Configured By",
                "Model URL",
                "Modified Date",
                "Modified By",
                "Sensitivity Label Id",
                "Endorsement",
                "Certified By",
            ]
        )

        for w in self.output.get("workspaces", []):
            for obj in w.get("dataflows", []):
                end = obj.get("endorsementDetails", {})
                new_data = {
                    "Workspace Name": w.get("name"),
                    "Workspace Id": w.get("id"),
                    "Dataflow Name": obj.get("name"),
                    "Dataflow Id": obj.get("objectId"),
                    "Description": obj.get("description"),
                    "Modified Date": obj.get("modifiedDateTime"),
                    "Modified By": obj.get("modfiedBy"),
                    "Configured By": obj.get("configuredBy"),
                    "Model URL": obj.get("modelUrl"),
                    "Sensitivity Label Id": obj.get("sensitivityLabel", {}).get(
                        "labelId"
                    ),
                    "Endorsement": end.get("endorsement") if end else None,
                    "Certified By": end.get("certifiedBy") if end else None,
                }
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )

        return df

    def list_datamarts(self) -> pd.DataFrame:

        df = pd.DataFrame(
            columns=[
                "Workspace Name",
                "Workspace Id",
                "Datamart Name",
                "Datamart Id",
                "Description",
                "State",
                "Modified Date",
                "Modified By",
                "Modified By Id",
                "Configured By",
                "Configured By Id",
                "Suspended Batch Id" "Sensitivity Label Id",
                "Endorsement",
                "Certified By",
            ]
        )

        for w in self.output.get("workspaces", []):
            for obj in w.get("datamarts", []):
                end = obj.get("endorsementDetails", {})
                new_data = {
                    "Workspace Name": w.get("name"),
                    "Workspace Id": w.get("id"),
                    "Datamart Name": obj.get("name"),
                    "Datamart Id": obj.get("id"),
                    "Description": obj.get("description"),
                    "State": obj.get("state"),
                    "Modified Date": obj.get("modifiedDateTime"),
                    "Modified By": obj.get("modfiedBy"),
                    "Modified By Id": obj.get("modfiedById"),
                    "Sensitivity Label Id": obj.get("sensitivityLabel", {}).get(
                        "labelId"
                    ),
                    "Endorsement": end.get("endorsement") if end else None,
                    "Certified By": end.get("certifiedBy") if end else None,
                    "Suspended Batch Id": obj.get("suspendedBatchId"),
                    "Configured By": obj.get("configuredBy"),
                    "Configured By Id": obj.get("configuredById"),
                }
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )

        return df

    def list_datasets(self) -> pd.DataFrame:

        df = pd.DataFrame(
            columns=[
                "Workspace Name",
                "Workspace Id",
                "Dataset Name",
                "Dataset Id",
                "Configured By",
                "Description",
                "Effective Identity Required",
                "Effective Identity Roles Required",
                "Target Storage Mode",
                "Schema May Not Be Up To Date",
                "Endorsement",
                "Certified By",
                "Created Date",
                "Content Provider Type",
                "Sensitivity Label Id",
                "Datasource Usages",
            ]
        )

        for w in self.output.get("workspaces", []):
            for obj in w.get("datasets", []):

                ds_list = []
                if "datasourceUsages" in obj:
                    ds_list = [
                        item["datasourceInstanceId"]
                        for item in obj.get("datasourceUsages")
                    ]
                end = obj.get("endorsementDetails", {})
                new_data = {
                    "Workspace Name": w.get("name"),
                    "Workspace Id": w.get("id"),
                    "Dataset Name": obj.get("name"),
                    "Dataset Id": obj.get("id"),
                    "Description": obj.get("description"),
                    "Configured By": obj.get("configuredBy"),
                    "Effective Identity Required": obj.get(
                        "isEffectiveIdentityRequired"
                    ),
                    "Effective Identity Roles Required": obj.get(
                        "isEffectiveIdentityRolesRequired"
                    ),
                    "Target Storage Mode": obj.get("targetStorageMode"),
                    "Created Date": obj.get("createdDate"),
                    "Content Provider Type": obj.get("contentProviderType"),
                    "Sensitivity Label Id": obj.get("sensitivityLabel", {}).get(
                        "labelId"
                    ),
                    "Datasource Usages": [ds_list],
                    "Endorsement": end.get("endorsement") if end else None,
                    "Certified By": end.get("certifiedBy") if end else None,
                    "Schema May Not Be Up To Date": obj.get("schemaMayNotBeUpToDate"),
                }
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )

        bool_cols = [
            "Effective Identity Required",
            "Effective Identity Roles Required",
            "Schema May Not Be Up To Date",
        ]
        df[bool_cols] = df[bool_cols].astype(bool)

        return df

    def list_eventhouses(self) -> pd.DataFrame:

        df = pd.DataFrame(
            columns=[
                "Workspace Name",
                "Workspace Id",
                "Eventhouse Name",
                "Eventhouse Id",
                "Description",
                "State",
                "Last Updated Date",
                "Created Date",
                "Modified Date",
                "Created By",
                "Modified By",
                "Modified By Id",
                "Created By Id",
                "Sensitivity Label Id",
            ]
        )

        for w in self.output.get("workspaces", []):
            for obj in w.get("Eventhouse", []):
                new_data = {
                    "Workspace Name": w.get("name"),
                    "Workspace Id": w.get("id"),
                    "Eventhouse Name": obj.get("name"),
                    "Eventhouse Id": obj.get("id"),
                    "Description": obj.get("description"),
                    "State": obj.get("state"),
                    "Last Updated Date": obj.get("lastUpdatedDate"),
                    "Created Date": obj.get("createdDate"),
                    "Modified Date": obj.get("modifiedDate"),
                    "Modified By": obj.get("modfiedBy"),
                    "Created By": obj.get("createdBy"),
                    "Modified By Id": obj.get("modfiedById"),
                    "Created By Id": obj.get("createdById"),
                    "Sensitivity Label Id": obj.get("sensitivityLabel", {}).get(
                        "labelId"
                    ),
                }
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )

        return df

    def list_kql_databases(self) -> pd.DataFrame:

        df = pd.DataFrame(
            columns=[
                "Workspace Name",
                "Workspace Id",
                "KQL Database Name",
                "KQL Database Id",
                "Description",
                "State",
                "Last Updated Date",
                "Created Date",
                "Modified Date",
                "Created By",
                "Modified By",
                "Modified By Id",
                "Created By Id",
                "Sensitivity Label Id",
                "Query Service URI",
                "Ingestion Service URI",
                "Region",
                "Kusto Database Type",
            ]
        )

        for w in self.output.get("workspaces", []):
            for obj in w.get("KQLDatabase", []):
                ep = obj.get("extendedProperties", {})
                new_data = {
                    "Workspace Name": w.get("name"),
                    "Workspace Id": w.get("id"),
                    "KQL Database Name": obj.get("name"),
                    "KQL Database Id": obj.get("id"),
                    "Description": obj.get("description"),
                    "State": obj.get("state"),
                    "Last Updated Date": obj.get("lastUpdatedDate"),
                    "Created Date": obj.get("createdDate"),
                    "Modified Date": obj.get("modifiedDate"),
                    "Modified By": obj.get("modfiedBy"),
                    "Modified By Id": obj.get("modfiedById"),
                    "Created By Id": obj.get("createdById"),
                    "Sensitivity Label Id": obj.get("sensitivityLabel", {}).get(
                        "labelId"
                    ),
                    "Query Service URI": ep.get("QueryServiceUri"),
                    "Ingestion Service URI": ep.get("IngestionServiceUri"),
                    "Region": ep.get("Region"),
                    "Kusto Database Type": ep.get("KustoDatabaseType"),
                }
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )

        return df

    def list_kql_querysets(self) -> pd.DataFrame:

        df = pd.DataFrame(
            columns=[
                "Workspace Name",
                "Workspace Id",
                "KQL Queryset Name",
                "KQL Queryset Id",
                "Description",
                "State",
                "Last Updated Date",
                "Created Date",
                "Modified Date",
                "Created By",
                "Modified By",
                "Modified By Id",
                "Created By Id",
                "Sensitivity Label Id",
            ]
        )

        for w in self.output.get("workspaces", []):
            for obj in w.get("KQLQueryset", []):
                new_data = {
                    "Workspace Name": w.get("name"),
                    "Workspace Id": w.get("id"),
                    "KQL Queryset Name": obj.get("name"),
                    "KQL Queryset Id": obj.get("id"),
                    "Description": obj.get("description"),
                    "State": obj.get("state"),
                    "Last Updated Date": obj.get("lastUpdatedDate"),
                    "Created Date": obj.get("createdDate"),
                    "Modified Date": obj.get("modifiedDate"),
                    "Modified By": obj.get("modfiedBy"),
                    "Created By": obj.get("createdBy"),
                    "Modified By Id": obj.get("modfiedById"),
                    "Created By Id": obj.get("createdById"),
                    "Sensitivity Label Id": obj.get("sensitivityLabel", {}).get(
                        "labelId"
                    ),
                }
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )

        return df

    def list_lakehouses(self) -> pd.DataFrame:

        df = pd.DataFrame(
            columns=[
                "Workspace Name",
                "Workspace Id",
                "Lakehouse Name",
                "Lakehouse Id",
                "Description",
                "State",
                "Last Updated Date",
                "Created Date",
                "Modified Date",
                "Created By",
                "Modified By",
                "Modified By Id",
                "Created By Id",
                "Sensitivity Label Id",
                "OneLake Tables Path",
                "OneLake Files Path",
                "DW Properties",
                "Datasource Usages",
            ]
        )

        for w in self.output.get("workspaces", []):
            for obj in w.get("Lakehouse", []):
                ep = obj.get("extendedProperties", {})

                ds_list = []
                if "datasourceUsages" in obj:
                    ds_list = [
                        item["datasourceInstanceId"]
                        for item in obj.get("datasourceUsages")
                    ]

                new_data = {
                    "Workspace Name": w.get("name"),
                    "Workspace Id": w.get("id"),
                    "Lakehouse Name": obj.get("name"),
                    "Lakehouse Id": obj.get("id"),
                    "Description": obj.get("description"),
                    "State": obj.get("state"),
                    "Last Updated Date": obj.get("lastUpdatedDate"),
                    "Created Date": obj.get("createdDate"),
                    "Modified Date": obj.get("modifiedDate"),
                    "Created By": obj.get("createdBy"),
                    "Modified By": obj.get("modifiedBy"),
                    "Modified By Id": obj.get("modfiedById"),
                    "Created By Id": obj.get("createdById"),
                    "Sensitivity Label Id": obj.get("sensitivityLabel", {}).get(
                        "labelId"
                    ),
                    "OneLake Tables Path": ep.get("OneLakeTablesPath"),
                    "OneLake Files Path": ep.get("OneLakeFilesPath"),
                    "DW Properties": ep.get("DwProperties"),
                    "Datasource Usages": [ds_list],
                }
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )

        return df

    def list_notebooks(self) -> pd.DataFrame:

        df = pd.DataFrame(
            columns=[
                "Workspace Name",
                "Workspace Id",
                "Notebook Name",
                "Notebook Id",
                "Description",
                "State",
                "Last Updated Date",
                "Created Date",
                "Modified By",
                "Created By",
                "Modified By Id",
                "Created By Id",
                "Sensitivity Label Id",
            ]
        )

        for w in self.output.get("workspaces", []):
            for obj in w.get("Notebook", []):
                new_data = {
                    "Workspace Name": w.get("name"),
                    "Workspace Id": w.get("id"),
                    "Notebook Name": obj.get("name"),
                    "Notebook Id": obj.get("id"),
                    "Description": obj.get("description"),
                    "State": obj.get("state"),
                    "Last Updated Date": obj.get("lastUpdatedDate"),
                    "Created Date": obj.get("createdDate"),
                    "Modified By": obj.get("modifiedBy"),
                    "Created By": obj.get("createdBy"),
                    "Modified By Id": obj.get("modifiedById"),
                    "Created By Id": obj.get("createdById"),
                    "Sensitivity Label Id": obj.get("sensitivityLabel", {}).get(
                        "labelId"
                    ),
                }
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )

        return df

    def list_reports(self) -> pd.DataFrame:

        df = pd.DataFrame(
            columns=[
                "Workspace Name",
                "Workspace Id",
                "Report Name",
                "Report Id",
                "Report Type",
                "Description",
                "Created Date",
                "Modified Date",
                "Modified By",
                "Created By",
                "Modified By Id",
                "Created By Id",
                "Sensitivity Label Id",
            ]
        )

        for w in self.output.get("workspaces", []):
            for obj in w.get("reports", []):
                new_data = {
                    "Workspace Name": w.get("name"),
                    "Workspace Id": w.get("id"),
                    "Report Name": obj.get("name"),
                    "Report Id": obj.get("id"),
                    "Report Type": obj.get("reportType"),
                    "Description": obj.get("id"),
                    "Created Date": obj.get("createdDateTime"),
                    "Modified Date": obj.get("modifiedDateTime"),
                    "Modified By": obj.get("modifiedBy"),
                    "Created By": obj.get("createdBy"),
                    "Modified By Id": obj.get("modifiedById"),
                    "Created By Id": obj.get("createdById"),
                    "Sensitivity Label Id": obj.get("sensitivityLabel", {}).get(
                        "labelId"
                    ),
                }
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )

        return df

    def list_sql_endpoints(self) -> pd.DataFrame:

        df = pd.DataFrame(
            columns=[
                "Workspace Name",
                "Workspace Id",
                "SQL Endpoint Name",
                "SQL Endpoint Id",
                "Configured By",
                "Configured By Id",
                "Modified By",
                "Modified By Id",
                "Modified Date",
                "Sensitivity Label Id",
            ]
        )

        for w in self.output.get("workspaces", []):
            for obj in w.get("SQLAnalyticsEndpoint", []):
                new_data = {
                    "Workspace Name": w.get("name"),
                    "Workspace Id": w.get("id"),
                    "SQL Endpoint Name": obj.get("name"),
                    "SQL Endpoint Id": obj.get("id"),
                    "Configured By": obj.get("configuredBy"),
                    "Configured By Id": obj.get("configuredById"),
                    "Modified By": obj.get("modifiedBy"),
                    "Modified By Id": obj.get("modifiedById"),
                    "Modified Date": obj.get("modifiedDateTime"),
                    "Sensitivity Label Id": obj.get("sensitivityLabel", {}).get(
                        "labelId"
                    ),
                }
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )

        return df

    def list_warehouses(self) -> pd.DataFrame:

        df = pd.DataFrame(
            columns=[
                "Workspace Name",
                "Workspace Id",
                "Warehouse Name",
                "Warehouse Id",
                "Configured By",
                "Configured By Id",
                "Modified By",
                "Modified By Id",
                "Modified Date",
                "Sensitivity Label Id",
            ]
        )

        for w in self.output.get("workspaces", []):
            for obj in w.get("warehouses", []):
                new_data = {
                    "Workspace Name": w.get("name"),
                    "Workspace Id": w.get("id"),
                    "Warehouse Name": obj.get("name"),
                    "Warehouse Id": obj.get("id"),
                    "Configured By": obj.get("configuredBy"),
                    "Configured By Id": obj.get("configuredById"),
                    "Modified By": obj.get("modifiedBy"),
                    "Modified By Id": obj.get("modifiedById"),
                    "Modified Date": obj.get("modifiedDateTime"),
                    "Sensitivity Label Id": obj.get("sensitivityLabel", {}).get(
                        "labelId"
                    ),
                }
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )

        return df

    def list_data_source_instances(self) -> pd.DataFrame:

        df = pd.DataFrame(
            columns=[
                "Data Source Type",
                "Server",
                "Path",
                "Data Source Id",
                "Gateway Id",
            ]
        )

        for obj in self.output.get("datasourceInstances", []):
            new_data = {
                "Data Source Type": obj.get("datasourceType"),
                "Server": obj.get("connectionDetails", {}).get("datasourceType"),
                "Path": obj.get("connectionDetails", {}).get("path"),
                "Datasource Id": obj.get("datasourceId"),
                "Gateway Id": obj.get("gatewayId"),
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

        return df

    def list_workspace_users(self) -> pd.DataFrame:

        df = pd.DataFrame(
            columns=[
                "Workspace Name",
                "Workspace Id",
                "Group User Access Right",
                "Email Address",
                "Display Name",
                "Identifier",
                "Graph Id",
                "Principal Type",
                "User Type",
            ]
        )

        for w in self.output.get("workspaces", []):
            for obj in w.get("users", []):
                new_data = {
                    "Workspace Name": w.get("name"),
                    "Workspace Id": w.get("id"),
                    "Group User Access Right": obj.get("groupUserAccessRight"),
                    "Email Address": obj.get("emailAddress"),
                    "Display Name": obj.get("displayName"),
                    "Identifier": obj.get("identifier"),
                    "Graph Id": obj.get("graphId"),
                    "Principal Type": obj.get("principalType"),
                    "User Type": obj.get("userType"),
                }

                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )

        return df

    # Dataset functions
    def list_dataset_tables(self) -> pd.DataFrame:

        df = pd.DataFrame(
            columns=[
                "Workspace Name",
                "Workspace Id",
                "Dataset Name",
                "Dataset Id",
                "Table Name",
                "Hidden",
                "Storage Mode",
                "Source Expression",
                "Description",
            ]
        )

        for w in self.output.get("workspaces", []):
            for obj in w.get("datasets", []):
                for t in obj.get("tables", []):
                    source = t.get("source", {})
                    new_data = {
                        "Workspace Name": w.get("name"),
                        "Workspace Id": w.get("id"),
                        "Dataset Name": obj.get("name"),
                        "Dataset Id": obj.get("id"),
                        "Table Name": t.get("name"),
                        "Hidden": t.get("isHidden"),
                        "Storage Mode": t.get("storageMode"),
                        "Source Expression": (
                            source[0].get("expression")
                            if source and isinstance(source[0], dict)
                            else None
                        ),
                        "Description": t.get("description"),
                    }

                    df = pd.concat(
                        [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                    )

        bool_cols = ["Hidden"]
        df[bool_cols] = df[bool_cols].astype(bool)

        return df

    def list_dataset_columns(self) -> pd.DataFrame:

        df = pd.DataFrame(
            columns=[
                "Workspace Name",
                "Workspace Id",
                "Dataset Name",
                "Dataset Id",
                "Table Name",
                "Column Name",
                "Data Type",
                "Hidden",
                "Column Type",
                "Expression",
                "Description",
                "Sort By Column",
                "Summarize By",
            ]
        )

        for w in self.output.get("workspaces", []):
            for obj in w.get("datasets", []):
                for t in obj.get("tables", []):
                    for c in t.get("columns", []):
                        new_data = {
                            "Workspace Name": w.get("name"),
                            "Workspace Id": w.get("id"),
                            "Dataset Name": obj.get("name"),
                            "Dataset Id": obj.get("id"),
                            "Table Name": t.get("name"),
                            "Column Name": c.get("name"),
                            "Data Type": c.get("dataType"),
                            "Hidden": c.get("isHidden"),
                            "Column Type": c.get("columnType"),
                            "Expression": c.get("expression"),
                            "Description": c.get("description"),
                            "Sort By Column": c.get("sortByColumn"),
                            "Summarize By": c.get("summarizeBy"),
                        }

                        df = pd.concat(
                            [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                        )

        bool_cols = ["Hidden"]
        df[bool_cols] = df[bool_cols].astype(bool)

        return df

    def list_dataset_measures(self) -> pd.DataFrame:

        df = pd.DataFrame(
            columns=[
                "Workspace Name",
                "Workspace Id",
                "Dataset Name",
                "Dataset Id",
                "Table Name",
                "Measure Name",
                "Expression",
                "Hidden",
                "Format String",
                "Description",
            ]
        )

        for w in self.output.get("workspaces", []):
            for obj in w.get("datasets", []):
                for t in obj.get("tables", []):
                    for m in t.get("measures", []):
                        new_data = {
                            "Workspace Name": w.get("name"),
                            "Workspace Id": w.get("id"),
                            "Dataset Name": obj.get("name"),
                            "Dataset Id": obj.get("id"),
                            "Table Name": t.get("name"),
                            "Measure Name": m.get("name"),
                            "Expression": m.get("expression"),
                            "Hidden": m.get("isHidden"),
                            "Format String": m.get("formatString"),
                            "Description": m.get("description"),
                        }

                        df = pd.concat(
                            [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                        )

        bool_cols = ["Hidden"]
        df[bool_cols] = df[bool_cols].astype(bool)

        return df

    def list_dataset_expressions(self) -> pd.DataFrame:

        df = pd.DataFrame(
            columns=[
                "Workspace Name",
                "Workspace Id",
                "Dataset Name",
                "Dataset Id",
                "Expression Name",
                "Expression",
                "Description",
            ]
        )

        for w in self.output.get("workspaces", []):
            for obj in w.get("datasets", []):
                for e in obj.get("expressions", []):

                    new_data = {
                        "Workspace Name": w.get("name"),
                        "Workspace Id": w.get("id"),
                        "Dataset Name": obj.get("name"),
                        "Dataset Id": obj.get("id"),
                        "Expression Name": e.get("name"),
                        "Expression": e.get("expression"),
                        "Description": e.get("description"),
                    }

                    df = pd.concat(
                        [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                    )

        return df

    def list_dataset_roles(self, include_members: bool = False) -> pd.DataFrame:

        df = pd.DataFrame(
            columns=[
                "Workspace Name",
                "Workspace Id",
                "Dataset Name",
                "Dataset Id",
                "Role Name",
                "Model Permission",
            ]
        )

        for w in self.output.get("workspaces", []):
            for obj in w.get("datasets", []):
                for e in obj.get("roles", []):

                    if not include_members:
                        new_data = {
                            "Workspace Name": w.get("name"),
                            "Workspace Id": w.get("id"),
                            "Dataset Name": obj.get("name"),
                            "Dataset Id": obj.get("id"),
                            "Role Name": e.get("name"),
                            "Model Permission": e.get("modelPermission"),
                        }

                        df = pd.concat(
                            [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                        )
                    else:
                        for rm in e.get("members", []):
                            new_data = {
                                "Workspace Name": w.get("name"),
                                "Workspace Id": w.get("id"),
                                "Dataset Name": obj.get("name"),
                                "Dataset Id": obj.get("id"),
                                "Role Name": e.get("name"),
                                "Model Permission": e.get("modelPermission"),
                                "Member Name": rm.get("memberName"),
                                "Member Id": rm.get("memberId"),
                                "Member Type": rm.get("memberType"),
                                "Identity Provider": rm.get("identityProvider"),
                            }

                            df = pd.concat(
                                [df, pd.DataFrame(new_data, index=[0])],
                                ignore_index=True,
                            )

        return df

    def list_dataset_row_level_security(self) -> pd.DataFrame:

        df = pd.DataFrame(
            columns=[
                "Workspace Name",
                "Workspace Id",
                "Dataset Name",
                "Dataset Id",
                "Role Name",
                "Model Permission",
                "Table Name",
                "Filter Expression",
            ]
        )

        for w in self.output.get("workspaces", []):
            for obj in w.get("datasets", []):
                for e in obj.get("roles", []):
                    for tp in e.get("tablePermissions", []):
                        new_data = {
                            "Workspace Name": w.get("name"),
                            "Workspace Id": w.get("id"),
                            "Dataset Name": obj.get("name"),
                            "Dataset Id": obj.get("id"),
                            "Role Name": e.get("name"),
                            "Model Permission": e.get("modelPermission"),
                            "Table Name": tp.get("name"),
                            "Filter Expression": tp.get("filterExpression"),
                        }

                        df = pd.concat(
                            [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                        )

        return df

    # Dashboard functions
    def list_dashboard_tiles(self) -> pd.DataFrame:

        df = pd.DataFrame(columns=[])

        for w in self.output.get("workspaces", []):
            for obj in w.get("dashboards", []):
                for t in obj.get("tiles", []):
                    new_data = {
                        "Workspace Name": w.get("name"),
                        "Workspace Id": w.get("id"),
                        "Dashboard Name": obj.get("displayName"),
                        "Dashboard Id": obj.get("id"),
                        "Tile Id": t.get("id"),
                        "Title": t.get("title"),
                        "Report Id": t.get("reportId"),
                        "Dataset Id": t.get("datasetId"),
                    }
                    df = pd.concat(
                        [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                    )

        return df

    # User functions
    def list_report_users(self) -> pd.DataFrame:

        df = pd.DataFrame(
            columns=[
                "Workspace Name",
                "Workspace Id",
                "Report Name",
                "Report Id",
                "Report Type",
                "Report User Access Right",
                "Email Address",
                "Display Name",
                "Identifier",
                "Graph Id",
                "Principal Type",
                "User Type",
            ]
        )

        for w in self.output.get("workspaces", []):
            for obj in w.get("reports", []):
                for u in obj.get("users", []):
                    new_data = {
                        "Workspace Name": w.get("name"),
                        "Workspace Id": w.get("id"),
                        "Report Name": obj.get("name"),
                        "Report Id": obj.get("id"),
                        "Report Type": obj.get("reportType"),
                        "Report User Access Right": u.get("reportUserAccessRight"),
                        "Email Address": u.get("emailAddress"),
                        "Display Name": u.get("displayName"),
                        "Identifier": u.get("identifier"),
                        "Graph Id": u.get("graphId"),
                        "Principal Type": u.get("principalType"),
                        "User Type": u.get("userType"),
                    }
                    df = pd.concat(
                        [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                    )

        return df

    def list_dataset_users(self) -> pd.DataFrame:

        df = pd.DataFrame(
            columns=[
                "Workspace Name",
                "Workspace Id",
                "Dataset Name",
                "Dataset Id",
                "Dataset User Access Right",
                "Email Address",
                "Display Name",
                "Identifier",
                "Graph Id",
                "Principal Type",
                "User Type",
            ]
        )

        for w in self.output.get("workspaces", []):
            for obj in w.get("datasets", []):
                for u in obj.get("users", []):
                    new_data = {
                        "Workspace Name": w.get("name"),
                        "Workspace Id": w.get("id"),
                        "Dataset Name": obj.get("name"),
                        "Dataset Id": obj.get("id"),
                        "Dataset User Access Right": u.get("datasetUserAccessRight"),
                        "Email Address": u.get("emailAddress"),
                        "Display Name": u.get("displayName"),
                        "Identifier": u.get("identifier"),
                        "Graph Id": u.get("graphId"),
                        "Principal Type": u.get("principalType"),
                        "User Type": u.get("userType"),
                    }
                    df = pd.concat(
                        [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                    )

        return df

    def list_lakehouse_users(self) -> pd.DataFrame:

        df = pd.DataFrame(
            columns=[
                "Workspace Name",
                "Workspace Id",
                "Lakehouse Name",
                "Lakehouse Id",
                "Artifact User Access Right",
                "Email Address",
                "Display Name",
                "Identifier",
                "Graph Id",
                "Principal Type",
                "User Type",
            ]
        )

        for w in self.output.get("workspaces", []):
            for obj in w.get("Lakehouse", []):
                for u in obj.get("users", []):
                    new_data = {
                        "Workspace Name": w.get("name"),
                        "Workspace Id": w.get("id"),
                        "Lakehouse Name": obj.get("name"),
                        "Lakehouse Id": obj.get("id"),
                        "Artifact User Access Right": u.get("artifactUserAccessRight"),
                        "Email Address": u.get("emailAddress"),
                        "Display Name": u.get("displayName"),
                        "Identifier": u.get("identifier"),
                        "Graph Id": u.get("graphId"),
                        "Principal Type": u.get("principalType"),
                        "User Type": u.get("userType"),
                    }
                    df = pd.concat(
                        [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                    )

        return df

    def list_notebook_users(self) -> pd.DataFrame:

        df = pd.DataFrame(
            columns=[
                "Workspace Name",
                "Workspace Id",
                "Notebook Name",
                "Notebook Id",
                "Artifact User Access Right",
                "Email Address",
                "Display Name",
                "Identifier",
                "Graph Id",
                "Principal Type",
                "User Type",
            ]
        )

        for w in self.output.get("workspaces", []):
            for obj in w.get("Notebook", []):
                for u in obj.get("users", []):
                    new_data = {
                        "Workspace Name": w.get("name"),
                        "Workspace Id": w.get("id"),
                        "Notebook Name": obj.get("name"),
                        "Notebook Id": obj.get("id"),
                        "Artifact User Access Right": u.get("artifactUserAccessRight"),
                        "Email Address": u.get("emailAddress"),
                        "Display Name": u.get("displayName"),
                        "Identifier": u.get("identifier"),
                        "Graph Id": u.get("graphId"),
                        "Principal Type": u.get("principalType"),
                        "User Type": u.get("userType"),
                    }
                    df = pd.concat(
                        [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                    )

        return df

    def list_warehouse_users(self) -> pd.DataFrame:

        df = pd.DataFrame(
            columns=[
                "Workspace Name",
                "Workspace Id",
                "Warehouse Name",
                "Warehouse Id",
                "Datamart User Access Right",
                "Email Address",
                "Display Name",
                "Identifier",
                "Graph Id",
                "Principal Type",
                "User Type",
            ]
        )

        for w in self.output.get("workspaces", []):
            for obj in w.get("warehouses", []):
                for u in obj.get("users", []):
                    new_data = {
                        "Workspace Name": w.get("name"),
                        "Workspace Id": w.get("id"),
                        "Warehouse Name": obj.get("name"),
                        "Warehouse Id": obj.get("id"),
                        "Datamart User Access Right": u.get("datamartUserAccessRight"),
                        "Email Address": u.get("emailAddress"),
                        "Display Name": u.get("displayName"),
                        "Identifier": u.get("identifier"),
                        "Graph Id": u.get("graphId"),
                        "Principal Type": u.get("principalType"),
                        "User Type": u.get("userType"),
                    }
                    df = pd.concat(
                        [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                    )

        return df

    def list_sql_endpoint_users(self) -> pd.DataFrame:

        df = pd.DataFrame(
            columns=[
                "Workspace Name",
                "Workspace Id",
                "SQL Endpoint Name",
                "SQL Endpoint Id",
                "Datamart User Access Right",
                "Email Address",
                "Display Name",
                "Identifier",
                "Graph Id",
                "Principal Type",
                "User Type",
            ]
        )

        for w in self.output.get("workspaces", []):
            for obj in w.get("SQLAnalyticsEndpoint", []):
                for u in obj.get("users", []):
                    new_data = {
                        "Workspace Name": w.get("name"),
                        "Workspace Id": w.get("id"),
                        "SQL Endpoint Name": obj.get("name"),
                        "SQL Endpoint Id": obj.get("id"),
                        "Datamart User Access Right": u.get("datamartUserAccessRight"),
                        "Email Address": u.get("emailAddress"),
                        "Display Name": u.get("displayName"),
                        "Identifier": u.get("identifier"),
                        "Graph Id": u.get("graphId"),
                        "Principal Type": u.get("principalType"),
                        "User Type": u.get("userType"),
                    }
                    df = pd.concat(
                        [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                    )

        return df

    def list_dashboard_users(self) -> pd.DataFrame:

        df = pd.DataFrame(
            columns=[
                "Workspace Name",
                "Workspace Id",
                "Dashboard Name",
                "Dashboard Id",
                "Dashboard User Access Right",
                "Email Address",
                "Display Name",
                "Identifier",
                "Graph Id",
                "Principal Type",
                "User Type",
            ]
        )

        for w in self.output.get("workspaces", []):
            for obj in w.get("dashboards", []):
                for u in obj.get("users", []):
                    new_data = {
                        "Workspace Name": w.get("name"),
                        "Workspace Id": w.get("id"),
                        "Dashboard Name": obj.get("displayName"),
                        "Dashboard Id": obj.get("id"),
                        "Dashboard User Access Right": u.get("appUserAccessRight"),
                        "Email Address": u.get("emailAddress"),
                        "Display Name": u.get("displayName"),
                        "Identifier": u.get("identifier"),
                        "Graph Id": u.get("graphId"),
                        "Principal Type": u.get("principalType"),
                        "User Type": u.get("userType"),
                    }
                    df = pd.concat(
                        [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                    )

        return df

    def list_dataflow_users(self) -> pd.DataFrame:

        df = pd.DataFrame(
            columns=[
                "Workspace Name",
                "Workspace Id",
                "Dataflow Name",
                "Dataflow Id",
                "Dataflow User Access Right",
                "Email Address",
                "Display Name",
                "Identifier",
                "Graph Id",
                "Principal Type",
                "User Type",
            ]
        )

        for w in self.output.get("workspaces", []):
            for obj in w.get("dataflows", []):
                for u in obj.get("users", []):
                    new_data = {
                        "Workspace Name": w.get("name"),
                        "Workspace Id": w.get("id"),
                        "Dataflow Name": obj.get("name"),
                        "Dataflow Id": obj.get("id"),
                        "Dataflow User Access Right": u.get("appUserAccessRight"),
                        "Email Address": u.get("emailAddress"),
                        "Display Name": u.get("displayName"),
                        "Identifier": u.get("identifier"),
                        "Graph Id": u.get("graphId"),
                        "Principal Type": u.get("principalType"),
                        "User Type": u.get("userType"),
                    }
                    df = pd.concat(
                        [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                    )

        return df
