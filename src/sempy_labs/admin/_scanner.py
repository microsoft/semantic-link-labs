import sempy.fabric as fabric
from typing import Optional, List
from sempy.fabric.exceptions import FabricHTTPException
import pandas as pd
import time
import sempy_labs._icons as icons


def scan_workspaces(
    data_source_details: bool = False,
    dataset_schema: bool = False,
    dataset_expressions: bool = False,
    lineage: bool = False,
    artifact_users: bool = False,
    workspace: Optional[str | List[str]] = None,
) -> dict:

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

    response_clause = f"/v1.0/myorg/admin/workspaces/getInfo?lineage={lineage}&datasourceDetails={data_source_details}&datasetSchema={dataset_schema}&datasetExpressions={dataset_expressions}&getArtifactUsers={artifact_users}"
    response = client.post(response_clause, json=request_body)

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

    def list_kql_databases(self):

        df = pd.DataFrame(columns=[])

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

    def list_eventhouses(self):

        df = pd.DataFrame(columns=[])

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

    def list_kql_querysets(self):

        df = pd.DataFrame(columns=[])

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

    def list_lakehouses(self):

        df = pd.DataFrame(columns=[])

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
                    "Modified By Id": obj.get("modfiedById"),
                    "Created By Id": obj.get("createdById"),
                    "OneLake Tables Path": ep.get("OneLakeTablesPath"),
                    "OneLake Files Path": ep.get("OneLakeFilesPath"),
                    "DW Properties": ep.get("DwProperties"),
                    "Datasource Usages": ds_list,
                }
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )

        return df

    def list_notebooks(self):

        df = pd.DataFrame(columns=[])

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

    def list_reports(self):

        df = pd.DataFrame(columns=[])

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
                }
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )

        return df

    def list_datasets(self):

        df = pd.DataFrame(columns=[])

        for w in self.output.get("workspaces", []):
            for obj in w.get("datasets", []):
                new_data = {
                    "Workspace Name": w.get("name"),
                    "Workspace Id": w.get("id"),
                    "Dataset Name": obj.get("name"),
                    "Dataset Id": obj.get("id"),
                    "Configured By": obj.get("configuredBy"),
                    "Configured By Id": obj.get("configuredById"),
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
                }
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )

        return df

    def list_warehouses(self):

        df = pd.DataFrame(columns=[])

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
                }
                df = pd.concat(
                    [df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )

        return df

    def list_sql_endpoints(self):

        df = pd.DataFrame(columns=[])

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

    def list_data_source_instances(self):

        df = pd.DataFrame(columns=[])

        for obj in self.output.get("datasourceInstances", []):
            new_data = {
                "Data Source Type": obj.get("datasourceType"),
                "Server": obj.get("connectionDetails", {}).get("datasourceType"),
                "Path": obj.get("connectionDetails", {}).get("path"),
                "datasourceId": obj.get("datasourceId"),
                "gatewayId": obj.get("gatewayId"),
            }
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

        return df
