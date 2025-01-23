import sempy.fabric as fabric
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    pagination,
)
import pandas as pd
from typing import Optional
from sempy.fabric.exceptions import FabricHTTPException
from uuid import UUID


def _list_api_wrapper(
    type: str,
    workspace: Optional[str | UUID] = None,
    additional_columns: Optional[dict] = None,
) -> pd.DataFrame:

    apis = {
        "Dashboard": ["Dashboard", "dashboards"],
        "DataPipeline": ["Data Pipeline", "dataPipelines"],
        "Datamart": ["Datamart", "datamarts"],
        "Environment": ["Environment", "environments"],
        "Eventhouse": ["Eventhouse", "eventhouses"],
        "Eventstream": ["Eventstream", "eventstream"],
        "KQLDashboard": ["KQL Dashboard", "kqlDashboards"],
        "KQLDatabase": ["KQL Database", "kqlDatabases"],
        "KQLQueryset": ["KQL Queryset", "kqlQuerysets"],
        "Lakehouse": ["Lakehouse", "lakehouses"],
        "MLExperiment": ["ML Experiment", "mlExperiments"],
        "MLModel": ["ML Model", "mlModels"],
        "MirroredDatabase": ["Mirrored Database", "mirroredDatabases"],
        "MirroredWarehouse": ["Mirrored Warehouse", "mirroredWarehouses"],
        "Notebook": ["Notebook", "notebooks"],
        "Reflex": ["Reflex", "reflexes"],
        "Report": ["Report", "reports"],
        "SQLEndpoint": ["SQL Endpoint", "sqlEndpoints"],
        "SemanticModel": ["Semantic Model", "semanticModels"],
        "SparkJobDefinition": ["Spark Job Definition", "sparkJobDefinitions"],
        "Warehouse": ["Warehouse", "warehouses"],
    }

    proper_type = apis.get(type)[0]
    api_call = apis.get(type)[1]

    base_col_mapping = {
        f"{proper_type} Id": {"id": "string"},
        f"{proper_type} Name": {"displayName": "string"},
        "Description": {"description": "string"},
    }

    if additional_columns is not None:
        col_mapping = base_col_mapping | additional_columns
    else:
        col_mapping = base_col_mapping

    df = pd.DataFrame(columns=list(col_mapping.keys()))

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}/{api_call}")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    for r in responses:
        for v in r.get("value", []):
            new_data = {k: v.get(vk) for k, vk in col_mapping.items()}
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df
