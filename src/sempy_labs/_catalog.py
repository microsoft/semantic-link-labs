import pandas as pd
from sempy._utils._log import log
from sempy_labs._helper_functions import (
    _base_api,
)


def _list_endorsements_or_favorites(type: str = "endorsements"):

    endorsement_map = {
        0: "None",
        1: "Promoted",
        2: "Certified",
        3: "Master data",
    }

    base_payload = {
        "filter": "Favorites" if "favorite" in type.lower() else "endorsement",
        "supportedTypes": [],
        "orderBy": "Default",
    }

    configs = [
        {"configurations": ["Environment", "Variables"]},
        {
            "solutions": [
                "HealthDataManager",
                "HLSCohort",
                "RetailDataManager",
                "SustainabilityDataManager",
            ]
        },
        {
            "processes": [
                "DataFlow",
                "CopyJob",
                "DatabricksCatalog",
                "DataFactory",
                "DataflowFabric",
                "DBTItem",
                "EventStream",
                "FunctionSet",
                "GraphQL",
                "KustoQueryWorkbench",
                "LLMPlugin",
                "MLExperiment",
                "MLModel",
                "MountedDataFactory",
                "MountedRelationalDatabase",
                "OperationalAgents",
                "Pipeline",
                "ApacheAirflowProject",
                "ReflexProject",
                "SparkJobDefinition",
                "SynapseNotebook",
            ]
        },
        {"insights": ["PowerBIReport", "PaginatedReport", "Dashboard"]},
        {
            "insights2": [
                "App",
                "DataExploration",
                "DigitalTwinBuilder",
                "GraphQuerySet",
                "KustoDashboard",
                "LLMPlugin",
                "Map",
                "Ontology",
                "OperationalAgents",
                "OrgApp",
            ]
        },
        {
            "data": [
                "Model",
                "Sql",
                "CosmosDB",
                "DatabricksCatalog",
                "GraphIndex",
                "KustoDatabase",
                "KustoEventHouse",
                "Lakehouse",
                "MountedRelationalDatabase",
                "PgSQLDbNative",
                "SnowflakeDatabase",
                "SqlAnalyticsEndpoint",
                "SQLDbNative",
                "Warehouse",
                "WarehouseSnapshot",
                "Lakewarehouse",
                "MountedWarehouse",
                "Datawarehouse",
            ]
        },
    ]

    rows = []
    for c in configs:
        config_name, supported_types = next(iter(c.items()))

        payload = {**base_payload, "supportedTypes": supported_types}

        response_list = _base_api(
            request="metadata/datahub/V2/artifacts",
            client="internal",
            method="post",
            payload=payload,
        ).json()

        for item in response_list:
            owner = item.get("ownerUser", {})
            artifact_endorsement = item.get("artifactEndorsement", {}).get("stage", 0)
            rows.append(
                {
                    "Item Name": item.get("displayName"),
                    "Item Id": item.get("artifactObjectId"),
                    "Workspace Name": item.get("workspaceName"),
                    "Workspace Id": item.get("workspaceObjectId"),
                    "Item Type": item.get("artifactType"),
                    "Owner": (
                        f"{owner.get('givenName')} {owner.get('familyName')}"
                        if owner
                        else None
                    ),
                    "Owner Email Address": owner.get("emailAddress"),
                    "Endorsement": endorsement_map.get(artifact_endorsement),
                    "Sensitivity Label": item.get(
                        "artifactInformationProtection", {}
                    ).get("name"),
                }
            )

    return pd.DataFrame(rows)


@log
def list_endorsements() -> pd.DataFrame:
    """
    Lists all endorsed items.

    Returns
    -------
    pandas.DataFrame
        A pandas DataFrame containing the list of endorsed items.
    """

    return _list_endorsements_or_favorites(type="endorsements")


@log
def list_favorites() -> pd.DataFrame:
    """
    Lists all favorite items.

    Returns
    -------
    pandas.DataFrame
        A pandas DataFrame containing the list of favorite items.
    """

    return _list_endorsements_or_favorites(type="favorites")
