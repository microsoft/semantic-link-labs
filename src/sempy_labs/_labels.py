import sempy.fabric as fabric
import requests
import pandas as pd
from typing import Optional, Union
from uuid import UUID
from sempy.fabric.exceptions import FabricHTTPException
from sempy._utils._log import log
from sempy_labs._helper_functions import (
    _get_url_prefix,
)


@log
def list_item_labels(workspace: Optional[Union[str, UUID]] = None) -> pd.DataFrame:
    """
    List all items within a workspace and shows their sensitivity labels.

    NOTE: This function uses an internal API and is subject to change/break without notice.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of all items within a workspace and their sensitivity labels.
    """

    import notebookutils

    token = notebookutils.credentials.getToken("pbi")
    headers = {"Authorization": f"Bearer {token}"}

    # Item types handled in special payload fields
    grouped_types = {
        "dashboards": "Dashboard",
        "reports": "Report",
        "models": "SemanticModel",
        "dataflows": "Dataflow",
        "datamarts": "Datamart",
    }

    # All other item types go into 'artifacts'
    fabric_items = [
        "Datamart",
        "Lakehouse",
        "Eventhouse",
        "Environment",
        "KQLDatabase",
        "KQLQueryset",
        "KQLDashboard",
        "DataPipeline",
        "Notebook",
        "SparkJobDefinition",
        "MLExperiment",
        "MLModel",
        "Warehouse",
        "Eventstream",
        "SQLEndpoint",
        "MirroredWarehouse",
        "MirroredDatabase",
        "Reflex",
        "GraphQLApi",
        "MountedDataFactory",
        "SQLDatabase",
        "CopyJob",
        "VariableLibrary",
        "Dataflow",
        "ApacheAirflowJob",
        "WarehouseSnapshot",
        "DigitalTwinBuilder",
        "DigitalTwinBuilderFlow",
        "MirroredAzureDatabricksCatalog",
        "DataAgent",
        "UserDataFunction",
    ]

    dfI = fabric.list_items(workspace=workspace)

    payload = {
        key: [{"artifactId": i} for i in dfI[dfI["Type"] == value]["Id"].tolist()]
        for key, value in grouped_types.items()
    }

    # Add generic artifact types
    artifact_ids = dfI[dfI["Type"].isin(fabric_items)]["Id"].tolist()
    if artifact_ids:
        payload["artifacts"] = [{"artifactId": i} for i in artifact_ids]

    prefix = _get_url_prefix()

    response = requests.post(
        f"{prefix}/metadata/informationProtection/artifacts",
        json=payload,
        headers=headers,
    )
    if response.status_code != 200:
        raise FabricHTTPException(f"Failed to retrieve labels: {response.text}")
    result = response.json()

    label_keys = [
        "artifactInformationProtections",
        "datasetInformationProtections",
        "reportInformationProtections",
        "dashboardInformationProtections",
    ]

    rows = [
        {
            "Id": item.get("artifactObjectId"),
            "Label Id": item.get("labelId"),
            "Label Name": item.get("name"),
            "Parent Label Name": item.get("parent", {}).get("name"),
            "Label Description": item.get("tooltip"),
        }
        for key in label_keys
        for item in result.get(key, [])
    ]

    df_labels = pd.DataFrame(rows)
    return dfI.merge(df_labels, on="Id", how="left")
