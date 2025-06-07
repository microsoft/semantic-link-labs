import pandas as pd
from sempy_labs._helper_functions import (
    _base_api,
)
from uuid import UUID
from typing import Optional
from sempy_labs.admin._basic_functions import (
    _resolve_workspace_name_and_id,
    _create_dataframe,
    _update_dataframe_datatypes,
)


def list_unused_artifacts(workspace: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Returns a list of datasets, reports, and dashboards that have not been used within 30 days for the specified workspace.

    This is a wrapper function for the following API: `Admin - Groups GetUnusedArtifactsAsAdmin <https://learn.microsoft.com/rest/api/power-bi/admin/groups-get-unused-artifacts-as-admin>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of datasets, reports, and dashboards that have not been used within 30 days for the specified workspace.
    """

    (workspace_name, workspace_id) = _resolve_workspace_name_and_id(workspace)

    columns = {
        "Artifact Name": "string",
        "Artifact Id": "string",
        "Artifact Type": "string",
        "Artifact Size in MB": "string",
        "Created Date Time": "datetime",
        "Last Accessed Date Time": "datetime",
    }

    df = _create_dataframe(columns=columns)

    responses = _base_api(
        request=f"/v1.0/myorg/admin/groups/{workspace_id}/unused",
        client="fabric_sp",
        uses_pagination=True,
    )

    for r in responses:
        for i in r.get("unusedArtifactEntities", []):
            new_data = {
                "Artifact Name": i.get("displayName"),
                "Artifact Id": i.get("artifactId"),
                "Artifact Type": i.get("artifactType"),
                "Artifact Size in MB": i.get("artifactSizeInMB"),
                "Created Date Time": i.get("createdDateTime"),
                "Last Accessed Date Time": i.get("lastAccessedDateTime"),
            }

            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df
