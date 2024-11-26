import sempy.fabric as fabric
from sempy.fabric.exceptions import FabricHTTPException
from sempy_labs._helper_functions import (
    pagination,
)
import pandas as pd
from sempy_labs.admin._basic_functions import list_workspaces


def list_git_connections() -> pd.DataFrame:
    """
    Shows a list of Git connections.

    This is a wrapper function for the following API: `Workspaces - List Git Connections <https://learn.microsoft.com/rest/api/fabric/admin/workspaces/list-git-connections>`_.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of Git connections.
    """

    client = fabric.FabricRestClient()
    response = client.get("/v1/admin/workspaces/discoverGitConnections")

    df = pd.DataFrame(
        columns=[
            "Workspace Id",
            "Organization Name",
            "Owner Name",
            "Project Name",
            "Git Provider Type",
            "Repository Name",
            "Branch Name",
            "Directory Name",
        ]
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)

    responses = pagination(client, response)

    for r in responses:
        for v in r.get("value", []):
            git = v.get("gitProviderDetails", {})
            new_data = {
                "Workspace Id": v.get("workspaceId"),
                "Organization Name": git.get("organizationName"),
                "Owner Name": git.get("ownerName"),
                "Project Name": git.get("projectName"),
                "Git Provider Type": git.get("gitProviderType"),
                "Repository Name": git.get("repositoryName"),
                "Branch Name": git.get("branchName"),
                "Directory Name": git.get("directoryName"),
            }

            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    dfW = list_workspaces()
    df = pd.merge(
        df, dfW[["Id", "Name"]], left_on="Workspace Id", right_on="Id", how="left"
    )
    new_col_name = "Workspace Name"
    df = df.rename(columns={"Name": new_col_name})
    df.insert(1, new_col_name, df.pop(new_col_name))

    df = df.drop(columns=["Id"])

    return df
