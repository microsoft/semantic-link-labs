from sempy_labs._helper_functions import (
    _base_api,
    _create_dataframe,
)
import pandas as pd
from sempy_labs.admin._basic_functions import list_workspaces
from sempy._utils._log import log


@log
def list_git_connections() -> pd.DataFrame:
    """
    Shows a list of Git connections.

    This is a wrapper function for the following API: `Workspaces - List Git Connections <https://learn.microsoft.com/rest/api/fabric/admin/workspaces/list-git-connections>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of Git connections.
    """

    columns = {
        "Workspace Id": "string",
        "Organization Name": "string",
        "Owner Name": "string",
        "Project Name": "string",
        "Git Provider Type": "string",
        "Repository Name": "string",
        "Branch Name": "string",
        "Directory Name": "string",
    }
    df = _create_dataframe(columns=columns)

    responses = _base_api(
        request="/v1/admin/workspaces/discoverGitConnections",
        client="fabric_sp",
        uses_pagination=True,
    )

    rows = []
    for r in responses:
        for v in r.get("value", []):
            git = v.get("gitProviderDetails", {})
            rows.append(
                {
                    "Workspace Id": v.get("workspaceId"),
                    "Organization Name": git.get("organizationName"),
                    "Owner Name": git.get("ownerName"),
                    "Project Name": git.get("projectName"),
                    "Git Provider Type": git.get("gitProviderType"),
                    "Repository Name": git.get("repositoryName"),
                    "Branch Name": git.get("branchName"),
                    "Directory Name": git.get("directoryName"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        dfW = list_workspaces()
        df = pd.merge(
            df, dfW[["Id", "Name"]], left_on="Workspace Id", right_on="Id", how="left"
        )
        new_col_name = "Workspace Name"
        df = df.rename(columns={"Name": new_col_name})
        df.insert(1, new_col_name, df.pop(new_col_name))

        df = df.drop(columns=["Id"])

    return df
