import pandas as pd
from sempy_labs._helper_functions import (
    _base_api,
    _create_dataframe,
)
from sempy._utils._log import log


@log
def list_widely_shared_artifacts(
    api_name: str = "LinksSharedToWholeOrganization",
) -> pd.DataFrame:
    """
    Returns a list of Power BI reports that are shared with the whole organization through links or a list of Power BI items (such as reports or dashboards) that are published to the web.

    This is a wrapper function for the following APIs:
    `Admin - WidelySharedArtifacts LinksSharedToWholeOrganization <https://learn.microsoft.com/rest/api/power-bi/admin/widely-shared-artifacts-links-shared-to-whole-organization>`_.
    `Admin - WidelySharedArtifacts PublishedToWeb <https://learn.microsoft.com/rest/api/power-bi/admin/widely-shared-artifacts-published-to-web>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    api_name : str, default = "LinksSharedToWholeOrganization"
        The name of the API to call. Either "LinksSharedToWholeOrganization" or "PublishedToWeb".

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of Power BI reports that are shared with the whole organization through links or a list of Power BI items (such as reports or dashboards) that are published to the web.
    """

    columns = {
        "Artifact Id": "string",
        "Artifact Name": "string",
        "Artifact Type": "string",
        "Access Right": "string",
        "Share Type": "string",
        "Sharer Name": "string",
        "Sharer Email Address": "string",
        "Sharer Identifier": "string",
        "Sharer Graph Id": "string",
        "Sharer Principal Type": "string",
    }

    df = _create_dataframe(columns=columns)

    api = (
        "linksSharedToWholeOrganization"
        if api_name == "LinksSharedToWholeOrganization"
        else "publishedToWeb"
    )

    responses = _base_api(
        request=f"/v1.0/myorg/admin/widelySharedArtifacts/{api}",
        client="fabric_sp",
        uses_pagination=True,
    )

    rows = []
    for r in responses:
        for v in r.get("ArtifactAccessEntities", []):
            sharer = v.get("sharer", {})
            rows.append(
                {
                    "Artifact Id": v.get("artifactId"),
                    "Artifact Name": v.get("displayName"),
                    "Artifact Type": v.get("artifactType"),
                    "Access Right": v.get("accessRight"),
                    "Share Type": v.get("shareType"),
                    "Sharer Name": sharer.get("displayName"),
                    "Sharer Email Address": sharer.get("emailAddress"),
                    "Sharer Identifier": sharer.get("identifier"),
                    "Sharer Graph Id": sharer.get("graphId"),
                    "Sharer Principal Type": sharer.get("principalType"),
                }
            )

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))

    return df
