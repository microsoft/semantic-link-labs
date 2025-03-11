import sempy.fabric as fabric
from typing import Optional
from uuid import UUID
from sempy_labs._helper_functions import (
    resolve_dataset_id,
)
import gzip
import base64
import urllib.parse


def generate_dax_query_view_url(
    dataset: str | UUID, dax_string: str, workspace: Optional[str | UUID] = None
):
    """
    Prints a URL to the DAX query view of a dataset in Power BI.
    This URL can be used to view the DAX query in the Power BI service.

    Parameters
    ----------
    dataset : str | uuid.UUID
        The dataset name or ID.
    dax_string : str
        The DAX query string.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    workspace_id = fabric.resolve_workspace_id(workspace)
    dataset_id = resolve_dataset_id(dataset=dataset, workspace=workspace_id)

    def gzip_base64_urlsafe(input_string):
        # Compress the string with gzip
        compressed_data = gzip.compress(input_string.encode("utf-8"))

        # Encode the compressed data in base64
        base64_data = base64.b64encode(compressed_data)

        # Make the base64 string URL-safe
        urlsafe_data = urllib.parse.quote_plus(base64_data.decode("utf-8"))

        return urlsafe_data

    formatted_query = gzip_base64_urlsafe(dax_string)

    url = f"https://app.powerbi.com/groups/{workspace_id}/modeling/{dataset_id}/daxQueryView?query={formatted_query}"

    print(url)
