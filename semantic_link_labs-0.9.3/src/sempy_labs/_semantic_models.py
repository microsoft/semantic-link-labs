from uuid import UUID
from typing import Optional
import pandas as pd
from sempy_labs._helper_functions import (
    _create_dataframe,
    _base_api,
    _update_dataframe_datatypes,
    resolve_workspace_name_and_id,
    resolve_dataset_name_and_id,
)


def get_semantic_model_refresh_schedule(
    dataset: str | UUID, workspace: Optional[str | UUID] = None
) -> pd.DataFrame:
    """
    Gets the refresh schedule for the specified dataset from the specified workspace.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        Shows the refresh schedule for the specified dataset from the specified workspace.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace)

    columns = {
        "Days": "str",
        "Times": "str",
        "Enabled": "bool",
        "Local Time Zone Id": "str",
        "Notify Option": "str",
    }

    column_map = {
        "days": "Days",
        "times": "Times",
        "enabled": "Enabled",
        "localTimeZoneId": "Local Time Zone Id",
        "notifyOption": "Notify Option",
    }

    df = _create_dataframe(columns)

    result = _base_api(
        request=f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshSchedule"
    ).json()

    df = (
        pd.json_normalize(result)
        .drop(columns=["@odata.context"], errors="ignore")
        .rename(columns=column_map)
    )

    _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df
