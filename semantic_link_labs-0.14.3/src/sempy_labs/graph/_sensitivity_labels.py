import pandas as pd
from uuid import UUID
from typing import Optional
from sempy_labs._helper_functions import (
    _base_api,
    _create_dataframe,
    _update_dataframe_datatypes,
    _is_valid_uuid,
)
from sempy._utils._log import log


@log
def list_sensitivity_labels(user: Optional[str | UUID] = None) -> pd.DataFrame:
    """
    Get a list of `sensitivity label <https://learn.microsoft.com/graph/api/resources/security-sensitivitylabel>`_ objects associated with a user or organization.

    This is a wrapper function for the following API: `List sensitivityLabels https://learn.microsoft.com/graph/api/security-informationprotection-list-sensitivitylabels>`_.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    user : str | uuid.UUID
        The user ID or user principal name.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing a list of `sensitivity label <https://learn.microsoft.com/graph/api/resources/security-sensitivitylabel>`_ objects associated with a user or organization.
    """
    from sempy_labs.graph import resolve_user_id

    url = "/security/informationProtection/sensitivityLabels"

    if user is not None:
        user_id = resolve_user_id(user=user)
        url = f"users/{user_id}{url}"

    result = _base_api(request=url, client="graph").json()

    columns = {
        "Sensitivity Label Id": "str",
        "Sensitivity Label Name": "str",
        "Description": "str",
        "Color": "str",
        "Sensitivity": "int",
        "Tooltip": "str",
        "Is Active": "bool",
        "Is Appliable": "bool",
        "Has Protection": "bool",
        "Parent Sensitivity Label Id": "str",
        "Parent Sensitivity Label Name": "str",
    }
    df = _create_dataframe(columns=columns)

    rows = []
    for item in result.get("value", []):
        row = {
            "Sensitivity Label Id": item.get("id"),
            "Sensitivity Label Name": item.get("name"),
            "Description": item.get("description"),
            "Color": item.get("color"),
            "Sensitivity": item.get("sensitivity"),
            "Tooltip": item.get("tooltip"),
            "Is Active": item.get("isActive"),
            "Is Appliable": item.get("isAppliable"),
            "Has Protection": item.get("hasProtection"),
            "Parent Sensitivity Label Id": (
                item.get("parent", {}).get("id") if item.get("parent") else None
            ),
            "Parent Sensitivity Label Name": (
                item.get("parent", {}).get("name") if item.get("parent") else None
            ),
        }
        rows.append(row)

    if rows:
        df = pd.DataFrame(rows, columns=list(columns.keys()))
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df


@log
def resolve_sensitivity_label_id(
    label: str | UUID, user: Optional[str | UUID] = None
) -> UUID | None:
    """
    Resolve a sensitivity label name or ID to its corresponding sensitivity label ID.

    Service Principal Authentication is required (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    label : str | uuid.UUID
        The name or ID of the sensitivity label.
    user : str | uuid.UUID, default=None
        The user ID or user principal name.

    Returns
    -------
    uuid.UUID | None
        The ID of the sensitivity label if found, otherwise None.
    """

    if _is_valid_uuid(label):
        return str(label)

    df = list_sensitivity_labels(user=user)

    if df.empty:
        return None

    # Try to find the label by name
    label_row = df[df["Sensitivity Label Name"] == label]
    if not label_row.empty:
        return label_row["Sensitivity Label Id"].iloc[0]

    return None
