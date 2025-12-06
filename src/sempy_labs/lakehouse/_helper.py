from uuid import UUID
from typing import Optional, Literal
import pyarrow.dataset as ds
from sempy_labs._helper_functions import (
    _mount,
    delete_item,
    _base_api,
    resolve_workspace_name_and_id,
    resolve_lakehouse_name_and_id,
)
from sempy._utils._log import log
import sempy_labs._icons as icons
import os
import json


@log
def is_v_ordered(
    table_name: str,
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
    schema: Optional[str] = None,
) -> bool:
    """
    Checks if a delta table is v-ordered.

    Parameters
    ----------
    table_name : str
        The name of the table to check.
    lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    schema : str, optional
        The schema of the table to check. If not provided, the default schema is used.

    Returns
    -------
    bool
        True if the table is v-ordered, False otherwise.
    """

    local_path = _mount(lakehouse=lakehouse, workspace=workspace)
    table_path = (
        f"{local_path}/Tables/{schema}/{table_name}"
        if schema
        else f"{local_path}/Tables/{table_name}"
    )
    ds_schema = ds.dataset(table_path).schema.metadata

    if ds_schema:
        return any(b"vorder" in key for key in ds_schema.keys())

    delta_log_path = os.path.join(table_path, "_delta_log")

    def read_vorder_tag(delta_log_path):
        json_files = sorted(
            [f for f in os.listdir(delta_log_path) if f.endswith(".json")], reverse=True
        )

        if not json_files:
            return False

        latest_file = os.path.join(delta_log_path, json_files[0])

        with open(latest_file, "r") as f:
            all_data = [
                json.loads(line) for line in f if line.strip()
            ]  # one dict per line
            for data in all_data:
                if "metaData" in data:
                    return (
                        data.get("metaData", {})
                        .get("configuration", {})
                        .get("delta.parquet.vorder.enabled", "false")
                        == "true"
                    )

            # If no metaData, fall back to commitInfo
            for data in all_data:
                if "commitInfo" in data:
                    tags = data["commitInfo"].get("tags", {})
                    return tags.get("VORDER", "false").lower() == "true"

        return False  # Default if not found

    return read_vorder_tag(delta_log_path)


@log
def delete_lakehouse(
    lakehouse: str | UUID, workspace: Optional[str | UUID] = None
) -> None:
    """
    Deletes a lakehouse.

    This is a wrapper function for the following API: `Items - Delete Lakehouse <https://learn.microsoft.com/rest/api/fabric/lakehouse/items/delete-lakehouse>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    lakehouse : str | uuid.UUID
        The name or ID of the lakehouse to delete.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    delete_item(item=lakehouse, item_type="lakehouse", workspace=workspace)


@log
def update_lakehouse(
    name: Optional[str] = None,
    description: Optional[str] = None,
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Updates a lakehouse.

    This is a wrapper function for the following API: `Items - Update Lakehouse <https://learn.microsoft.com/rest/api/fabric/lakehouse/items/update-lakehouse>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    name: str, default=None
        The new name of the lakehouse.
        Defaults to None which does not update the name.
    description: str, default=None
        The new description of the lakehouse.
        Defaults to None which does not update the description.
    lakehouse : str | uuid.UUID, default=None
        The name or ID of the lakehouse to update.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    if not name and not description:
        raise ValueError(
            f"{icons.red_dot} Either name or description must be provided."
        )

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (lakehouse_name, lakehouse_id) = resolve_lakehouse_name_and_id(
        lakehouse, workspace_id
    )

    payload = {}
    if name:
        payload["displayName"] = name
    if description:
        payload["description"] = description

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}",
        method="patch",
        client="fabric_sp",
        payload=payload,
    )

    print(
        f"{icons.green_dot} The '{lakehouse_name}' lakehouse within the '{workspace_name}' workspace has been updated accordingly."
    )


@log
def load_table(
    table_name: str,
    file_path: str,
    mode: Literal["Overwrite", "Append"],
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Loads a table into a lakehouse. Currently only files are supported, not folders.

    This is a wrapper function for the following API: `Tables - Load Table <https://learn.microsoft.com/rest/api/fabric/lakehouse/tables/load-table>`_.

    Service Principal Authentication is supported (see `here <https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb>`_ for examples).

    Parameters
    ----------
    table_name : str
        The name of the table to load.
    file_path : str
        The path to the data to load.
    mode : Literal["Overwrite", "Append"]
        The mode to use when loading the data.
        "Overwrite" will overwrite the existing data.
        "Append" will append the data to the existing data.
    lakehouse : str | uuid.UUID, default=None
        The name or ID of the lakehouse to load the table into.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (lakehouse_name, lakehouse_id) = resolve_lakehouse_name_and_id(
        lakehouse, workspace_id
    )

    file_extension = os.path.splitext(file_path)[1]

    payload = {
        "relativePath": file_path,
        "pathType": "File",
        "mode": mode,
        "formatOptions": {},
    }

    if file_extension == ".csv":
        payload["formatOptions"] = {"format": "Csv", "header": True, "delimiter": ","}
    elif file_extension == ".parquet":
        payload["formatOptions"] = {
            "format": "Parquet",
            "header": True,
        }
    # Solve for loading folders
    # elif file_extension == '':
    #    payload['pathType'] = "Folder"
    #    payload["recursive"] = recursive
    #    payload['formatOptions']
    else:
        raise NotImplementedError()

    _base_api(
        request=f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables/{table_name}/load",
        client="fabric_sp",
        method="post",
        status_codes=202,
        lro_return_status_code=True,
    )

    print(
        f"{icons.green_dot} The '{table_name}' table has been loaded into the '{lakehouse_name}' lakehouse within the '{workspace_name}' workspace."
    )
