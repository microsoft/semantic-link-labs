import sempy.fabric as fabric
from sempy_labs._helper_functions import resolve_dataset_id, is_default_semantic_model
from typing import Optional
import sempy_labs._icons as icons


def clear_cache(dataset: str, workspace: Optional[str] = None):
    """
    Clears the cache of a semantic model.
    See `here <https://learn.microsoft.com/analysis-services/instances/clear-the-analysis-services-caches?view=asallproducts-allversions>`_ for documentation.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    workspace = fabric.resolve_workspace_name(workspace)
    if is_default_semantic_model(dataset=dataset, workspace=workspace):
        raise ValueError(
            f"{icons.red_dot} Cannot run XMLA operations against a default semantic model. Please choose a different semantic model. "
            "See here for more information: https://learn.microsoft.com/fabric/data-warehouse/semantic-models"
        )

    dataset_id = resolve_dataset_id(dataset=dataset, workspace=workspace)

    xmla = f"""
            <ClearCache xmlns="http://schemas.microsoft.com/analysisservices/2003/engine">
                <Object>
                    <DatabaseID>{dataset_id}</DatabaseID>
                </Object>
            </ClearCache>
            """
    fabric.execute_xmla(dataset=dataset, xmla_command=xmla, workspace=workspace)

    outputtext = f"{icons.green_dot} Cache cleared for the '{dataset}' semantic model within the '{workspace}' workspace."

    return outputtext


def backup_semantic_model(
    dataset: str,
    file_path: str,
    allow_overwrite: Optional[bool] = True,
    apply_compression: Optional[bool] = True,
    workspace: Optional[str] = None,
):
    # https://learn.microsoft.com/en-us/power-bi/enterprise/service-premium-backup-restore-dataset

    if not file_path.endswith(".abf"):
        raise ValueError(
            f"{icons.red_dot} The backup file for restoring must be in the .abf format."
        )

    workspace = fabric.resolve_workspace_name(workspace)

    tmsl = {
        "backup": {
            "database": dataset,
            "file": file_path,
            "allowOverwrite": allow_overwrite,
            "applyCompression": apply_compression,
        }
    }

    fabric.execute_tmsl(script=tmsl, workspace=workspace)
    print(
        f"{icons.green_dot} The '{dataset}' semantic model within the '{workspace}' workspace has been backed up to the '{file_path}' location."
    )


def restore_semantic_model(
    dataset: str,
    file_path: str,
    allow_overwrite: Optional[bool] = True,
    ignore_incompatibilities: Optional[bool] = True,
    workspace: Optional[str] = None,
):
    # https://learn.microsoft.com/en-us/power-bi/enterprise/service-premium-backup-restore-dataset

    if not file_path.endswith(".abf"):
        raise ValueError(
            f"{icons.red_dot} The backup file for restoring must be in the .abf format."
        )

    workspace = fabric.resolve_workspace_name(workspace)

    tmsl = {
        "restore": {
            "database": dataset,
            "file": file_path,
            "allowOverwrite": allow_overwrite,
            "security": "copyAll",
            "ignoreIncompatibilities": ignore_incompatibilities,
        }
    }

    fabric.execute_tmsl(script=tmsl, workspace=workspace)
    print(
        f"{icons.green_dot} The '{dataset}' semantic model has been restored to the '{workspace}' workspace based on teh '{file_path}' backup file."
    )
