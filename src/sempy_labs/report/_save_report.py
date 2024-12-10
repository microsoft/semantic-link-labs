import os
import base64
import json
import sempy.fabric as fabric
import sempy_labs._icons as icons
from typing import Literal
from sempy_labs.report._generate_report import get_report_definition
from sempy_labs._generate_semantic_model import get_semantic_model_definition
from sempy_labs._helper_functions import _is_valid_uuid
from uuid import UUID
from sempy_labs.lakehouse import lakehouse_attached


def save_report_as_pbip(
    report: str | UUID,
    workspace: str | UUID,
    thin_or_thick: Literal["thick", "thin"] = "thick",
    live_connect: bool = True,
):
    """
    Saves a report as a .pbip file to the default lakehouse attached to the notebook.

    Parameters
    ----------
    report : str | UUID
        Name or Id of the Power BI report.
    workspace : str | UUID, default=None
        The name of the Fabric workspace in which the report resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    thin_or_thick : str, default="thick"
        Specifying 'thin' will save just the report, not the semantic model.
        Specifing 'thick' will save both the report and underlying semantic model.
    live_connect : bool, default=True
        If set to True, saves a .pbip live-connected to the workspace in the Power BI / Fabric service.
        If set to False, saves a .pbip with a local model, independent from the Power BI / Fabric service.
    """

    if thin_or_thick not in ["thin", "thick"]:
        raise ValueError(
            f"{icons.red_dot} The parameter 'thin_or_thick' must be either 'thin or 'thick'."
        )

    if not lakehouse_attached():
        raise ValueError(
            f"{icons.red_dot} A lakehouse must be attached to the notebook. Please attach a lakehouse to the notebook."
        )

    save_location = "/lakehouse/default/Files"
    report_workspace = fabric.resolve_workspace_name(workspace)
    indent = 2

    # Find semantic model info
    dfR = fabric.list_reports(workspace=report_workspace)
    if _is_valid_uuid(report):
        dfR_filt = dfR[(dfR["Report Type"] == "PowerBIReport") & (dfR["Id"] == report)]
    else:
        dfR_filt = dfR[
            (dfR["Report Type"] == "PowerBIReport") & (dfR["Name"] == report)
        ]
    if dfR_filt.empty:
        raise ValueError(
            f"{icons.red_dot} The '{report} report does not exist within the '{report_workspace} workspace."
        )

    report_name = dfR_filt["Name"].iloc[0]
    dataset_id = dfR_filt["Dataset Id"].iloc[0]
    dataset_workspace_id = dfR_filt["Dataset Workspace Id"].iloc[0]
    dataset_name = fabric.resolve_item_name(
        item_id=dataset_id, workspace=dataset_workspace_id, type="SemanticModel"
    )
    dataset_workspace = fabric.resolve_workspace_name(dataset_workspace_id)
    path_prefix = f"{save_location}/{report_workspace}/{report_name}/{report_name}"

    # Local model not supported if the report and model are in different workspaces
    if dataset_workspace != report_workspace and not live_connect:
        live_connect = True
        print(
            f"{icons.warning} The '{report_name}' report from the '{report_workspace}' workspace is being saved as a live-connected report/model."
        )

    def add_files(name, type, object_workspace):

        path_prefix_full = f"{path_prefix}.{type}"

        if type == "Report":
            dataframe = get_report_definition(report=name, workspace=report_workspace)
        elif type == "SemanticModel":
            dataframe = get_semantic_model_definition(
                dataset=name, workspace=object_workspace
            )
        else:
            raise NotImplementedError

        # Create and save files based on dataset/report definition
        for _, r in dataframe.iterrows():
            path = r["path"]
            file_content = base64.b64decode(r["payload"])
            file_path = f"{path_prefix_full}/{path}"
            os.makedirs(os.path.dirname(file_path), exist_ok=True)

            # Update the definition.pbir file for local models
            if not live_connect and type == "Report" and path == "definition.pbir":
                file_content = {
                    "version": "1.0",
                    "datasetReference": {
                        "byPath": {"path": f"../{report_name}.SemanticModel"},
                        "byConnection": None,
                    },
                }

                with open(file_path, "w") as f:
                    json.dump(file_content, f, indent=indent)
            else:
                with open(file_path, "wb") as f:
                    f.write(file_content)

        # Create and save .pbip file for report, converting the file extension
        if type == "Report":
            # Standard .pbip file content
            pbip = {
                "version": "1.0",
                "artifacts": [{"report": {"path": f"{report_name}.Report"}}],
                "settings": {"enableAutoRecovery": True},
            }
            # Ensure the directory exists
            os.makedirs(os.path.dirname(path_prefix), exist_ok=True)
            # Write the .pbip file directly
            pbip_final = f"{path_prefix}.pbip"
            with open(pbip_final, "w") as file:
                json.dump(pbip, file, indent=indent)

    add_files(name=report_name, type="Report", object_workspace=report_workspace)
    if thin_or_thick == "thick":
        add_files(
            name=dataset_name, type="SemanticModel", object_workspace=dataset_workspace
        )
    print(
        f"{icons.green_dot} The '{report_name}' report within the '{report_workspace}' workspace has been saved to this location: {save_location}."
    )
