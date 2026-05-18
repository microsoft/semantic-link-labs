from uuid import UUID
from typing import Optional
import pandas as pd
from sempy_labs._helper_functions import (
    _create_dataframe,
    resolve_workspace_id,
    resolve_item_id,
    _update_dataframe_datatypes,
)
from sempy._utils._log import log
import ast
import sempy_labs._icons as icons


@log
def list_mini_models(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    show_filters: bool = False,
) -> pd.DataFrame:

    from sempy_labs.tom import connect_semantic_model

    with connect_semantic_model(
        dataset=dataset, workspace=workspace, readonly=True
    ) as tom:
        return tom.list_mini_models(show_filters=shows_filters)


@log
def _get_master_model(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    show_filters: bool = False,
) -> pd.DataFrame:
    from sempy_labs.tom import connect_semantic_model

    workspace_id = resolve_workspace_id(workspace)
    item_id = resolve_item_id(
        item=dataset, type="SemanticModel", workspace=workspace_id
    )

    columns = {
        "Mini Model Name": "str",
        "Semantic Model Name": "str",
        "Semantic Model Id": "str",
        "Workspace Name": "str",
        "Workspace Id": "str",
        "Last Updated Date": "datetime",
    }

    if show_filters:
        columns.update(
            {
                "Table Name": "str",
                "Filter Value": "str",
            }
        )

    df = _create_dataframe(columns=columns)

    rows = []
    with connect_semantic_model(
        dataset=item_id, workspace=workspace_id, readonly=True
    ) as tom:
        for a in tom.model.Annotations:
            if not a.Name.startswith(icons.prefix_master):
                continue

            mini_model_name = a.Name.split("_")[2]

            try:
                ann = ast.literal_eval(a.Value) if a.Value else {}
            except Exception:
                continue

            rows.append(
                {
                    "Mini Model Name": mini_model_name,
                    "Semantic Model Name": ann.get("datasetName"),
                    "Semantic Model Id": ann.get("datasetId"),
                    "Workspace Name": ann.get("workspaceName"),
                    "Workspace Id": ann.get("workspaceId"),
                    "Last Updated Date": ann.get("lastUpdatedDate"),
                }
            )

            if show_filters:
                filters = ann.get("filters") or {}

                if not filters:
                    rows.append({**rows[-1], "Table Name": None, "Filter Value": None})
                else:
                    for table, value in filters.items():
                        rows.append(
                            {
                                **rows[-1],
                                "Table Name": table,
                                "Filter Value": value,
                            }
                        )

    if rows:
        df = pd.DataFrame(rows)
        _update_dataframe_datatypes(dataframe=df, column_map=columns)

    return df
