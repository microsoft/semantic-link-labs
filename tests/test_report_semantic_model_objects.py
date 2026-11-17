from types import SimpleNamespace
from unittest.mock import Mock, call

import pandas as pd
import pytest
from requests import Response
from sempy.fabric.exceptions import FabricHTTPException
from sempy_labs import _list_functions as lists
import sempy_labs.report as report
from sempy_labs.report import _report_helper as helper
import sempy_labs.tom as tom


def test_hierarchy_level_maps_to_column():
    data = {
        "filter": {
            "HierarchyLevel": {
                "Expression": {"SourceRef": {"Entity": "Date"}},
                "Property": "Year",
            }
        }
    }

    result = helper.find_entity_property_pairs(data)

    assert result == {"Year": ("Date", "Column")}


@pytest.fixture
def report_dependencies(monkeypatch):
    """Mock report access while retaining downstream workspace resolution."""
    monkeypatch.setattr(
        lists,
        "resolve_workspace_name_and_id",
        Mock(return_value=("Source", "source-ws")),
    )
    monkeypatch.setattr(
        lists, "resolve_dataset_name_and_id", Mock(return_value=("Model", "model-id"))
    )
    monkeypatch.setattr(
        "sempy_labs._helper_functions.get_downstream_relations",
        Mock(
            return_value={
                "workspaces": [
                    {"id": "other-ws", "displayName": "Other"},
                    {"id": "app-ws", "displayName": "App"},
                ],
                "items": [
                    {
                        "id": report_id,
                        "type": "Report",
                        "displayName": report_id,
                        "workspaceId": workspace_id,
                    }
                    for report_id, workspace_id in [
                        ("local-report", "source-ws"),
                        ("app-report", "app-ws"),
                        ("thin-report", "other-ws"),
                    ]
                ],
                "relations": [
                    {"itemId": report_id, "dependentOnItemId": "model-id"}
                    for report_id in ["local-report", "app-report", "thin-report"]
                ],
            }
        ),
    )
    objects = pd.DataFrame(
        {
            "Table Name": ["Sales"],
            "Object Name": ["Revenue"],
            "Object Type": ["Measure"],
            "Report Source": ["Visual"],
            "Report Source Object": ["visual-id"],
        }
    )
    wrapper = Mock()
    wrapper.return_value.list_semantic_model_objects.side_effect = (
        lambda: objects.copy()
    )
    monkeypatch.setattr(report, "ReportWrapper", wrapper)
    connection = Mock()
    connection.return_value.__enter__ = Mock(
        return_value=SimpleNamespace(
            all_measures=lambda: [SimpleNamespace(Name="Revenue")]
        )
    )
    connection.return_value.__exit__ = Mock(return_value=False)
    monkeypatch.setattr(tom, "connect_semantic_model", connection)
    return wrapper, connection


@pytest.mark.parametrize("extended", [False, True])
def test_report_objects_use_each_reports_workspace(report_dependencies, extended):
    """Reports outside the model workspace are read from their own workspace."""
    wrapper, connection = report_dependencies

    result = lists.list_report_semantic_model_objects(
        dataset="model-id", workspace="source-ws", extended=extended
    )

    assert wrapper.call_args_list == [
        call(report="local-report", workspace="source-ws"),
        call(report="app-report", workspace="app-ws"),
        call(report="thin-report", workspace="other-ws"),
    ]
    assert result["Report Workspace Name"].tolist() == ["Source", "App", "Other"]
    if extended:
        assert result["Valid Semantic Model Object"].tolist() == [True, True, True]
        connection.assert_called_once_with(
            dataset="model-id", readonly=True, workspace="source-ws"
        )
    else:
        connection.assert_not_called()


@pytest.mark.parametrize("status_code", [None, 401, 403, 404])
def test_report_objects_skip_inaccessible_reports(
    report_dependencies, capsys, status_code
):
    """An inaccessible App report does not discard other reports' objects."""
    wrapper, _ = report_dependencies
    error = ValueError("The 'app-report' item was not found in the 'App' workspace.")
    if status_code is not None:
        response = Response()
        response.status_code = status_code
        error = FabricHTTPException(response)
    wrapper.side_effect = [wrapper.return_value, error, wrapper.return_value]

    result = lists.list_report_semantic_model_objects(
        dataset="model-id", workspace="source-ws", extended=True
    )

    assert result["Report Name"].tolist() == ["local-report", "thin-report"]
    assert result["Valid Semantic Model Object"].tolist() == [True, True]
    output = capsys.readouterr().out
    assert "Skipping" in output
    assert "app-report" in output
    assert "app-ws" in output


def test_report_objects_all_inaccessible_returns_empty(report_dependencies):
    """An empty result retains the standard columns when no reports are readable."""
    wrapper, _ = report_dependencies
    wrapper.side_effect = ValueError("Report not found")

    result = lists.list_report_semantic_model_objects(
        dataset="model-id", workspace="source-ws"
    )

    assert result.empty
    assert result.columns.tolist() == [
        "Report Name",
        "Report Workspace Name",
        "Table Name",
        "Object Name",
        "Object Type",
        "Report Source",
        "Report Source Object",
    ]


def test_report_objects_analysis_errors_are_not_suppressed(report_dependencies):
    """Errors unrelated to accessing a report remain visible to callers."""
    wrapper, _ = report_dependencies
    wrapper.return_value.list_semantic_model_objects.side_effect = ValueError(
        "Invalid report object"
    )

    with pytest.raises(ValueError, match="Invalid report object"):
        lists.list_report_semantic_model_objects(
            dataset="model-id", workspace="source-ws"
        )
