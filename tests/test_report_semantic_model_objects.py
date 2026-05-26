from sempy_labs.report import _report_helper as helper
from sempy_labs.report._reportwrapper import ReportWrapper


def test_find_entity_property_pairs_maps_hierarchy_level_to_column():
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


def test_list_visual_objects_maps_hierarchy_level_to_column():
    payload = {
        "name": "visual-1",
        "visual": {
            "query": {
                "queryState": {},
                "prototypeQuery": {
                    "Select": [
                        {
                            "HierarchyLevel": {
                                "Expression": {"SourceRef": {"Entity": "Date"}},
                                "Property": "Year",
                            }
                        }
                    ]
                },
            }
        },
    }

    wrapper = ReportWrapper.__new__(ReportWrapper)
    wrapper._ensure_pbir = lambda: None
    wrapper._visual_page_mapping = lambda: {"/tmp/visual.json": ("page-1", "Page 1")}
    wrapper._ReportWrapper__all_visuals = lambda: [
        {"path": "/tmp/visual.json", "payload": payload}
    ]

    df = wrapper.list_visual_objects()

    assert len(df) == 1
    assert df.iloc[0]["Table Name"] == "Date"
    assert df.iloc[0]["Object Name"] == "Year"
    assert df.iloc[0]["Object Type"] == "Column"
