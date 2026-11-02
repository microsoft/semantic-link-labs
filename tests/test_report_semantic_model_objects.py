from sempy_labs.report import _report_helper as helper


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
