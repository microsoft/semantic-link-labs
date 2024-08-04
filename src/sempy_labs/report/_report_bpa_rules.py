import pandas as pd


def report_bpa_rules() -> pd.DataFrame:
    rules = pd.DataFrame(
        [
            (
                "Performance",
                "Custom Visual",
                "Warning",
                "Remove custom visuals which are not used in the report",
                lambda df: df["Used In Report"] == False,
                "Removing unused custom visuals from a report may lead to faster report performance.",
            ),
            (
                "Performance",
                "Page",
                "Warning",
                "Reduce the number of visible visuals on the page",
                lambda df: df["Visible Visual Count"] > 15,
                'Reducing the number of visable visuals on a page will lead to faster report performance. This rule flags pages with over " + visVisuals + " visible visuals.',
            ),
            (
                "Performance",
                "Visual",
                "Warning",
                "Reduce the number of objects within visuals",
                lambda df: df["Visual Object Count"] > 5,
                "Reducing the number of objects (i.e. measures, columns) which are used in a visual will lead to faster report performance.",
            ),
            (
                "Performance",
                ["Report Filter", "Page Filter", "Visual Filter"],
                "Warning",
                "Reduce usage of TopN filtering within visuals",
                lambda df: df["Type"] == "TopN",
                "TopN filtering may cause performance degradation, especially against a high cardinality column.",
            ),
            (
                "Performance",
                ["Report Filter", "Page Filter", "Visual Filter"],
                "Warning",
                "Reduce usage of filters on measures",
                lambda df: df["Object Type"] == "Measure",
                "Measure filters may cause performance degradation, especially against a large dataset.",
            ),
            (
                "Performance",
                "Visual",
                "Warning",
                "Avoid setting 'Show items with no data' on columns",
                lambda df: df["Show Items With No Data"],
                "This setting will show all column values for all columns in the visual which may lead to performance degradation.",
            ),
            (
                "Performance",
                "Page",
                "Warning",
                "Avoid tall report pages with vertical scrolling",
                lambda df: df["Height"] > 720,
                "Report pages are designed to be in a single view and not scroll. Pages with scrolling is an indicator that the page has too many elements.",
            ),
            (
                "Performance",
                "Custom Visual",
                "Warning",
                "Reduce usage of custom visuals",
                lambda df: df["Custom Visual Name"] == df["Custom Visual Name"],
                "Using custom visuals may lead to performance degradation.",
            ),
            # ('Performance', 'Custom Visual', 'Warning', "Set 'Edit Interactions' for non-data visuals to 'none'",
            # lambda df: df['Custom Visual Name'] == df['Custom Visual Name'],
            # "Setting 'Edit Interactions' to 'None' for non-data visuals may improve performance (since these visuals do not necessitate interactions between other visuals). 'Edit Interactions' may be found in the 'Format' tab of the ribbon in Power BI Desktop.",
            # )
        ],
        columns=[
            "Category",
            "Scope",
            "Severity",
            "Rule Name",
            "Expression",
            "Description",
        ],
    )

    return rules
