



_vis_type_mapping = {
    "barChart": "Bar chart",
    "columnChart": "Column chart",
    "clusteredBarChart": "Clustered bar chart",
    "clusteredColumnChart": "Clustered column chart",
    "hundredPercentStackedBarChart": "100% Stacked bar chart",
    "hundredPercentStackedColumnChart": "100% Stacked column chart",
    "lineChart": "Line chart",
    "areaChart": "Area chart",
    "stackedAreaChart": "Stacked area chart",
    "lineStackedColumnComboChart": "Line and stacked column chart",
    "lineClusteredColumnComboChart": "Line and clustered column chart",
    "ribbonChart": "Ribbon chart",
    "waterfallChart": "Waterfall chart",
    "funnel": "Funnel chart",
    "scatterChart": "Scatter chart",
    "pieChart": "Pie chart",
    "donutChart": "Donut chart",
    "treemap": "Treemap",
    "map": "Map",
    "filledMap": "Filled map",
    "shapeMap": "Shape map",
    "azureMap": "Azure map",
    "gauge": "Gauge",
    "card": "Card",
    "multiRowCard": "Multi-row card",
    "kpi": "KPI",
    "slicer": "Slicer",
    "tableEx": "Table",
    "pivotTable": "Matrix",
    "scriptVisual": "R script visual",
    "pythonVisual": "Python visual",
    "keyDriversVisual": "Key influencers",
    "decompositionTreeVisual": "Decomposition tree",
    "qnaVisual": "Q&A",
    "aiNarratives": "Narrative",
    "scorecard": "Metrics (Preview)",
    "rdlVisual": "Paginated report",
    "cardVisual": "Card (new)",
    "advancedSlicerVisual": "Slicer (new)",
    "actionButton": "Button",
    "bookmarkNavigator": "Bookmark navigator",
    "image": "Image",
    "textbox": "Textbox",
    "pageNavigator": "Page navigator",
    "shape": "Shape",
    "Group": "Group",
}


custom_visual_base_url = "https://catalogapi.azure.com/offers?api-version=2018-08-01-beta&storefront=appsource&$filter=offerType+eq+%27PowerBIVisuals%27"
custom_visual_end_url_1 = f"{custom_visual_base_url}&$skiptoken=W3sidG9rZW4iOiIrUklEOn4yVk53QUxkRkVIeEJhUUFBQUFCQUNBPT0jUlQ6MSNUUkM6MTk3I0lTVjoyI0lFTzo2NTU2NyNRQ0Y6OCIsInJhbmdlIjp7Im1pbiI6IjA1QzFFNzBCM0IzOTUwIiwibWF4IjoiMDVDMUU3QUIxN0U3QTYifX1d"
custom_visual_end_url_2 = f"{custom_visual_base_url}&$skiptoken=W3sidG9rZW4iOiIrUklEOn4yVk53QUxkRkVId1pyQVVBQUFBQURBPT0jUlQ6MiNUUkM6NDg3I0lTVjoyI0lFTzo2NTU2NyNRQ0Y6OCIsInJhbmdlIjp7Im1pbiI6IjA1QzFFNzBCM0IzOTUwIiwibWF4IjoiMDVDMUU3QUIxN0U3QTYifX1d"

custom_visual_urls = [custom_visual_base_url, custom_visual_end_url_1, custom_visual_end_url_2]