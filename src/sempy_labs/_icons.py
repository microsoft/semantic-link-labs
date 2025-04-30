green_dot = "\U0001f7e2"
yellow_dot = "\U0001f7e1"
red_dot = "\U0001f534"
in_progress = "⌛"
checked = "\u2611"
unchecked = "\u2610"
start_bold = "\033[1m"
end_bold = "\033[0m"
bullet = "\u2022"
warning = "⚠️"
error = "\u274c"
info = "ℹ️"
measure_icon = "\u2211"
table_icon = "\u229e"
column_icon = "\u229f"
model_bpa_name = "ModelBPA"
report_bpa_name = "ReportBPA"
severity_mapping = {warning: "Warning", error: "Error", info: "Info"}
special_characters = ['"', "/", '"', ":", "|", "<", ">", "*", "?", "'", "!"]

language_map = {
    "it-IT": "Italian",
    "es-ES": "Spanish",
    "he-IL": "Hebrew",
    "pt-PT": "Portuguese",
    "zh-CN": "Chinese",
    "fr-FR": "French",
    "da-DK": "Danish",
    "cs-CZ": "Czech",
    "de-DE": "German",
    "el-GR": "Greek",
    "fa-IR": "Persian",
    "ga-IE": "Irish",
    "hi-IN": "Hindi",
    "hu-HU": "Hungarian",
    "is-IS": "Icelandic",
    "ja-JP": "Japanese",
    "nl-NL": "Dutch",
    "pl-PL": "Polish",
    "pt-BR": "Portuguese",
    "ru-RU": "Russian",
    "te-IN": "Telugu",
    "ta-IN": "Tamil",
    "th-TH": "Thai",
    "zu-ZA": "Zulu",
    "am-ET": "Amharic",
    "ar-AE": "Arabic",
    "sv-SE": "Swedish",
    "ko-KR": "Korean",
    "id-ID": "Indonesian",
    "mt-MT": "Maltese",
    "ro-RO": "Romanian",
    "sk-SK": "Slovak",
    "sl-SL": "Slovenian",
    "tr-TR": "Turkish",
    "uk-UA": "Ukrainian",
    "bg-BG": "Bulgarian",
    "ca-ES": "Catalan",
    "fi-FI": "Finnish",
}
workspace_roles = ["Admin", "Member", "Viewer", "Contributor"]
principal_types = ["App", "Group", "None", "User"]
azure_api_version = "2023-11-01"
migrate_capacity_suffix = "fsku"
sku_mapping = {
    "A1": "F8",
    "EM1": "F8",
    "A2": "F16",
    "EM2": "F16",
    "A3": "F32",
    "EM3": "F32",
    "A4": "F64",
    "P1": "F64",
    "A5": "F128",
    "P2": "F128",
    "A6": "F256",
    "P3": "F256",
    "A7": "F512",
    "P4": "F512",
    "P5": "F1024",
}

refresh_type_mapping = {
    "full": "full",
    "auto": "automatic",
    "data": "dataOnly",
    "calc": "calculate",
    "clear": "clearValues",
    "defrag": "defragment",
}

itemTypes = {
    "DataPipeline": "dataPipelines",
    "Eventstream": "eventstreams",
    "KQLDatabase": "kqlDatabases",
    "KQLQueryset": "kqlQuerysets",
    "Lakehouse": "lakehouses",
    "MLExperiment": "mlExperiments",
    "MLModel": "mlModels",
    "Notebook": "notebooks",
    "Warehouse": "warehouses",
}
default_schema = "dbo"

data_type_string = "string"
data_type_long = "long"
data_type_timestamp = "timestamp"
data_type_double = "double"
data_type_bool = "bool"
int_format = "int"
pct_format = "pct"
no_format = ""

bpa_schema = {
    "Capacity Name": data_type_string,
    "Capacity Id": data_type_string,
    "Workspace Name": data_type_string,
    "Workspace Id": data_type_string,
    "Dataset Name": data_type_string,
    "Dataset Id": data_type_string,
    "Configured By": data_type_string,
    "Rule Name": data_type_string,
    "Category": data_type_string,
    "Severity": data_type_string,
    "Object Type": data_type_string,
    "Object Name": data_type_string,
    "Description": data_type_string,
    "URL": data_type_string,
    "RunId": data_type_long,
    "Timestamp": data_type_timestamp,
}

sll_ann_name = "PBI_ProTooling"
sll_prefix = "SLL_"
sll_tags = []
base_cols = ["EventClass", "EventSubclass", "CurrentTime", "TextData"]
end_cols = base_cols + [
    "StartTime",
    "EndTime",
    "Duration",
    "CpuTime",
    "Success",
    "IntegerData",
    "ObjectID",
]
refresh_event_schema = {
    "JobGraph": base_cols,
    "ProgressReportEnd": end_cols,
}
