import requests
from typing import List, Optional
from sempy_labs._a_lib_info import lib_name, lib_version
from sempy._utils._log import log


@log
def _format_dax(
    expressions: str | List[str],
    skip_space_after_function_name: bool = False,
    metadata: Optional[List[dict]] = None,
) -> List[str]:

    if isinstance(expressions, str):
        expressions = [expressions]
        metadata = [metadata] if metadata else [{}]

    # Add variable assignment to each expression
    expressions = [f"x :={item}" for item in expressions]

    url = "https://daxformatter.azurewebsites.net/api/daxformatter/daxtextformatmulti"

    payload = {
        "Dax": expressions,
        "MaxLineLength": 0,
        "SkipSpaceAfterFunctionName": skip_space_after_function_name,
        "ListSeparator": ",",
        "DecimalSeparator": ".",
    }

    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Encoding": "gzip,deflate",
        "Accept-Language": "en-US,en;q=0.8",
        "Content-Type": "application/json; charset=UTF-8",
        "Host": "daxformatter.azurewebsites.net",
        "Expect": "100-continue",
        "Connection": "Keep-Alive",
        "CallerApp": lib_name,
        "CallerVersion": lib_version,
    }

    response = requests.post(url, json=payload, headers=headers)
    result = []
    for idx, dax in enumerate(response.json()):
        formatted_dax = dax.get("formatted")
        errors = dax.get("errors")
        if errors:
            meta = metadata[idx] if metadata and idx < len(metadata) else {}
            obj_name = meta.get("name", "Unknown")
            table_name = meta.get("table", "Unknown")
            obj_type = meta.get("type", "Unknown")
            if obj_type == "calculated_tables":
                raise ValueError(
                    f"DAX formatting failed for the '{obj_name}' calculated table: {errors}"
                )
            elif obj_type == "calculated_columns":
                raise ValueError(
                    f"DAX formatting failed for the '{table_name}'[{obj_name}] calculated column: {errors}"
                )
            elif obj_type == "calculation_items":
                raise ValueError(
                    f"DAX formatting failed for the '{table_name}'[{obj_name}] calculation item: {errors}"
                )
            elif obj_type == "measures":
                raise ValueError(
                    f"DAX formatting failed for the '{obj_name}' measure: {errors}"
                )
            elif obj_type == "rls":
                raise ValueError(
                    f"DAX formatting failed for the row level security expression on the '{table_name}' table within the '{obj_name}' role: {errors}"
                )
            else:
                NotImplementedError()
        else:
            if formatted_dax.startswith("x :="):
                formatted_dax = formatted_dax[4:]
            formatted_dax = formatted_dax.strip()
            result.append(formatted_dax)
    return result
