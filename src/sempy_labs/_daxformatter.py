import requests
from typing import List


def _format_dax(expressions: str | List[str], skip_space_after_function_name: bool = False) -> List[str]:

    if isinstance(expressions, str):
        expressions = [expressions]

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
        "Connection": "Keep-Alive"
    }

    response = requests.post(url, json=payload, headers=headers)
    result = []
    for dax in response.json():
        formatted_dax = dax.get('formatted')
        errors = dax.get('errors')
        if errors:
            raise ValueError(errors)
        else:
            result.append(formatted_dax)
    return result
