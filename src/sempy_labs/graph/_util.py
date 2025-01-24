import requests
from sempy_labs._authentication import _get_headers
from sempy.fabric._token_provider import TokenProvider
from sempy.fabric.exceptions import FabricHTTPException
from typing import Optional


def _ms_graph_base(
    api_name,
    token_provider: TokenProvider,
    status_success_code: int = 200,
    call_type: str = "get",
    payload: Optional[str] = None,
    return_json: bool = True,
):

    if isinstance(status_success_code, int):
        status_success_codes = [status_success_code]

    headers = _get_headers(token_provider, audience="graph")
    url = f"https://graph.microsoft.com/v1.0/{api_name}"

    if payload:
        if call_type == "post":
            response = requests.post(url, headers=headers, json=payload)
        elif call_type == "get":
            response = requests.get(url, headers=headers, json=payload)
        elif call_type == "put":
            response = requests.put(url, headers=headers, json=payload)
    else:
        if call_type == "post":
            response = requests.post(url, headers=headers)
        elif call_type == "get":
            response = requests.get(url, headers=headers)
        elif call_type == "put":
            response = requests.put(url, headers=headers)

    if response.status_code not in status_success_codes:
        raise FabricHTTPException(response)

    if return_json:
        return response.json()
    else:
        return response.status_code
