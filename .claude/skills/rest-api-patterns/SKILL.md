---
name: rest-api-patterns
description: Guide for implementing REST API wrapper functions. Use this when adding new API wrappers or troubleshooting API calls.
---

# REST API Patterns

This skill covers the patterns and utilities for implementing REST API wrapper functions in Semantic Link Labs.

## When to Use This Skill

Use this skill when you need to:
- Implement new REST API wrappers
- Understand the `_base_api` helper function
- Handle pagination, long-running operations, or errors
- Debug API-related issues

---

## Finding API Documentation

Before implementing a wrapper, use the API search tool to find the relevant documentation:

### Using `search_public_api_doc.py`

```bash
# Navigate to the scripts directory
cd .claude/skills/rest-api-patterns/scripts

# Search both Fabric and Power BI APIs
python search_public_api_doc.py "dataset refresh"

# Search Fabric APIs only
python search_public_api_doc.py "create item" --source fabric

# Search Power BI APIs only
python search_public_api_doc.py "gateway" --source powerbi

# Limit results
python search_public_api_doc.py "workspace" --limit 10
```

### Example Output

```
🔍 Searching for: 'dataset refresh' in Fabric + Power BI
================================================================================
📥 Fetching Microsoft Fabric TOC...
   ✅ Loaded 15 top-level categories from Microsoft Fabric
📥 Fetching Power BI TOC...
   ✅ Loaded 20 top-level categories from Power BI

Found 5 results:

1. [POWERBI] Datasets - Refresh Dataset In Group (score: 95.0)
   URL: https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/refresh-dataset-in-group
   Path: Datasets > Refresh Dataset In Group

2. [POWERBI] Datasets - Get Refresh History In Group (score: 90.0)
   URL: https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/get-refresh-history-in-group
   Path: Datasets > Get Refresh History In Group
```

### Requirements

The script requires `rapidfuzz` and `requests`:

```bash
pip install rapidfuzz requests
```

### API Documentation References

| API | Base URL | Documentation |
|-----|----------|---------------|
| Fabric REST API | `https://api.fabric.microsoft.com/v1/` | [Fabric REST API](https://learn.microsoft.com/rest/api/fabric/) |
| Power BI REST API | `https://api.powerbi.com/v1.0/myorg/` | [Power BI REST API](https://learn.microsoft.com/rest/api/power-bi/) |

---

## API Client Architecture

Semantic Link Labs uses `sempy.fabric.FabricRestClient` as the underlying HTTP client, wrapped by the `_base_api` helper function.

### Key Components

| Component | Purpose |
|-----------|---------|
| `_base_api` | Main helper for all API calls |
| `FabricRestClient` | HTTP client from sempy |
| `pagination` | Handles paginated responses |
| `lro` | Handles long-running operations |

---

## The `_base_api` Function

Located in `src/sempy_labs/_helper_functions.py`, this is the standard way to make API calls.

### Function Signature

```python
def _base_api(
    request: str,                      # API endpoint path
    client: str = "fabric",            # Client type
    method: str = "get",               # HTTP method
    payload: Optional[str] = None,     # Request body
    status_codes: Optional[int] = 200, # Expected status codes
    uses_pagination: bool = False,     # Enable pagination
    lro_return_json: bool = False,     # Wait for LRO, return JSON
    lro_return_status_code: bool = False,  # Wait for LRO, return status
    lro_return_df: bool = False,       # Wait for LRO, return DataFrame
):
```

### Client Types

| Client | Use Case | Authentication |
|--------|----------|----------------|
| `fabric` | Standard Fabric API | Default notebook credentials |
| `fabric_sp` | Fabric API with SP support | Service Principal or default |
| `azure` | Azure Resource Manager | Service Principal |
| `graph` | Microsoft Graph | Service Principal |
| `onelake` | OneLake storage | Storage token |

### Return Types

**The `_base_api` function returns different types depending on the parameters used:**

| Parameters | Return Type | How to Access Data |
|------------|-------------|-------------------|
| Default (no special flags) | `Response` object | Call `.json()` to get dict |
| `uses_pagination=True` | `list[dict]` | Iterate over list, each item has `.get("value", [])` |
| `lro_return_json=True` | `dict` | Access directly, already parsed JSON |
| `lro_return_status_code=True` | `int` | HTTP status code |
| `lro_return_df=True` | `DataFrame` | Use directly |

**⚠️ COMMON MISTAKE:** Forgetting to call `.json()` on the response for simple GET requests.

```python
# ❌ WRONG - response is a Response object, not a dict
response = _base_api(request=f"/v1/workspaces/{workspace_id}/items/{item_id}")
name = response.get("displayName")  # AttributeError: 'Response' object has no attribute 'get'

# ✅ CORRECT - call .json() to get the dict
response = _base_api(request=f"/v1/workspaces/{workspace_id}/items/{item_id}").json()
name = response.get("displayName")  # Works!
```

---

## Common API Patterns

### Simple GET Request

```python
from sempy_labs._helper_functions import _base_api

response = _base_api(
    request=f"/v1/workspaces/{workspace_id}/items",
    client="fabric_sp",
)

data = response.json()
```

### POST Request with Payload

```python
payload = {
    "displayName": name,
    "description": description,
}

response = _base_api(
    request=f"/v1/workspaces/{workspace_id}/items",
    method="post",
    payload=payload,
    status_codes=[201, 202],
    client="fabric_sp",
)
```

### DELETE Request

```python
_base_api(
    request=f"/v1/workspaces/{workspace_id}/items/{item_id}",
    method="delete",
    client="fabric_sp",
)
```

### PATCH Request

```python
payload = {
    "displayName": new_name,
}

_base_api(
    request=f"/v1/workspaces/{workspace_id}/items/{item_id}",
    method="patch",
    payload=payload,
    client="fabric_sp",
)
```

---

## Handling Pagination

For APIs that return paginated results:

```python
from sempy_labs._helper_functions import _base_api, _create_dataframe

columns = {
    "Id": "string",
    "Name": "string",
}
df = _create_dataframe(columns=columns)

# Get all pages
responses = _base_api(
    request=f"/v1/workspaces/{workspace_id}/items",
    uses_pagination=True,
    client="fabric_sp",
)

# Process all responses
rows = []
for r in responses:
    for item in r.get("value", []):
        rows.append({
            "Id": item.get("id"),
            "Name": item.get("displayName"),
        })

if rows:
    df = pd.DataFrame(rows)

return df
```

---

## Handling Long-Running Operations (LRO)

Some APIs return 202 Accepted and require polling for completion.

### Return JSON When Complete

```python
result = _base_api(
    request=f"/v1/workspaces/{workspace_id}/items/{item_id}/getDefinition",
    method="post",
    lro_return_json=True,
    client="fabric_sp",
)

# Result contains the final JSON response
definition = result.get("definition")
```

### Return Status Code

```python
status = _base_api(
    request=f"/v1/workspaces/{workspace_id}/items",
    method="post",
    payload=payload,
    lro_return_status_code=True,
    client="fabric_sp",
)

# status is the final HTTP status code
if status == 200:
    print("Operation completed successfully")
```

---

## Error Handling

### Expected Status Codes

Specify expected status codes to avoid exceptions:

```python
# Accept 200, 201, or 202 as success
response = _base_api(
    request=url,
    method="post",
    payload=payload,
    status_codes=[200, 201, 202],
    client="fabric_sp",
)
```

### FabricHTTPException

When status code doesn't match, `FabricHTTPException` is raised:

```python
from sempy.fabric.exceptions import FabricHTTPException

try:
    response = _base_api(
        request=f"/v1/workspaces/{workspace_id}/items/{item_id}",
        client="fabric_sp",
    )
except FabricHTTPException as e:
    if e.response.status_code == 404:
        print(f"Item not found")
    else:
        raise
```

---

## Building URLs with Parameters

Use the `_build_url` helper for query parameters:

```python
from sempy_labs._helper_functions import _build_url

url = "/v1/admin/workspaces"
params = {
    "capacityId": capacity_id,
    "state": "Active",
}

url = _build_url(url, params)
# Result: "/v1/admin/workspaces?capacityId=xxx&state=Active"

responses = _base_api(
    request=url,
    uses_pagination=True,
    client="fabric_sp",
)
```

---

## API Endpoint Patterns

### Fabric Core API

```python
# List items in workspace
f"/v1/workspaces/{workspace_id}/items"

# Get specific item
f"/v1/workspaces/{workspace_id}/items/{item_id}"

# Item operations
f"/v1/workspaces/{workspace_id}/items/{item_id}/getDefinition"
f"/v1/workspaces/{workspace_id}/items/{item_id}/updateDefinition"
```

### Fabric Admin API

```python
# Admin workspaces
"/v1/admin/workspaces"

# Admin items
"/v1/admin/items"

# Capacities
"/v1/admin/capacities"
```

### Power BI REST API

```python
# Groups (workspaces)
f"/v1.0/myorg/groups/{workspace_id}/..."

# Datasets
f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/..."

# Reports
f"/v1.0/myorg/groups/{workspace_id}/reports/{report_id}/..."
```

### Azure Resource Manager

```python
# Fabric capacities
f"https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.Fabric/capacities"

# Resource groups
f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_group}"
```

---

## Authentication Headers

For non-Fabric clients (Azure, Graph), use `_get_headers`:

```python
from sempy_labs._authentication import _get_headers
import sempy_labs._authentication as auth

headers = _get_headers(auth.token_provider.get(), audience="azure")

response = requests.get(
    url,
    headers=headers,
)
```

---

## Creating Result DataFrames

Use `_create_dataframe` for consistent empty DataFrames:

```python
from sempy_labs._helper_functions import _create_dataframe

columns = {
    "Id": "string",
    "Name": "string",
    "Type": "string",
    "Created Date": "datetime",
    "Size": "int",
}

df = _create_dataframe(columns=columns)
```

### Updating DataFrame Types

```python
from sempy_labs._helper_functions import _update_dataframe_datatypes

column_map = {
    "Created Date": "datetime",
    "Size": "int",
    "Is Active": "bool",
}

_update_dataframe_datatypes(df, column_map)
```

---

## Complete Example

```python
from sempy._utils._log import log
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    _base_api,
    _create_dataframe,
    _build_url,
)
import sempy_labs._icons as icons
from typing import Optional
from uuid import UUID
import pandas as pd


@log
def list_my_items(
    item_type: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Lists items in a workspace.

    This is a wrapper function for the following API: `Items - List Items <https://learn.microsoft.com/rest/api/fabric/core/items/list-items>`_.

    Service Principal Authentication is supported.

    Parameters
    ----------
    item_type : str, default=None
        Filter by item type.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing items in the workspace.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    columns = {
        "Id": "string",
        "Name": "string",
        "Type": "string",
    }
    df = _create_dataframe(columns=columns)

    url = f"/v1/workspaces/{workspace_id}/items"
    params = {}
    if item_type:
        params["type"] = item_type
    if params:
        url = _build_url(url, params)

    responses = _base_api(
        request=url,
        uses_pagination=True,
        client="fabric_sp",
    )

    rows = []
    for r in responses:
        for item in r.get("value", []):
            rows.append({
                "Id": item.get("id"),
                "Name": item.get("displayName"),
                "Type": item.get("type"),
            })

    if rows:
        df = pd.DataFrame(rows)

    return df
```

---

## Debugging API Calls

### Print Response Details

```python
response = _base_api(
    request=url,
    client="fabric_sp",
)

print(f"Status: {response.status_code}")
print(f"Headers: {response.headers}")
print(f"Body: {response.json()}")
```

### Check Request Being Made

Add temporary debug prints:

```python
print(f"Making request to: {url}")
print(f"Payload: {payload}")
```

---

## API Documentation References

| API | Documentation |
|-----|---------------|
| Fabric Core | [https://learn.microsoft.com/rest/api/fabric/core/](https://learn.microsoft.com/rest/api/fabric/core/) |
| Fabric Admin | [https://learn.microsoft.com/rest/api/fabric/admin/](https://learn.microsoft.com/rest/api/fabric/admin/) |
| Power BI | [https://learn.microsoft.com/rest/api/power-bi/](https://learn.microsoft.com/rest/api/power-bi/) |
| Azure Fabric | [https://learn.microsoft.com/rest/api/microsoftfabric/](https://learn.microsoft.com/rest/api/microsoftfabric/) |
| Graph | [https://learn.microsoft.com/graph/api/overview](https://learn.microsoft.com/graph/api/overview) |
