import httpx
import json
from typing import Optional
from uuid import UUID
from sempy_labs._helper_functions import (
    resolve_workspace_id,
    resolve_item_id,
)
 
MCP_SERVER_URL = "https://msitapi.fabric.microsoft.com/v1/mcp/powerbi"
 
async def ask_powerbi_question(dataset: str | UUID, question: str, workspace: Optional[str | UUID] = None):
    """
    Complete workflow to ask a question against Power BI
    """
    import notebookutils
    token = notebookutils.credentials.getToken("pbi")

    workspace_id = resolve_workspace_id(workspace)
    item_id = resolve_item_id(item=dataset, type="SemanticModel", workspace=workspace_id)
   
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
   
    async with httpx.AsyncClient(timeout=120.0) as client:
       
        # Step 1: Get the semantic model schema
        print("Step 1: Getting semantic model schema...")
        schema_payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "GetSemanticModelSchema",
                "arguments": {
                    "artifactId": item_id
                }
            }
        }
       
        response = await client.post(MCP_SERVER_URL, headers=headers, json=schema_payload)
        schema_data = parse_sse_response(response.text)
       
        if 'error' in schema_data:
            print(f"Error getting schema: {schema_data['error']}")
            return schema_data
       
        # Extract structured schema
        structured_schema = schema_data.get('result', {}).get('structuredContent', {})
        tables = structured_schema.get('Tables', [])
       
        print(f"Found {len(tables)} tables:")
        for table in tables:
            print(f"  - {table['Name']}: {table.get('Description', 'No description')}")
        print()
       
        # Step 2: Generate DAX query
        # Build schema selection - for demo, include Sales and related tables
        print("Step 2: Generating DAX query...")
       
        # Auto-select relevant tables based on question keywords
        schema_selection = build_schema_selection(tables, question)
       
        generate_payload = {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "GenerateQuery",
                "arguments": {
                    "artifactId": item_id,
                    "userInput": question,
                    "schemaSelection": schema_selection
                }
            }
        }
       
        response = await client.post(MCP_SERVER_URL, headers=headers, json=generate_payload)
        query_data = parse_sse_response(response.text)
       
        if 'error' in query_data:
            print(f"Error generating query: {query_data['error']}")
            return query_data
       
        result = query_data.get('result', {})
        content = result.get('content', [])
        dax_query = content[0].get('text', '') if len(content) > 0 else ''
        print(f"Generated DAX Query:\n{dax_query}\n")
       
        # Step 3: Execute the query
        print("Step 3: Executing query...")
        execute_payload = {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "ExecuteQuery",
                "arguments": {
                    "artifactId": item_id,
                    "daxQuery": dax_query,
                    "maxRows": 100
                }
            }
        }
       
        response = await client.post(MCP_SERVER_URL, headers=headers, json=execute_payload)
        result_data = parse_sse_response(response.text)
       
        if 'error' in result_data:
            print(f"Error executing query: {result_data['error']}")
            return result_data
       
        result = result_data.get('result', {})
        content = result.get('content', [])
        results = content[0].get('text', '') if len(content) > 0 else ''
        print(f"Query Results:\n{results[:2000]}...\n")  # First 2000 chars
       
        return {
            'schema': structured_schema,
            'dax_query': dax_query,
            'results': results
        }
 
def build_schema_selection(tables, question):
    """Build schema selection based on available tables"""
    # For simplicity, include main tables - you can make this smarter
    schema_tables = []
   
    for table in tables:
        if table['Name'] in ['Sales', 'Customer', 'Product', 'Store', 'Calendar']:
            table_selection = {
                "name": table['Name'],
                "columns": [col['Name'] for col in table.get('Columns', [])],
                "measures": [m['Name'] for m in table.get('Measures', [])]
            }
            schema_tables.append(table_selection)
   
    return {"tables": schema_tables}
 
def parse_sse_response(text):
    """Parse Server-Sent Events response"""
    lines = text.split('\n')
    for line in lines:
        if line.startswith('data: '):
            return json.loads(line[6:])
    return {}
 
# Test with different questions
result = await ask_powerbi_question(
    dataset="347b15b9-cdaa-4f3c-b46a-032e32964a62",
    question="What are the top 10 products by sales amount?"
)