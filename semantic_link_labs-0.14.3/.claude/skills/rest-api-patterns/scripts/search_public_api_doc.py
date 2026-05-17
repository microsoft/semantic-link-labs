"""
Microsoft Fabric & Power BI REST API Fuzzy Search Tool

This module provides fuzzy/ambiguous search functionality for Fabric and Power BI TOC JSON.
It uses the `rapidfuzz` library for efficient fuzzy string matching.

Usage:
    from fabric_api_search import search_api, fetch_toc

    toc = fetch_toc()  # Fetches both Fabric and Power BI
    results = search_api(toc, "copy fabric job")

    # Search only Fabric APIs
    results = search_api(toc, "copy job", source="fabric")

    # Search only Power BI APIs
    results = search_api(toc, "dataset", source="powerbi")

    # Or run directly:
    python fabric_api_search.py "copy fabric job"
    python fabric_api_search.py "dataset" --source powerbi

Requirements:
    pip install rapidfuzz requests
"""

import argparse
import json
from typing import Dict, List, Literal

import requests
from rapidfuzz import fuzz, process

# Constants
FABRIC_BASE_URL = "https://learn.microsoft.com/en-us/rest/api/fabric"
POWERBI_BASE_URL = "https://learn.microsoft.com/en-us/rest/api/power-bi"

TOC_SOURCES = {
    "fabric": {
        "toc_url": f"{FABRIC_BASE_URL}/toc.json",
        "base_url": FABRIC_BASE_URL,
        "name": "Microsoft Fabric"
    },
    "powerbi": {
        "toc_url": f"{POWERBI_BASE_URL}/toc.json",
        "base_url": POWERBI_BASE_URL,
        "name": "Power BI"
    }
}

SourceType = Literal["fabric", "powerbi", None]


def fetch_toc(source: SourceType = None) -> Dict:
    """
    Fetch the Table of Contents JSON from Microsoft Learn.

    Args:
        source: Which API source to fetch:
            - "fabric": Only Fabric APIs
            - "powerbi": Only Power BI APIs
            - None: Both Fabric and Power BI APIs (default)

    Returns:
        Dict: The combined TOC data with source information
    """
    sources_to_fetch = []

    if source is None:
        sources_to_fetch = ["fabric", "powerbi"]
    elif source in TOC_SOURCES:
        sources_to_fetch = [source]
    else:
        raise ValueError(f"Invalid source: {source}. Must be 'fabric', 'powerbi', or None")

    combined_items = []

    for src in sources_to_fetch:
        src_config = TOC_SOURCES[src]
        print(f"📥 Fetching {src_config['name']} TOC...")

        try:
            response = requests.get(src_config["toc_url"], timeout=30)
            response.raise_for_status()
            toc_data = response.json()

            # Add source metadata to each top-level item
            items = toc_data.get("items", [])
            for item in items:
                item["_source"] = src
                item["_base_url"] = src_config["base_url"]

            combined_items.extend(items)
            print(f"   ✅ Loaded {len(items)} top-level categories from {src_config['name']}")

        except requests.RequestException as e:
            print(f"   ⚠️ Failed to fetch {src_config['name']} TOC: {e}")

    return {"items": combined_items}


def _build_full_url(href: str, base_url: str) -> str:
    """
    Build the full documentation URL from a relative href.

    Args:
        href: The relative API path (e.g., "core/connections/list-supported-connection-types")
        base_url: The base URL for the API source

    Returns:
        str: Full URL (e.g., "https://learn.microsoft.com/en-us/rest/api/fabric/core/connections/list-supported-connection-types")
    """
    if href.startswith("http"):
        return href
    return f"{base_url}/{href}"


def _flatten_toc_items(items: List[Dict], parent_path: str = "",
                       source: str = None, base_url: str = None) -> List[Dict]:
    """
    Recursively flatten TOC items into a list of leaf nodes with href.

    Args:
        items: List of TOC items
        parent_path: Accumulated path from parent items
        source: The source identifier (fabric/powerbi)
        base_url: The base URL for building full URLs

    Returns:
        List of flattened items with href, full URL, and searchable text
    """
    flattened = []

    for item in items:
        title = item.get("toc_title", "")
        current_path = f"{parent_path} > {title}" if parent_path else title

        # Get source info from item or inherit from parent
        item_source = item.get("_source", source)
        item_base_url = item.get("_base_url", base_url)

        # If item has href, it's a leaf node (API endpoint)
        if "href" in item:
            href = item["href"]
            full_url = _build_full_url(href, item_base_url) if item_base_url else href

            # Convert href path to readable text
            href_text = href.replace("/", " ").replace("-", " ").replace("(", " ").replace(")", " ")

            # Add source name to searchable text for filtering
            source_name = TOC_SOURCES.get(item_source, {}).get("name", "") if item_source else ""

            flattened.append({
                "href": href,
                "url": full_url,
                "toc_title": title,
                "path": current_path,
                "source": item_source,
                "searchable_text": f"{title} {href_text} {current_path} {source_name}".lower()
            })

        # Recursively process children
        if "children" in item:
            flattened.extend(_flatten_toc_items(
                item["children"],
                current_path,
                item_source,
                item_base_url
            ))

    return flattened


def search_api(toc: Dict, query: str, limit: int = 20, score_threshold: int = 50,
               source: SourceType = None) -> List[Dict]:
    """
    Perform fuzzy search on the Fabric/Power BI TOC JSON to find matching API entries.

    This function uses rapidfuzz for fuzzy string matching, supporting ambiguous
    queries like "copy fabric job" to find entries like "Create Copy Job".

    Args:
        toc: The raw TOC JSON dictionary (as returned by fetch_toc())
        query: The search query string (e.g., "copy fabric job")
        limit: Maximum number of results to return (default: 20)
        score_threshold: Minimum fuzzy match score (0-100) to include in results (default: 50)
        source: Filter results by source:
            - "fabric": Only Fabric APIs
            - "powerbi": Only Power BI APIs
            - None: Both (default)

    Returns:
        List of matching API entries, each containing:
        - href: The relative API path
        - url: The full documentation URL
        - toc_title: The API title
        - path: The hierarchical path
        - source: The API source (fabric/powerbi)
        - score: The fuzzy match score (0-100)

    Example:
        >>> toc = fetch_toc()
        >>> results = search_api(toc, "copy fabric job")
        >>> for r in results:
        ...     print(f"{r['toc_title']}: {r['url']}")
        Create Copy Job: https://learn.microsoft.com/en-us/rest/api/fabric/copyjob/items/create-copy-job
        ...
    """
    # Get items from TOC
    items = toc.get("items", [])
    if not items:
        return []

    # Flatten the TOC structure
    flattened = _flatten_toc_items(items)

    # Filter by source if specified
    if source:
        flattened = [item for item in flattened if item.get("source") == source]

    if not flattened:
        return []

    # Normalize query for matching
    query_normalized = query.lower().strip()

    # Use rapidfuzz to find matches
    choices = {i: item["searchable_text"] for i, item in enumerate(flattened)}

    # Use token_set_ratio for best matching with different word orders and partial matches
    matches = process.extract(
        query_normalized,
        choices,
        scorer=fuzz.token_set_ratio,
        limit=limit * 2
    )

    # Also try partial_ratio for substring matches
    partial_matches = process.extract(
        query_normalized,
        choices,
        scorer=fuzz.partial_ratio,
        limit=limit * 2
    )

    # Combine and deduplicate matches, taking the best score for each item
    score_map: Dict[int, float] = {}
    for match_text, score, idx in matches:
        if idx not in score_map or score > score_map[idx]:
            score_map[idx] = score

    for match_text, score, idx in partial_matches:
        combined_score = (score_map.get(idx, 0) * 0.6) + (score * 0.4)
        if idx not in score_map or combined_score > score_map[idx]:
            score_map[idx] = combined_score

    # Filter by threshold and sort by score
    results = []
    for idx, score in sorted(score_map.items(), key=lambda x: x[1], reverse=True):
        if score >= score_threshold:
            item = flattened[idx]
            results.append({
                "href": item["href"],
                "url": item["url"],
                "toc_title": item["toc_title"],
                "path": item["path"],
                "source": item["source"],
                "score": round(score, 2)
            })

    return results[:limit]


def search_api_simple(toc: Dict, query: str, limit: int = 20,
                      source: SourceType = None) -> List[Dict]:
    """
    Simplified version that returns only essential fields.

    Args:
        toc: The raw TOC JSON dictionary
        query: The search query string
        limit: Maximum number of results to return
        source: Filter by source (fabric/powerbi/None)

    Returns:
        List of dicts with 'href', 'url', 'toc_title', and 'source' keys
    """
    results = search_api(toc, query, limit=limit, source=source)
    return [{
        "href": r["href"],
        "url": r["url"],
        "toc_title": r["toc_title"],
        "source": r["source"]
    } for r in results]


def main():
    """Command-line interface for the search tool."""
    parser = argparse.ArgumentParser(
        description="Fuzzy search for Microsoft Fabric and Power BI REST APIs"
    )
    parser.add_argument(
        "query",
        type=str,
        help="Search query (e.g., 'copy fabric job', 'dataset refresh')"
    )
    parser.add_argument(
        "--source",
        type=str,
        choices=["fabric", "powerbi"],
        default=None,
        help="Filter by API source: 'fabric', 'powerbi', or omit for both"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum number of results (default: 20)"
    )
    parser.add_argument(
        "--threshold",
        type=int,
        default=50,
        help="Minimum match score 0-100 (default: 50)"
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output results as JSON"
    )
    parser.add_argument(
        "--simple",
        action="store_true",
        help="Output simplified format"
    )

    args = parser.parse_args()

    source_desc = args.source.upper() if args.source else "Fabric + Power BI"
    print(f"🔍 Searching for: '{args.query}' in {source_desc}")
    print("=" * 80)

    # Fetch TOC (fetch both, filter later for flexibility)
    toc = fetch_toc(source=args.source)

    print()

    # Perform search
    if args.simple:
        results = search_api_simple(toc, args.query, limit=args.limit, source=args.source)
    else:
        results = search_api(toc, args.query, limit=args.limit,
                             score_threshold=args.threshold, source=args.source)

    # Output results
    if args.json:
        print(json.dumps(results, indent=2))
    else:
        if not results:
            print("No results found.")
        else:
            print(f"Found {len(results)} results:\n")
            for i, result in enumerate(results, 1):
                source_tag = f"[{result['source'].upper()}]" if result.get('source') else ""
                if args.simple:
                    print(f"{i}. {source_tag} {result['toc_title']}")
                    print(f"   URL: {result['url']}")
                else:
                    print(f"{i}. {source_tag} {result['toc_title']} (score: {result['score']})")
                    print(f"   URL: {result['url']}")
                    print(f"   Path: {result['path']}")
                print()


if __name__ == "__main__":
    main()
