#!/bin/bash
set -e

echo "Getting commit SHA for PR #1059..."
COMMIT_SHA=$(gh api repos/microsoft/semantic-link-labs/pulls/1059 --jq '.head.sha')
echo "Commit SHA: $COMMIT_SHA"

echo "Posting inline review comments..."

# Create a JSON payload file
cat > /tmp/review_payload.json << 'JSONEOF'
{
  "event": "COMMENT",
  "comments": [
    {
      "path": "src/sempy_labs/semantic_model/_caching.py",
      "line": 40,
      "body": "🔴 **High:** Missing validation for `get_model_id()` return value.\n\n`get_model_id()` can return `None` if the API response is missing the expected `model.id` structure. This would result in a request to `{prefix}/metadata/models/None/caching`, causing an unclear failure.\n\n**Suggested fix:**\n```python\nmodel_id = get_model_id(item_id=item_id, headers=headers, prefix=prefix)\nif model_id is None:\n    raise ValueError(f\"Failed to retrieve model ID for semantic model '{item_name}'\")\n```"
    },
    {
      "path": "src/sempy_labs/semantic_model/_caching.py",
      "line": 21,
      "body": "🟡 **Minor:** Malformed RST link syntax.\n\nThe backtick should come before the underscore:\n\n**Current:**\n```python\n`query caching <http://aka.ms/queryCaching>_`\n```\n\n**Should be:**\n```python\n`query caching <http://aka.ms/queryCaching>`_\n```"
    }
  ]
}
JSONEOF

# Add the commit_id to the payload using jq
jq --arg sha "$COMMIT_SHA" '.commit_id = $sha' /tmp/review_payload.json > /tmp/review_payload_final.json

echo "Payload:"
cat /tmp/review_payload_final.json

echo ""
echo "Sending review..."
gh api repos/microsoft/semantic-link-labs/pulls/1059/reviews --method POST --input /tmp/review_payload_final.json

echo ""
echo "Done! Review posted to PR #1059"
