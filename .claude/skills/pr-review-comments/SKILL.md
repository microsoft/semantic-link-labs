---
name: pr-review-comments
description: Guide for submitting/posting inline PR review comments to GitHub. Use this when you need to post code review comments on specific lines of a Pull Request.
---

# Posting Inline PR Review Comments to GitHub

This skill documents how to post inline review comments to GitHub Pull Requests using the GitHub CLI (`gh`) and GitHub API. Inline comments appear directly on specific lines of code in the PR diff, making it easier for PR authors to identify and address issues.

## When to Use This Skill

Use this skill when you need to:
- Post inline review comments on specific lines of a PR
- Automate code review feedback with targeted comments
- Submit batch review comments via the GitHub API

---

## Prerequisites

1. **GitHub CLI installed**: Check with `gh --version`
   - If not installed on Linux:
     ```bash
     curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg | sudo dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg
     sudo chmod go+r /usr/share/keyrings/githubcli-archive-keyring.gpg
     echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" | sudo tee /etc/apt/sources.list.d/github-cli.list > /dev/null
     sudo apt update && sudo apt install gh -y
     ```

2. **GitHub CLI authenticated**: Check with `gh auth status`
   - If not authenticated: `gh auth login -h github.com -p https --web`
   - Follow the device code flow to complete authentication

---

## Method: Using GitHub API with JSON Payload

### Step 1: Get the PR's Head Commit SHA

```bash
COMMIT_SHA=$(gh api repos/{OWNER}/{REPO}/pulls/{PR_NUMBER} --jq '.head.sha')
echo "Commit SHA: $COMMIT_SHA"
```

### Step 2: Create the Review JSON Payload

Create a JSON file with the review comments. Each comment needs:
- `path`: The file path relative to repository root
- `line`: The line number in the **new version** of the file (right side of diff)
- `body`: The comment content (supports Markdown)

```bash
cat > /tmp/review.json << 'EOF'
{
  "commit_id": "YOUR_COMMIT_SHA_HERE",
  "event": "COMMENT",
  "comments": [
    {
      "path": "src/path/to/file.py",
      "line": 40,
      "body": "🔴 **Critical:** Description of the issue.\n\n**Suggested fix:**\n```python\n# your code here\n```"
    },
    {
      "path": "src/path/to/another_file.py",
      "line": 21,
      "body": "🟡 **Minor:** Another issue description."
    }
  ]
}
EOF
```

### Step 3: Post the Review

```bash
gh api repos/{OWNER}/{REPO}/pulls/{PR_NUMBER}/reviews --method POST --input /tmp/review.json
```

---

## Important Notes

### Line Numbers
- The `line` field refers to the line number in the **new version** of the file (right side of the diff)
- For deleted lines, use `side: "LEFT"` and the line number from the old version
- Make sure the line number exists in the diff, otherwise the API will reject the comment

### Event Types
| Event | Description |
|-------|-------------|
| `"COMMENT"` | Just add comments without approving/requesting changes |
| `"APPROVE"` | Approve the PR with comments |
| `"REQUEST_CHANGES"` | Request changes with comments |

### Comment Body Formatting
- Supports full GitHub Markdown
- Use `\n` for newlines in JSON
- Escape special characters properly
- Use emoji prefixes for severity: 🔴 Critical/High, 🟡 Medium/Minor, 🟢 Suggestion

### Multi-line Comments

For commenting on a range of lines:

```json
{
  "path": "src/file.py",
  "start_line": 10,
  "line": 15,
  "body": "This comment spans lines 10-15"
}
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| **"Validation Failed" error** | Check that the line number exists in the PR diff; verify the file path is correct |
| **"Resource not accessible" error** | Ensure you have write access to the repository; re-authenticate with `gh auth login` |
| **Comments not appearing on specific lines** | The line must be part of the diff (added, modified, or within context lines) |

---

## Alternative: Using `gh pr review` Command

For simpler reviews without inline comments:

```bash
gh pr review {PR_NUMBER} --repo {OWNER}/{REPO} --comment --body "Your review comment here"
```

This posts a general review comment but does **not** support inline comments on specific lines.

---

## Repository-Specific Information

- **Repository**: `microsoft/semantic-link-labs`
- **Common file paths**:
  - `src/sempy_labs/{module}/_items.py`
  - `src/sempy_labs/{module}/__init__.py`
- **Review comment conventions**:
  - Use severity emojis: 🔴 Critical, 🟡 Medium, 🟢 Minor
  - Include "Suggested fix" with code blocks
  - Reference specific line numbers

---

## Examples

See the [examples/](examples/) folder for complete working scripts:

- [post_pr1059_review.sh](examples/post_pr1059_review.sh) - Example script posting inline review comments to PR #1059
