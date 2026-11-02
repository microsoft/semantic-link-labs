---
name: github-repo-explore
description: Guide for searching and exploring external GitHub repositories using the gh CLI. Use this when you need reference implementations, patterns, or code examples from open-source projects to help complete your task.
---

# GitHub Repository Search and Exploration

This skill enables agents to discover, clone, and explore external GitHub repositories when implementing features that may benefit from reference implementations in open-source projects.

## When to Use This Skill

Use this skill when you need to:
- Find reference implementations for features you're building
- Explore how other projects implement similar patterns
- Search for code examples across GitHub
- Clone external repos for local exploration and learning
- Find libraries or tools that could inform your implementation

**Do NOT use this skill when**:
- The task is straightforward and doesn't need external references
- The codebase already has established patterns to follow
- You're working on proprietary code that shouldn't reference external sources

---

## Resource Directory

All external GitHub repositories **MUST** be stored in:

```
.agent_cache/resources/
```

This is the **centralized** location for all cloned external repositories. Always check here first before searching GitHub.

### Directory Structure

```
.agent_cache/
└── resources/
    ├── microsoft/
    │   ├── semantic-kernel/
    │   └── TypeChat/
    ├── langchain-ai/
    │   └── langchain/
    └── Significant-Gravitas/
        └── AutoGPT/
```

Repositories are organized by `{owner}/{repo}` structure to prevent naming conflicts.

---

## Workflow Overview

```
┌─────────────────────────────────────────────────────────────────┐
│  1. Check Local Cache (.agent_cache/resources/)                 │
│     └─ If found → Use local repo for exploration                │
└────────────────────────────┬────────────────────────────────────┘
                             │ Not found
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  2. Authenticate with GitHub (if needed)                        │
│     └─ gh auth login                                            │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  3. Search GitHub for Repositories                              │
│     └─ gh search repos <query>                                  │
│     └─ gh search code <query>                                   │
└────────────────────────────┬────────────────────────────────────┘
                             │ Found match
                             ▼
┌───────────────────────────────────────────────────────────────────┐
│  4. Clone to Cache Directory                                      │
│     └─ gh repo clone owner/repo .agent_cache/resources/owner/repo │
└────────────────────────────┬──────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  5. Explore Repository                                          │
│     └─ Use gh commands and file reading tools                   │
└─────────────────────────────────────────────────────────────────┘
```

---

## Phase 1: Check Local Cache First

Before searching GitHub, always check if a relevant repository is already cloned locally.

### List Cached Repositories

```bash
# List all cached repositories
ls -la .agent_cache/resources/

# List repos by owner
ls -la .agent_cache/resources/microsoft/

# Search for specific repo by name
find .agent_cache/resources -type d -name "*semantic*" 2>/dev/null
```

### Explore Local Repository

If the repository exists locally, use standard file tools:

```bash
# View repository structure
tree -L 2 .agent_cache/resources/microsoft/semantic-kernel/

# Search for patterns in local repo
grep -r "planning" .agent_cache/resources/microsoft/semantic-kernel/python/ --include="*.py" | head -20

# Find specific files
find .agent_cache/resources/microsoft/semantic-kernel -name "*.md" -path "*skills*"
```

---

## Phase 2: GitHub Authentication

If you need to search or clone from GitHub, ensure authentication is set up.

### Check Authentication Status

```bash
gh auth status
```

### Interactive Login (if needed)

```bash
# Interactive login - user will authenticate via browser
gh auth login
```

This opens a browser for the user to authenticate with their GitHub account.

### Verify Login

```bash
# Verify authentication worked
gh auth status
```

---

## Phase 3: Search GitHub

Use `gh search` commands to find relevant repositories or code.

### Search for Repositories

```bash
# Search repos by keywords
gh search repos "agent planning skills" --limit 10

# Search with language filter
gh search repos "agent planning" --language python --limit 10

# Search by owner
gh search repos --owner microsoft "semantic kernel" --limit 10

# Search with JSON output for parsing
gh search repos "agent planning" --json fullName,description,stargazersCount --limit 5

# Sort by stars (most popular)
gh search repos "agent planning" --sort stars --limit 10

# Filter by number of stars
gh search repos "agent framework" --stars ">1000" --limit 10
```

### Search for Code

```bash
# Search code across GitHub
gh search code "TaskLedger" --limit 10

# Search code in specific language
gh search code "def plan" --language python --limit 10

# Search code in specific repo
gh search code "planning" --repo microsoft/semantic-kernel --limit 20

# Search code by filename
gh search code "skills" --filename "*.md" --limit 10

# Search with JSON output
gh search code "agent skills" --json path,repository --limit 10
```

### View Repository Details

```bash
# View repo README and description
gh repo view microsoft/semantic-kernel

# View repo with specific fields as JSON
gh repo view microsoft/semantic-kernel --json name,description,stargazerCount,defaultBranchRef

# Open repo in browser
gh repo view microsoft/semantic-kernel --web
```

---

## Phase 4: Clone Repository to Cache

When you find a useful repository, clone it to the cache directory.

### Clone Commands

```bash
# Create cache directory structure
mkdir -p .agent_cache/resources/{owner}

# Clone repository
gh repo clone microsoft/semantic-kernel .agent_cache/resources/microsoft/semantic-kernel

# Clone with shallow history (faster, uses less disk)
gh repo clone microsoft/semantic-kernel .agent_cache/resources/microsoft/semantic-kernel -- --depth 1

# Clone specific branch
gh repo clone microsoft/semantic-kernel .agent_cache/resources/microsoft/semantic-kernel -- --branch main --depth 1
```

### Update Existing Clone

```bash
# Pull latest changes
cd .agent_cache/resources/microsoft/semantic-kernel && git pull && cd -
```

---

## Phase 5: Explore Repository

Once cloned, use a combination of gh commands and file reading tools.

### Repository Structure Exploration

```bash
# View directory structure
tree -L 3 .agent_cache/resources/microsoft/semantic-kernel/python/ | head -50

# Find README files
find .agent_cache/resources/microsoft/semantic-kernel -name "README.md" | head -10

# Find Python files related to agents
find .agent_cache/resources/microsoft/semantic-kernel -path "*agents*" -name "*.py" | head -20
```

### Code Search in Local Clone

```bash
# Search for specific patterns
grep -r "class.*Agent" .agent_cache/resources/microsoft/semantic-kernel/python --include="*.py" | head -20

# Find function definitions
grep -rn "def plan" .agent_cache/resources/microsoft/semantic-kernel/python --include="*.py"

# Search for imports
grep -r "from.*import" .agent_cache/resources/microsoft/semantic-kernel/python --include="*.py" | grep planning
```

### Read Specific Files

After identifying relevant files with grep/find, use the `read_file` tool to examine content:

```bash
# Example files to read
read_file .agent_cache/resources/microsoft/semantic-kernel/python/semantic_kernel/agents/orchestration/magentic.py
```

---

## Useful gh Commands Reference

### Repository Commands

| Command | Description |
|---------|-------------|
| `gh repo view <owner/repo>` | View repo README and details |
| `gh repo view <owner/repo> --json <fields>` | Get repo metadata as JSON |
| `gh repo clone <owner/repo> <path>` | Clone repo to specific path |
| `gh repo list <owner>` | List repos owned by user/org |

### Search Commands

| Command | Description |
|---------|-------------|
| `gh search repos <query>` | Search for repositories |
| `gh search code <query>` | Search within code |
| `gh search issues <query>` | Search issues |
| `gh search prs <query>` | Search pull requests |

### Search Filters

| Filter | Example | Description |
|--------|---------|-------------|
| `--language` | `--language python` | Filter by programming language |
| `--owner` | `--owner microsoft` | Filter by repository owner |
| `--stars` | `--stars ">1000"` | Filter by star count |
| `--limit` | `--limit 20` | Limit number of results |
| `--sort` | `--sort stars` | Sort results (stars, forks, updated) |
| `--json` | `--json fullName,description` | Output as JSON |

### JSON Fields for Repos

Common fields for `--json` output:
- `fullName` - owner/repo
- `name` - repo name only
- `description` - repo description
- `stargazersCount` - number of stars
- `language` - primary language
- `url` - GitHub URL
- `defaultBranchRef` - main branch name

---

## Example Workflows

### Example 1: Find Reference Implementations for REST API Patterns

```bash
# 1. Check local cache
ls .agent_cache/resources/ 2>/dev/null || echo "Cache empty"

# 2. Search GitHub for Fabric/Power BI SDK examples
gh search repos "fabric rest api python" --stars ">50" --limit 10

# 3. View promising repo
gh repo view microsoft/semantic-link-labs

# 4. Clone to cache
mkdir -p .agent_cache/resources/microsoft
gh repo clone microsoft/semantic-link-labs .agent_cache/resources/microsoft/semantic-link-labs -- --depth 1

# 5. Explore API patterns
grep -r "_base_api" .agent_cache/resources/microsoft/semantic-link-labs --include="*.py" | head -20
```

### Example 2: Search for Code Patterns

```bash
# Search for specific pattern across GitHub
gh search code "FabricRestClient" --language python --limit 5

# Clone repo containing the pattern
gh repo clone owner/repo .agent_cache/resources/owner/repo -- --depth 1

# Find and read the file
grep -rn "FabricRestClient" .agent_cache/resources/owner/repo --include="*.py"
```

### Example 3: Explore Multiple Repos

```bash
# Search for agent frameworks
gh search repos "agent framework" --language python --stars ">2000" --json fullName,description,stargazersCount

# Clone top candidates
for repo in "langchain-ai/langchain" "Significant-Gravitas/AutoGPT"; do
    owner=$(dirname $repo)
    mkdir -p ".agent_cache/resources/$owner"
    gh repo clone "$repo" ".agent_cache/resources/$repo" -- --depth 1
done

# Compare implementations
diff <(grep -r "def plan" .agent_cache/resources/langchain-ai/langchain --include="*.py" | head -5) \
     <(grep -r "def plan" .agent_cache/resources/Significant-Gravitas/AutoGPT --include="*.py" | head -5)
```

---

## Best Practices

1. **Always check local cache first** -- Avoid redundant cloning
2. **Use shallow clones** -- Add `-- --depth 1` to save disk space and time
3. **Organize by owner/repo** -- Prevents naming conflicts
4. **Use JSON output** -- Easier to parse programmatically
5. **Start with grep/find** -- Narrow down before reading full files
6. **Respect rate limits** -- GitHub has API rate limits; cache results
7. **Clean up periodically** -- Remove unused repos from cache

---

## Troubleshooting

### Authentication Issues

```bash
# Check current auth status
gh auth status

# Re-authenticate
gh auth login

# Use different auth method
gh auth login --with-token < token.txt
```

### Rate Limiting

```bash
# Check rate limit status
gh api rate_limit

# If rate limited, wait or use cached repos
```

### Clone Failures

```bash
# If clone fails, try with HTTPS explicitly
git clone https://github.com/owner/repo.git .agent_cache/resources/owner/repo

# Or try shallow clone
git clone --depth 1 https://github.com/owner/repo.git .agent_cache/resources/owner/repo
```
