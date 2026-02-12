---
name: planning-with-files
description: "**USE THIS FOR COMPLEX TASKS.** Implements Manus-style file-based planning for multi-step tasks. Creates task_plan.md, findings.md, and progress.md in .agent_cache/<task-name>/. Use when: implementing multiple APIs, refactoring modules, research tasks, or ANY task requiring >5 tool calls."
---

# Planning with Files

Work like Manus: Use persistent markdown files as your "working memory on disk." This skill helps you plan, track, and checkpoint complex tasks that span multiple files, APIs, or sessions.

## WHEN TO USE THIS SKILL

**ALWAYS use this skill when:**
- Task involves 3+ phases or steps
- Implementing wrappers for multiple REST APIs
- Migrating or refactoring code across multiple files
- Research tasks requiring exploration
- Any task requiring >5 tool calls
- Work that may span multiple sessions
- Tasks with dependencies between components

**Skip this skill for:**
- Simple questions or quick lookups
- Single-file edits
- Tasks completable in 1-2 tool calls

---

## The Core Principle

```
Context Window = RAM (volatile, limited)
Filesystem = Disk (persistent, unlimited)

-> Anything important gets written to disk.
-> After ~50 tool calls, you FORGET original goals.
-> Re-reading plan files keeps goals in your attention window.
```

---

## File Structure

All planning files go in `.agent_cache/<task-name>/`:

```
.agent_cache/
└── <task-name>/
    ├── task_plan.md    # Phases, progress, decisions (YOUR ROADMAP)
    ├── findings.md     # Research, discoveries, technical decisions
    └── progress.md     # Session log, test results, files modified
```

### Naming Convention

- Use lowercase with hyphens for `<task-name>`
- Be descriptive but concise
- Examples:
  - `.agent_cache/add-admin-apis/`
  - `.agent_cache/migrate-lakehouse-module/`
  - `.agent_cache/add-direct-lake-functions/`

---

## The 3-File Pattern

| File | Purpose | When to Update |
|------|---------|----------------|
| `task_plan.md` | Phases, progress, current status | After each phase completes |
| `findings.md` | Research, discoveries, decisions | After ANY discovery |
| `progress.md` | Session log, test results, errors | Throughout session |

---

## Quick Start: Before ANY Complex Task

```bash
# 1. Create the planning directory
mkdir -p .agent_cache/<task-name>

# 2. Create all 3 files using templates below
# 3. Re-read plan before major decisions
# 4. Update after each phase completes
```

---

## Critical Rules

### Rule 1: Create Plan FIRST
Never start a complex task without `task_plan.md`. **Non-negotiable.**

### Rule 2: The 2-Action Rule
> "After every 2 view/search/explore operations, IMMEDIATELY save key findings to files."

This prevents information from being lost as context grows.

### Rule 3: Read Before Decide
Before major decisions, read the plan file. This pushes goals into your attention window.

### Rule 4: Update After Act
After completing any phase:
- Mark phase status: `in_progress` -> `complete`
- Log files created/modified
- Note any errors encountered

### Rule 5: Log ALL Errors
Every error goes in the plan file. This builds knowledge and prevents repetition.

### Rule 6: Never Repeat Failures
```
if action_failed:
    next_action != same_action
```
Track what you tried. Mutate the approach.

---

## The 3-Strike Error Protocol

```
ATTEMPT 1: Diagnose & Fix
  -> Read error carefully
  -> Identify root cause
  -> Apply targeted fix

ATTEMPT 2: Alternative Approach
  -> Same error? Try different method
  -> Different tool? Different pattern?
  -> NEVER repeat exact same failing action

ATTEMPT 3: Broader Rethink
  -> Question assumptions
  -> Search for solutions (use github-repo-explore skill)
  -> Consider updating the plan

AFTER 3 FAILURES: Escalate to User
  -> Explain what you tried
  -> Share the specific error
  -> Ask for guidance
```

---

## Workflow: The Agent Loop

```
+--------------------------------------------+
|  1. READ PLAN                              |
|     - cat .agent_cache/<task>/task_plan.md |
|     - Understand current phase             |
|     - Review goals                         |
+--------------------------------------------+
|  2. ANALYZE                                |
|     - What's the next task?                |
|     - Are there blockers?                  |
|     - Do I have what I need?               |
+--------------------------------------------+
|  3. EXECUTE                                |
|     - Perform ONE logical action           |
|     - Write code to files                  |
|     - Run tests                            |
+--------------------------------------------+
|  4. UPDATE FILES                           |
|     - Log progress in progress.md          |
|     - Save discoveries in findings.md      |
|     - Update status in task_plan.md        |
+--------------------------------------------+
|  5. REPEAT                                 |
|     - Read plan again before next phase    |
+--------------------------------------------+
```

---

## Template: task_plan.md

```markdown
# Task Plan: [Brief Description]

## Goal
[One clear sentence describing the end state]

## Current Phase
Phase 1

## Guidelines
<!-- Project-specific constraints and references -->
- Follow numpydoc style for docstrings
- Use `@log` decorator for all public functions
- Use `_base_api` helper for REST calls
- Reference: [add-function skill](../../.claude/skills/add-function/SKILL.md)
- Reference: [rest-api-patterns skill](../../.claude/skills/rest-api-patterns/SKILL.md)

---

## Phases

### Phase 1: Requirements & Discovery
- [ ] Understand user intent and scope
- [ ] Identify files/APIs involved
- [ ] Document findings in findings.md
- **Status:** in_progress

### Phase 2: Dependency Analysis
- [ ] Map dependencies between components
- [ ] Determine implementation order
- [ ] Update plan with detailed tasks
- **Status:** pending

### Phase 3: Implementation
- [ ] Implement each component in order
- [ ] Write code following project conventions
- [ ] Document decisions in findings.md
- **Status:** pending

### Phase 4: Testing
- [ ] Write unit tests (see write-tests skill)
- [ ] Run tests with `pytest -sv tests/ -k <test_name>`
- [ ] Document results in progress.md
- **Status:** pending

### Phase 5: Completion
- [ ] Run code style checks: `black src/sempy_labs tests && flake8 src/sempy_labs tests`
- [ ] Verify all checkboxes complete
- [ ] Mark directory as done
- **Status:** pending

---

## Errors Encountered
| Error | Attempt | Resolution |
|-------|---------|------------|
|       |         |            |

---

## Decisions Made
| Decision | Rationale |
|----------|-----------|
|          |           |
```

---

## Template: findings.md

```markdown
# Findings & Decisions

## Task
[Brief description of the task]

---

## Requirements
<!-- Captured from user request -->
-

## Research Findings
<!-- Key discoveries during exploration -->
<!-- UPDATE AFTER EVERY 2 SEARCH/EXPLORE OPERATIONS -->
-

## Code Patterns Found
<!-- Existing patterns in codebase to follow -->
| Pattern | Location | Usage |
|---------|----------|-------|
|         |          |       |

## Technical Decisions
| Decision | Rationale |
|----------|-----------|
|          |           |

## Files to Modify
| File | Change |
|------|--------|
|      |        |

## API/Function Inventory
<!-- For API wrapper tasks -->
| Function | Source | Status |
|----------|--------|--------|
|          |        |        |

## Issues Encountered
| Issue | Resolution |
|-------|------------|
|       |            |

## External References
<!-- Links to docs, GitHub repos, etc. -->
-
```

---

## Template: progress.md

```markdown
# Progress Log

## Session: [DATE]

### Phase 1: [Title]
- **Status:** in_progress
- **Started:** [timestamp]
- Actions taken:
  -
- Files created/modified:
  -

### Phase 2: [Title]
- **Status:** pending
- Actions taken:
  -
- Files created/modified:
  -

---

## Test Results
| Test | Expected | Actual | Status |
|------|----------|--------|--------|
|      |          |        |        |

---

## Commands Run
```bash
# Useful commands executed during this session
```

---

## Session Notes
<!-- Any observations, blockers, or context for next session -->
```

---

## Project-Specific Patterns

### For Semantic Link Labs API Wrapper Tasks

When implementing REST API wrappers, include in findings.md:

```markdown
## API Analysis
| API | Fabric Path | Power BI Path | Notes |
|-----|-------------|---------------|-------|
| list_items | /v1/workspaces/{id}/items | /v1.0/myorg/groups/{id}/... | Paginated |

## Implementation Pattern
- Use `_base_api` helper for all REST calls
- Use `resolve_workspace_name_and_id` for workspace resolution
- Use `_create_dataframe` for empty DataFrame initialization
- Return `pandas.DataFrame` for list operations

## Required Decorator
- `@log` -- Enable logging and telemetry

## Function Naming
| Prefix | Use Case |
|--------|----------|
| `list_` | Retrieves a collection |
| `get_` | Retrieves a single item |
| `create_` | Creates a new resource |
| `update_` | Modifies existing resource |
| `delete_` | Removes a resource |
```

### For Test Writing Tasks

Include in findings.md:

```markdown
## Test Structure
| Type | Location |
|------|----------|
| Unit tests | tests/ |

## Key Patterns
- Use `pytest.mark.parametrize` for multiple inputs
- Mock `_base_api` for API wrapper tests
- Use `_create_dataframe` to verify empty DataFrame structure
- See write-tests skill for full patterns
```

---

## Completion Workflow

After finishing all work:

```bash
# 1. Verify all checkboxes are marked complete
grep -c "\[x\]" .agent_cache/<task-name>/task_plan.md
grep -c "\[ \]" .agent_cache/<task-name>/task_plan.md

# 2. Rename directory with [done] suffix
mv .agent_cache/<task-name> .agent_cache/<task-name>-[done]
```

---

## The 5-Question Reboot Test

When resuming work, verify you can answer:

| Question | Answer Source |
|----------|---------------|
| Where am I? | Current phase in task_plan.md |
| Where am I going? | Remaining phases |
| What's the goal? | Goal statement in plan |
| What have I learned? | findings.md |
| What have I done? | progress.md |

If you can't answer these, read all 3 planning files before continuing.

---

## Read vs Write Decision Matrix

| Situation | Action | Reason |
|-----------|--------|--------|
| Just wrote a file | DON'T read | Content still in context |
| After 2+ searches | Write findings NOW | Before info is lost |
| Starting new phase | Read plan/findings | Re-orient context |
| Error occurred | Read relevant file | Need current state |
| Resuming after gap | Read ALL planning files | Recover state |
| Before major decision | Read task_plan.md | Refresh goals |

---

## Anti-Patterns

| Don't | Do Instead |
|-------|------------|
| Start executing immediately | Create plan file FIRST |
| State goals once and forget | Re-read plan before decisions |
| Hide errors and retry silently | Log errors to plan file |
| Stuff everything in context | Store large content in files |
| Repeat failed actions | Track attempts, mutate approach |
| Skip testing | Always include test tasks |

---

## Example: Adding Admin API Wrappers

### task_plan.md
```markdown
# Task Plan: Add Admin Workspace APIs

## Goal
Implement wrapper functions for Fabric Admin Workspace APIs in the sempy_labs.admin module.

## Current Phase
Phase 3

## Guidelines
- Use `@log` decorator on all public functions
- Use `_base_api` helper for REST calls
- Reference: add-function skill, rest-api-patterns skill

---

## Phases

### Phase 1: Requirements & Discovery
- [x] Read Fabric Admin API documentation
- [x] Identify workspace-related endpoints
- [x] Document in findings.md
- **Status:** complete

### Phase 2: Dependency Analysis
- [x] Check existing workspace implementations
- [x] Create function inventory
- **Status:** complete

### Phase 3: Implementation
- [x] `list_workspaces`
- [x] `get_workspace`
- [ ] `list_workspace_access_details`
- [ ] `update_workspace`
- **Status:** in_progress

### Phase 4: Testing
- [ ] Write unit tests with mocks
- [ ] Run tests
- **Status:** pending

### Phase 5: Completion
- [ ] Run black, flake8
- [ ] Verify all tests pass
- **Status:** pending

---

## Errors Encountered
| Error | Attempt | Resolution |
|-------|---------|------------|
| Pagination not working | 1 | Need uses_pagination=True in _base_api |
```

---

## Related Skills

| Skill | When to Use |
|-------|-------------|
| [add-function](../add-function/SKILL.md) | Adding new API wrapper functions |
| [rest-api-patterns](../rest-api-patterns/SKILL.md) | REST API implementation patterns |
| [write-tests](../write-tests/SKILL.md) | Writing unit tests |
| [code-style](../code-style/SKILL.md) | Running linters and formatters |
| [run-tests](../run-tests/SKILL.md) | Running pytest locally |
| [github-repo-explore](../github-repo-explore/SKILL.md) | Finding reference implementations |
