---
name: task-management
description: Guide for planning, tracking, and checkpointing multi-step tasks. Use this for large workloads that span multiple files, APIs, or sessions.
---

# Task Management

This skill covers how to plan, track, and checkpoint multi-step tasks that may span multiple sessions or require staged execution.

## When to Use This Skill

Use this skill when you need to:
- Implement wrappers for a large set of APIs
- Refactor code across multiple files with dependencies
- Any task that cannot be completed in a single session
- Work that requires understanding dependencies between components

---

## Checkpoint Location

All checkpoint files must be stored in:

```
.agent_cache/<work-item-name>/checkpoint.md
```

### Naming Convention

- Use lowercase with hyphens for `<work-item-name>`
- Be descriptive but concise
- Examples:
  - `.agent_cache/add-admin-apis/checkpoint.md`
  - `.agent_cache/refactor-lakehouse-module/checkpoint.md`
  - `.agent_cache/add-direct-lake-functions/checkpoint.md`

### Completion Marker

After completing a large task, rename the directory to include `[done]`:

```bash
# Before completion
.agent_cache/add-admin-apis/

# After completion
.agent_cache/add-admin-apis-[done]/
```

This allows users to periodically review and clean up stale cache directories.

---

## Checkpoint Workflow

### Phase 1: Initial Planning

Before writing any code, create a checkpoint file with:

1. **Overall Goal** — Clear description of the end state
2. **Guidelines** — Constraints, requirements, and references
3. **Notes** — Important considerations and gotchas
4. **Implementation Plan** — Staged breakdown with dependencies

```bash
# Create the checkpoint directory
mkdir -p .agent_cache/<work-item-name>

# Create initial checkpoint file
touch .agent_cache/<work-item-name>/checkpoint.md
```

### Phase 2: Dependency Analysis

Before starting work, analyze and document:

1. **Dependency Graph** — Which components depend on others
2. **Stage Ordering** — Logical sequence based on dependencies
3. **Function Inventory** — All items to be implemented
4. **Estimated Scope** — Total count of functions/files

### Phase 3: Execution with Updates

During implementation:

1. **Mark tasks in progress** — Update checkbox when starting
2. **Mark tasks complete** — Check off `[x]` when done
3. **Add notes** — Document decisions, issues, or changes
4. **Update regularly** — Save checkpoint after each significant milestone

### Phase 4: Completion

After finishing all work:

1. Verify all checkboxes are marked complete
2. Rename directory with `[done]` suffix
3. Optionally archive or delete after review

---

## Checkpoint File Structure

A checkpoint file should contain these sections:

### Required Sections

```markdown
# Overall Goal
[Clear description of what needs to be accomplished]

# Guidelines
[Constraints, coding standards, references to follow]

# Notes
[Important considerations, gotchas, decisions made]

---

# Implementation Plan

## Summary
[Scope overview with counts and dependency graph]

---

## Stage N: [Stage Name]

**Source:** [Source location(s) or API docs]
**Target:** [Target file(s)]
**Dependencies:** [What this stage depends on]

- [ ] Task 1
- [ ] Task 2
- [x] Completed Task
- [ ] Unit tests
```

### Template Reference

See the full template at: [templates/checkpoint-template.md](templates/checkpoint-template.md)

---

## Best Practices

### 1. Start with Dependencies

Always identify the dependency graph first:

```
Stage 1: Base utilities (no dependencies)
    ↓
Stage 2: Components using Stage 1
    ↓
Stage 3: Components using Stage 1 + 2
    ↓
...
```

### 2. Group Related Items

Group functions/files by:
- Shared dependencies
- Same source/target modules
- Similar functionality
- Testing requirements

### 3. Keep Granularity Appropriate

- **Too granular**: Every line of code as a task
- **Too broad**: Entire module as single task
- **Just right**: One function or logical unit per task

### 4. Update Frequently

Update the checkpoint file:
- After completing each function
- After completing each stage
- When encountering blockers or decisions
- Before ending a session

### 5. Include Testing Tasks

Always include test tasks for each stage:

```markdown
- [ ] `function_name`
- [ ] `another_function`
- [ ] Unit tests for this stage
```

### 6. Document Decisions

Add notes when making non-obvious decisions:

```markdown
### Notes
- Changed parameter name from `group_id` to `workspace_id` for consistency
- Skipped `deprecated_function` as it's no longer in the API
```

---

## Restoring from Checkpoint

When resuming work from a checkpoint:

1. **Read the checkpoint file** to understand current state
2. **Identify next incomplete task** (first unchecked item)
3. **Review dependencies** to ensure prerequisites are complete
4. **Continue from where you left off**

### Example Restoration Prompt

```
Resume work from checkpoint at .agent_cache/add-admin-apis/checkpoint.md

Continue with the next incomplete task according to the plan.
```

---

## Checkpoint File Checklist

Before starting work, ensure your checkpoint includes:

- [ ] Clear overall goal
- [ ] Guidelines and constraints
- [ ] Dependency graph showing stage order
- [ ] All stages defined with source/target
- [ ] All tasks listed with checkboxes
- [ ] Testing tasks for each stage
- [ ] Notes section for decisions

---

## Example Commands

### Create New Checkpoint

```bash
mkdir -p .agent_cache/my-work-item
# Then create checkpoint.md with the template
```

### Check Progress

```bash
# Count completed vs total tasks
grep -c "\[x\]" .agent_cache/my-work-item/checkpoint.md
grep -c "\[ \]" .agent_cache/my-work-item/checkpoint.md
```

### Mark as Done

```bash
mv .agent_cache/my-work-item .agent_cache/my-work-item-[done]
```

### Clean Up Old Checkpoints

```bash
# List completed work items
ls -d .agent_cache/*-\[done\]

# Remove completed items older than 30 days
find .agent_cache -name "*-\[done\]" -type d -mtime +30 -exec rm -rf {} \;
```

---

## Related Skills

| Skill | Purpose |
|-------|---------|
| [add-function](../add-function/SKILL.md) | Patterns for adding new functions |
| [rest-api-patterns](../rest-api-patterns/SKILL.md) | REST API wrapper patterns |
| [write-tests](../write-tests/SKILL.md) | Writing unit tests |
