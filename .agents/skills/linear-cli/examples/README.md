# Examples for linear-cli

## Quick Prompts

### List my issues
```
Show me my Linear issues
```

### Create an issue
```
Create a Linear issue titled "Fix payment processing bug" with priority high and label it as a bug
```

### Start working on an issue
```
Start working on Linear issue ABC-123
```

### Create a PR for current issue
```
Create a GitHub PR for the Linear issue I'm working on
```

### Update issue status
```
Mark Linear issue ABC-123 as done
```

### View project status
```
Show me the status of the "Q1 Roadmap" project in Linear
```

## Common Workflows

### Bug Triage
```bash
# List unassigned bugs
linear issue list --unassigned --label "bug"

# Assign and start one
linear issue update ABC-123 --assignee @me
linear issue start ABC-123
```

### Sprint Planning
```bash
# See all team issues
linear issue list --all-assignees --state "Backlog"

# Check project progress
linear project list --status active
linear project view PROJECT-ID
```

### End of Day
```bash
# See what you worked on
linear issue list --state "In Progress"

# Add status update comment
linear issue comment add "Made progress on X, will continue tomorrow"
```
