# dataxlr8-scheduler-mcp

MCP server for scheduling and managing timed tasks (emails, follow-ups, reminders) with support for one-time and recurring execution patterns (daily, weekly, monthly).

## Tools

| Tool | Description |
|------|-------------|
| schedule_task | Schedule a future action (send_email, follow_up, reminder) with a run_at timestamp |
| list_scheduled | List upcoming scheduled tasks with optional filters by status and task_type |
| cancel_task | Cancel a scheduled task by ID |
| process_due | Find and mark all tasks where run_at <= now as running, returning them for execution |
| reschedule | Change the run_at timestamp for an existing pending task |
| create_recurring | Create a recurring task that repeats daily, weekly, or monthly |
| scheduler_stats | Get task counts grouped by status and type |
| overdue_tasks | Find tasks past their run_at that haven't been executed (status still pending) |

## Setup

```bash
DATABASE_URL=postgres://dataxlr8:dataxlr8@localhost:5432/dataxlr8 cargo run
```

## Schema

Creates `scheduler.*` schema in PostgreSQL with tables:
- `scheduler.tasks` - Scheduled tasks with recurrence patterns
- `scheduler.execution_log` - Task execution history

## Part of

[DataXLR8](https://github.com/pdaxt) - AI-powered recruitment platform
