use dataxlr8_mcp_core::mcp::{empty_schema, error_result, get_str, get_i64, json_result, make_schema};
use dataxlr8_mcp_core::Database;
use rmcp::model::*;
use rmcp::service::{RequestContext, RoleServer};
use rmcp::ServerHandler;
use serde::{Deserialize, Serialize};
use tracing::info;

// ============================================================================
// Data types
// ============================================================================

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct ScheduledTask {
    pub id: String,
    pub task_type: String,
    pub payload: serde_json::Value,
    pub run_at: chrono::DateTime<chrono::Utc>,
    pub status: String,
    pub recurring_pattern: Option<String>,
    pub last_run: Option<chrono::DateTime<chrono::Utc>>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
#[allow(dead_code)]
pub struct ExecutionLog {
    pub id: String,
    pub task_id: String,
    pub executed_at: chrono::DateTime<chrono::Utc>,
    pub result: serde_json::Value,
    pub success: bool,
}

// ============================================================================
// Tool definitions
// ============================================================================

fn build_tools() -> Vec<Tool> {
    vec![
        Tool {
            name: "schedule_task".into(),
            title: None,
            description: Some("Schedule a future action (send_email, follow_up, reminder) with a run_at timestamp".into()),
            input_schema: make_schema(
                serde_json::json!({
                    "task_type": { "type": "string", "enum": ["send_email", "follow_up", "reminder"], "description": "Type of task" },
                    "run_at": { "type": "string", "description": "ISO 8601 timestamp for when to execute (e.g. 2026-03-06T09:00:00Z)" },
                    "payload": { "type": "object", "description": "JSON payload with task-specific data (e.g. recipient, subject, body)" }
                }),
                vec!["task_type", "run_at"],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "list_scheduled".into(),
            title: None,
            description: Some("List upcoming scheduled tasks with optional filters by status and task_type".into()),
            input_schema: make_schema(
                serde_json::json!({
                    "status": { "type": "string", "enum": ["pending", "running", "completed", "failed", "cancelled"], "description": "Filter by status" },
                    "task_type": { "type": "string", "enum": ["send_email", "follow_up", "reminder"], "description": "Filter by task type" },
                    "limit": { "type": "integer", "description": "Max results (default 50)" }
                }),
                Vec::<&str>::new(),
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "cancel_task".into(),
            title: None,
            description: Some("Cancel a scheduled task by ID".into()),
            input_schema: make_schema(
                serde_json::json!({
                    "task_id": { "type": "string", "description": "ID of the task to cancel" }
                }),
                vec!["task_id"],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "process_due".into(),
            title: None,
            description: Some("Find and mark all tasks where run_at <= now as running, returning them for execution".into()),
            input_schema: empty_schema(),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "reschedule".into(),
            title: None,
            description: Some("Change the run_at timestamp for an existing pending task".into()),
            input_schema: make_schema(
                serde_json::json!({
                    "task_id": { "type": "string", "description": "ID of the task to reschedule" },
                    "run_at": { "type": "string", "description": "New ISO 8601 timestamp" }
                }),
                vec!["task_id", "run_at"],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "create_recurring".into(),
            title: None,
            description: Some("Create a recurring task that repeats daily, weekly, or monthly".into()),
            input_schema: make_schema(
                serde_json::json!({
                    "task_type": { "type": "string", "enum": ["send_email", "follow_up", "reminder"], "description": "Type of task" },
                    "run_at": { "type": "string", "description": "ISO 8601 timestamp for first execution" },
                    "recurring_pattern": { "type": "string", "enum": ["daily", "weekly", "monthly"], "description": "Recurrence pattern" },
                    "payload": { "type": "object", "description": "JSON payload with task-specific data" }
                }),
                vec!["task_type", "run_at", "recurring_pattern"],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "scheduler_stats".into(),
            title: None,
            description: Some("Get task counts grouped by status and type".into()),
            input_schema: empty_schema(),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "overdue_tasks".into(),
            title: None,
            description: Some("Find tasks past their run_at that haven't been executed (status still pending)".into()),
            input_schema: make_schema(
                serde_json::json!({
                    "limit": { "type": "integer", "description": "Max results (default 50)" }
                }),
                Vec::<&str>::new(),
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
    ]
}

// ============================================================================
// MCP Server
// ============================================================================

#[derive(Clone)]
pub struct SchedulerMcpServer {
    db: Database,
}

impl SchedulerMcpServer {
    pub fn new(db: Database) -> Self {
        Self { db }
    }

    fn parse_timestamp(s: &str) -> Option<chrono::DateTime<chrono::Utc>> {
        s.parse::<chrono::DateTime<chrono::Utc>>().ok()
    }

    // ---- Tool handlers ----

    async fn handle_schedule_task(&self, args: &serde_json::Value) -> CallToolResult {
        let task_type = match get_str(args, "task_type") {
            Some(t) => t,
            None => return error_result("Missing required parameter: task_type"),
        };
        if !["send_email", "follow_up", "reminder"].contains(&task_type.as_str()) {
            return error_result("task_type must be one of: send_email, follow_up, reminder");
        }

        let run_at_str = match get_str(args, "run_at") {
            Some(r) => r,
            None => return error_result("Missing required parameter: run_at"),
        };
        let run_at = match Self::parse_timestamp(&run_at_str) {
            Some(t) => t,
            None => return error_result("Invalid run_at timestamp. Use ISO 8601 format (e.g. 2026-03-06T09:00:00Z)"),
        };

        let payload = args.get("payload").cloned().unwrap_or(serde_json::json!({}));
        let id = uuid::Uuid::new_v4().to_string();

        match sqlx::query_as::<_, ScheduledTask>(
            "INSERT INTO scheduler.tasks (id, task_type, payload, run_at) VALUES ($1, $2, $3, $4) RETURNING *",
        )
        .bind(&id)
        .bind(&task_type)
        .bind(&payload)
        .bind(run_at)
        .fetch_one(self.db.pool())
        .await
        {
            Ok(task) => {
                info!(id = id, task_type = task_type, "Scheduled task");
                json_result(&task)
            }
            Err(e) => error_result(&format!("Failed to schedule task: {e}")),
        }
    }

    async fn handle_list_scheduled(&self, args: &serde_json::Value) -> CallToolResult {
        let status = get_str(args, "status");
        let task_type = get_str(args, "task_type");
        let limit = get_i64(args, "limit").unwrap_or(50);

        // Build dynamic query
        let mut conditions = Vec::new();
        let mut param_idx = 1u32;

        if status.is_some() {
            conditions.push(format!("status = ${param_idx}"));
            param_idx += 1;
        }
        if task_type.is_some() {
            conditions.push(format!("task_type = ${param_idx}"));
            param_idx += 1;
        }

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        let query_str = format!(
            "SELECT * FROM scheduler.tasks {where_clause} ORDER BY run_at ASC LIMIT ${param_idx}"
        );

        let mut query = sqlx::query_as::<_, ScheduledTask>(&query_str);

        if let Some(ref s) = status {
            query = query.bind(s);
        }
        if let Some(ref t) = task_type {
            query = query.bind(t);
        }
        query = query.bind(limit);

        match query.fetch_all(self.db.pool()).await {
            Ok(tasks) => json_result(&tasks),
            Err(e) => error_result(&format!("Database error: {e}")),
        }
    }

    async fn handle_cancel_task(&self, task_id: &str) -> CallToolResult {
        match sqlx::query_as::<_, ScheduledTask>(
            "UPDATE scheduler.tasks SET status = 'cancelled' WHERE id = $1 AND status = 'pending' RETURNING *",
        )
        .bind(task_id)
        .fetch_optional(self.db.pool())
        .await
        {
            Ok(Some(task)) => {
                info!(id = task_id, "Cancelled task");
                json_result(&task)
            }
            Ok(None) => error_result(&format!("Task '{task_id}' not found or not in pending status")),
            Err(e) => error_result(&format!("Failed to cancel task: {e}")),
        }
    }

    async fn handle_process_due(&self) -> CallToolResult {
        // Atomically claim all due pending tasks
        let tasks: Vec<ScheduledTask> = match sqlx::query_as(
            "UPDATE scheduler.tasks SET status = 'running' WHERE status = 'pending' AND run_at <= now() RETURNING *",
        )
        .fetch_all(self.db.pool())
        .await
        {
            Ok(t) => t,
            Err(e) => return error_result(&format!("Database error: {e}")),
        };

        if tasks.is_empty() {
            return json_result(&serde_json::json!({ "processed": 0, "tasks": [] }));
        }

        // Log execution and mark completed for each task
        let mut results = Vec::new();
        for task in &tasks {
            let log_id = uuid::Uuid::new_v4().to_string();
            let exec_result = serde_json::json!({
                "task_type": task.task_type,
                "payload": task.payload,
                "claimed_at": chrono::Utc::now(),
            });

            // Insert execution log
            let _ = sqlx::query(
                "INSERT INTO scheduler.execution_log (id, task_id, result, success) VALUES ($1, $2, $3, true)",
            )
            .bind(&log_id)
            .bind(&task.id)
            .bind(&exec_result)
            .execute(self.db.pool())
            .await;

            // For recurring tasks: reset to pending with next run_at
            if let Some(ref pattern) = task.recurring_pattern {
                let next_run = match pattern.as_str() {
                    "daily" => task.run_at + chrono::Duration::days(1),
                    "weekly" => task.run_at + chrono::Duration::weeks(1),
                    "monthly" => task.run_at + chrono::Duration::days(30),
                    _ => task.run_at + chrono::Duration::days(1),
                };
                let _ = sqlx::query(
                    "UPDATE scheduler.tasks SET status = 'pending', run_at = $1, last_run = now() WHERE id = $2",
                )
                .bind(next_run)
                .bind(&task.id)
                .execute(self.db.pool())
                .await;
            } else {
                // Non-recurring: mark completed
                let _ = sqlx::query(
                    "UPDATE scheduler.tasks SET status = 'completed', last_run = now() WHERE id = $1",
                )
                .bind(&task.id)
                .execute(self.db.pool())
                .await;
            }

            results.push(serde_json::json!({
                "id": task.id,
                "task_type": task.task_type,
                "payload": task.payload,
                "recurring": task.recurring_pattern,
            }));
        }

        info!(count = results.len(), "Processed due tasks");
        json_result(&serde_json::json!({ "processed": results.len(), "tasks": results }))
    }

    async fn handle_reschedule(&self, task_id: &str, run_at_str: &str) -> CallToolResult {
        let run_at = match Self::parse_timestamp(run_at_str) {
            Some(t) => t,
            None => return error_result("Invalid run_at timestamp. Use ISO 8601 format"),
        };

        match sqlx::query_as::<_, ScheduledTask>(
            "UPDATE scheduler.tasks SET run_at = $1 WHERE id = $2 AND status = 'pending' RETURNING *",
        )
        .bind(run_at)
        .bind(task_id)
        .fetch_optional(self.db.pool())
        .await
        {
            Ok(Some(task)) => {
                info!(id = task_id, "Rescheduled task");
                json_result(&task)
            }
            Ok(None) => error_result(&format!("Task '{task_id}' not found or not in pending status")),
            Err(e) => error_result(&format!("Failed to reschedule: {e}")),
        }
    }

    async fn handle_create_recurring(&self, args: &serde_json::Value) -> CallToolResult {
        let task_type = match get_str(args, "task_type") {
            Some(t) => t,
            None => return error_result("Missing required parameter: task_type"),
        };
        if !["send_email", "follow_up", "reminder"].contains(&task_type.as_str()) {
            return error_result("task_type must be one of: send_email, follow_up, reminder");
        }

        let run_at_str = match get_str(args, "run_at") {
            Some(r) => r,
            None => return error_result("Missing required parameter: run_at"),
        };
        let run_at = match Self::parse_timestamp(&run_at_str) {
            Some(t) => t,
            None => return error_result("Invalid run_at timestamp. Use ISO 8601 format"),
        };

        let pattern = match get_str(args, "recurring_pattern") {
            Some(p) => p,
            None => return error_result("Missing required parameter: recurring_pattern"),
        };
        if !["daily", "weekly", "monthly"].contains(&pattern.as_str()) {
            return error_result("recurring_pattern must be one of: daily, weekly, monthly");
        }

        let payload = args.get("payload").cloned().unwrap_or(serde_json::json!({}));
        let id = uuid::Uuid::new_v4().to_string();

        match sqlx::query_as::<_, ScheduledTask>(
            "INSERT INTO scheduler.tasks (id, task_type, payload, run_at, recurring_pattern) VALUES ($1, $2, $3, $4, $5) RETURNING *",
        )
        .bind(&id)
        .bind(&task_type)
        .bind(&payload)
        .bind(run_at)
        .bind(&pattern)
        .fetch_one(self.db.pool())
        .await
        {
            Ok(task) => {
                info!(id = id, task_type = task_type, pattern = pattern, "Created recurring task");
                json_result(&task)
            }
            Err(e) => error_result(&format!("Failed to create recurring task: {e}")),
        }
    }

    async fn handle_scheduler_stats(&self) -> CallToolResult {
        #[derive(Debug, Serialize, sqlx::FromRow)]
        struct StatusCount {
            status: String,
            count: i64,
        }

        #[derive(Debug, Serialize, sqlx::FromRow)]
        struct TypeCount {
            task_type: String,
            count: i64,
        }

        let by_status: Vec<StatusCount> = match sqlx::query_as(
            "SELECT status, COUNT(*)::bigint as count FROM scheduler.tasks GROUP BY status ORDER BY status",
        )
        .fetch_all(self.db.pool())
        .await
        {
            Ok(s) => s,
            Err(e) => return error_result(&format!("Database error: {e}")),
        };

        let by_type: Vec<TypeCount> = match sqlx::query_as(
            "SELECT task_type, COUNT(*)::bigint as count FROM scheduler.tasks GROUP BY task_type ORDER BY task_type",
        )
        .fetch_all(self.db.pool())
        .await
        {
            Ok(t) => t,
            Err(e) => return error_result(&format!("Database error: {e}")),
        };

        let total: i64 = by_status.iter().map(|s| s.count).sum();

        json_result(&serde_json::json!({
            "total": total,
            "by_status": by_status,
            "by_type": by_type,
        }))
    }

    async fn handle_overdue_tasks(&self, args: &serde_json::Value) -> CallToolResult {
        let limit = get_i64(args, "limit").unwrap_or(50);

        let tasks: Vec<ScheduledTask> = match sqlx::query_as(
            "SELECT * FROM scheduler.tasks WHERE status = 'pending' AND run_at < now() ORDER BY run_at ASC LIMIT $1",
        )
        .bind(limit)
        .fetch_all(self.db.pool())
        .await
        {
            Ok(t) => t,
            Err(e) => return error_result(&format!("Database error: {e}")),
        };

        json_result(&serde_json::json!({
            "overdue_count": tasks.len(),
            "tasks": tasks,
        }))
    }
}

// ============================================================================
// ServerHandler trait implementation
// ============================================================================

impl ServerHandler for SchedulerMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::V_2024_11_05,
            capabilities: ServerCapabilities::builder()
                .enable_tools()
                .build(),
            server_info: Implementation::from_build_env(),
            instructions: Some(
                "DataXLR8 Scheduler MCP — schedule, manage, and process timed tasks (emails, follow-ups, reminders)"
                    .into(),
            ),
        }
    }

    fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListToolsResult, rmcp::ErrorData>> + Send + '_ {
        async {
            Ok(ListToolsResult {
                tools: build_tools(),
                next_cursor: None,
                meta: None,
            })
        }
    }

    fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<CallToolResult, rmcp::ErrorData>> + Send + '_ {
        async move {
            let args = serde_json::to_value(&request.arguments).unwrap_or(serde_json::Value::Null);
            let name_str: &str = request.name.as_ref();

            let result = match name_str {
                "schedule_task" => self.handle_schedule_task(&args).await,
                "list_scheduled" => self.handle_list_scheduled(&args).await,
                "cancel_task" => {
                    match get_str(&args, "task_id") {
                        Some(id) => self.handle_cancel_task(&id).await,
                        None => error_result("Missing required parameter: task_id"),
                    }
                }
                "process_due" => self.handle_process_due().await,
                "reschedule" => {
                    let task_id = match get_str(&args, "task_id") {
                        Some(id) => id,
                        None => return Ok(error_result("Missing required parameter: task_id")),
                    };
                    let run_at = match get_str(&args, "run_at") {
                        Some(r) => r,
                        None => return Ok(error_result("Missing required parameter: run_at")),
                    };
                    self.handle_reschedule(&task_id, &run_at).await
                }
                "create_recurring" => self.handle_create_recurring(&args).await,
                "scheduler_stats" => self.handle_scheduler_stats().await,
                "overdue_tasks" => self.handle_overdue_tasks(&args).await,
                _ => error_result(&format!("Unknown tool: {}", request.name)),
            };

            Ok(result)
        }
    }
}
