use anyhow::Result;
use sqlx::PgPool;

pub async fn setup_schema(pool: &PgPool) -> Result<()> {
    sqlx::raw_sql(
        r#"
        CREATE SCHEMA IF NOT EXISTS scheduler;

        CREATE TABLE IF NOT EXISTS scheduler.tasks (
            id                TEXT PRIMARY KEY,
            task_type         TEXT NOT NULL CHECK (task_type IN ('send_email', 'follow_up', 'reminder')),
            payload           JSONB NOT NULL DEFAULT '{}',
            run_at            TIMESTAMPTZ NOT NULL,
            status            TEXT NOT NULL DEFAULT 'pending'
                              CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled')),
            recurring_pattern TEXT DEFAULT NULL
                              CHECK (recurring_pattern IS NULL OR recurring_pattern IN ('daily', 'weekly', 'monthly')),
            last_run          TIMESTAMPTZ DEFAULT NULL,
            created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS scheduler.execution_log (
            id          TEXT PRIMARY KEY,
            task_id     TEXT NOT NULL REFERENCES scheduler.tasks(id) ON DELETE CASCADE,
            executed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            result      JSONB NOT NULL DEFAULT '{}',
            success     BOOLEAN NOT NULL DEFAULT true
        );

        CREATE INDEX IF NOT EXISTS idx_tasks_status ON scheduler.tasks(status);
        CREATE INDEX IF NOT EXISTS idx_tasks_run_at ON scheduler.tasks(run_at);
        CREATE INDEX IF NOT EXISTS idx_tasks_type ON scheduler.tasks(task_type);
        CREATE INDEX IF NOT EXISTS idx_execution_log_task_id ON scheduler.execution_log(task_id);
        "#,
    )
    .execute(pool)
    .await?;

    Ok(())
}
