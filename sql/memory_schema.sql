-- Ultra Tiger Memory + Orchestration Schema v4
-- Adds persistent audit logs, task timeline, and HITL queue support.

CREATE TABLE IF NOT EXISTS memory_records (
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  source TEXT NOT NULL,
  content TEXT NOT NULL,
  created_at_unix INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS memory_embeddings (
  record_id TEXT NOT NULL,
  model TEXT NOT NULL,
  chunk_index INTEGER NOT NULL,
  chunk_text TEXT NOT NULL,
  dimensions INTEGER NOT NULL,
  embedding_blob BLOB NOT NULL,
  created_at_unix INTEGER NOT NULL,
  PRIMARY KEY (record_id, model, chunk_index),
  FOREIGN KEY (record_id) REFERENCES memory_records(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS approval_events (
  id TEXT PRIMARY KEY,
  action TEXT NOT NULL,
  decision TEXT NOT NULL,
  actor TEXT NOT NULL,
  reason TEXT,
  created_at_unix INTEGER NOT NULL,
  updated_at_unix INTEGER
);

CREATE TABLE IF NOT EXISTS guardian_daily_spend (
  spend_date TEXT PRIMARY KEY,
  total_spent_usd REAL NOT NULL,
  keys_revoked INTEGER NOT NULL DEFAULT 0,
  updated_at_unix INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS task_queue (
  id TEXT PRIMARY KEY,
  task_type TEXT NOT NULL,
  payload TEXT NOT NULL,
  status TEXT NOT NULL,
  attempts INTEGER NOT NULL,
  max_attempts INTEGER NOT NULL,
  available_at_unix INTEGER NOT NULL,
  last_error TEXT,
  created_at_unix INTEGER NOT NULL,
  updated_at_unix INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS scheduled_jobs (
  id TEXT PRIMARY KEY,
  task_type TEXT NOT NULL,
  payload TEXT NOT NULL,
  trigger_kind TEXT NOT NULL,
  trigger_expr TEXT NOT NULL,
  max_attempts INTEGER NOT NULL,
  enabled INTEGER NOT NULL,
  next_run_unix INTEGER NOT NULL,
  updated_at_unix INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS dead_letter_queue (
  task_id TEXT PRIMARY KEY,
  task_type TEXT NOT NULL,
  payload TEXT NOT NULL,
  attempts INTEGER NOT NULL,
  max_attempts INTEGER NOT NULL,
  last_error TEXT,
  failed_at_unix INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_logs (
  id TEXT PRIMARY KEY,
  action TEXT NOT NULL,
  detail TEXT NOT NULL,
  severity TEXT NOT NULL,
  task_id TEXT,
  created_at_unix INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS task_timeline (
  id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  stage TEXT NOT NULL,
  payload TEXT NOT NULL,
  created_at_unix INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_memory_records_session_time
  ON memory_records(session_id, created_at_unix DESC);

CREATE INDEX IF NOT EXISTS idx_memory_records_source_time
  ON memory_records(source, created_at_unix DESC);

CREATE INDEX IF NOT EXISTS idx_memory_embeddings_model
  ON memory_embeddings(model, created_at_unix DESC);

CREATE INDEX IF NOT EXISTS idx_task_queue_due
  ON task_queue(status, available_at_unix);

CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_due
  ON scheduled_jobs(enabled, next_run_unix);

CREATE INDEX IF NOT EXISTS idx_approval_pending
  ON approval_events(decision, created_at_unix DESC);

CREATE INDEX IF NOT EXISTS idx_audit_created
  ON audit_logs(created_at_unix DESC);

CREATE INDEX IF NOT EXISTS idx_task_timeline
  ON task_timeline(task_id, created_at_unix ASC);
