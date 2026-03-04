-- Ultra Tiger Memory Schema v1
-- Local-first layout with support for semantic retrieval and auditability.

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
  dimensions INTEGER NOT NULL,
  embedding_blob BLOB NOT NULL,
  created_at_unix INTEGER NOT NULL,
  PRIMARY KEY (record_id, model),
  FOREIGN KEY (record_id) REFERENCES memory_records(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS approval_events (
  id TEXT PRIMARY KEY,
  action TEXT NOT NULL,
  decision TEXT NOT NULL,
  actor TEXT NOT NULL,
  reason TEXT,
  created_at_unix INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS guardian_daily_spend (
  spend_date TEXT PRIMARY KEY,
  total_spent_usd REAL NOT NULL,
  keys_revoked INTEGER NOT NULL DEFAULT 0,
  updated_at_unix INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_memory_records_session_time
  ON memory_records(session_id, created_at_unix DESC);

CREATE INDEX IF NOT EXISTS idx_memory_records_source_time
  ON memory_records(source, created_at_unix DESC);
