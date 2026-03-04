use std::path::Path;

use rusqlite::{params, Connection};

use crate::memory::{MemoryQuery, MemoryRecord, MemoryStore};

#[derive(Debug, Clone)]
pub struct ApprovalEvent {
    pub id: String,
    pub action: String,
    pub decision: String,
    pub actor: String,
    pub reason: Option<String>,
    pub created_at_unix: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum PersistenceError {
    #[error("sqlite error: {0}")]
    Sqlite(String),
}

pub struct SqliteMemoryStore {
    conn: Connection,
}

impl SqliteMemoryStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, PersistenceError> {
        let conn =
            Connection::open(path).map_err(|err| PersistenceError::Sqlite(err.to_string()))?;
        let store = Self { conn };
        store.init_schema()?;
        Ok(store)
    }

    fn init_schema(&self) -> Result<(), PersistenceError> {
        self.conn
            .execute_batch(include_str!("../../../../sql/memory_schema.sql"))
            .map_err(|err| PersistenceError::Sqlite(err.to_string()))
    }

    pub fn persist_guardian_daily_spend(
        &self,
        spend_date: &str,
        total_spent_usd: f64,
        keys_revoked: bool,
        updated_at_unix: u64,
    ) -> Result<(), PersistenceError> {
        self.conn
            .execute(
                "INSERT INTO guardian_daily_spend (spend_date, total_spent_usd, keys_revoked, updated_at_unix)
                VALUES (?1, ?2, ?3, ?4)
                ON CONFLICT(spend_date) DO UPDATE SET
                total_spent_usd = excluded.total_spent_usd,
                keys_revoked = excluded.keys_revoked,
                updated_at_unix = excluded.updated_at_unix",
                params![spend_date, total_spent_usd, if keys_revoked { 1 } else { 0 }, updated_at_unix],
            )
            .map(|_| ())
            .map_err(|err| PersistenceError::Sqlite(err.to_string()))
    }

    pub fn append_approval_event(&self, event: &ApprovalEvent) -> Result<(), PersistenceError> {
        self.conn
            .execute(
                "INSERT INTO approval_events (id, action, decision, actor, reason, created_at_unix)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    event.id,
                    event.action,
                    event.decision,
                    event.actor,
                    event.reason,
                    event.created_at_unix as i64
                ],
            )
            .map(|_| ())
            .map_err(|err| PersistenceError::Sqlite(err.to_string()))
    }
}

impl MemoryStore for SqliteMemoryStore {
    fn put(&mut self, record: MemoryRecord) {
        let _ = self.conn.execute(
            "INSERT OR REPLACE INTO memory_records (id, session_id, source, content, created_at_unix)
            VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                record.id,
                record.session_id,
                record.source,
                record.content,
                record.created_at_unix as i64
            ],
        );
    }

    fn query(&self, query: &MemoryQuery) -> Vec<MemoryRecord> {
        let mut stmt = match self.conn.prepare(
            "SELECT id, session_id, source, content, created_at_unix FROM memory_records
             WHERE (?1 IS NULL OR session_id = ?1)
               AND (?2 IS NULL OR source = ?2)
             ORDER BY created_at_unix DESC
             LIMIT ?3",
        ) {
            Ok(stmt) => stmt,
            Err(_) => return vec![],
        };

        let rows = stmt.query_map(
            params![query.session_id, query.source, query.limit as i64],
            |row| {
                Ok(MemoryRecord {
                    id: row.get(0)?,
                    session_id: row.get(1)?,
                    source: row.get(2)?,
                    content: row.get(3)?,
                    created_at_unix: row.get::<_, i64>(4)? as u64,
                })
            },
        );

        match rows {
            Ok(mapped) => mapped.filter_map(Result::ok).collect(),
            Err(_) => vec![],
        }
    }

    fn delete(&mut self, id: &str) -> bool {
        self.conn
            .execute("DELETE FROM memory_records WHERE id = ?1", params![id])
            .map(|affected| affected > 0)
            .unwrap_or(false)
    }
}
