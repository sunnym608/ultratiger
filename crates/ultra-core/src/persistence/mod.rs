use std::path::Path;

use rusqlite::{params, Connection, OptionalExtension};

use crate::autonomy::{
    compute_retry_delay_seconds, next_schedule_run_unix, SchedulerTrigger, TaskItem,
};
use crate::memory::{
    cosine_similarity, embed_text_deterministic, keyword_overlap_score, MemoryChunk, MemoryQuery,
    MemoryRecord, MemoryStore, RetrievedMemory,
};

#[derive(Debug, Clone)]
pub struct ApprovalEvent {
    pub id: String,
    pub action: String,
    pub decision: String,
    pub actor: String,
    pub reason: Option<String>,
    pub created_at_unix: u64,
}

#[derive(Debug, Clone)]
pub struct ScheduledJob {
    pub id: String,
    pub task_type: String,
    pub payload: String,
    pub trigger: SchedulerTrigger,
    pub max_attempts: u32,
    pub enabled: bool,
    pub next_run_unix: u64,
}

#[derive(Debug, Clone)]
pub struct QueueCounts {
    pub pending: usize,
    pub dead_letter: usize,
}

#[derive(Debug, Clone)]
pub struct DeadLetterTask {
    pub task_id: String,
    pub task_type: String,
    pub payload: String,
    pub attempts: u32,
    pub max_attempts: u32,
    pub last_error: Option<String>,
    pub failed_at_unix: u64,
}

#[derive(Debug, Clone)]
pub struct MemoryRetrievalResult {
    pub record_id: String,
    pub session_id: String,
    pub source: String,
    pub chunk_index: u32,
    pub chunk_text: String,
    pub semantic_score: f32,
    pub keyword_score: f32,
    pub final_score: f32,
    pub created_at_unix: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum PersistenceError {
    #[error("sqlite error: {0}")]
    Sqlite(String),
    #[error("serialization error: {0}")]
    Serialization(String),
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

    pub fn ingest_memory_with_embeddings(
        &self,
        record: &MemoryRecord,
        model: &str,
        chunk_size: usize,
        dimensions: usize,
    ) -> Result<usize, PersistenceError> {
        self.conn
            .execute(
                "INSERT OR REPLACE INTO memory_records (id, session_id, source, content, created_at_unix) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    record.id,
                    record.session_id,
                    record.source,
                    record.content,
                    record.created_at_unix as i64
                ],
            )
            .map_err(|err| PersistenceError::Sqlite(err.to_string()))?;

        let chunks = crate::memory::chunk_text(&record.content, chunk_size)
            .into_iter()
            .map(|(idx, text)| MemoryChunk {
                chunk_index: idx,
                embedding: embed_text_deterministic(&text, dimensions),
                chunk_text: text,
            })
            .collect::<Vec<_>>();

        for chunk in &chunks {
            self.store_embedding(&record.id, model, chunk, record.created_at_unix)?;
        }

        Ok(chunks.len())
    }

    fn store_embedding(
        &self,
        record_id: &str,
        model: &str,
        chunk: &MemoryChunk,
        created_at_unix: u64,
    ) -> Result<(), PersistenceError> {
        let embedding_blob = serde_json::to_vec(&chunk.embedding)
            .map_err(|err| PersistenceError::Serialization(err.to_string()))?;

        self.conn
            .execute(
                "INSERT OR REPLACE INTO memory_embeddings
                 (record_id, model, chunk_index, chunk_text, dimensions, embedding_blob, created_at_unix)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    record_id,
                    model,
                    chunk.chunk_index as i64,
                    chunk.chunk_text,
                    chunk.embedding.len() as i64,
                    embedding_blob,
                    created_at_unix as i64,
                ],
            )
            .map(|_| ())
            .map_err(|err| PersistenceError::Sqlite(err.to_string()))
    }

    pub fn retrieve_hybrid(
        &self,
        query: &str,
        model: &str,
        session_id: Option<&str>,
        source: Option<&str>,
        top_k: usize,
        dimensions: usize,
    ) -> Result<Vec<MemoryRetrievalResult>, PersistenceError> {
        let query_emb = embed_text_deterministic(query, dimensions);

        let mut stmt = self
            .conn
            .prepare(
                "SELECT mr.id, mr.session_id, mr.source, mr.created_at_unix,
                        me.chunk_index, me.chunk_text, me.embedding_blob
                 FROM memory_records mr
                 JOIN memory_embeddings me ON me.record_id = mr.id
                 WHERE me.model = ?1
                   AND (?2 IS NULL OR mr.session_id = ?2)
                   AND (?3 IS NULL OR mr.source = ?3)
                 ORDER BY mr.created_at_unix DESC",
            )
            .map_err(|err| PersistenceError::Sqlite(err.to_string()))?;

        let rows = stmt
            .query_map(params![model, session_id, source], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)? as u64,
                    row.get::<_, i64>(4)? as u32,
                    row.get::<_, String>(5)?,
                    row.get::<_, Vec<u8>>(6)?,
                ))
            })
            .map_err(|err| PersistenceError::Sqlite(err.to_string()))?;

        let mut scored = Vec::new();
        for row in rows.flatten() {
            let (record_id, session_id, source, created_at_unix, chunk_index, chunk_text, blob) =
                row;
            let emb: Vec<f32> = serde_json::from_slice(&blob)
                .map_err(|err| PersistenceError::Serialization(err.to_string()))?;
            let semantic = cosine_similarity(&query_emb, &emb);
            let keyword = keyword_overlap_score(query, &chunk_text);
            let final_score = (0.7 * semantic) + (0.3 * keyword);

            scored.push(MemoryRetrievalResult {
                record_id,
                session_id,
                source,
                chunk_index,
                chunk_text,
                semantic_score: semantic,
                keyword_score: keyword,
                final_score,
                created_at_unix,
            });
        }

        scored.sort_by(|a, b| b.final_score.total_cmp(&a.final_score));
        scored.truncate(top_k.max(1));
        Ok(scored)
    }

    pub fn purge_expired_memories(
        &self,
        ttl_seconds: u64,
        now_unix: u64,
    ) -> Result<usize, PersistenceError> {
        let threshold = now_unix.saturating_sub(ttl_seconds);
        self.conn
            .execute(
                "DELETE FROM memory_records WHERE created_at_unix < ?1",
                params![threshold as i64],
            )
            .map(|count| count as usize)
            .map_err(|err| PersistenceError::Sqlite(err.to_string()))
    }

    pub fn delete_memory_record(&self, id: &str) -> Result<bool, PersistenceError> {
        self.conn
            .execute("DELETE FROM memory_records WHERE id = ?1", params![id])
            .map(|affected| affected > 0)
            .map_err(|err| PersistenceError::Sqlite(err.to_string()))
    }

    pub fn fetch_memory_by_filters(
        &self,
        query: &MemoryQuery,
    ) -> Result<Vec<MemoryRecord>, PersistenceError> {
        self.query(query)
            .map_err(|err| PersistenceError::Sqlite(err.to_string()))
    }

    pub fn enqueue_task(&self, task: &TaskItem, now_unix: u64) -> Result<(), PersistenceError> {
        self.conn
            .execute(
                "INSERT OR REPLACE INTO task_queue
                (id, task_type, payload, status, attempts, max_attempts, available_at_unix, last_error, created_at_unix, updated_at_unix)
                VALUES (?1, ?2, ?3, 'pending', ?4, ?5, ?6, NULL, COALESCE((SELECT created_at_unix FROM task_queue WHERE id = ?1), ?7), ?7)",
                params![
                    task.id,
                    task.task_type,
                    task.payload,
                    task.attempts as i64,
                    task.max_attempts as i64,
                    task.available_at_unix as i64,
                    now_unix as i64
                ],
            )
            .map(|_| ())
            .map_err(|err| PersistenceError::Sqlite(err.to_string()))
    }

    pub fn claim_due_task(&self, now_unix: u64) -> Result<Option<TaskItem>, PersistenceError> {
        let tx = self
            .conn
            .unchecked_transaction()
            .map_err(|err| PersistenceError::Sqlite(err.to_string()))?;

        let candidate: Option<(String, String, String, i64, i64, i64)> = tx
            .query_row(
                "SELECT id, task_type, payload, attempts, max_attempts, available_at_unix
                 FROM task_queue
                 WHERE status IN ('pending', 'retry') AND available_at_unix <= ?1
                 ORDER BY available_at_unix ASC, created_at_unix ASC
                 LIMIT 1",
                params![now_unix as i64],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .optional()
            .map_err(|err| PersistenceError::Sqlite(err.to_string()))?;

        let Some((id, task_type, payload, attempts, max_attempts, available_at_unix)) = candidate
        else {
            tx.commit()
                .map_err(|err| PersistenceError::Sqlite(err.to_string()))?;
            return Ok(None);
        };

        tx.execute(
            "UPDATE task_queue SET status = 'running', updated_at_unix = ?2 WHERE id = ?1",
            params![id, now_unix as i64],
        )
        .map_err(|err| PersistenceError::Sqlite(err.to_string()))?;

        tx.commit()
            .map_err(|err| PersistenceError::Sqlite(err.to_string()))?;

        Ok(Some(TaskItem {
            id,
            task_type,
            payload,
            attempts: attempts as u32,
            max_attempts: max_attempts as u32,
            available_at_unix: available_at_unix as u64,
        }))
    }

    pub fn complete_task(&self, task_id: &str) -> Result<(), PersistenceError> {
        self.conn
            .execute("DELETE FROM task_queue WHERE id = ?1", params![task_id])
            .map(|_| ())
            .map_err(|err| PersistenceError::Sqlite(err.to_string()))
    }

    pub fn fail_task(
        &self,
        task: &TaskItem,
        now_unix: u64,
        base_delay_seconds: u64,
        jitter_seconds: u64,
        error_message: &str,
    ) -> Result<(), PersistenceError> {
        let next_attempts = task.attempts + 1;
        if next_attempts >= task.max_attempts {
            self.conn
                .execute("DELETE FROM task_queue WHERE id = ?1", params![task.id])
                .map_err(|err| PersistenceError::Sqlite(err.to_string()))?;

            self.conn
                .execute(
                    "INSERT OR REPLACE INTO dead_letter_queue
                     (task_id, task_type, payload, attempts, max_attempts, last_error, failed_at_unix)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        task.id,
                        task.task_type,
                        task.payload,
                        next_attempts as i64,
                        task.max_attempts as i64,
                        error_message,
                        now_unix as i64
                    ],
                )
                .map_err(|err| PersistenceError::Sqlite(err.to_string()))?;
            return Ok(());
        }

        let delay = compute_retry_delay_seconds(
            next_attempts,
            base_delay_seconds,
            jitter_seconds,
            now_unix,
        );
        self.conn
            .execute(
                "UPDATE task_queue SET status='retry', attempts=?2, available_at_unix=?3, last_error=?4, updated_at_unix=?5 WHERE id = ?1",
                params![
                    task.id,
                    next_attempts as i64,
                    now_unix.saturating_add(delay) as i64,
                    error_message,
                    now_unix as i64,
                ],
            )
            .map(|_| ())
            .map_err(|err| PersistenceError::Sqlite(err.to_string()))
    }

    pub fn queue_counts(&self) -> Result<QueueCounts, PersistenceError> {
        let pending =
            self.conn
                .query_row(
                    "SELECT COUNT(*) FROM task_queue WHERE status IN ('pending','retry','running')",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(|err| PersistenceError::Sqlite(err.to_string()))? as usize;
        let dead_letter =
            self.conn
                .query_row("SELECT COUNT(*) FROM dead_letter_queue", [], |row| {
                    row.get::<_, i64>(0)
                })
                .map_err(|err| PersistenceError::Sqlite(err.to_string()))? as usize;

        Ok(QueueCounts {
            pending,
            dead_letter,
        })
    }

    pub fn list_dead_letter(&self) -> Result<Vec<DeadLetterTask>, PersistenceError> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT task_id, task_type, payload, attempts, max_attempts, last_error, failed_at_unix
                 FROM dead_letter_queue ORDER BY failed_at_unix DESC",
            )
            .map_err(|err| PersistenceError::Sqlite(err.to_string()))?;

        let rows = stmt
            .query_map([], |row| {
                Ok(DeadLetterTask {
                    task_id: row.get(0)?,
                    task_type: row.get(1)?,
                    payload: row.get(2)?,
                    attempts: row.get::<_, i64>(3)? as u32,
                    max_attempts: row.get::<_, i64>(4)? as u32,
                    last_error: row.get(5)?,
                    failed_at_unix: row.get::<_, i64>(6)? as u64,
                })
            })
            .map_err(|err| PersistenceError::Sqlite(err.to_string()))?;

        Ok(rows.filter_map(Result::ok).collect())
    }

    pub fn requeue_dead_letter(
        &self,
        task_id: &str,
        now_unix: u64,
    ) -> Result<bool, PersistenceError> {
        let row = self
            .conn
            .query_row(
                "SELECT task_type, payload, max_attempts FROM dead_letter_queue WHERE task_id = ?1",
                params![task_id],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, i64>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(|err| PersistenceError::Sqlite(err.to_string()))?;

        let Some((task_type, payload, max_attempts)) = row else {
            return Ok(false);
        };

        self.conn
            .execute(
                "DELETE FROM dead_letter_queue WHERE task_id = ?1",
                params![task_id],
            )
            .map_err(|err| PersistenceError::Sqlite(err.to_string()))?;

        self.conn
            .execute(
                "INSERT OR REPLACE INTO task_queue
                (id, task_type, payload, status, attempts, max_attempts, available_at_unix, last_error, created_at_unix, updated_at_unix)
                VALUES (?1, ?2, ?3, 'pending', 0, ?4, ?5, NULL, ?5, ?5)",
                params![task_id, task_type, payload, max_attempts, now_unix as i64],
            )
            .map_err(|err| PersistenceError::Sqlite(err.to_string()))?;

        Ok(true)
    }

    pub fn upsert_scheduled_job(
        &self,
        job: &ScheduledJob,
        now_unix: u64,
    ) -> Result<(), PersistenceError> {
        let (trigger_kind, trigger_expr) = match &job.trigger {
            SchedulerTrigger::EverySeconds(v) => ("every_seconds".to_owned(), v.to_string()),
            SchedulerTrigger::Cron(expr) => ("cron".to_owned(), expr.clone()),
        };

        self.conn
            .execute(
                "INSERT INTO scheduled_jobs
                (id, task_type, payload, trigger_kind, trigger_expr, max_attempts, enabled, next_run_unix, updated_at_unix)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                ON CONFLICT(id) DO UPDATE SET
                    task_type=excluded.task_type,
                    payload=excluded.payload,
                    trigger_kind=excluded.trigger_kind,
                    trigger_expr=excluded.trigger_expr,
                    max_attempts=excluded.max_attempts,
                    enabled=excluded.enabled,
                    next_run_unix=excluded.next_run_unix,
                    updated_at_unix=excluded.updated_at_unix",
                params![
                    job.id,
                    job.task_type,
                    job.payload,
                    trigger_kind,
                    trigger_expr,
                    job.max_attempts as i64,
                    if job.enabled { 1 } else { 0 },
                    job.next_run_unix as i64,
                    now_unix as i64,
                ],
            )
            .map(|_| ())
            .map_err(|err| PersistenceError::Sqlite(err.to_string()))
    }

    pub fn run_scheduler_tick(&self, now_unix: u64) -> Result<usize, PersistenceError> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT id, task_type, payload, trigger_kind, trigger_expr, max_attempts
                 FROM scheduled_jobs
                 WHERE enabled = 1 AND next_run_unix <= ?1",
            )
            .map_err(|err| PersistenceError::Sqlite(err.to_string()))?;

        let jobs = stmt
            .query_map(params![now_unix as i64], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, i64>(5)? as u32,
                ))
            })
            .map_err(|err| PersistenceError::Sqlite(err.to_string()))?
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        let mut fired = 0usize;
        for (id, task_type, payload, trigger_kind, trigger_expr, max_attempts) in jobs {
            let trigger = if trigger_kind == "every_seconds" {
                SchedulerTrigger::EverySeconds(trigger_expr.parse::<u64>().unwrap_or(0))
            } else {
                SchedulerTrigger::Cron(trigger_expr.clone())
            };

            let task = TaskItem {
                id: format!("sched-{id}-{now_unix}"),
                task_type,
                payload,
                attempts: 0,
                max_attempts,
                available_at_unix: now_unix,
            };
            self.enqueue_task(&task, now_unix)?;

            let next_run =
                next_schedule_run_unix(&trigger, now_unix).unwrap_or(now_unix.saturating_add(60));
            self.conn
                .execute(
                    "UPDATE scheduled_jobs SET next_run_unix = ?2, updated_at_unix = ?3 WHERE id = ?1",
                    params![id, next_run as i64, now_unix as i64],
                )
                .map_err(|err| PersistenceError::Sqlite(err.to_string()))?;
            fired += 1;
        }

        Ok(fired)
    }
}

impl MemoryStore for SqliteMemoryStore {
    fn put(&mut self, record: MemoryRecord) -> Result<(), String> {
        self.conn
            .execute(
                "INSERT OR REPLACE INTO memory_records (id, session_id, source, content, created_at_unix)
            VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    record.id,
                    record.session_id,
                    record.source,
                    record.content,
                    record.created_at_unix as i64
                ],
            )
            .map(|_| ())
            .map_err(|err| err.to_string())
    }

    fn query(&self, query: &MemoryQuery) -> Result<Vec<MemoryRecord>, String> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT id, session_id, source, content, created_at_unix FROM memory_records
             WHERE (?1 IS NULL OR session_id = ?1)
               AND (?2 IS NULL OR source = ?2)
             ORDER BY created_at_unix DESC
             LIMIT ?3",
            )
            .map_err(|err| err.to_string())?;

        let rows = stmt
            .query_map(
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
            )
            .map_err(|err| err.to_string())?;

        Ok(rows.filter_map(Result::ok).collect())
    }

    fn delete(&mut self, id: &str) -> Result<bool, String> {
        self.conn
            .execute("DELETE FROM memory_records WHERE id = ?1", params![id])
            .map(|affected| affected > 0)
            .map_err(|err| err.to_string())
    }
}
