use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryRecord {
    pub id: String,
    pub session_id: String,
    pub content: String,
    pub source: String,
    pub created_at_unix: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MemoryQuery {
    pub session_id: Option<String>,
    pub source: Option<String>,
    pub limit: usize,
}

impl Default for MemoryQuery {
    fn default() -> Self {
        Self {
            session_id: None,
            source: None,
            limit: 20,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MemoryChunk {
    pub chunk_index: u32,
    pub chunk_text: String,
    pub embedding: Vec<f32>,
}

#[derive(Debug, Clone)]
pub struct RetrievedMemory {
    pub record_id: String,
    pub session_id: String,
    pub source: String,
    pub chunk_index: u32,
    pub chunk_text: String,
    pub score: f32,
    pub created_at_unix: u64,
}

pub trait MemoryStore: Send + Sync {
    fn put(&mut self, record: MemoryRecord) -> Result<(), String>;
    fn query(&self, query: &MemoryQuery) -> Result<Vec<MemoryRecord>, String>;
    fn delete(&mut self, id: &str) -> Result<bool, String>;
}

#[derive(Debug, Default)]
pub struct InMemoryStore {
    records: HashMap<String, MemoryRecord>,
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl MemoryStore for InMemoryStore {
    fn put(&mut self, record: MemoryRecord) -> Result<(), String> {
        self.records.insert(record.id.clone(), record);
        Ok(())
    }

    fn query(&self, query: &MemoryQuery) -> Result<Vec<MemoryRecord>, String> {
        let mut records = self
            .records
            .values()
            .filter(|record| {
                query
                    .session_id
                    .as_ref()
                    .map(|session| &record.session_id == session)
                    .unwrap_or(true)
            })
            .filter(|record| {
                query
                    .source
                    .as_ref()
                    .map(|source| &record.source == source)
                    .unwrap_or(true)
            })
            .cloned()
            .collect::<Vec<_>>();

        records.sort_by_key(|record| std::cmp::Reverse(record.created_at_unix));
        records.truncate(query.limit);
        Ok(records)
    }

    fn delete(&mut self, id: &str) -> Result<bool, String> {
        Ok(self.records.remove(id).is_some())
    }
}

pub fn new_record(
    id: impl Into<String>,
    session_id: impl Into<String>,
    content: impl Into<String>,
    source: impl Into<String>,
) -> MemoryRecord {
    MemoryRecord {
        id: id.into(),
        session_id: session_id.into(),
        content: content.into(),
        source: source.into(),
        created_at_unix: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .unwrap_or(0),
    }
}

pub fn chunk_text(content: &str, chunk_size: usize) -> Vec<(u32, String)> {
    let chunk_size = chunk_size.max(32);
    content
        .as_bytes()
        .chunks(chunk_size)
        .enumerate()
        .map(|(idx, bytes)| (idx as u32, String::from_utf8_lossy(bytes).to_string()))
        .collect()
}

pub fn embed_text_deterministic(text: &str, dimensions: usize) -> Vec<f32> {
    let dimensions = dimensions.max(8);
    let mut vec = vec![0.0f32; dimensions];
    for (i, b) in text.as_bytes().iter().enumerate() {
        let slot = i % dimensions;
        vec[slot] += (*b as f32) / 255.0;
    }
    normalize(vec)
}

pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.is_empty() || b.is_empty() || a.len() != b.len() {
        return 0.0;
    }
    let mut dot = 0.0;
    let mut na = 0.0;
    let mut nb = 0.0;
    for i in 0..a.len() {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
    }
    if na == 0.0 || nb == 0.0 {
        0.0
    } else {
        dot / (na.sqrt() * nb.sqrt())
    }
}

pub fn keyword_overlap_score(query: &str, text: &str) -> f32 {
    let q = query
        .split_whitespace()
        .map(|s| s.to_lowercase())
        .collect::<Vec<_>>();
    if q.is_empty() {
        return 0.0;
    }
    let t = text.to_lowercase();
    let hits = q.iter().filter(|token| t.contains(token.as_str())).count();
    hits as f32 / q.len() as f32
}

fn normalize(mut vector: Vec<f32>) -> Vec<f32> {
    let norm = vector.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in &mut vector {
            *x /= norm;
        }
    }
    vector
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_store_filters_by_session_and_source() {
        let mut store = InMemoryStore::new();
        store
            .put(MemoryRecord {
                id: "1".to_owned(),
                session_id: "session-a".to_owned(),
                content: "alpha".to_owned(),
                source: "chat".to_owned(),
                created_at_unix: 1,
            })
            .expect("insert should succeed");
        store
            .put(MemoryRecord {
                id: "2".to_owned(),
                session_id: "session-b".to_owned(),
                content: "beta".to_owned(),
                source: "skill".to_owned(),
                created_at_unix: 2,
            })
            .expect("insert should succeed");

        let records = store
            .query(&MemoryQuery {
                session_id: Some("session-a".to_owned()),
                source: Some("chat".to_owned()),
                limit: 10,
            })
            .expect("query should succeed");

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].id, "1");
    }

    #[test]
    fn memory_store_delete_removes_record() {
        let mut store = InMemoryStore::new();
        store
            .put(MemoryRecord {
                id: "x".to_owned(),
                session_id: "session-a".to_owned(),
                content: "to-remove".to_owned(),
                source: "chat".to_owned(),
                created_at_unix: 1,
            })
            .expect("insert should succeed");

        assert!(store.delete("x").expect("delete should succeed"));
        assert!(store
            .query(&MemoryQuery::default())
            .expect("query should succeed")
            .is_empty());
    }

    #[test]
    fn deterministic_embeddings_are_comparable() {
        let a = embed_text_deterministic("hello world", 16);
        let b = embed_text_deterministic("hello world", 16);
        let c = embed_text_deterministic("different text", 16);
        assert!(cosine_similarity(&a, &b) > cosine_similarity(&a, &c));
    }
}
