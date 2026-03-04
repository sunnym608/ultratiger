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

pub trait MemoryStore: Send + Sync {
    fn put(&mut self, record: MemoryRecord);
    fn query(&self, query: &MemoryQuery) -> Vec<MemoryRecord>;
    fn delete(&mut self, id: &str) -> bool;
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
    fn put(&mut self, record: MemoryRecord) {
        self.records.insert(record.id.clone(), record);
    }

    fn query(&self, query: &MemoryQuery) -> Vec<MemoryRecord> {
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
        records
    }

    fn delete(&mut self, id: &str) -> bool {
        self.records.remove(id).is_some()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_store_filters_by_session_and_source() {
        let mut store = InMemoryStore::new();
        store.put(MemoryRecord {
            id: "1".to_owned(),
            session_id: "session-a".to_owned(),
            content: "alpha".to_owned(),
            source: "chat".to_owned(),
            created_at_unix: 1,
        });
        store.put(MemoryRecord {
            id: "2".to_owned(),
            session_id: "session-b".to_owned(),
            content: "beta".to_owned(),
            source: "skill".to_owned(),
            created_at_unix: 2,
        });

        let records = store.query(&MemoryQuery {
            session_id: Some("session-a".to_owned()),
            source: Some("chat".to_owned()),
            limit: 10,
        });

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].id, "1");
    }

    #[test]
    fn memory_store_delete_removes_record() {
        let mut store = InMemoryStore::new();
        store.put(MemoryRecord {
            id: "x".to_owned(),
            session_id: "session-a".to_owned(),
            content: "to-remove".to_owned(),
            source: "chat".to_owned(),
            created_at_unix: 1,
        });

        assert!(store.delete("x"));
        assert!(store.query(&MemoryQuery::default()).is_empty());
    }
}
