use serde::Serialize;
use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize)]
pub struct Heartbeat {
    pub service: &'static str,
    pub status: &'static str,
    pub timestamp_unix: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ActionLog {
    pub action: String,
    pub detail: String,
    pub timestamp_unix: u64,
}

#[derive(Debug, Default)]
pub struct LogBuffer {
    entries: VecDeque<ActionLog>,
    capacity: usize,
}

impl LogBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            capacity,
        }
    }

    pub fn push(&mut self, action: impl Into<String>, detail: impl Into<String>) {
        if self.entries.len() >= self.capacity {
            self.entries.pop_front();
        }
        self.entries.push_back(ActionLog {
            action: action.into(),
            detail: detail.into(),
            timestamp_unix: now_unix(),
        });
    }

    pub fn list(&self) -> Vec<ActionLog> {
        self.entries.iter().cloned().collect()
    }
}

pub fn heartbeat() -> Heartbeat {
    Heartbeat {
        service: "ultra-core",
        status: "alive",
        timestamp_unix: now_unix(),
    }
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
