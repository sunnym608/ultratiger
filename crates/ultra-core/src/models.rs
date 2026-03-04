use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub service: &'static str,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PreflightRequest {
    pub task_name: String,
    pub estimated_tokens: u32,
    pub estimated_cost_usd: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct PreflightResponse {
    pub allowed: bool,
    pub requires_authorization: bool,
    pub reason: String,
    pub projected_daily_total_usd: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SpendUpdateRequest {
    pub amount_usd: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SpendUpdateResponse {
    pub total_spent_today_usd: f64,
    pub keys_revoked: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct GuardianStatusResponse {
    pub total_spent_today_usd: f64,
    pub daily_budget_limit_usd: f64,
    pub preflight_authorize_threshold_usd: f64,
    pub keys_revoked: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct GuardianResetResponse {
    pub total_spent_today_usd: f64,
    pub keys_revoked: bool,
    pub message: &'static str,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PermissionCheckRequest {
    pub action: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct PermissionCheckResponse {
    pub requires_human_approval: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct QueueTaskRequest {
    pub id: String,
    pub task_type: String,
    pub payload: String,
    pub max_attempts: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueueStatusResponse {
    pub pending: usize,
    pub dead_letter: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ScheduleTaskRequest {
    pub id: String,
    pub task_type: String,
    pub payload: String,
    pub trigger_kind: String,
    pub trigger_expr: String,
    pub max_attempts: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RequeueDeadLetterRequest {
    pub task_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct DeadLetterTaskResponse {
    pub task_id: String,
    pub task_type: String,
    pub attempts: u32,
    pub max_attempts: u32,
    pub last_error: Option<String>,
    pub failed_at_unix: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SchedulerTickResponse {
    pub fired_jobs: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MemoryIngestRequest {
    pub id: Option<String>,
    pub session_id: String,
    pub source: String,
    pub content: String,
    pub model: Option<String>,
    pub chunk_size: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MemoryIngestResponse {
    pub record_id: String,
    pub chunks_stored: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MemoryRetrieveRequest {
    pub query: String,
    pub session_id: Option<String>,
    pub source: Option<String>,
    pub top_k: Option<usize>,
    pub model: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MemoryCitation {
    pub record_id: String,
    pub session_id: String,
    pub source: String,
    pub chunk_index: u32,
    pub quote: String,
    pub semantic_score: f32,
    pub keyword_score: f32,
    pub final_score: f32,
    pub created_at_unix: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct MemoryRetrieveResponse {
    pub citations: Vec<MemoryCitation>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MemoryQueryParams {
    pub session_id: Option<String>,
    pub source: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MemoryRecordResponse {
    pub id: String,
    pub session_id: String,
    pub source: String,
    pub content: String,
    pub created_at_unix: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MemoryPurgeRequest {
    pub ttl_seconds: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct MemoryPurgeResponse {
    pub deleted_records: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct DeleteMemoryResponse {
    pub deleted: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}
