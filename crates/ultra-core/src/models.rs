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

#[derive(Debug, Clone, Deserialize)]
pub struct PermissionCheckRequest {
    pub action: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct PermissionCheckResponse {
    pub requires_human_approval: bool,
}
