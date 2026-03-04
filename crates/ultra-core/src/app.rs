use std::sync::{Arc, Mutex};

use axum::{
    extract::State,
    routing::{get, post},
    Json, Router,
};

use crate::{
    config::AppConfig,
    guardian::Guardian,
    models::{
        HealthResponse, PermissionCheckRequest, PermissionCheckResponse, PreflightRequest,
        PreflightResponse, SpendUpdateRequest, SpendUpdateResponse,
    },
    permissions::requires_human_approval,
};

#[derive(Clone)]
pub struct AppState {
    guardian: Arc<Mutex<Guardian>>,
}

impl AppState {
    pub fn new(config: AppConfig) -> Self {
        Self {
            guardian: Arc::new(Mutex::new(Guardian::new(config))),
        }
    }
}

pub fn router(config: AppConfig) -> Router {
    let state = AppState::new(config);
    Router::new()
        .route("/health", get(health))
        .route("/guardian/preflight", post(preflight))
        .route("/guardian/spend", post(register_spend))
        .route("/guardian/permission-check", post(permission_check))
        .with_state(state)
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        service: "ultra-core",
    })
}

async fn preflight(
    State(state): State<AppState>,
    Json(request): Json<PreflightRequest>,
) -> Json<PreflightResponse> {
    let guardian = state.guardian.lock().expect("guardian lock poisoned");
    Json(guardian.preflight_check(&request))
}

async fn register_spend(
    State(state): State<AppState>,
    Json(request): Json<SpendUpdateRequest>,
) -> Json<SpendUpdateResponse> {
    let mut guardian = state.guardian.lock().expect("guardian lock poisoned");
    guardian.register_spend(request.amount_usd);
    Json(SpendUpdateResponse {
        total_spent_today_usd: guardian.total_spent_today_usd(),
        keys_revoked: guardian.keys_revoked(),
    })
}

async fn permission_check(
    Json(request): Json<PermissionCheckRequest>,
) -> Json<PermissionCheckResponse> {
    Json(PermissionCheckResponse {
        requires_human_approval: requires_human_approval(&request.action),
    })
}
