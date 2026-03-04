use std::sync::{Arc, Mutex};

use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};

use crate::{
    config::AppConfig,
    guardian::Guardian,
    models::{
        ErrorResponse, GuardianResetResponse, GuardianStatusResponse, HealthResponse,
        PermissionCheckRequest, PermissionCheckResponse, PreflightRequest, PreflightResponse,
        SpendUpdateRequest, SpendUpdateResponse,
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
        .route("/guardian/status", get(guardian_status))
        .route("/guardian/preflight", post(preflight))
        .route("/guardian/spend", post(register_spend))
        .route("/guardian/reset", post(reset_guardian))
        .route("/guardian/permission-check", post(permission_check))
        .with_state(state)
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        service: "ultra-core",
    })
}

async fn guardian_status(State(state): State<AppState>) -> Json<GuardianStatusResponse> {
    let guardian = state.guardian.lock().expect("guardian lock poisoned");
    Json(guardian.status())
}

async fn preflight(
    State(state): State<AppState>,
    Json(request): Json<PreflightRequest>,
) -> impl IntoResponse {
    if request.estimated_cost_usd < 0.0 {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "estimated_cost_usd must be non-negative".to_owned(),
            }),
        )
            .into_response();
    }

    let guardian = state.guardian.lock().expect("guardian lock poisoned");
    (StatusCode::OK, Json(guardian.preflight_check(&request))).into_response()
}

async fn register_spend(
    State(state): State<AppState>,
    Json(request): Json<SpendUpdateRequest>,
) -> impl IntoResponse {
    if request.amount_usd < 0.0 {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "amount_usd must be non-negative".to_owned(),
            }),
        )
            .into_response();
    }

    let mut guardian = state.guardian.lock().expect("guardian lock poisoned");
    guardian.register_spend(request.amount_usd);
    (
        StatusCode::OK,
        Json(SpendUpdateResponse {
            total_spent_today_usd: guardian.total_spent_today_usd(),
            keys_revoked: guardian.keys_revoked(),
        }),
    )
        .into_response()
}

async fn reset_guardian(State(state): State<AppState>) -> Json<GuardianResetResponse> {
    let mut guardian = state.guardian.lock().expect("guardian lock poisoned");
    guardian.manual_reset();

    Json(GuardianResetResponse {
        total_spent_today_usd: guardian.total_spent_today_usd(),
        keys_revoked: guardian.keys_revoked(),
        message: "guardian budget state reset",
    })
}

async fn permission_check(
    Json(request): Json<PermissionCheckRequest>,
) -> Json<PermissionCheckResponse> {
    Json(PermissionCheckResponse {
        requires_human_approval: requires_human_approval(&request.action),
    })
}
