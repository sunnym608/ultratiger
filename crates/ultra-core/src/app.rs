use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};

use crate::{
    autonomy::{TaskItem, TaskQueue},
    config::AppConfig,
    guardian::Guardian,
    models::{
        ErrorResponse, GuardianResetResponse, GuardianStatusResponse, HealthResponse,
        PermissionCheckRequest, PermissionCheckResponse, PreflightRequest, PreflightResponse,
        QueueStatusResponse, QueueTaskRequest, SpendUpdateRequest, SpendUpdateResponse,
    },
    observability::{heartbeat, ActionLog, LogBuffer},
    permissions::requires_human_approval,
    persistence::{ApprovalEvent, SqliteMemoryStore},
};

#[derive(Clone)]
pub struct AppState {
    guardian: Arc<Mutex<Guardian>>,
    queue: Arc<Mutex<TaskQueue>>,
    logs: Arc<Mutex<LogBuffer>>,
    sqlite: Arc<Mutex<Option<SqliteMemoryStore>>>,
}

impl AppState {
    pub fn new(config: AppConfig) -> Self {
        let sqlite = SqliteMemoryStore::open("ultra_core.db").ok();
        Self {
            guardian: Arc::new(Mutex::new(Guardian::new(config))),
            queue: Arc::new(Mutex::new(TaskQueue::default())),
            logs: Arc::new(Mutex::new(LogBuffer::new(250))),
            sqlite: Arc::new(Mutex::new(sqlite)),
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
        .route("/queue/status", get(queue_status))
        .route("/queue/enqueue", post(queue_enqueue))
        .route("/queue/worker-tick", post(worker_tick))
        .route("/observability/heartbeat", get(observability_heartbeat))
        .route("/observability/actions", get(observability_actions))
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
    persist_guardian_snapshot(
        &state,
        guardian.total_spent_today_usd(),
        guardian.keys_revoked(),
    );
    state
        .logs
        .lock()
        .expect("log lock poisoned")
        .push("guardian_spend", format!("+${:.4}", request.amount_usd));

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
    persist_guardian_snapshot(
        &state,
        guardian.total_spent_today_usd(),
        guardian.keys_revoked(),
    );
    state
        .logs
        .lock()
        .expect("log lock poisoned")
        .push("guardian_reset", "daily budget state reset");

    Json(GuardianResetResponse {
        total_spent_today_usd: guardian.total_spent_today_usd(),
        keys_revoked: guardian.keys_revoked(),
        message: "guardian budget state reset",
    })
}

async fn permission_check(
    State(state): State<AppState>,
    Json(request): Json<PermissionCheckRequest>,
) -> Json<PermissionCheckResponse> {
    let requires_human_approval = requires_human_approval(&request.action);
    if requires_human_approval {
        append_approval_event(&state, &request.action, "pending");
    }

    state.logs.lock().expect("log lock poisoned").push(
        "permission_check",
        format!(
            "action='{}' requires_human_approval={requires_human_approval}",
            request.action
        ),
    );

    Json(PermissionCheckResponse {
        requires_human_approval,
    })
}

async fn queue_status(State(state): State<AppState>) -> Json<QueueStatusResponse> {
    let queue = state.queue.lock().expect("queue lock poisoned");
    Json(QueueStatusResponse {
        pending: queue.pending_count(),
        dead_letter: queue.dead_letter_count(),
    })
}

async fn queue_enqueue(
    State(state): State<AppState>,
    Json(request): Json<QueueTaskRequest>,
) -> impl IntoResponse {
    if request.max_attempts == 0 {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "max_attempts must be greater than 0".to_owned(),
            }),
        )
            .into_response();
    }

    state
        .queue
        .lock()
        .expect("queue lock poisoned")
        .enqueue(TaskItem {
            id: request.id,
            task_type: request.task_type,
            payload: request.payload,
            attempts: 0,
            max_attempts: request.max_attempts,
        });
    state
        .logs
        .lock()
        .expect("log lock poisoned")
        .push("queue_enqueue", "task added to queue");

    queue_status(State(state)).await.into_response()
}

async fn worker_tick(State(state): State<AppState>) -> Json<QueueStatusResponse> {
    let mut queue = state.queue.lock().expect("queue lock poisoned");
    if let Some(task) = queue.dequeue() {
        if task.payload.contains("fail") {
            queue.mark_failed(task);
            state
                .logs
                .lock()
                .expect("log lock poisoned")
                .push("worker_tick", "task failed and retried/dead-lettered");
        } else {
            state
                .logs
                .lock()
                .expect("log lock poisoned")
                .push("worker_tick", "task processed successfully");
        }
    }

    Json(QueueStatusResponse {
        pending: queue.pending_count(),
        dead_letter: queue.dead_letter_count(),
    })
}

async fn observability_heartbeat() -> Json<crate::observability::Heartbeat> {
    Json(heartbeat())
}

async fn observability_actions(State(state): State<AppState>) -> Json<Vec<ActionLog>> {
    let logs = state.logs.lock().expect("log lock poisoned");
    Json(logs.list())
}

fn persist_guardian_snapshot(state: &AppState, total_spent_usd: f64, keys_revoked: bool) {
    if let Some(store) = state.sqlite.lock().expect("sqlite lock poisoned").as_ref() {
        let _ = store.persist_guardian_daily_spend(
            "1970-01-01",
            total_spent_usd,
            keys_revoked,
            now_unix(),
        );
    }
}

fn append_approval_event(state: &AppState, action: &str, decision: &str) {
    if let Some(store) = state.sqlite.lock().expect("sqlite lock poisoned").as_ref() {
        let event = ApprovalEvent {
            id: format!("approval-{}", now_unix()),
            action: action.to_owned(),
            decision: decision.to_owned(),
            actor: "system".to_owned(),
            reason: Some("awaiting user approval".to_owned()),
            created_at_unix: now_unix(),
        };
        let _ = store.append_approval_event(&event);
    }
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
