use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use tracing::{info, warn};

use crate::{
    autonomy::{next_schedule_run_unix, SchedulerTrigger, TaskItem},
    config::AppConfig,
    guardian::Guardian,
    models::{
        DeadLetterTaskResponse, ErrorResponse, GuardianResetResponse, GuardianStatusResponse,
        HealthResponse, PermissionCheckRequest, PermissionCheckResponse, PreflightRequest,
        PreflightResponse, QueueStatusResponse, QueueTaskRequest, RequeueDeadLetterRequest,
        ScheduleTaskRequest, SchedulerTickResponse, SpendUpdateRequest, SpendUpdateResponse,
    },
    observability::{heartbeat, ActionLog, LogBuffer},
    permissions::requires_human_approval,
    persistence::{ApprovalEvent, ScheduledJob, SqliteMemoryStore},
};

#[derive(Clone)]
pub struct AppState {
    guardian: Arc<Mutex<Guardian>>,
    logs: Arc<Mutex<LogBuffer>>,
    sqlite: Arc<Mutex<SqliteMemoryStore>>,
    config: AppConfig,
}

impl AppState {
    pub fn new(config: AppConfig) -> Self {
        let sqlite = SqliteMemoryStore::open("ultra_core.db")
            .expect("failed to open SQLite store for persistent orchestration");
        let state = Self {
            guardian: Arc::new(Mutex::new(Guardian::new(config.clone()))),
            logs: Arc::new(Mutex::new(LogBuffer::new(500))),
            sqlite: Arc::new(Mutex::new(sqlite)),
            config,
        };
        state.start_worker_pool();
        state
    }

    fn start_worker_pool(&self) {
        for worker_id in 0..self.config.worker_concurrency {
            let state = self.clone();
            tokio::spawn(async move {
                info!("starting background worker {worker_id}");
                loop {
                    run_single_worker_cycle(&state);
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            });
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
        .route("/queue/dead-letter", get(dead_letter_list))
        .route("/queue/requeue", post(requeue_dead_letter))
        .route("/scheduler/register", post(register_schedule))
        .route("/scheduler/tick", post(scheduler_tick))
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
    log_action(
        &state,
        "guardian_spend",
        format!("+${:.4}", request.amount_usd),
    );

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
    log_action(
        &state,
        "guardian_reset",
        "daily budget state reset".to_owned(),
    );

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

    log_action(
        &state,
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
    let counts = state
        .sqlite
        .lock()
        .expect("sqlite lock poisoned")
        .queue_counts()
        .unwrap_or(crate::persistence::QueueCounts {
            pending: 0,
            dead_letter: 0,
        });

    Json(QueueStatusResponse {
        pending: counts.pending,
        dead_letter: counts.dead_letter,
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

    let task = TaskItem {
        id: request.id,
        task_type: request.task_type,
        payload: request.payload,
        attempts: 0,
        max_attempts: request.max_attempts,
        available_at_unix: now_unix(),
    };

    let enqueue_result = state
        .sqlite
        .lock()
        .expect("sqlite lock poisoned")
        .enqueue_task(&task, now_unix());

    if let Err(err) = enqueue_result {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("enqueue failed: {err}"),
            }),
        )
            .into_response();
    }

    log_action(&state, "queue_enqueue", "task persisted".to_owned());
    queue_status(State(state)).await.into_response()
}

async fn worker_tick(State(state): State<AppState>) -> Json<QueueStatusResponse> {
    run_single_worker_cycle(&state);
    queue_status(State(state)).await
}

async fn dead_letter_list(State(state): State<AppState>) -> Json<Vec<DeadLetterTaskResponse>> {
    let rows = state
        .sqlite
        .lock()
        .expect("sqlite lock poisoned")
        .list_dead_letter()
        .unwrap_or_default();

    Json(
        rows.into_iter()
            .map(|task| DeadLetterTaskResponse {
                task_id: task.task_id,
                task_type: task.task_type,
                attempts: task.attempts,
                max_attempts: task.max_attempts,
                last_error: task.last_error,
                failed_at_unix: task.failed_at_unix,
            })
            .collect(),
    )
}

async fn requeue_dead_letter(
    State(state): State<AppState>,
    Json(request): Json<RequeueDeadLetterRequest>,
) -> impl IntoResponse {
    let ok = state
        .sqlite
        .lock()
        .expect("sqlite lock poisoned")
        .requeue_dead_letter(&request.task_id, now_unix());

    match ok {
        Ok(true) => {
            log_action(
                &state,
                "requeue_dead_letter",
                format!("requeued task {}", request.task_id),
            );
            (
                StatusCode::OK,
                Json(SchedulerTickResponse { fired_jobs: 1 }),
            )
                .into_response()
        }
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "dead-letter task not found".to_owned(),
            }),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("requeue failed: {err}"),
            }),
        )
            .into_response(),
    }
}

async fn register_schedule(
    State(state): State<AppState>,
    Json(request): Json<ScheduleTaskRequest>,
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

    let trigger = match parse_trigger(&request.trigger_kind, &request.trigger_expr) {
        Some(t) => t,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "invalid scheduler trigger".to_owned(),
                }),
            )
                .into_response();
        }
    };

    let now = now_unix();
    let next_run_unix = next_schedule_run_unix(&trigger, now).unwrap_or(now.saturating_add(60));
    let job = ScheduledJob {
        id: request.id,
        task_type: request.task_type,
        payload: request.payload,
        trigger,
        max_attempts: request.max_attempts,
        enabled: true,
        next_run_unix,
    };

    let result = state
        .sqlite
        .lock()
        .expect("sqlite lock poisoned")
        .upsert_scheduled_job(&job, now);

    match result {
        Ok(()) => {
            log_action(&state, "scheduler_register", format!("job={}", job.id));
            (
                StatusCode::OK,
                Json(SchedulerTickResponse { fired_jobs: 0 }),
            )
                .into_response()
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("scheduler register failed: {err}"),
            }),
        )
            .into_response(),
    }
}

async fn scheduler_tick(State(state): State<AppState>) -> impl IntoResponse {
    let now = now_unix();
    let result = state
        .sqlite
        .lock()
        .expect("sqlite lock poisoned")
        .run_scheduler_tick(now);

    match result {
        Ok(fired_jobs) => {
            if fired_jobs > 0 {
                log_action(&state, "scheduler_tick", format!("fired_jobs={fired_jobs}"));
            }
            (StatusCode::OK, Json(SchedulerTickResponse { fired_jobs })).into_response()
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("scheduler tick failed: {err}"),
            }),
        )
            .into_response(),
    }
}

async fn observability_heartbeat() -> Json<crate::observability::Heartbeat> {
    Json(heartbeat())
}

async fn observability_actions(State(state): State<AppState>) -> Json<Vec<ActionLog>> {
    let logs = state.logs.lock().expect("log lock poisoned");
    Json(logs.list())
}

fn run_single_worker_cycle(state: &AppState) {
    let now = now_unix();

    let _ = state
        .sqlite
        .lock()
        .expect("sqlite lock poisoned")
        .run_scheduler_tick(now);

    let maybe_task = state
        .sqlite
        .lock()
        .expect("sqlite lock poisoned")
        .claim_due_task(now);

    let Ok(Some(task)) = maybe_task else {
        return;
    };

    if task.payload.contains("fail") {
        let fail_result = state
            .sqlite
            .lock()
            .expect("sqlite lock poisoned")
            .fail_task(
                &task,
                now,
                state.config.retry_base_delay_seconds,
                state.config.retry_jitter_seconds,
                "simulated execution failure",
            );

        match fail_result {
            Ok(()) => log_action(
                state,
                "worker_retry",
                format!("task={} attempt={}", task.id, task.attempts + 1),
            ),
            Err(err) => warn!("failed to persist retry state for task {}: {err}", task.id),
        }
        return;
    }

    let complete_result = state
        .sqlite
        .lock()
        .expect("sqlite lock poisoned")
        .complete_task(&task.id);

    match complete_result {
        Ok(()) => log_action(
            state,
            "worker_complete",
            format!("task={} complete", task.id),
        ),
        Err(err) => warn!("failed to mark task complete {}: {err}", task.id),
    }
}

fn parse_trigger(kind: &str, expr: &str) -> Option<SchedulerTrigger> {
    match kind {
        "every_seconds" => expr.parse::<u64>().ok().map(SchedulerTrigger::EverySeconds),
        "cron" => Some(SchedulerTrigger::Cron(expr.to_owned())),
        _ => None,
    }
}

fn persist_guardian_snapshot(state: &AppState, total_spent_usd: f64, keys_revoked: bool) {
    let _ = state
        .sqlite
        .lock()
        .expect("sqlite lock poisoned")
        .persist_guardian_daily_spend("1970-01-01", total_spent_usd, keys_revoked, now_unix());
}

fn append_approval_event(state: &AppState, action: &str, decision: &str) {
    let event = ApprovalEvent {
        id: format!("approval-{}", now_unix()),
        action: action.to_owned(),
        decision: decision.to_owned(),
        actor: "system".to_owned(),
        reason: Some("awaiting user approval".to_owned()),
        created_at_unix: now_unix(),
    };
    let _ = state
        .sqlite
        .lock()
        .expect("sqlite lock poisoned")
        .append_approval_event(&event);
}

fn log_action(state: &AppState, action: impl Into<String>, detail: impl Into<String>) {
    state
        .logs
        .lock()
        .expect("log lock poisoned")
        .push(action, detail);
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
