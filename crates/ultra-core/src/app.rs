use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use tracing::{info, warn};

use crate::{
    autonomy::{next_schedule_run_unix, SchedulerTrigger, TaskItem},
    bridges::{
        BridgeConfig, BridgeHub, BridgesHealthResponse as InternalBridgesHealth, OutboundReply,
    },
    config::AppConfig,
    guardian::Guardian,
    memory::{new_record, MemoryQuery},
    models::{
        BridgeReplyRequest, BridgeReplyResponse, BridgeWebhookRequest, BridgeWebhookResponse,
        BridgesHealthResponse, DeadLetterTaskResponse, DeleteMemoryResponse, ErrorResponse,
        GuardianResetResponse, GuardianStatusResponse, HealthResponse, MemoryCitation,
        MemoryIngestRequest, MemoryIngestResponse, MemoryPurgeRequest, MemoryPurgeResponse,
        MemoryQueryParams, MemoryRecordResponse, MemoryRetrieveRequest, MemoryRetrieveResponse,
        PermissionCheckRequest, PermissionCheckResponse, PreflightRequest, PreflightResponse,
        QueueStatusResponse, QueueTaskRequest, RequeueDeadLetterRequest, ScheduleTaskRequest,
        SchedulerTickResponse, SpendUpdateRequest, SpendUpdateResponse,
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
    bridge_hub: Arc<Mutex<BridgeHub>>,
    config: AppConfig,
}

impl AppState {
    pub fn new(config: AppConfig) -> Self {
        let sqlite = SqliteMemoryStore::open("ultra_core.db")
            .expect("failed to open SQLite store for persistent orchestration");
        let bridge_config = BridgeConfig {
            telegram_bot_token: config.telegram_bot_token.clone(),
            telegram_signing_secret: config.telegram_signing_secret.clone(),
            whatsapp_api_url: config.whatsapp_api_url.clone(),
            whatsapp_access_token: config.whatsapp_access_token.clone(),
            whatsapp_signing_secret: config.whatsapp_signing_secret.clone(),
            rate_limit_per_minute: config.bridge_rate_limit_per_minute,
            outbound_max_retries: config.bridge_outbound_max_retries,
        };
        let state = Self {
            guardian: Arc::new(Mutex::new(Guardian::new(config.clone()))),
            logs: Arc::new(Mutex::new(LogBuffer::new(500))),
            sqlite: Arc::new(Mutex::new(sqlite)),
            bridge_hub: Arc::new(Mutex::new(BridgeHub::new(bridge_config))),
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
        .route("/memory/ingest", post(memory_ingest))
        .route("/memory/query", get(memory_query))
        .route("/memory/retrieve", post(memory_retrieve))
        .route("/memory/purge", post(memory_purge))
        .route("/memory/:id", delete(memory_delete))
        .route("/bridges/telegram/webhook", post(telegram_webhook))
        .route("/bridges/whatsapp/webhook", post(whatsapp_webhook))
        .route("/bridges/reply", post(bridge_reply))
        .route("/bridges/health", get(bridges_health))
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

async fn memory_ingest(
    State(state): State<AppState>,
    Json(request): Json<MemoryIngestRequest>,
) -> impl IntoResponse {
    let model = request
        .model
        .unwrap_or_else(|| "deterministic-v1".to_owned());
    let chunk_size = request.chunk_size.unwrap_or(512);
    let record_id = request.id.unwrap_or_else(|| format!("mem-{}", now_unix()));

    let record = new_record(
        record_id.clone(),
        request.session_id,
        request.content,
        request.source,
    );

    let chunks_stored = state
        .sqlite
        .lock()
        .expect("sqlite lock poisoned")
        .ingest_memory_with_embeddings(&record, &model, chunk_size, 32);

    match chunks_stored {
        Ok(chunks_stored) => {
            log_action(
                &state,
                "memory_ingest",
                format!("record={} chunks={chunks_stored}", record_id),
            );
            (
                StatusCode::OK,
                Json(MemoryIngestResponse {
                    record_id,
                    chunks_stored,
                }),
            )
                .into_response()
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("memory ingest failed: {err}"),
            }),
        )
            .into_response(),
    }
}

async fn memory_query(
    State(state): State<AppState>,
    Query(params): Query<MemoryQueryParams>,
) -> impl IntoResponse {
    let query = MemoryQuery {
        session_id: params.session_id,
        source: params.source,
        limit: params.limit.unwrap_or(20),
    };

    let result = state
        .sqlite
        .lock()
        .expect("sqlite lock poisoned")
        .fetch_memory_by_filters(&query);

    match result {
        Ok(records) => (
            StatusCode::OK,
            Json(
                records
                    .into_iter()
                    .map(|record| MemoryRecordResponse {
                        id: record.id,
                        session_id: record.session_id,
                        source: record.source,
                        content: record.content,
                        created_at_unix: record.created_at_unix,
                    })
                    .collect::<Vec<_>>(),
            ),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("memory query failed: {err}"),
            }),
        )
            .into_response(),
    }
}

async fn memory_retrieve(
    State(state): State<AppState>,
    Json(request): Json<MemoryRetrieveRequest>,
) -> impl IntoResponse {
    let model = request
        .model
        .unwrap_or_else(|| "deterministic-v1".to_owned());
    let top_k = request.top_k.unwrap_or(5);

    let retrieved = state
        .sqlite
        .lock()
        .expect("sqlite lock poisoned")
        .retrieve_hybrid(
            &request.query,
            &model,
            request.session_id.as_deref(),
            request.source.as_deref(),
            top_k,
            32,
        );

    match retrieved {
        Ok(citations) => {
            let response = MemoryRetrieveResponse {
                citations: citations
                    .into_iter()
                    .map(|item| MemoryCitation {
                        record_id: item.record_id,
                        session_id: item.session_id,
                        source: item.source,
                        chunk_index: item.chunk_index,
                        quote: item.chunk_text,
                        semantic_score: item.semantic_score,
                        keyword_score: item.keyword_score,
                        final_score: item.final_score,
                        created_at_unix: item.created_at_unix,
                    })
                    .collect(),
            };
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("memory retrieve failed: {err}"),
            }),
        )
            .into_response(),
    }
}

async fn memory_purge(
    State(state): State<AppState>,
    Json(request): Json<MemoryPurgeRequest>,
) -> impl IntoResponse {
    let deleted = state
        .sqlite
        .lock()
        .expect("sqlite lock poisoned")
        .purge_expired_memories(request.ttl_seconds, now_unix());

    match deleted {
        Ok(deleted_records) => (
            StatusCode::OK,
            Json(MemoryPurgeResponse { deleted_records }),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("memory purge failed: {err}"),
            }),
        )
            .into_response(),
    }
}

async fn memory_delete(State(state): State<AppState>, Path(id): Path<String>) -> impl IntoResponse {
    let deleted = state
        .sqlite
        .lock()
        .expect("sqlite lock poisoned")
        .delete_memory_record(&id);

    match deleted {
        Ok(deleted) => (StatusCode::OK, Json(DeleteMemoryResponse { deleted })).into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("memory delete failed: {err}"),
            }),
        )
            .into_response(),
    }
}

async fn telegram_webhook(
    State(state): State<AppState>,
    Json(request): Json<BridgeWebhookRequest>,
) -> impl IntoResponse {
    let payload = request.payload.to_string();
    let normalized = state
        .bridge_hub
        .lock()
        .expect("bridge hub lock poisoned")
        .ingest_telegram_webhook(&payload, request.signature.as_deref());

    match normalized {
        Ok(event) => {
            let task_id = format!("bridge-inbound-telegram-{}", now_unix());
            let task = TaskItem {
                id: task_id.clone(),
                task_type: "bridge.inbound".to_owned(),
                payload: serde_json::json!({
                    "bridge": event.bridge,
                    "user_id": event.user_id,
                    "channel_id": event.channel_id,
                    "text": event.text,
                    "received_at_unix": event.received_at_unix,
                })
                .to_string(),
                attempts: 0,
                max_attempts: 5,
                available_at_unix: now_unix(),
            };

            let _ = state
                .sqlite
                .lock()
                .expect("sqlite lock poisoned")
                .enqueue_task(&task, now_unix());
            log_action(
                &state,
                "bridge_webhook_telegram",
                format!("queued={}", task_id),
            );
            (
                StatusCode::OK,
                Json(BridgeWebhookResponse {
                    queued_task_id: task_id,
                }),
            )
                .into_response()
        }
        Err(err) => (
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: format!("telegram webhook rejected: {err}"),
            }),
        )
            .into_response(),
    }
}

async fn whatsapp_webhook(
    State(state): State<AppState>,
    Json(request): Json<BridgeWebhookRequest>,
) -> impl IntoResponse {
    let payload = request.payload.to_string();
    let normalized = state
        .bridge_hub
        .lock()
        .expect("bridge hub lock poisoned")
        .ingest_whatsapp_webhook(&payload, request.signature.as_deref());

    match normalized {
        Ok(event) => {
            let task_id = format!("bridge-inbound-whatsapp-{}", now_unix());
            let task = TaskItem {
                id: task_id.clone(),
                task_type: "bridge.inbound".to_owned(),
                payload: serde_json::json!({
                    "bridge": event.bridge,
                    "user_id": event.user_id,
                    "channel_id": event.channel_id,
                    "text": event.text,
                    "received_at_unix": event.received_at_unix,
                })
                .to_string(),
                attempts: 0,
                max_attempts: 5,
                available_at_unix: now_unix(),
            };

            let _ = state
                .sqlite
                .lock()
                .expect("sqlite lock poisoned")
                .enqueue_task(&task, now_unix());
            log_action(
                &state,
                "bridge_webhook_whatsapp",
                format!("queued={}", task_id),
            );
            (
                StatusCode::OK,
                Json(BridgeWebhookResponse {
                    queued_task_id: task_id,
                }),
            )
                .into_response()
        }
        Err(err) => (
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: format!("whatsapp webhook rejected: {err}"),
            }),
        )
            .into_response(),
    }
}

async fn bridge_reply(
    State(state): State<AppState>,
    Json(request): Json<BridgeReplyRequest>,
) -> impl IntoResponse {
    let task_id = format!("bridge-reply-{}", now_unix());
    let payload = serde_json::to_string(&OutboundReply {
        bridge: request.bridge,
        channel_id: request.channel_id,
        text: request.text,
    })
    .unwrap_or_else(|_| "{}".to_owned());

    let task = TaskItem {
        id: task_id.clone(),
        task_type: "bridge.reply".to_owned(),
        payload,
        attempts: 0,
        max_attempts: state.config.bridge_outbound_max_retries.max(1),
        available_at_unix: now_unix(),
    };

    match state
        .sqlite
        .lock()
        .expect("sqlite lock poisoned")
        .enqueue_task(&task, now_unix())
    {
        Ok(()) => {
            log_action(&state, "bridge_reply_enqueued", format!("task={}", task_id));
            (
                StatusCode::OK,
                Json(BridgeReplyResponse {
                    queued_task_id: task_id,
                }),
            )
                .into_response()
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("failed to queue bridge reply: {err}"),
            }),
        )
            .into_response(),
    }
}

async fn bridges_health(State(state): State<AppState>) -> Json<BridgesHealthResponse> {
    let health: InternalBridgesHealth = state
        .bridge_hub
        .lock()
        .expect("bridge hub lock poisoned")
        .health();

    Json(BridgesHealthResponse {
        telegram_status: health.telegram.status,
        whatsapp_status: health.whatsapp.status,
        telegram_inbound_events: health.telegram.inbound_events,
        whatsapp_inbound_events: health.whatsapp.inbound_events,
    })
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

    if task.task_type == "bridge.reply" {
        let parsed = serde_json::from_str::<OutboundReply>(&task.payload);
        match parsed {
            Ok(reply) => {
                let send_result = state
                    .bridge_hub
                    .lock()
                    .expect("bridge hub lock poisoned")
                    .send_outbound_with_retry(reply);

                match send_result {
                    Ok(()) => {
                        let _ = state
                            .sqlite
                            .lock()
                            .expect("sqlite lock poisoned")
                            .complete_task(&task.id);
                        log_action(
                            state,
                            "bridge_reply_sent",
                            format!("task={} delivered", task.id),
                        );
                    }
                    Err(err) => {
                        let _ = state
                            .sqlite
                            .lock()
                            .expect("sqlite lock poisoned")
                            .fail_task(
                                &task,
                                now,
                                state.config.retry_base_delay_seconds,
                                state.config.retry_jitter_seconds,
                                &format!("bridge send error: {err}"),
                            );
                        log_action(
                            state,
                            "bridge_reply_retry",
                            format!("task={} err={}", task.id, err),
                        );
                    }
                }
            }
            Err(err) => {
                let _ = state
                    .sqlite
                    .lock()
                    .expect("sqlite lock poisoned")
                    .fail_task(
                        &task,
                        now,
                        state.config.retry_base_delay_seconds,
                        state.config.retry_jitter_seconds,
                        &format!("bridge payload parse error: {err}"),
                    );
            }
        }
        return;
    }

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
