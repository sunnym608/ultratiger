use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Query, State,
    },
    http::StatusCode,
    response::{Html, IntoResponse},
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
        ApprovalDecisionRequest, ApprovalItemResponse, AuditLogResponse, AuditQueryParams,
        BridgeReplyRequest, BridgeReplyResponse, BridgeWebhookRequest, BridgeWebhookResponse,
        BridgesHealthResponse, DeadLetterTaskResponse, DeleteMemoryResponse, ErrorResponse,
        GuardianResetResponse, GuardianStatusResponse, HealthResponse, MemoryCitation,
        MemoryIngestRequest, MemoryIngestResponse, MemoryPurgeRequest, MemoryPurgeResponse,
        MemoryQueryParams, MemoryRecordResponse, MemoryRetrieveRequest, MemoryRetrieveResponse,
        MetricsResponse, PermissionCheckRequest, PermissionCheckResponse, PreflightRequest,
        PreflightResponse, QueueStatusResponse, QueueTaskRequest, RequeueDeadLetterRequest,
        ScheduleTaskRequest, SchedulerTickResponse, SpendUpdateRequest, SpendUpdateResponse,
        TaskTimelineItemResponse,
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
    readiness: Arc<AtomicBool>,
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
            readiness: Arc::new(AtomicBool::new(true)),
        };
        state.start_worker_pool();
        state
    }

    pub fn is_ready(&self) -> bool {
        self.readiness.load(Ordering::SeqCst)
    }

    pub fn begin_shutdown(&self) {
        self.readiness.store(false, Ordering::SeqCst);
        let now = now_unix();
        let _ = self
            .sqlite
            .lock()
            .expect("sqlite lock poisoned")
            .append_audit_log(
                &format!("audit-shutdown-{}", now_millis()),
                "shutdown",
                "received shutdown signal; flushing state",
                "warn",
                None,
                now,
            );
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

pub fn router(config: AppConfig) -> (Router, AppState) {
    let state = AppState::new(config);
    let router = Router::new()
        .route("/health", get(health))
        .route("/health/liveness", get(liveness))
        .route("/health/readiness", get(readiness))
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
        .route("/metrics", get(metrics))
        .route("/audit/logs", get(audit_logs))
        .route("/tasks/:id/timeline", get(task_timeline))
        .route("/approvals/pending", get(approvals_pending))
        .route("/approvals/:id/decision", post(approval_decision))
        .route("/ui/approvals", get(approval_ui))
        .route("/ws/stream", get(ws_stream))
        .route("/observability/heartbeat", get(observability_heartbeat))
        .route("/observability/actions", get(observability_actions))
        .with_state(state.clone());

    (router, state)
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        service: "ultra-core",
    })
}

async fn liveness() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "alive",
        service: "ultra-core",
    })
}

async fn readiness(State(state): State<AppState>) -> impl IntoResponse {
    if state.is_ready() {
        (
            StatusCode::OK,
            Json(HealthResponse {
                status: "ready",
                service: "ultra-core",
            }),
        )
            .into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "service is shutting down".to_owned(),
            }),
        )
            .into_response()
    }
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
        "info",
        None,
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
        "warn",
        None,
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
        if requires_human_approval {
            "warn"
        } else {
            "info"
        },
        None,
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

    append_timeline(&state, &task.id, "input", &task.payload);
    log_action(
        &state,
        "queue_enqueue",
        "task persisted",
        "info",
        Some(&task.id),
    );
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
                "warn",
                Some(&request.task_id),
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
            log_action(
                &state,
                "scheduler_register",
                format!("job={}", job.id),
                "info",
                None,
            );
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
                log_action(
                    &state,
                    "scheduler_tick",
                    format!("fired_jobs={fired_jobs}"),
                    "info",
                    None,
                );
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
                "info",
                Some(&record_id),
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
    Json(request): Json<crate::models::MemoryPurgeRequest>,
) -> impl IntoResponse {
    let deleted = state
        .sqlite
        .lock()
        .expect("sqlite lock poisoned")
        .purge_expired_memories(request.ttl_seconds, now_unix());

    match deleted {
        Ok(deleted_records) => (
            StatusCode::OK,
            Json(crate::models::MemoryPurgeResponse { deleted_records }),
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
            let payload = serde_json::json!({
                "bridge": event.bridge,
                "user_id": event.user_id,
                "channel_id": event.channel_id,
                "text": event.text,
                "received_at_unix": event.received_at_unix,
            })
            .to_string();
            let task = TaskItem {
                id: task_id.clone(),
                task_type: "bridge.inbound".to_owned(),
                payload: payload.clone(),
                attempts: 0,
                max_attempts: 5,
                available_at_unix: now_unix(),
            };

            let _ = state
                .sqlite
                .lock()
                .expect("sqlite lock poisoned")
                .enqueue_task(&task, now_unix());
            append_timeline(&state, &task_id, "input", &payload);
            log_action(
                &state,
                "bridge_webhook_telegram",
                format!("queued={}", task_id),
                "info",
                Some(&task_id),
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
            let payload = serde_json::json!({
                "bridge": event.bridge,
                "user_id": event.user_id,
                "channel_id": event.channel_id,
                "text": event.text,
                "received_at_unix": event.received_at_unix,
            })
            .to_string();
            let task = TaskItem {
                id: task_id.clone(),
                task_type: "bridge.inbound".to_owned(),
                payload: payload.clone(),
                attempts: 0,
                max_attempts: 5,
                available_at_unix: now_unix(),
            };

            let _ = state
                .sqlite
                .lock()
                .expect("sqlite lock poisoned")
                .enqueue_task(&task, now_unix());
            append_timeline(&state, &task_id, "input", &payload);
            log_action(
                &state,
                "bridge_webhook_whatsapp",
                format!("queued={}", task_id),
                "info",
                Some(&task_id),
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
        payload: payload.clone(),
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
            append_timeline(&state, &task_id, "input", &payload);
            log_action(
                &state,
                "bridge_reply_enqueued",
                format!("task={}", task_id),
                "info",
                Some(&task_id),
            );
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

async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    let snapshot = state
        .sqlite
        .lock()
        .expect("sqlite lock poisoned")
        .metrics_snapshot();

    match snapshot {
        Ok(m) => (
            StatusCode::OK,
            Json(MetricsResponse {
                queue_pending: m.queue_pending,
                queue_dead_letter: m.queue_dead_letter,
                task_failures_total: m.task_failures_total,
                approvals_pending: m.approvals_pending,
                spend_today_usd: m.spend_today_usd,
            }),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("metrics failed: {err}"),
            }),
        )
            .into_response(),
    }
}

async fn audit_logs(
    State(state): State<AppState>,
    Query(params): Query<AuditQueryParams>,
) -> impl IntoResponse {
    let limit = params.limit.unwrap_or(100).min(1000);
    let rows = state
        .sqlite
        .lock()
        .expect("sqlite lock poisoned")
        .list_audit_logs(limit);

    match rows {
        Ok(rows) => (
            StatusCode::OK,
            Json(
                rows.into_iter()
                    .map(|r| AuditLogResponse {
                        id: r.id,
                        action: r.action,
                        detail: r.detail,
                        severity: r.severity,
                        task_id: r.task_id,
                        created_at_unix: r.created_at_unix,
                    })
                    .collect::<Vec<_>>(),
            ),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("audit logs failed: {err}"),
            }),
        )
            .into_response(),
    }
}

async fn task_timeline(State(state): State<AppState>, Path(id): Path<String>) -> impl IntoResponse {
    let rows = state
        .sqlite
        .lock()
        .expect("sqlite lock poisoned")
        .get_task_timeline(&id);

    match rows {
        Ok(rows) => (
            StatusCode::OK,
            Json(
                rows.into_iter()
                    .map(|r| TaskTimelineItemResponse {
                        stage: r.stage,
                        payload: r.payload,
                        created_at_unix: r.created_at_unix,
                    })
                    .collect::<Vec<_>>(),
            ),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("task timeline failed: {err}"),
            }),
        )
            .into_response(),
    }
}

async fn approvals_pending(State(state): State<AppState>) -> impl IntoResponse {
    let rows = state
        .sqlite
        .lock()
        .expect("sqlite lock poisoned")
        .list_pending_approvals(200);

    match rows {
        Ok(rows) => (
            StatusCode::OK,
            Json(
                rows.into_iter()
                    .map(|r| ApprovalItemResponse {
                        id: r.id,
                        action: r.action,
                        decision: r.decision,
                        actor: r.actor,
                        reason: r.reason,
                        created_at_unix: r.created_at_unix,
                    })
                    .collect::<Vec<_>>(),
            ),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("approvals fetch failed: {err}"),
            }),
        )
            .into_response(),
    }
}

async fn approval_decision(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(request): Json<ApprovalDecisionRequest>,
) -> impl IntoResponse {
    let decision = request.decision.to_lowercase();
    if decision != "approved" && decision != "rejected" {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "decision must be 'approved' or 'rejected'".to_owned(),
            }),
        )
            .into_response();
    }

    let result = state
        .sqlite
        .lock()
        .expect("sqlite lock poisoned")
        .set_approval_decision(
            &id,
            &decision,
            &request.actor,
            request.reason.as_deref(),
            now_unix(),
        );

    match result {
        Ok(true) => {
            log_action(
                &state,
                "approval_decision",
                format!("approval={} decision={}", id, decision),
                "warn",
                None,
            );
            (StatusCode::OK, Json(serde_json::json!({"ok": true}))).into_response()
        }
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "approval item not found".to_owned(),
            }),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("approval update failed: {err}"),
            }),
        )
            .into_response(),
    }
}

async fn approval_ui() -> Html<String> {
    Html(
        r#"<!doctype html>
<html>
<head>
  <meta charset='utf-8' />
  <title>Ultra Tiger - Approval Queue</title>
  <style>
    body { font-family: Inter, Arial, sans-serif; margin: 20px; background:#0b1020; color:#e6e8ef; }
    .card { background:#151c33; border:1px solid #2a355f; border-radius:10px; padding:14px; margin-bottom:12px; }
    button { margin-right:8px; padding:8px 12px; border-radius:8px; border:0; cursor:pointer; }
    .ok { background:#2ecc71; color:#071d0f; }
    .no { background:#ff6b6b; color:#2a0707; }
    code { color:#9dc1ff; }
  </style>
</head>
<body>
  <h1>Approval Queue</h1>
  <p>Pending sensitive actions requiring HITL decision.</p>
  <div id='list'></div>
  <script>
    async function load() {
      const res = await fetch('/approvals/pending');
      const items = await res.json();
      const list = document.getElementById('list');
      list.innerHTML = '';
      if (!items.length) { list.innerHTML = '<div class="card">No pending approvals.</div>'; return; }
      for (const item of items) {
        const card = document.createElement('div');
        card.className = 'card';
        card.innerHTML = `<div><b>ID:</b> <code>${item.id}</code></div><div><b>Action:</b> ${item.action}</div>`;
        const approve = document.createElement('button');
        approve.className = 'ok';
        approve.textContent = 'Approve';
        approve.onclick = () => decide(item.id, 'approved');
        const reject = document.createElement('button');
        reject.className = 'no';
        reject.textContent = 'Reject';
        reject.onclick = () => decide(item.id, 'rejected');
        card.appendChild(approve);
        card.appendChild(reject);
        list.appendChild(card);
      }
    }
    async function decide(id, decision) {
      await fetch(`/approvals/${id}/decision`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ decision, actor: 'ui-operator', reason: 'manual-review' })
      });
      load();
    }
    load();
    setInterval(load, 3000);
  </script>
</body>
</html>"#
            .to_owned(),
    )
}

async fn ws_stream(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| async move {
        stream_socket(socket, state).await;
    })
}

async fn stream_socket(mut socket: WebSocket, state: AppState) {
    loop {
        let heartbeat = heartbeat();
        let metrics = state
            .sqlite
            .lock()
            .expect("sqlite lock poisoned")
            .metrics_snapshot();

        let payload = match metrics {
            Ok(m) => serde_json::json!({
                "type": "heartbeat",
                "heartbeat": heartbeat,
                "metrics": {
                    "queue_pending": m.queue_pending,
                    "queue_dead_letter": m.queue_dead_letter,
                    "task_failures_total": m.task_failures_total,
                    "approvals_pending": m.approvals_pending,
                    "spend_today_usd": m.spend_today_usd,
                }
            }),
            Err(err) => serde_json::json!({
                "type": "heartbeat",
                "heartbeat": heartbeat,
                "error": err.to_string()
            }),
        };

        if socket
            .send(Message::Text(payload.to_string()))
            .await
            .is_err()
        {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
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

    append_timeline(state, &task.id, "tools", "worker claimed task");

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
                        append_timeline(state, &task.id, "output", "bridge message delivered");
                        log_action(
                            state,
                            "bridge_reply_sent",
                            format!("task={} delivered", task.id),
                            "info",
                            Some(&task.id),
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
                        append_timeline(
                            state,
                            &task.id,
                            "output",
                            &format!("bridge delivery failed: {err}"),
                        );
                        log_action(
                            state,
                            "bridge_reply_retry",
                            format!("task={} err={}", task.id, err),
                            "warn",
                            Some(&task.id),
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
            Ok(()) => {
                append_timeline(state, &task.id, "output", "task retry scheduled");
                log_action(
                    state,
                    "worker_retry",
                    format!("task={} attempt={}", task.id, task.attempts + 1),
                    "warn",
                    Some(&task.id),
                )
            }
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
        Ok(()) => {
            append_timeline(state, &task.id, "output", "task complete");
            log_action(
                state,
                "worker_complete",
                format!("task={} complete", task.id),
                "info",
                Some(&task.id),
            )
        }
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
        id: format!("approval-{}", now_millis()),
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

fn append_timeline(state: &AppState, task_id: &str, stage: &str, payload: &str) {
    let id = format!("timeline-{}-{}", task_id, now_millis());
    let _ = state
        .sqlite
        .lock()
        .expect("sqlite lock poisoned")
        .append_task_timeline(&id, task_id, stage, payload, now_unix());
}

fn log_action(
    state: &AppState,
    action: impl Into<String>,
    detail: impl Into<String>,
    severity: &str,
    task_id: Option<&str>,
) {
    let action = action.into();
    let detail = detail.into();

    state
        .logs
        .lock()
        .expect("log lock poisoned")
        .push(action.clone(), detail.clone());

    let id = format!("audit-{}", now_millis());
    let _ = state
        .sqlite
        .lock()
        .expect("sqlite lock poisoned")
        .append_audit_log(&id, &action, &detail, severity, task_id, now_unix());
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
