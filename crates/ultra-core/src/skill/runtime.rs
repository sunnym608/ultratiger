use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::time;
use tracing::{info, warn};
use wasmtime::{Caller, Config, Engine, Linker, Module, Store};
use wasmtime_wasi::WasiCtxBuilder;

use super::package::{load_signed_package, SkillPackage};
use super::{check_host_call_allowed, validate_capabilities, HostCall, SkillError};

#[derive(Debug, Clone)]
pub struct SkillRuntimeConfig {
    pub timeout: Duration,
    pub memory_limit_bytes: usize,
}

impl Default for SkillRuntimeConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(10),
            memory_limit_bytes: 16 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SkillExecutionResult {
    pub stdout: String,
    pub stderr: String,
    pub host_call_logs: Vec<String>,
}

#[derive(Debug)]
struct RuntimeState {
    package: SkillPackage,
    host_call_logs: Arc<Mutex<Vec<String>>>,
}

pub async fn execute_signed_skill(
    package_dir: &str,
    config: SkillRuntimeConfig,
) -> Result<SkillExecutionResult, SkillError> {
    let package = load_signed_package(package_dir)?;
    validate_capabilities(&package.manifest)?;

    let execution = time::timeout(
        config.timeout,
        tokio::task::spawn_blocking(move || execute_blocking(package, config)),
    )
    .await
    .map_err(|_| SkillError::Runtime("skill execution timed out".to_owned()))?;

    execution.map_err(|join_err| SkillError::Runtime(format!("worker join error: {join_err}")))?
}

fn execute_blocking(
    package: SkillPackage,
    config: SkillRuntimeConfig,
) -> Result<SkillExecutionResult, SkillError> {
    let mut engine_config = Config::new();
    engine_config.consume_fuel(true);
    engine_config.wasm_multi_memory(false);
    let engine = Engine::new(&engine_config)
        .map_err(|err| SkillError::Runtime(format!("engine init failed: {err}")))?;

    let module = Module::from_binary(&engine, &package.wasm_bytes)
        .map_err(|err| SkillError::Runtime(format!("module load failed: {err}")))?;

    let stdout_pipe = wasmtime_wasi::pipe::MemoryOutputPipe::new(1024 * 1024);
    let stderr_pipe = wasmtime_wasi::pipe::MemoryOutputPipe::new(1024 * 1024);
    let wasi_ctx = WasiCtxBuilder::new()
        .stdout(stdout_pipe.clone())
        .stderr(stderr_pipe.clone())
        .build();

    let host_call_logs = Arc::new(Mutex::new(Vec::<String>::new()));

    let state = RuntimeState {
        package,
        host_call_logs: Arc::clone(&host_call_logs),
    };

    let mut store = Store::new(&engine, (state, wasi_ctx));
    store.set_fuel(2_000_000).map_err(|err| {
        SkillError::Runtime(format!("unable to set fuel for runtime guardrails: {err}"))
    })?;

    // Best-effort memory guard; if module requests more than configured, instantiation/traps.
    store.limiter(|(_, _wasi)| {
        Box::new(
            wasmtime::StoreLimitsBuilder::new()
                .memory_size(config.memory_limit_bytes)
                .build(),
        )
    });

    let mut linker: Linker<(RuntimeState, wasmtime_wasi::WasiCtx)> = Linker::new(&engine);
    wasmtime_wasi::add_to_linker_sync(&mut linker)
        .map_err(|err| SkillError::Runtime(format!("failed to add wasi linker: {err}")))?;

    add_host_shims(&mut linker)?;

    let instance = linker
        .instantiate(&mut store, &module)
        .map_err(|err| SkillError::Runtime(format!("instantiate failed: {err}")))?;

    let run = instance
        .get_typed_func::<(), ()>(&mut store, "run")
        .or_else(|_| instance.get_typed_func::<(), ()>(&mut store, "_start"))
        .map_err(|err| SkillError::Runtime(format!("entrypoint run/_start missing: {err}")))?;

    info!(
        "executing skill in package {:?}",
        store.data().0.package.package_dir
    );
    run.call(&mut store, ())
        .map_err(|err| SkillError::Runtime(format!("execution trap: {err}")))?;

    let stdout = String::from_utf8_lossy(&stdout_pipe.contents()).to_string();
    let stderr = String::from_utf8_lossy(&stderr_pipe.contents()).to_string();
    let host_call_logs = host_call_logs
        .lock()
        .map(|guard| guard.clone())
        .unwrap_or_default();

    if !stderr.is_empty() {
        warn!("skill stderr: {stderr}");
    }

    Ok(SkillExecutionResult {
        stdout,
        stderr,
        host_call_logs,
    })
}

fn add_host_shims(
    linker: &mut Linker<(RuntimeState, wasmtime_wasi::WasiCtx)>,
) -> Result<(), SkillError> {
    linker
        .func_wrap(
            "ultra",
            "host_fs_read",
            |caller: Caller<'_, (RuntimeState, wasmtime_wasi::WasiCtx)>| -> i32 {
                run_host_call_policy(caller, HostCall::FsRead)
            },
        )
        .map_err(|err| SkillError::Runtime(format!("host_fs_read shim failed: {err}")))?;

    linker
        .func_wrap(
            "ultra",
            "host_fs_write",
            |caller: Caller<'_, (RuntimeState, wasmtime_wasi::WasiCtx)>| -> i32 {
                run_host_call_policy(caller, HostCall::FsWrite)
            },
        )
        .map_err(|err| SkillError::Runtime(format!("host_fs_write shim failed: {err}")))?;

    linker
        .func_wrap(
            "ultra",
            "host_http_request",
            |caller: Caller<'_, (RuntimeState, wasmtime_wasi::WasiCtx)>| -> i32 {
                run_host_call_policy(caller, HostCall::NetOutbound)
            },
        )
        .map_err(|err| SkillError::Runtime(format!("host_http_request shim failed: {err}")))?;

    linker
        .func_wrap(
            "ultra",
            "host_browser_control",
            |caller: Caller<'_, (RuntimeState, wasmtime_wasi::WasiCtx)>| -> i32 {
                run_host_call_policy(caller, HostCall::BrowserControl)
            },
        )
        .map_err(|err| SkillError::Runtime(format!("host_browser_control shim failed: {err}")))?;

    Ok(())
}

fn run_host_call_policy(
    caller: Caller<'_, (RuntimeState, wasmtime_wasi::WasiCtx)>,
    host_call: HostCall,
) -> i32 {
    let state = &caller.data().0;
    let check = check_host_call_allowed(&state.package.manifest, host_call.clone());

    let outcome = match check {
        Ok(()) => {
            push_log(
                &state.host_call_logs,
                format!("allowed host call: {:?}", host_call),
            );
            0
        }
        Err(err) => {
            push_log(
                &state.host_call_logs,
                format!("denied host call: {:?} reason={}", host_call, err),
            );
            -1
        }
    };

    outcome
}

fn push_log(buffer: &Arc<Mutex<Vec<String>>>, message: String) {
    if let Ok(mut guard) = buffer.lock() {
        guard.push(message);
    }
}
