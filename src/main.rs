use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{
    future::Future,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    panic::AssertUnwindSafe,
};

use futures::FutureExt;
use tikv_client::TransactionClient;
use tonic::transport::{Endpoint, Server};
use tracing::{debug, error, info, warn};

use pathlockd::config::Config;
use pathlockd::events::Broadcaster;
use pathlockd::proto::path_lock_client::PathLockClient;
use pathlockd::proto::path_lock_server::PathLockServer;
use pathlockd::proto::HealthRequest;
use pathlockd::service::PathLockService;
use pathlockd::{otel, store};

const HEALTH_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
const GC_COORDINATION_LEASE_MS: u64 = 30_000;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (cfg, health_check) = Config::load()?;

    // One-shot health probe (container HEALTHCHECK): dial the local instance,
    // call Health, exit 0/1. Kept quiet — no tracing, no server startup.
    if health_check {
        return health_probe(&cfg.listen).await;
    }

    let telemetry = otel::init(&cfg.log_level)?;

    info!(
        listen = %cfg.listen,
        pd_endpoints = ?cfg.pd_endpoints,
        peers = ?cfg.peers,
        gc_interval_secs = cfg.gc_interval_secs,
        mvcc_gc_interval_secs = cfg.mvcc_gc_interval_secs,
        mvcc_gc_safe_point_retention_secs = cfg.mvcc_gc_safe_point_retention_secs,
        request_timeout_ms = cfg.request_timeout_ms,
        max_concurrent_requests_per_connection = cfg.max_concurrent_requests_per_connection,
        otel_traces = telemetry.traces_enabled(),
        otel_metrics = telemetry.metrics_enabled(),
        "starting pathlockd"
    );

    let client = Arc::new(
        TransactionClient::new(cfg.pd_endpoints.clone())
            .await
            .map_err(|e| anyhow::anyhow!("connecting to TiKV PD {:?}: {e}", cfg.pd_endpoints))?,
    );
    let instance_id = runtime_instance_id(&cfg.listen);
    let broadcaster = Broadcaster::new(cfg.event_buffer, &cfg.peers)?;

    if cfg.gc_interval_secs > 0 {
        spawn_logical_gc(
            client.clone(),
            instance_id.clone(),
            cfg.gc_interval_secs,
            cfg.gc_page,
        );
    }
    if cfg.mvcc_gc_interval_secs > 0 {
        spawn_mvcc_gc(
            client.clone(),
            instance_id.clone(),
            cfg.mvcc_gc_interval_secs,
            cfg.mvcc_gc_safe_point_retention_secs.saturating_mul(1000),
        );
    }

    let addr = cfg
        .listen
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid listen address {}: {e}", cfg.listen))?;

    let path_lock = PathLockService::new(client.clone(), broadcaster.clone());
    let router = Server::builder()
        .timeout(Duration::from_millis(cfg.request_timeout_ms))
        .concurrency_limit_per_connection(cfg.max_concurrent_requests_per_connection)
        .load_shed(true)
        .add_service(PathLockServer::new(path_lock));

    info!(%addr, "pathlockd listening");
    let serve_result = router.serve_with_shutdown(addr, shutdown_signal()).await;

    match &serve_result {
        Ok(_) => info!("pathlockd stopped"),
        Err(e) => error!(error = %e, "pathlockd stopped with server error"),
    }
    if let Err(e) = telemetry.shutdown() {
        warn!(error = %e, "OpenTelemetry shutdown failed");
    }

    serve_result?;
    Ok(())
}

fn runtime_instance_id(listen: &str) -> String {
    let host = std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("HOST"))
        .unwrap_or_else(|_| "unknown-host".to_string());
    format!("{host}:{}:{listen}", std::process::id())
}

/// Connect to a locally-running instance and call the `Health` RPC. Returns
/// `Ok` only when the server reports ready; any failure is an error so the
/// process exits non-zero. The listen address's bind host (`0.0.0.0` / `[::]`)
/// is mapped to loopback for dialing.
async fn health_probe(listen: &str) -> anyhow::Result<()> {
    let url = health_probe_url(listen)?;
    let endpoint = Endpoint::from_shared(url.clone())
        .map_err(|e| anyhow::anyhow!("invalid health probe endpoint {url}: {e}"))?
        .connect_timeout(HEALTH_PROBE_TIMEOUT)
        .timeout(HEALTH_PROBE_TIMEOUT);
    let channel = endpoint
        .connect()
        .await
        .map_err(|e| anyhow::anyhow!("health probe could not connect to {url}: {e}"))?;
    let mut client = PathLockClient::new(channel);
    let resp = client.health(HealthRequest {}).await?.into_inner();
    if resp.ok {
        Ok(())
    } else {
        anyhow::bail!("not ready: {}", resp.detail)
    }
}

fn health_probe_url(listen: &str) -> anyhow::Result<String> {
    let addr: SocketAddr = listen
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid listen address {listen}: {e}"))?;
    let ip = match addr.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(ip) if ip.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
        ip => ip,
    };
    Ok(match ip {
        IpAddr::V4(ip) => format!("http://{ip}:{}", addr.port()),
        IpAddr::V6(ip) => format!("http://[{ip}]:{}", addr.port()),
    })
}

fn spawn_logical_gc(
    client: Arc<TransactionClient>,
    instance_id: String,
    interval_secs: u64,
    page: u32,
) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(interval_secs));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await; // consume the immediate first tick
        loop {
            tick.tick().await;
            run_background_step("logical gc", logical_gc_pass(&client, &instance_id, page)).await;
        }
    });
}

async fn logical_gc_pass(client: &TransactionClient, instance_id: &str, page: u32) {
    match store::try_acquire_gc_lease(client, "logical", instance_id, GC_COORDINATION_LEASE_MS)
        .await
    {
        Ok(true) => {}
        Ok(false) => {
            debug!("logical gc skipped; another replica holds the gc lease");
            return;
        }
        Err(e) => {
            otel::record_gc_sweep(0, Duration::ZERO, false);
            error!(error = %e, "logical gc lease acquisition failed");
            return;
        }
    }

    let started = Instant::now();
    match store::gc_once(client, page).await {
        Ok(n) if n > 0 => {
            otel::record_gc_sweep(n, started.elapsed(), true);
            info!(reclaimed = n, "gc sweep");
        }
        Ok(_) => {
            otel::record_gc_sweep(0, started.elapsed(), true);
        }
        Err(e) => {
            otel::record_gc_sweep(0, started.elapsed(), false);
            error!(error = %e, "gc sweep failed");
        }
    }
}

fn spawn_mvcc_gc(
    client: Arc<TransactionClient>,
    instance_id: String,
    interval_secs: u64,
    retention_ms: u64,
) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(interval_secs));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await; // consume the immediate first tick
        loop {
            tick.tick().await;
            run_background_step(
                "tikv mvcc gc",
                mvcc_gc_pass(&client, &instance_id, retention_ms),
            )
            .await;
        }
    });
}

async fn mvcc_gc_pass(client: &TransactionClient, instance_id: &str, retention_ms: u64) {
    match store::try_acquire_gc_lease(client, "mvcc", instance_id, GC_COORDINATION_LEASE_MS).await {
        Ok(true) => {}
        Ok(false) => {
            debug!("tikv mvcc gc skipped; another replica holds the gc lease");
            return;
        }
        Err(e) => {
            error!(error = %e, "tikv mvcc gc lease acquisition failed");
            return;
        }
    }

    let started = Instant::now();
    match store::mvcc_gc_once(client, retention_ms).await {
        Ok(updated) => {
            info!(
                updated,
                retention_ms,
                elapsed_ms = started.elapsed().as_secs_f64() * 1000.0,
                "tikv mvcc gc sweep"
            );
        }
        Err(e) => {
            error!(error = %e, "tikv mvcc gc sweep failed");
        }
    }
}

async fn run_background_step<F>(name: &'static str, step: F)
where
    F: Future<Output = ()>,
{
    if let Err(panic) = AssertUnwindSafe(step).catch_unwind().await {
        error!(
            task = name,
            panic = %panic_message(&*panic),
            "background task step panicked; continuing"
        );
    }
}

fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = panic.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            warn!(error = %e, "failed to install SIGINT handler");
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    let term = async {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(e) => {
                warn!(error = %e, "failed to install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => info!("received SIGINT, shutting down"),
        _ = term => info!("received SIGTERM, shutting down"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_probe_url_maps_unspecified_binds_to_loopback() {
        assert_eq!(
            health_probe_url("0.0.0.0:50051").unwrap(),
            "http://127.0.0.1:50051"
        );
        assert_eq!(
            health_probe_url("[::]:50051").unwrap(),
            "http://[::1]:50051"
        );
    }

    #[test]
    fn health_probe_url_rejects_invalid_listen_address() {
        assert!(health_probe_url("not-a-socket").is_err());
    }
}
