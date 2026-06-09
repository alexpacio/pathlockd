use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{
    future::Future,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    panic::AssertUnwindSafe,
};

use futures::FutureExt;
use tonic::transport::{Endpoint, Server};
use tracing::{debug, error, info, warn};

use pathlockd::cluster::gossip;
use pathlockd::cluster::router::{Router, WriterOptions};
use pathlockd::config::Config;
use pathlockd::events::Broadcaster;
use pathlockd::otel;
use pathlockd::proto::path_lock_client::PathLockClient;
use pathlockd::proto::path_lock_server::PathLockServer;
use pathlockd::proto::HealthRequest;
use pathlockd::service::PathLockService;
use pathlockd::store_keys;
use pathlockd::store_rocksdb::{open_db, DbTuning};

const HEALTH_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
const HTTP2_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(20);
const HTTP2_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);
const TCP_KEEPALIVE: Duration = Duration::from_secs(30);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (cfg, health_check) = Config::load()?;

    if health_check {
        return health_probe(&cfg.listen).await;
    }

    let telemetry = otel::init(&cfg.log_level)?;

    info!(
        listen = %cfg.listen,
        node_id = %cfg.node_id,
        data_dir = %cfg.data_dir.display(),
        group_count = cfg.group_count,
        replication_factor = cfg.replication_factor,
        gossip_addr = %cfg.gossip_addr,
        seed_nodes = ?cfg.seed_nodes,
        request_timeout_ms = cfg.request_timeout_ms,
        otel_traces = telemetry.traces_enabled(),
        otel_metrics = telemetry.metrics_enabled(),
        "starting pathlockd"
    );

    if cfg.clustering_requested() {
        warn!(
            replication_factor = cfg.replication_factor,
            seed_nodes = ?cfg.seed_nodes,
            join = cfg.join,
            "clustered replication is configured but NOT implemented yet: every \
             replica grants locks from its own private store, so running more \
             than one replica against the same clients silently breaks mutual \
             exclusion. Run exactly one replica until Raft replication lands. \
             (Cross-instance event fan-out via peers/peer_discovery_dns is \
             best-effort delivery only and does not replicate lock state.)"
        );
    }

    // Ensure data directory exists
    std::fs::create_dir_all(&cfg.data_dir)?;

    // Open the local RocksDB for single-process/single-group mode (P1-P2).
    let db_path = cfg.data_dir.join("groups").join("g000001").join("db");
    std::fs::create_dir_all(&db_path)?;

    let db = open_db(
        &db_path,
        &DbTuning {
            max_open_files: cfg.rocksdb_max_open_files,
            max_total_wal_size_mb: cfg.rocksdb_max_total_wal_size_mb,
            max_background_jobs: cfg.rocksdb_max_background_jobs,
            block_cache_mb: cfg.rocksdb_block_cache_mb,
            write_buffer_mb: cfg.rocksdb_write_buffer_mb,
        },
    )?;

    // Start gossip (SWIM stub in P0-P2)
    let gossip_addr: SocketAddr = cfg.gossip_addr.parse()?;
    let _members = gossip::start_gossip(1, gossip_addr, cfg.seed_nodes.clone()).await?;

    // Router owns the serialized writer thread (bounded queue + group commit).
    let router = Arc::new(Router::new(
        db.clone(),
        WriterOptions {
            queue_depth: cfg.write_queue_depth,
            wal_sync: cfg.rocksdb_wal_sync,
        },
    ));
    otel::register_writer_queue_depth(router.write_queue_depth());

    // Events: cross-instance fan-out
    let broadcaster = Broadcaster::new(cfg.event_buffer, &cfg.peers)?;

    // Start per-group GC tasks (routed through the serialized writer).
    if cfg.group_gc_interval_secs > 0 {
        spawn_group_gc(
            router.clone(),
            cfg.group_gc_interval_secs,
            cfg.group_gc_batch,
        );
    }

    // Periodically drop the already-swept region of the expiry index from
    // disk so its tombstones never pile up in front of future scans.
    if cfg.gc_compact_interval_secs > 0 {
        spawn_expiry_maintenance(db.clone(), cfg.gc_compact_interval_secs);
    }

    // Peer discovery (DNS-based)
    if let Some(dns) = cfg.peer_discovery_dns.clone() {
        let self_ip = parse_self_ip(cfg.self_ip.as_deref());
        info!(%dns, refresh_secs = cfg.peer_refresh_secs, self_ip = ?self_ip, "peer discovery enabled");
        spawn_peer_discovery(broadcaster.clone(), dns, self_ip, cfg.peer_refresh_secs);
    }

    let path_lock = PathLockService::new(router, broadcaster.clone());
    let addr: SocketAddr = cfg
        .listen
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid listen address {}: {e}", cfg.listen))?;

    let grpc_router = Server::builder()
        .timeout(Duration::from_millis(cfg.request_timeout_ms))
        .concurrency_limit_per_connection(cfg.max_concurrent_requests_per_connection)
        .http2_keepalive_interval(Some(HTTP2_KEEPALIVE_INTERVAL))
        .http2_keepalive_timeout(Some(HTTP2_KEEPALIVE_TIMEOUT))
        .tcp_keepalive(Some(TCP_KEEPALIVE))
        .load_shed(true)
        .add_service(PathLockServer::new(path_lock));

    info!(%addr, "pathlockd listening");
    let serve_result = grpc_router
        .serve_with_shutdown(addr, shutdown_signal())
        .await;

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

// --- Background GC ---

/// Per-pass wall-clock budget. Each sweep is one bounded command through the
/// serialized writer; the pass keeps issuing sweeps until the backlog is
/// drained or the budget is spent, so GC throughput adapts to the write rate
/// instead of being capped at `batch` keys per tick.
const GC_PASS_BUDGET: Duration = Duration::from_millis(250);

fn spawn_group_gc(router: Arc<Router>, interval_secs: u64, batch: u32) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(interval_secs));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await;
        loop {
            tick.tick().await;
            run_background_step("group gc", group_gc_pass(router.clone(), batch)).await;
        }
    });
}

async fn group_gc_pass(router: Arc<Router>, batch: u32) {
    let started = Instant::now();
    let mut total_scanned = 0u64;
    let mut total_reclaimed = 0u64;
    loop {
        match router.gc_sweep(batch).await {
            Ok((scanned, reclaimed)) => {
                total_scanned += u64::from(scanned);
                total_reclaimed += reclaimed;
                // A short page means the backlog is drained.
                if scanned < batch || started.elapsed() >= GC_PASS_BUDGET {
                    break;
                }
            }
            Err(e) => {
                otel::record_gc_sweep(total_scanned, total_reclaimed, started.elapsed(), false);
                // Under write saturation the sweep is rejected by the bounded
                // queue (client traffic takes priority); retry next tick.
                warn!(error = %e, "group gc sweep failed; retrying next tick");
                return;
            }
        }
    }
    otel::record_gc_sweep(total_scanned, total_reclaimed, started.elapsed(), true);
}

// --- Expiry index physical maintenance ---

fn spawn_expiry_maintenance(db: Arc<rocksdb::DB>, interval_secs: u64) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(interval_secs));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await;
        loop {
            tick.tick().await;
            run_background_step("expiry maintenance", expiry_maintenance_pass(db.clone())).await;
        }
    });
}

/// Physically reclaim the swept region of the expiry index. Everything below
/// the persisted GC cursor is already logically deleted; dropping whole SST
/// files in that range (then compacting the remainder) keeps the queue-shaped
/// column family from accreting a tombstone wall in front of the cursor.
async fn expiry_maintenance_pass(db: Arc<rocksdb::DB>) {
    let result = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        let meta = db
            .cf_handle(store_keys::CF_META)
            .ok_or_else(|| anyhow::anyhow!("missing meta column family"))?;
        let Some(cursor) = db.get_cf(&meta, store_keys::META_GC_CURSOR_KEY)? else {
            return Ok(());
        };
        let expiry = db
            .cf_handle(store_keys::CF_EXPIRY)
            .ok_or_else(|| anyhow::anyhow!("missing expiry column family"))?;
        db.delete_file_in_range_cf(&expiry, &[] as &[u8], cursor.as_slice())
            .map_err(|e| anyhow::anyhow!("delete_file_in_range: {e}"))?;
        db.compact_range_cf(&expiry, None::<&[u8]>, Some(cursor.as_slice()));
        Ok(())
    })
    .await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => error!(error = %e, "expiry index maintenance failed"),
        Err(e) => error!(error = %e, "expiry index maintenance task failed"),
    }
}

// --- Health probe ---

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

// --- Peer discovery ---

fn parse_self_ip(self_ip: Option<&str>) -> Option<IpAddr> {
    let raw = self_ip?;
    match raw.parse::<IpAddr>() {
        Ok(ip) => Some(ip),
        Err(e) => {
            warn!(self_ip = %raw, error = %e, "ignoring unparseable self_ip");
            None
        }
    }
}

fn spawn_peer_discovery(
    broadcaster: Broadcaster,
    dns: String,
    self_ip: Option<IpAddr>,
    refresh_secs: u64,
) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(refresh_secs));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            match resolve_peers(&dns, self_ip).await {
                Ok(peers) => {
                    debug!(dns = %dns, count = peers.len(), ?peers, "resolved pathlockd peers");
                    broadcaster.reconcile_dynamic_peers(&peers);
                }
                Err(e) => {
                    warn!(dns = %dns, error = %e, "peer discovery resolution failed; keeping current peer set");
                }
            }
        }
    });
}

async fn resolve_peers(dns: &str, self_ip: Option<IpAddr>) -> anyhow::Result<Vec<String>> {
    let addrs = tokio::net::lookup_host(dns)
        .await
        .map_err(|e| anyhow::anyhow!("resolving peer discovery dns {dns}: {e}"))?;
    let mut peers = std::collections::BTreeSet::new();
    for addr in addrs {
        if Some(addr.ip()) == self_ip {
            continue;
        }
        peers.insert(peer_url(addr));
    }
    Ok(peers.into_iter().collect())
}

fn peer_url(addr: SocketAddr) -> String {
    match addr.ip() {
        IpAddr::V4(ip) => format!("http://{ip}:{}", addr.port()),
        IpAddr::V6(ip) => format!("http://[{ip}]:{}", addr.port()),
    }
}

// --- Shared helpers ---

async fn run_background_step<F>(name: &'static str, step: F)
where
    F: Future<Output = ()>,
{
    if let Err(panic) = AssertUnwindSafe(step).catch_unwind().await {
        error!(task = name, panic = %panic_message(&*panic), "background task step panicked; continuing");
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
    fn peer_url_brackets_ipv6() {
        assert_eq!(
            peer_url(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
                50051
            )),
            "http://10.0.0.1:50051"
        );
        assert_eq!(
            peer_url(SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 50051)),
            "http://[::1]:50051"
        );
    }

    #[test]
    fn parse_self_ip_handles_valid_and_invalid() {
        assert_eq!(parse_self_ip(None), None);
        assert_eq!(
            parse_self_ip(Some("10.0.0.5")),
            Some("10.0.0.5".parse().unwrap())
        );
        assert_eq!(parse_self_ip(Some("not-an-ip")), None);
    }
}
