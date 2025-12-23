use crate::runtime::Runtime;
use crate::time::Clock;
use anyhow::{Context, Result};
use clustor::telemetry::MetricsSnapshot;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;
use tracing_subscriber::reload;

pub type LogHandle = reload::Handle<EnvFilter, tracing_subscriber::Registry>;

/// Initialize JSON logging with reloadable level.
pub fn init_tracing(log_level: Option<&str>) -> Result<LogHandle> {
    let level = log_level.unwrap_or("info");
    let filter = EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new("info"));
    let (filter_layer, handle) = reload::Layer::new(filter);
    let fmt_layer = fmt::layer()
        .json()
        .with_target(true)
        .with_timer(fmt::time::UtcTime::rfc_3339());
    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .try_init()
        .map_err(|e| anyhow::anyhow!("failed to init tracing: {e}"))?;
    Ok(handle)
}

/// Start a minimal HTTP endpoint serving metrics, health, and loglevel controls.
pub async fn start_http<C>(
    bind: &str,
    runtime: Arc<Runtime<C>>,
    log_handle: Option<LogHandle>,
) -> Result<()>
where
    C: Clock + Send + Sync + 'static,
{
    let listener = TcpListener::bind(bind)
        .await
        .with_context(|| format!("failed to bind telemetry endpoint on {bind}"))?;
    tracing::info!("telemetry endpoint listening on {}", bind);
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((mut socket, addr)) => {
                    let runtime = runtime.clone();
                    let log_handle = log_handle.clone();
                    tokio::spawn(async move {
                        if let Err(err) = handle_conn(&mut socket, addr, runtime, log_handle).await
                        {
                            tracing::warn!("telemetry handler error: {err:?}");
                        }
                    });
                }
                Err(err) => {
                    tracing::warn!("telemetry accept error: {err:?}");
                }
            }
        }
    });
    Ok(())
}

async fn handle_conn<C>(
    socket: &mut tokio::net::TcpStream,
    _addr: SocketAddr,
    runtime: Arc<Runtime<C>>,
    log_handle: Option<LogHandle>,
) -> Result<()>
where
    C: Clock + Send + Sync + 'static,
{
    let mut buf = [0u8; 4096];
    let n = socket.read(&mut buf).await?;
    let req = String::from_utf8_lossy(&buf[..n]);
    let mut iter = req.lines();
    let first = iter.next().unwrap_or("");
    let path = first
        .split_whitespace()
        .nth(1)
        .unwrap_or("/")
        .split('?')
        .collect::<Vec<_>>();
    let route = path[0];
    let query = if path.len() > 1 { path[1] } else { "" };
    let (status, body, content_type) = match route {
        "/metrics" => (200, collect_metrics(&runtime).await, "text/plain"),
        "/readyz" => readyz(&runtime).await,
        "/livez" => livez(&runtime).await,
        "/v1/version" => {
            let minor = runtime.version_gate().current_minor();
            let body = format!("{{\"minor\":{}}}", minor);
            (200, body, "application/json")
        }
        "/v1/loglevel" => {
            if let Some(handle) = log_handle {
                if let Some(level) = query.strip_prefix("level=") {
                    if let Ok(filter) = EnvFilter::try_new(level) {
                        let _ = handle.modify(|f| *f = filter);
                    }
                }
            }
            (200, "{\"status\":\"ok\"}".to_string(), "application/json")
        }
        "/v1/chaos" => {
            let mut status = "ok".to_string();
            for part in query.split('&') {
                if let Some(val) = part.strip_prefix("fsync_slow=") {
                    let enable = val == "1" || val.eq_ignore_ascii_case("true");
                    runtime.fault_injector().enable_fsync_slow(enable);
                } else if let Some(val) = part.strip_prefix("election_delay=") {
                    let enable = val == "1" || val.eq_ignore_ascii_case("true");
                    runtime.fault_injector().enable_election_delay(enable);
                } else if let Some(val) = part.strip_prefix("kms_outage=") {
                    let enable = val == "1" || val.eq_ignore_ascii_case("true");
                    runtime.fault_injector().enable_kms_outage(enable);
                } else if let Some(val) = part.strip_prefix("latency_ms=") {
                    if let Ok(ms) = val.parse::<u8>() {
                        runtime.fault_injector().set_latency_ms(ms);
                    }
                } else if part == "promote=controlled" {
                    let dr = runtime.dr_manager().clone();
                    tokio::spawn(async move { dr.controlled_promotion().await });
                } else if part == "promote=uncontrolled" {
                    runtime.dr_manager().uncontrolled_promotion();
                } else if !part.is_empty() {
                    status = format!("unknown param {part}");
                }
            }
            (
                200,
                format!("{{\"status\":\"{}\"}}", status),
                "application/json",
            )
        }
        _ => (404, "not found".to_string(), "text/plain"),
    };
    let resp = format!(
        "HTTP/1.1 {} OK\r\nContent-Type: {}\r\nContent-Length: {}\r\n\r\n{}",
        status,
        content_type,
        body.len(),
        body
    );
    socket.write_all(resp.as_bytes()).await?;
    Ok(())
}

async fn collect_metrics<C: Clock>(runtime: &Runtime<C>) -> String {
    let now = runtime.clock().now();
    let sessions = runtime.sessions_len().await;
    let per_prg_replication = runtime.metrics().per_prg_replication_lag();
    let offline_usage = runtime.metrics().offline_snapshot();
    let (offline_entries, offline_bytes, offline_by_tenant) = {
        let mut counts = (0u64, 0u64);
        let mut per_tenant: HashMap<String, (u64, u64)> = HashMap::new();
        if let Ok(guard) = runtime.sessions().try_lock() {
            for state in guard.values() {
                counts.0 = counts.0.saturating_add(state.offline.len() as u64);
                counts.1 = counts.1.saturating_add(state.offline.bytes());
                let entry = per_tenant.entry(state.tenant_id.clone()).or_insert((0, 0));
                entry.0 = entry.0.saturating_add(state.offline.len() as u64);
                entry.1 = entry.1.saturating_add(state.offline.bytes());
            }
        }
        (counts.0, counts.1, per_tenant)
    };
    let cp_fresh = runtime.control_plane_cache_fresh();
    let cp_snapshot = runtime.control_plane().snapshot();
    let _cache_age_ms = now
        .saturating_duration_since(cp_snapshot.last_refresh)
        .as_millis();
    let _routing_epoch = cp_snapshot.routing_epoch.0;
    let _placement_count = cp_snapshot.placements.len() as u64;
    let fences = runtime.ack_contract().durability_fence_active() as u8;
    let prg_ready = runtime.prg_ready() as u8;
    let strict_fallback = runtime.strict_fallback() as u8;
    let active_hosts = runtime.prg_manager().active_hosts_len().await as u64;
    let clustor_floor = runtime.ack_contract().clustor_floor();
    let dr_lag = runtime.dr_manager().lag_seconds();
    let dr_last_ship = runtime.dr_manager().last_ship_age_seconds();
    let rebalance_grace = runtime.control_plane().rebalance_grace_active() as u8;
    let promoted = runtime.dr_manager().promoted() as u8;
    let uncontrolled = runtime.dr_manager().uncontrolled() as u8;
    let _dr_shipping = runtime.dr_manager().shipping_snapshot();
    let dr_region_cert = runtime.dr_manager().region_certificate().is_some() as u8;
    let dr_region_mismatch = runtime.dr_manager().region_mismatch() as u8;
    let backpressure = runtime.metrics().snapshot();
    let audit_events = crate::audit::sink_len();
    let mut body = format!(
        "quantum_sessions {}\nquantum_controlplane_fresh {}\nquantum_prg_ready {}\nquantum_strict_fallback {}\nquantum_wal_durability_fence {}\nquantum_dr_lag_seconds {}\nquantum_dr_last_ship_seconds {}\nquantum_dr_promoted {}\nquantum_dr_uncontrolled {}\nquantum_dr_region_certificate_present {}\nquantum_dr_region_certificate_mismatch {}\nqueue_depth{{type=\"commit_apply\"}} {}\nqueue_depth{{type=\"apply_delivery\"}} {}\napply_lag_seconds {}\nreplication_lag_seconds {}\nreject_overload_total {}\ntenant_throttle_events_total {}\nleader_credit_hint {}\nforward_backpressure_seconds {}\nforward_backpressure_events_total {}\naudit_events_total {}\n",
        sessions,
        if cp_fresh { 1 } else { 0 },
        prg_ready,
        strict_fallback,
        fences,
        dr_lag,
        dr_last_ship,
        promoted,
        uncontrolled,
        dr_region_cert,
        dr_region_mismatch,
        backpressure.queue_depth_apply,
        backpressure.queue_depth_delivery,
        backpressure.apply_lag_seconds,
        backpressure.replication_lag_seconds,
        backpressure.reject_overload_total,
        backpressure.tenant_throttle_events_total,
        backpressure.leader_credit_hint,
        backpressure.forward_backpressure_seconds,
        backpressure.forward_backpressure_events,
        audit_events,
    );
    for (prg, lag) in per_prg_replication {
        let (tenant, partition) = prg
            .split_once(':')
            .map(|(tenant, partition)| (tenant, partition.to_string()))
            .unwrap_or(("", prg.clone()));
        body.push_str(&format!(
            "replication_lag_seconds_per_prg{{tenant=\"{}\",partition=\"{}\"}} {}\n",
            tenant, partition, lag
        ));
    }
    body.push_str(&format!(
        "offline_entries_total {}\noffline_bytes_total {}\nactive_hosts {}\nclustor_floor {}\nrebalance_grace {}\n",
        offline_entries, offline_bytes, active_hosts, clustor_floor, rebalance_grace
    ));
    for (tenant, (entries, bytes)) in offline_by_tenant {
        body.push_str(&format!(
            "offline_entries_tenant_total{{tenant=\"{}\"}} {}\n",
            tenant, entries
        ));
        body.push_str(&format!(
            "offline_bytes_tenant_total{{tenant=\"{}\"}} {}\n",
            tenant, bytes
        ));
    }
    for (tenant, usage) in offline_usage {
        if usage.dropped == 0 {
            continue;
        }
        body.push_str(&format!(
            "offline_dropped_tenant_total{{tenant=\"{}\"}} {}\n",
            tenant, usage.dropped
        ));
    }
    let cp_metrics = match cp_snapshot.metrics.snapshot() {
        Ok(snapshot) => snapshot,
        Err(err) => {
            tracing::debug!("control-plane metrics snapshot failed: {err:?}");
            MetricsSnapshot {
                counters: HashMap::new(),
                histograms: HashMap::new(),
                gauges: HashMap::new(),
            }
        }
    };
    let sanitize = |name: &str| name.replace('.', "_");
    for (name, value) in cp_metrics.counters {
        body.push_str(&format!("{} {}\n", sanitize(&name), value));
    }
    for (name, value) in cp_metrics.gauges {
        body.push_str(&format!("{} {}\n", sanitize(&name), value));
    }
    body
}

pub async fn readyz<C: Clock>(runtime: &Runtime<C>) -> (u16, String, &'static str) {
    let fresh = runtime.control_plane_cache_fresh();
    let prg_ready = runtime.prg_ready();
    let strict = runtime.strict_fallback();
    let fence = runtime.ack_contract().durability_fence_active();
    let backpressure = runtime.metrics().snapshot();
    let dr_lag = runtime.dr_manager().lag_seconds();
    let ready = runtime.ready();
    let code = if ready { 200 } else { 503 };
    let body = format!(
        "{{\"ready\":{},\"control_plane_fresh\":{},\"prg_ready\":{},\"strict_fallback\":{},\"durability_fence\":{},\"apply_lag\":{},\"replication_lag\":{},\"dr_lag\":{},\"backpressure_depth\":{},\"dr_uncontrolled\":{}}}",
        ready, fresh, prg_ready, strict, fence, backpressure.apply_lag_seconds, backpressure.replication_lag_seconds, dr_lag, backpressure.queue_depth_apply, runtime.dr_manager().uncontrolled()
    );
    (code, body, "application/json")
}

pub async fn livez<C: Clock>(runtime: &Runtime<C>) -> (u16, String, &'static str) {
    let fences = runtime.ack_contract().durability_fence_active();
    let dr_ok = !runtime.dr_manager().uncontrolled();
    let prg_ready = runtime.prg_ready();
    let live = !fences && dr_ok && prg_ready;
    let code = if live { 200 } else { 503 };
    let body = format!(
        "{{\"live\":{},\"durability_fence\":{},\"dr_ok\":{},\"prg_ready\":{}}}",
        live, fences, dr_ok, prg_ready
    );
    (code, body, "application/json")
}
