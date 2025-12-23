mod edge;
pub mod protocol;

use crate::audit;
use crate::control::capabilities::ProtocolType;
use crate::control::{ControlPlaneClient, ControlPlaneError};
use crate::flow::{BackpressureState, QueueThresholds};
use crate::net::security::{AuthContext, SecurityError, SecurityManager};
use crate::ops::FaultInjector;
use crate::prg::{ForwardError, PrgManager};
use crate::replication::consensus::AckContract;
use crate::routing::RoutingError;
use crate::runtime::Runtime;
use crate::time::Clock;
use crate::workloads::mqtt::protocol::{
    read_connect, read_packet, write_auth, write_connack, write_disconnect, write_pingresp,
    write_puback, write_pubcomp, write_publish, write_pubrec, write_pubrel, write_suback,
    write_unsuback, ConnAckProperties, ControlPacket, InternalCode, ProtocolVersion, Qos,
    SubAckResult,
};
use crate::workloads::mqtt::session::{SessionProcessor, SessionState, SessionStatus};
use crate::workloads::mqtt::subscriptions::SharedSubscriptionBalancer;
use anyhow::{anyhow, Context, Result};
use uuid::Uuid;

/// Action to take after handling an incoming packet.
enum SessionAction {
    /// Continue processing packets.
    Continue,
    /// Client sent DISCONNECT; exit session loop gracefully.
    Disconnect,
}
use edge::EdgeTlsEndpoint;
#[cfg(feature = "quic")]
use edge::{EdgeError, EdgePolicy, ListenerTlsReloader};
use parking_lot::Mutex;
use protocol::{
    ProtocolDescriptor, ProtocolError, ProtocolIo, ProtocolPlugin, ProtocolRegistry,
    ProtocolSession, ProtocolTransport,
};
use std::collections::HashMap;
#[cfg(feature = "quic")]
use std::fs;
use std::future::Future;
#[cfg(feature = "quic")]
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
#[cfg(feature = "quic")]
use tokio::time::sleep;
#[derive(Clone)]
pub(crate) struct SessionCtx<C: Clock + Clone> {
    security: Arc<SecurityManager<C>>,
    prg_manager: PrgManager<C>,
    ack_contract: AckContract,
    control_plane: ControlPlaneClient<C>,
    sessions: Arc<tokio::sync::Mutex<HashMap<String, SessionState>>>,
    metrics: Arc<BackpressureState>,
    clock: C,
    thresholds: QueueThresholds,
    shared_balancer: Arc<tokio::sync::Mutex<SharedSubscriptionBalancer>>,
    shared_seq: Arc<AtomicU64>,
    protocol_registry: Arc<ProtocolRegistry<C>>,
    listener_metrics: Arc<ListenerMetrics>,
    fault_injector: FaultInjector,
}

const OUTBOUND_RETRY_BASE: Duration = Duration::from_secs(2);
const OUTBOUND_RETRY_MAX: Duration = Duration::from_secs(30);

#[derive(Clone, Default)]
struct ListenerMetrics {
    connections: Arc<Mutex<HashMap<String, u64>>>,
    negotiation_failures: Arc<Mutex<HashMap<String, u64>>>,
    fault_injections: Arc<Mutex<HashMap<String, u64>>>,
}

impl ListenerMetrics {
    fn record_connection(&self, protocol: &str, transport: &str, tenant: &str) {
        self.bump(&self.connections, Self::key(protocol, transport, tenant));
    }

    fn record_failure(&self, protocol: &str, transport: &str, reason: &str) {
        self.bump(
            &self.negotiation_failures,
            Self::key(protocol, transport, reason),
        );
    }

    fn record_fault_injection(&self, protocol: &str, reason: &str) {
        self.bump(&self.fault_injections, format!("{protocol}:{reason}"));
    }

    fn bump(&self, map: &Mutex<HashMap<String, u64>>, key: String) {
        let mut guard = map.lock();
        *guard.entry(key).or_insert(0) += 1;
    }

    fn key(protocol: &str, transport: &str, value: &str) -> String {
        format!("{protocol}:{transport}:{value}")
    }
}

impl<C: Clock + Clone> SessionCtx<C> {
    fn new(runtime: &Runtime<C>, registry: Arc<ProtocolRegistry<C>>) -> Self {
        let thresholds = if let Some(cfg) = &runtime.config.tenants.thresholds {
            QueueThresholds {
                commit_to_apply_pause_ack: cfg.commit_to_apply_pause_ack,
                apply_to_delivery_drop_qos0: cfg.apply_to_delivery_drop_qos0,
                ..QueueThresholds::default()
            }
        } else {
            QueueThresholds::default()
        };
        Self {
            security: Arc::new(runtime.security().clone()),
            prg_manager: runtime.prg_manager(),
            ack_contract: runtime.ack_contract().clone(),
            control_plane: runtime.control_plane(),
            sessions: runtime.sessions(),
            metrics: runtime.metrics(),
            clock: runtime.clock(),
            thresholds,
            shared_balancer: Arc::new(tokio::sync::Mutex::new(SharedSubscriptionBalancer::new())),
            shared_seq: Arc::new(AtomicU64::new(1)),
            protocol_registry: registry,
            listener_metrics: Arc::new(ListenerMetrics::default()),
            fault_injector: runtime.fault_injector().clone(),
        }
    }

    fn protocol_registry(&self) -> Arc<ProtocolRegistry<C>> {
        self.protocol_registry.clone()
    }

    fn listener_metrics(&self) -> Arc<ListenerMetrics> {
        self.listener_metrics.clone()
    }

    fn fault_injector(&self) -> FaultInjector {
        self.fault_injector.clone()
    }
}

/// Edge listener wiring for TCP/TLS and QUIC.
pub struct Listeners;

impl Listeners {
    pub async fn start<C>(runtime: &Runtime<C>) -> Result<()>
    where
        C: Clock + Send + Sync + 'static,
    {
        runtime
            .ensure_routing_epoch(runtime.prg_manager().routing_epoch())
            .await?;
        let registry = Arc::new(ProtocolRegistry::<C>::default());
        register_builtin_protocols(&registry);
        let ctx = SessionCtx::new(runtime, registry);
        Self::start_tcp(runtime.config.listeners.tcp.clone(), ctx.clone()).await?;
        if let Some(quic_cfg) = &runtime.config.listeners.quic {
            #[cfg(feature = "quic")]
            {
                Self::start_quic(quic_cfg.clone(), ctx.clone()).await?;
            }
            #[cfg(not(feature = "quic"))]
            {
                let _ = quic_cfg;
                tracing::warn!("quic listener configured but 'quic' feature is disabled");
            }
        }
        Ok(())
    }

    async fn start_tcp<C>(cfg: crate::config::TcpConfig, ctx: SessionCtx<C>) -> Result<()>
    where
        C: Clock + Send + Sync + 'static,
    {
        let endpoint = Arc::new(EdgeTlsEndpoint::new(&cfg)?);
        let bind_addr = cfg.bind.clone();
        let listener = TcpListener::bind(&bind_addr)
            .await
            .with_context(|| format!("failed to bind TCP listener on {}", bind_addr))?;
        #[cfg(unix)]
        use std::os::unix::io::AsRawFd;
        #[cfg(unix)]
        tracing::info!(
            "TCP/TLS listener bound on {} (fd={})",
            bind_addr,
            listener.as_raw_fd()
        );
        #[cfg(not(unix))]
        tracing::info!("TCP/TLS listener bound on {}", bind_addr);

        tokio::spawn(async move {
            loop {
                let (stream, peer) = match listener.accept().await {
                    Ok(pair) => pair,
                    Err(err) => {
                        tracing::warn!("tcp accept error: {err:?}");
                        continue;
                    }
                };
                let ctx = ctx.clone();
                let endpoint = endpoint.clone();
                tokio::spawn(async move {
                    let accepted = match endpoint.accept(stream, peer).await {
                        Ok(stream) => stream,
                        Err(err) => {
                            tracing::warn!("edge reject {peer}: {err}");
                            ctx.listener_metrics().record_failure(
                                "unknown",
                                "tcp",
                                &format!("{err}"),
                            );
                            return;
                        }
                    };
                    let tenant_id = accepted.identity.tenant_id.clone();
                    let alpn = accepted.identity.alpn.clone();
                    let sni = accepted.identity.sni.clone();
                    if !ctx
                        .control_plane
                        .routing_snapshot()
                        .placements
                        .keys()
                        .any(|p| p.tenant_id == tenant_id)
                    {
                        tracing::warn!(
                            "rejecting {peer} tenant={} due to missing placement at epoch {}",
                            tenant_id,
                            ctx.control_plane.routing_epoch().0
                        );
                        ctx.listener_metrics().record_failure(
                            "unknown",
                            "tcp",
                            "missing_placement",
                        );
                        return;
                    }
                    let Some(alpn) = alpn.clone() else {
                        ctx.listener_metrics()
                            .record_failure("unknown", "tcp", "no_alpn");
                        return;
                    };
                    let Some(plugin) = ctx.protocol_registry().plugin_for_alpn(&alpn) else {
                        tracing::warn!("tenant {} peer {peer} unknown ALPN {}", tenant_id, alpn);
                        ctx.listener_metrics()
                            .record_failure("unknown", "tcp", "registry_miss");
                        return;
                    };
                    let descriptor = plugin.descriptor().clone();
                    if !ctx
                        .control_plane
                        .has_protocol(&tenant_id, &descriptor.protocol)
                    {
                        tracing::warn!(
                            "tenant {} protocol {} disabled",
                            tenant_id,
                            descriptor.protocol
                        );
                        ctx.listener_metrics().record_failure(
                            descriptor.workload_label,
                            "tcp",
                            "capability_disabled",
                        );
                        return;
                    }
                    let caps = ctx.control_plane.get_tenant_capabilities(&tenant_id);
                    let capability_epoch = caps.as_ref().map(|c| c.epoch);
                    let session = ProtocolSession {
                        tenant_id: tenant_id.clone(),
                        sni,
                        alpn: Some(alpn),
                        transport: ProtocolTransport::Tcp,
                        peer_addr: Some(accepted.peer_addr),
                        capability_epoch,
                    };
                    let stream: ProtocolIo = Box::new(accepted.stream);
                    let metrics = ctx.listener_metrics();
                    let proto_label = descriptor.workload_label;
                    match plugin.handle_connection(stream, session, ctx.clone()).await {
                        Ok(_) => {
                            metrics.record_connection(proto_label, "tcp", &tenant_id);
                        }
                        Err(err) => {
                            metrics.record_failure(proto_label, "tcp", &format!("{err:?}"));
                            tracing::warn!(
                                "protocol {} session error tenant={} peer={:?} err={:?}",
                                proto_label,
                                tenant_id,
                                accepted.peer_addr,
                                err
                            );
                        }
                    }
                });
            }
        });
        Ok(())
    }

    #[cfg(feature = "quic")]
    async fn start_quic<C>(cfg: crate::config::QuicConfig, ctx: SessionCtx<C>) -> Result<()>
    where
        C: Clock + Send + Sync + 'static,
    {
        use quinn::Endpoint;
        use std::net::ToSocketAddrs;

        let reloadable = ListenerTlsReloader::new(
            cfg.tls_chain_path.clone(),
            cfg.tls_key_path.clone(),
            cfg.client_ca_path.clone(),
            cfg.alpn.clone(),
        );
        let server_config = build_quic_server_config(
            &cfg.tls_chain_path,
            &cfg.tls_key_path,
            &cfg.client_ca_path,
            &cfg,
        )?;
        let policy = Arc::new(EdgePolicy::from_quic_config(&cfg));

        let bind_addr = cfg
            .bind
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| anyhow::anyhow!("invalid QUIC bind address {}", cfg.bind))?;
        let endpoint = Endpoint::server(server_config, bind_addr)
            .with_context(|| format!("failed to bind QUIC on {}", cfg.bind))?;
        spawn_quic_hot_reload(endpoint.clone(), cfg.clone(), reloadable)?;
        tracing::info!(
            "QUIC listener bound on {} (idle={}s ack_delay={}ms)",
            endpoint.local_addr()?,
            cfg.max_idle_timeout_seconds,
            cfg.max_ack_delay_ms
        );

        tokio::spawn(async move {
            loop {
                let conn = match endpoint.accept().await {
                    Some(c) => c,
                    None => break,
                };
                let ctx = ctx.clone();
                let policy = policy.clone();
                tokio::spawn(async move {
                    let connecting = conn;
                    let connection = match connecting.await {
                        Ok(c) => c,
                        Err(err) => {
                            tracing::warn!("quic handshake failed: {err:?}");
                            return;
                        }
                    };
                    let mut raw_alpn = None;
                    let mut raw_sni = None;
                    if let Some(data) = connection.handshake_data() {
                        if let Some(hs) =
                            data.downcast_ref::<quinn::crypto::rustls::HandshakeData>()
                        {
                            if let Some(proto) = hs
                                .protocol
                                .as_ref()
                                .and_then(|p| std::str::from_utf8(p.as_ref()).ok())
                            {
                                raw_alpn = Some(proto.to_string());
                            }
                            if let Some(name) = hs.server_name.as_ref().map(|s| s.to_string()) {
                                raw_sni = Some(name);
                            }
                        }
                    }
                    let identity = match policy.resolve(raw_sni.clone(), raw_alpn.clone()) {
                        Ok(identity) => identity,
                        Err(err) => {
                            ctx.listener_metrics().record_failure(
                                "unknown",
                                "quic",
                                edge_error_label(&err),
                            );
                            tracing::warn!(
                                "quic connection {:?} rejected: {err}",
                                connection.remote_address()
                            );
                            connection.close(0u32.into(), b"EDGE_POLICY");
                            return;
                        }
                    };
                    if raw_alpn.is_none() {
                        if let Some(alpn) = &identity.alpn {
                            tracing::warn!(
                                "missing ALPN from {:?}; falling back to {alpn} per configuration",
                                connection.remote_address()
                            );
                        }
                    }
                    if raw_sni.is_none() {
                        tracing::warn!(
                            "missing SNI from {:?}; defaulting to tenant {} per configuration",
                            connection.remote_address(),
                            identity.tenant_id
                        );
                    }
                    let Some(alpn) = identity.alpn.clone() else {
                        ctx.listener_metrics()
                            .record_failure("unknown", "quic", "no_alpn");
                        connection.close(0u32.into(), b"ALPN_REQUIRED");
                        return;
                    };
                    let Some(plugin) = ctx.protocol_registry().plugin_for_alpn(&alpn) else {
                        tracing::warn!(
                            "quic tenant {} peer {:?} unknown ALPN {}",
                            identity.tenant_id,
                            connection.remote_address(),
                            alpn
                        );
                        ctx.listener_metrics()
                            .record_failure("unknown", "quic", "registry_miss");
                        connection.close(0u32.into(), b"UNKNOWN_ALPN");
                        return;
                    };
                    if !ctx
                        .control_plane
                        .routing_snapshot()
                        .placements
                        .keys()
                        .any(|p| p.tenant_id == identity.tenant_id)
                    {
                        tracing::warn!(
                            "rejecting {:?} tenant={} due to missing placement at epoch {}",
                            connection.remote_address(),
                            identity.tenant_id,
                            ctx.control_plane.routing_epoch().0
                        );
                        ctx.listener_metrics().record_failure(
                            "unknown",
                            "quic",
                            "missing_placement",
                        );
                        connection.close(0u32.into(), b"UNKNOWN_TENANT");
                        return;
                    }
                    let descriptor = plugin.descriptor().clone();
                    if !ctx
                        .control_plane
                        .has_protocol(&identity.tenant_id, &descriptor.protocol)
                    {
                        tracing::warn!(
                            "tenant {} protocol {} disabled (quic)",
                            identity.tenant_id,
                            descriptor.protocol
                        );
                        ctx.listener_metrics().record_failure(
                            descriptor.workload_label,
                            "quic",
                            "capability_disabled",
                        );
                        connection.close(0u32.into(), b"PROTO_DISABLED");
                        return;
                    }
                    let caps = ctx
                        .control_plane
                        .get_tenant_capabilities(&identity.tenant_id);
                    let capability_epoch = caps.as_ref().map(|c| c.epoch);
                    let session = ProtocolSession {
                        tenant_id: identity.tenant_id.clone(),
                        sni: identity.sni.clone(),
                        alpn: Some(alpn.clone()),
                        transport: ProtocolTransport::Quic,
                        peer_addr: Some(connection.remote_address()),
                        capability_epoch,
                    };
                    spawn_quic_migration_guard(connection.clone(), ctx.clone());
                    tracing::info!("accepted QUIC client {:?}", connection.remote_address());
                    if let Ok((send, recv)) = connection.accept_bi().await {
                        let stream = QuinnBiStream { recv, send };
                        let stream: ProtocolIo = Box::new(stream);
                        let metrics = ctx.listener_metrics();
                        match plugin.handle_connection(stream, session, ctx.clone()).await {
                            Ok(_) => metrics.record_connection(
                                plugin.descriptor().workload_label,
                                "quic",
                                &identity.tenant_id,
                            ),
                            Err(err) => {
                                metrics.record_failure(
                                    plugin.descriptor().workload_label,
                                    "quic",
                                    &format!("{err:?}"),
                                );
                                tracing::warn!(
                                    "quic protocol {} error {:?}: {err:?}",
                                    plugin.descriptor().workload_label,
                                    connection.remote_address()
                                );
                            }
                        }
                    }
                });
            }
        });
        Ok(())
    }

    /// Enforce 0-RTT policy: allowed only for CONNECT; QoS1/2 rejected.
    pub fn validate_zero_rtt(is_connect: bool) -> bool {
        is_connect
    }

    /// Retry token validity (60s).
    pub fn retry_token_valid(age: Duration) -> bool {
        age <= Duration::from_secs(60)
    }
}

#[cfg(feature = "quic")]
fn edge_error_label(err: &EdgeError) -> &'static str {
    match err {
        EdgeError::MissingAlpn => "missing_alpn",
        EdgeError::UnsupportedAlpn(_) => "unsupported_alpn",
        EdgeError::MissingSni => "missing_sni",
        EdgeError::MissingTenant => "missing_tenant",
        EdgeError::Tls(_) => "tls_error",
    }
}

#[cfg(feature = "quic")]
fn spawn_quic_hot_reload(
    endpoint: quinn::Endpoint,
    cfg: crate::config::QuicConfig,
    mut reloadable: ListenerTlsReloader,
) -> Result<()> {
    if tokio::runtime::Handle::try_current().is_err() {
        return Ok(());
    }
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            match reloadable.reload_quic_if_changed(&cfg) {
                Ok(Some(new_cfg)) => {
                    endpoint.set_server_config(Some(new_cfg));
                    tracing::info!("reloaded QUIC certificates");
                }
                Ok(None) => {}
                Err(err) => tracing::warn!("quic tls reload failed: {err:?}"),
            }
        }
    });
    Ok(())
}

#[cfg(feature = "quic")]
fn build_quic_server_config(
    chain_path: &PathBuf,
    key_path: &PathBuf,
    client_ca_path: &PathBuf,
    cfg: &crate::config::QuicConfig,
) -> Result<quinn::ServerConfig> {
    use quinn::crypto::rustls::QuicServerConfig;
    use quinn::rustls::{
        self,
        pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs1KeyDer, PrivatePkcs8KeyDer},
        version,
    };
    use quinn::{IdleTimeout, ServerConfig as QuinnServerConfig};
    use rustls_pemfile::{certs, pkcs8_private_keys, rsa_private_keys};
    use std::io::BufReader;
    // Load certs and key using the rustls version bundled with quinn.
    let cert_file = fs::File::open(chain_path)
        .with_context(|| format!("open quic chain {}", chain_path.display()))?;
    let mut cert_reader = BufReader::new(cert_file);
    let cert_chain: Vec<CertificateDer<'static>> = certs(&mut cert_reader)
        .map_err(|e| anyhow::anyhow!("read quic certs: {e}"))?
        .into_iter()
        .map(CertificateDer::from)
        .collect();
    let key_file = fs::File::open(key_path)
        .with_context(|| format!("open quic key {}", key_path.display()))?;
    let mut key_reader = BufReader::new(key_file);
    let keys = pkcs8_private_keys(&mut key_reader)
        .map_err(|e| anyhow::anyhow!("read quic pkcs8 key: {e}"))?;
    let key_der: PrivateKeyDer<'static> = if let Some(pkcs8) = keys.into_iter().next() {
        PrivatePkcs8KeyDer::from(pkcs8).into()
    } else {
        // Retry RSA (PKCS#1) if no PKCS#8 found.
        let key_file = fs::File::open(key_path)
            .with_context(|| format!("open quic key {}", key_path.display()))?;
        let mut key_reader = BufReader::new(key_file);
        let rsa_keys = rsa_private_keys(&mut key_reader)
            .map_err(|e| anyhow::anyhow!("read quic rsa key: {e}"))?;
        let first = rsa_keys.into_iter().next();
        first
            .map(|k| PrivatePkcs1KeyDer::from(k).into())
            .ok_or_else(|| anyhow::anyhow!("no private key found at {}", key_path.display()))?
    };

    let ca_file = fs::File::open(client_ca_path)
        .with_context(|| format!("open quic client ca {}", client_ca_path.display()))?;
    let mut ca_reader = BufReader::new(ca_file);
    let mut roots = rustls::RootCertStore::empty();
    for ca in certs(&mut ca_reader).map_err(|e| anyhow::anyhow!("read quic ca: {e}"))? {
        roots
            .add(CertificateDer::from(ca))
            .map_err(|e| anyhow::anyhow!("add quic ca: {e:?}"))?;
    }
    let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .map_err(|e| anyhow::anyhow!("build quic client verifier: {e:?}"))?;
    let mut tls_cfg = rustls::ServerConfig::builder_with_provider(
        rustls::crypto::ring::default_provider().into(),
    )
    .with_protocol_versions(&[&version::TLS13])?
    .with_client_cert_verifier(verifier)
    .with_single_cert(cert_chain, key_der)?;
    tls_cfg.alpn_protocols = cfg.alpn.iter().map(|a| a.as_bytes().to_vec()).collect();
    tls_cfg.max_early_data_size = 0;
    let crypto = QuicServerConfig::try_from(Arc::new(tls_cfg))
        .map_err(|e| anyhow::anyhow!("invalid QUIC rustls config: {e:?}"))?;
    let mut server_config = QuinnServerConfig::with_crypto(Arc::new(crypto));
    server_config.retry_token_lifetime(Duration::from_secs(60));
    let mut transport = quinn::TransportConfig::default();
    transport.max_idle_timeout(Some(IdleTimeout::try_from(Duration::from_secs(
        cfg.max_idle_timeout_seconds,
    ))?));
    transport.keep_alive_interval(Some(Duration::from_secs(30)));
    server_config.transport_config(Arc::new(transport));
    Ok(server_config)
}

#[cfg(feature = "quic")]
fn spawn_quic_migration_guard<C: Clock + Clone>(connection: quinn::Connection, ctx: SessionCtx<C>) {
    let mut last_addr = connection.remote_address();
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(5)).await;
            if connection.close_reason().is_some() {
                break;
            }
            let addr = connection.remote_address();
            if addr != last_addr {
                last_addr = addr;
                if !ctx.control_plane.cache_is_fresh() {
                    tracing::warn!("quic migration blocked due to stale control-plane cache");
                    connection.close(0u32.into(), b"PERMANENT_DURABILITY");
                    break;
                }
                let _ = ctx.prg_manager.refresh_routing_table().await;
                if let Err(err) = ctx
                    .prg_manager
                    .check_epoch(ctx.prg_manager.routing_epoch())
                    .await
                {
                    tracing::warn!("quic migration stale epoch: {err:?}");
                    connection.close(0u32.into(), b"PERMANENT_EPOCH");
                    break;
                }
            }
        }
    });
}

#[cfg(feature = "quic")]
struct QuinnBiStream {
    recv: quinn::RecvStream,
    send: quinn::SendStream,
}

#[cfg(feature = "quic")]
impl AsyncRead for QuinnBiStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.recv).poll_read(cx, buf)
    }
}

#[cfg(feature = "quic")]
impl AsyncWrite for QuinnBiStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        data: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match std::pin::Pin::new(&mut self.send).poll_write(cx, data) {
            std::task::Poll::Ready(Ok(n)) => std::task::Poll::Ready(Ok(n)),
            std::task::Poll::Ready(Err(e)) => {
                std::task::Poll::Ready(Err(std::io::Error::other(e.to_string())))
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match std::pin::Pin::new(&mut self.send).poll_flush(cx) {
            std::task::Poll::Ready(Ok(())) => std::task::Poll::Ready(Ok(())),
            std::task::Poll::Ready(Err(e)) => {
                std::task::Poll::Ready(Err(std::io::Error::other(e.to_string())))
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match std::pin::Pin::new(&mut self.send).poll_shutdown(cx) {
            std::task::Poll::Ready(Ok(())) => std::task::Poll::Ready(Ok(())),
            std::task::Poll::Ready(Err(e)) => {
                std::task::Poll::Ready(Err(std::io::Error::other(e.to_string())))
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

fn register_builtin_protocols<C>(registry: &Arc<ProtocolRegistry<C>>)
where
    C: Clock + Clone + Send + Sync + 'static,
{
    registry.register(Arc::new(MqttProtocolPlugin::default()));
}

struct MqttProtocolPlugin {
    descriptor: ProtocolDescriptor,
}

impl Default for MqttProtocolPlugin {
    fn default() -> Self {
        Self {
            descriptor: ProtocolDescriptor {
                name: "mqtt",
                protocol: ProtocolType::Mqtt,
                default_port: 8883,
                workload_label: "mqtt",
                workload_version: 1,
                requires_sni: true,
                supported_alpns: &["mqtt"],
                rate_limit_classes: &["standard"],
                requires_capabilities: &["mqtt5_features"],
            },
        }
    }
}

impl<C> ProtocolPlugin<C> for MqttProtocolPlugin
where
    C: Clock + Clone + Send + Sync + 'static,
{
    fn descriptor(&self) -> &ProtocolDescriptor {
        &self.descriptor
    }

    fn alpns(&self) -> &[&'static str] {
        self.descriptor.supported_alpns
    }

    fn handle_connection(
        &self,
        stream: ProtocolIo,
        session: ProtocolSession,
        ctx: SessionCtx<C>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ProtocolError>> + Send + '_>>
    where
        C: Clock + Clone + Send + Sync + 'static,
    {
        let tenant = session.tenant_id.clone();
        let sni = session.sni.clone();
        Box::pin(async move {
            let injector = ctx.fault_injector();
            let delay_ms = injector.latency_ms();
            if delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(delay_ms as u64)).await;
                ctx.listener_metrics()
                    .record_fault_injection(self.descriptor.workload_label, "delay");
            }
            handle_mqtt_session(stream, tenant, sni, ctx)
                .await
                .map_err(ProtocolError::from)
        })
    }
}

async fn handle_mqtt_session<C>(
    mut stream: ProtocolIo,
    tenant_id: String,
    sni: Option<String>,
    ctx: SessionCtx<C>,
) -> Result<()>
where
    C: Clock + Clone + Send + Sync + 'static,
{
    let mut connect = match read_connect(&mut stream).await {
        Ok(pkt) => pkt,
        Err(err) => {
            let _ = write_disconnect(
                &mut stream,
                ProtocolVersion::V3_1_1,
                InternalCode::ProtocolError,
                None,
            )
            .await;
            return Err(err);
        }
    };
    let mut assigned_client_id: Option<String> = None;
    if connect.client_id.is_empty() {
        if !connect.clean_start {
            tracing::warn!(
                "session rejected tenant={} due to empty client_id with clean_start=false",
                tenant_id
            );
            let _ = write_connack(
                &mut stream,
                connect.protocol,
                false,
                InternalCode::ProtocolError,
                ConnAckProperties::default(),
            )
            .await;
            return Ok(());
        }
        let generated = format!("anon-{}", Uuid::new_v4());
        tracing::info!(
            "assigning anonymous client_id tenant={} assigned={}",
            tenant_id,
            generated
        );
        assigned_client_id = Some(generated.clone());
        connect.client_id = generated;
    }
    if let Err(err) = ctx
        .control_plane
        .guard_read_gate(!ctx.ack_contract.durability_fence_active())
    {
        let reason = internal_from_control_plane(&err);
        let _ = write_connack(
            &mut stream,
            connect.protocol,
            false,
            reason,
            ConnAckProperties::default(),
        )
        .await;
        tracing::warn!(
            "session rejected tenant={} client_id={} due to control-plane/read-gate {:?}",
            tenant_id,
            connect.client_id,
            err
        );
        return Ok(());
    }
    let bearer_token = connect.username.clone().or_else(|| {
        connect
            .password
            .as_ref()
            .and_then(|p| std::str::from_utf8(p).ok())
            .map(|s| s.to_string())
    });
    let auth_ctx = AuthContext {
        tenant_id: &tenant_id,
        sni: sni.as_deref(),
        presented_spiffe: None,
        bearer_token: bearer_token.as_deref(),
        topic: None,
    };
    if let Err(err) = ctx.security.authorize_connect(auth_ctx) {
        let reason = internal_from_security(&err);
        let _ = write_connack(
            &mut stream,
            connect.protocol,
            false,
            reason,
            ConnAckProperties::default(),
        )
        .await;
        tracing::warn!(
            "session rejected tenant={} client_id={} reason={:?}",
            tenant_id,
            connect.client_id,
            err
        );
        audit::emit(
            "connect_rejected",
            &tenant_id,
            &connect.client_id,
            "auth failed",
        );
        return Ok(());
    }
    if let Err(err) = ctx.security.admit_session(&tenant_id) {
        let reason = internal_from_security(&err);
        let _ = write_connack(
            &mut stream,
            connect.protocol,
            false,
            reason,
            ConnAckProperties::default(),
        )
        .await;
        audit::emit(
            "connect_rejected",
            &tenant_id,
            &connect.client_id,
            "quota exceeded",
        );
        return Ok(());
    }
    let session_prg = ctx.prg_manager.session_prg(&tenant_id, &connect.client_id);
    if let Err(err) = ctx.prg_manager.validate_prg_epoch(&session_prg).await {
        let reason = internal_from_control_plane(&err);
        let _ = write_connack(
            &mut stream,
            connect.protocol,
            false,
            reason,
            ConnAckProperties::default(),
        )
        .await;
        return Ok(());
    }
    let (session_present, session_epoch) = match register_or_resume(
        &ctx,
        &tenant_id,
        &connect.client_id,
        connect.clean_start,
        &session_prg,
    )
    .await
    {
        Ok(vals) => vals,
        Err(err) => {
            let reason = internal_from_control_plane(&err);
            let _ = write_connack(
                &mut stream,
                connect.protocol,
                false,
                reason,
                ConnAckProperties::default(),
            )
            .await;
            return Ok(());
        }
    };
    let (outbound_tx, outbound_rx) = mpsc::channel(64);
    {
        let mut guard = ctx.sessions.lock().await;
        if let Some(state) = guard.get_mut(&format!("{tenant_id}:{}", connect.client_id)) {
            state.keep_alive = connect.keep_alive;
            state.session_expiry_interval = connect.properties.session_expiry_interval;
            state.last_packet_at = Some(ctx.clock.now());
            state.last_packet_at_wall = Some(std::time::SystemTime::now());
            state.will = connect.will.clone();
            state.outbound_tx = Some(outbound_tx);
        }
    }
    let mut processor = SessionProcessor::new(ctx.clock.clone(), ctx.ack_contract.clone());
    {
        let mut guard = ctx.sessions.lock().await;
        if let Some(state) = guard.get_mut(&format!("{tenant_id}:{}", connect.client_id)) {
            processor.hydrate(state);
        }
    }
    deliver_offline(
        &mut stream,
        &ctx,
        &tenant_id,
        &connect.client_id,
        connect.protocol,
        &mut processor,
    )
    .await?;
    let receive_max = receive_max_hint(&ctx.metrics, &mut processor, &ctx.thresholds);
    let session_key = format!("{tenant_id}:{}", connect.client_id);
    write_connack(
        &mut stream,
        connect.protocol,
        session_present,
        InternalCode::Success,
        ConnAckProperties {
            receive_max: Some(receive_max),
            session_expiry: connect.properties.session_expiry_interval,
            assigned_client_identifier: assigned_client_id.clone(),
            ..Default::default()
        },
    )
    .await?;
    tracing::info!(
        "MQTT CONNECT accepted tenant={} client_id={} proto={:?} clean_start={} epoch={}",
        tenant_id,
        connect.client_id,
        connect.protocol,
        connect.clean_start,
        session_epoch
    );
    audit::emit(
        "session_connected",
        &tenant_id,
        &connect.client_id,
        "accepted",
    );
    let persisted_meta =
        persisted_session_snapshot(&ctx, &session_key, &connect.client_id, None, &processor).await;
    if let Err(err) = ctx
        .prg_manager
        .persist_session_state(&session_prg, persisted_meta)
        .await
    {
        tracing::warn!(
            "failed to persist session metadata tenant={} client_id={} err={:?}",
            tenant_id,
            connect.client_id,
            err
        );
    }
    process_packets(
        &mut stream,
        ctx,
        tenant_id,
        connect.client_id,
        connect.protocol,
        &mut processor,
        outbound_rx,
        session_prg,
        connect.will,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn process_packets<S, C>(
    stream: &mut S,
    ctx: SessionCtx<C>,
    tenant_id: String,
    client_id: String,
    proto: ProtocolVersion,
    processor: &mut SessionProcessor<C>,
    mut outbound_rx: mpsc::Receiver<crate::workloads::mqtt::session::OutboundMessage>,
    session_prg: crate::routing::PrgId,
    will: Option<crate::mqtt::Will>,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    C: Clock + Clone + Send + Sync + 'static,
{
    let session_key = format!("{tenant_id}:{client_id}");
    let mut last_seen: Instant = {
        let guard = ctx.sessions.lock().await;
        guard
            .get(&session_key)
            .and_then(|s| s.last_packet_at)
            .unwrap_or_else(|| ctx.clock.now())
    };
    loop {
        let keep_alive = {
            let guard = ctx.sessions.lock().await;
            guard.get(&session_key).map(|s| s.keep_alive).unwrap_or(0)
        };
        let keepalive_deadline = if keep_alive > 0 {
            Some(
                last_seen
                    + Duration::from_secs(((keep_alive as u64) * 3) / 2)
                        .max(Duration::from_secs(1)),
            )
        } else {
            None
        };
        let retry_deadline = next_retry_deadline(&ctx, &session_key).await;
        let next_deadline = match (keepalive_deadline, retry_deadline) {
            (Some(a), Some(b)) => Some(std::cmp::min(a, b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            _ => None,
        };
        let sleep_dur = next_deadline
            .map(|d| {
                d.saturating_duration_since(ctx.clock.now())
                    .max(Duration::from_millis(10))
            })
            .unwrap_or(Duration::from_secs(5));
        tokio::select! {
            maybe_out = outbound_rx.recv() => {
                if let Some(out) = maybe_out {
                    if let Err(err) = send_outbound(stream, &ctx, &tenant_id, &client_id, proto, processor, out.clone(), &session_prg).await {
                        tracing::warn!("outbound delivery failed tenant={} client_id={} err={err:?}", tenant_id, client_id);
                        publish_will(&ctx, &tenant_id, will.as_ref()).await;
                        return Err(err);
                    }
                    continue;
                }
            }
            res = read_packet(stream, proto) => {
                match res {
                    Ok(packet) => {
                        tracing::info!(
                            "MQTT packet received tenant={} client_id={} packet={:?}",
                            tenant_id,
                            client_id,
                            std::mem::discriminant(&packet)
                        );
                        last_seen = ctx.clock.now();
                        match handle_incoming(
                            stream,
                            &ctx,
                            &tenant_id,
                            &client_id,
                            proto,
                            processor,
                            packet,
                            &session_key,
                            &session_prg,
                            will.as_ref(),
                        ).await? {
                            SessionAction::Continue => continue,
                            SessionAction::Disconnect => return Ok(()),
                        }
                    }
                    Err(err) => {
                        // Check if this is a graceful EOF (client closed connection)
                        let err_str = err.to_string().to_lowercase();
                        let is_eof = err_str.contains("eof")
                            || err_str.contains("connection reset")
                            || err_str.contains("broken pipe")
                            || err.downcast_ref::<std::io::Error>()
                                .map(|e| matches!(
                                    e.kind(),
                                    std::io::ErrorKind::UnexpectedEof
                                        | std::io::ErrorKind::ConnectionReset
                                        | std::io::ErrorKind::BrokenPipe
                                ))
                                .unwrap_or(false)
                            || err.chain().any(|cause| {
                                cause.downcast_ref::<std::io::Error>()
                                    .map(|e| matches!(
                                        e.kind(),
                                        std::io::ErrorKind::UnexpectedEof
                                            | std::io::ErrorKind::ConnectionReset
                                            | std::io::ErrorKind::BrokenPipe
                                    ))
                                    .unwrap_or(false)
                            });
                        if is_eof {
                            // Graceful disconnect - client closed connection
                            tracing::debug!(
                                "client disconnected tenant={} client_id={} (connection closed)",
                                tenant_id,
                                client_id
                            );
                            return Ok(());
                        }
                        publish_will(&ctx, &tenant_id, will.as_ref()).await;
                        return Err(err);
                    }
                }
            }
            _ = tokio::time::sleep(sleep_dur) => {
                let now = ctx.clock.now();
                if let Some(deadline) = keepalive_deadline {
                    if now >= deadline {
                        publish_will(&ctx, &tenant_id, will.as_ref()).await;
                        return Err(anyhow!("keep-alive timeout"));
                    }
                }
                if let Some(deadline) = next_deadline {
                    if now >= deadline {
                        resend_outbound(stream, &ctx, proto, processor, &session_key, &session_prg, now).await?;
                    }
                }
            }
        }
        if let Some(expiry) = {
            let guard = ctx.sessions.lock().await;
            guard
                .get(&session_key)
                .and_then(|s| s.session_expiry_interval)
        } {
            if ctx
                .clock
                .now()
                .saturating_duration_since(last_seen)
                .as_secs()
                >= expiry as u64
            {
                if let Err(err) = ctx
                    .prg_manager
                    .purge_session(&session_prg, &client_id)
                    .await
                {
                    tracing::warn!(
                        "failed to purge expired session {}:{}: {err:?}",
                        tenant_id,
                        client_id
                    );
                }
                ctx.security.release_session(&tenant_id);
                return Ok(());
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_incoming<S, C>(
    stream: &mut S,
    ctx: &SessionCtx<C>,
    tenant_id: &str,
    client_id: &str,
    proto: ProtocolVersion,
    processor: &mut SessionProcessor<C>,
    packet: ControlPacket,
    session_key: &str,
    session_prg: &crate::routing::PrgId,
    will: Option<&crate::mqtt::Will>,
) -> Result<SessionAction>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    C: Clock + Clone + Send + Sync + 'static,
{
    match packet {
        ControlPacket::Publish(publish) => {
            if let Err(err) =
                handle_publish(stream, ctx, tenant_id, client_id, proto, publish, processor).await
            {
                tracing::warn!(
                    "publish handling failed tenant={} client_id={} err={err:?}",
                    tenant_id,
                    client_id
                );
                let _ = write_disconnect(stream, proto, internal_from_error(&err), None).await;
                publish_will(ctx, tenant_id, will).await;
                return Ok(SessionAction::Continue);
            }
        }
        ControlPacket::Subscribe(sub) => {
            handle_subscribe(stream, ctx, tenant_id, client_id, proto, sub).await?;
        }
        ControlPacket::Unsubscribe(unsub) => {
            handle_unsubscribe(stream, ctx, tenant_id, client_id, proto, unsub).await?;
        }
        ControlPacket::PubAck(mid) => {
            complete_outbound(ctx, session_key, session_prg, mid, true).await;
        }
        ControlPacket::PubRec(mid) => {
            if mark_pubrec(ctx, session_key, mid).await {
                let snapshot =
                    persisted_session_snapshot(ctx, session_key, client_id, None, processor).await;
                if let Err(err) = ctx
                    .prg_manager
                    .persist_session_state(session_prg, snapshot)
                    .await
                {
                    tracing::warn!(
                        "failed to persist pubrec progress for {:?}: {err:?}",
                        session_prg
                    );
                }
                write_pubrel(stream, proto, mid).await?;
            }
        }
        ControlPacket::PubComp(mid) => {
            complete_outbound(ctx, session_key, session_prg, mid, true).await;
        }
        ControlPacket::PubRel(mid) => {
            let mut wal_index = ctx.ack_contract.clustor_floor();
            {
                let mut guard = ctx.sessions.lock().await;
                if let Some(state) = guard.get_mut(session_key) {
                    if let Some(idx) =
                        processor.dedupe_wal_index(state, mid, crate::dedupe::Direction::Publish)
                    {
                        wal_index = wal_index.max(idx);
                    }
                    processor.record_ack_progress(state, mid, wal_index);
                    state.inflight.remove(&mid);
                }
            }
            if let Err(err) = ctx.ack_contract.wait_for_floor(wal_index).await {
                write_disconnect(stream, proto, InternalCode::PermanentDurability, None).await?;
                return Err(anyhow!("durability wait failed: {err:?}"));
            }
            write_pubcomp(stream, proto, mid).await?;
            if ctx.sessions.lock().await.contains_key(session_key) {
                let snapshot =
                    persisted_session_snapshot(ctx, session_key, client_id, None, processor).await;
                if let Err(err) = ctx
                    .prg_manager
                    .persist_session_state(session_prg, snapshot)
                    .await
                {
                    tracing::warn!(
                        "failed to persist pubrel progress for {:?}: {err:?}",
                        session_prg
                    );
                }
            }
        }
        ControlPacket::PingReq => {
            write_pingresp(stream).await?;
        }
        ControlPacket::Auth(auth) => {
            if !matches!(proto, ProtocolVersion::V5) {
                write_disconnect(stream, proto, InternalCode::ProtocolError, None).await?;
                return Ok(SessionAction::Continue);
            }
            if auth.reason_code != crate::mqtt::ReasonCodes::SUCCESS {
                write_auth(stream, proto, InternalCode::Unauthorized, None).await?;
                return Ok(SessionAction::Continue);
            }
            if let Err(err) = ctx.security.refresh_credentials(tenant_id, client_id) {
                let reason = internal_from_security(&err);
                write_auth(stream, proto, reason, None).await?;
                return Ok(SessionAction::Continue);
            }
            write_auth(stream, proto, InternalCode::Success, None).await?;
            return Ok(SessionAction::Continue);
        }
        ControlPacket::Disconnect(_) => {
            {
                let mut guard = ctx.sessions.lock().await;
                if let Some(state) = guard.get_mut(session_key) {
                    state.status = SessionStatus::Disconnected;
                    state.outbound_tx = None;
                }
            }
            ctx.security.release_session(tenant_id);
            return Ok(SessionAction::Disconnect);
        }
        _ => {
            tracing::debug!(
                "ignoring unhandled packet tenant={} client_id={}",
                tenant_id,
                client_id
            );
        }
    }
    let mut guard = ctx.sessions.lock().await;
    if let Some(state) = guard.get_mut(session_key) {
        processor.record_activity(state);
    }
    Ok(SessionAction::Continue)
}

async fn next_retry_deadline<C: Clock + Clone>(
    ctx: &SessionCtx<C>,
    session_key: &str,
) -> Option<Instant> {
    let guard = ctx.sessions.lock().await;
    guard
        .get(session_key)
        .and_then(|s| s.outbound.values().map(|t| t.retry_at).min())
}

async fn resend_outbound<S, C>(
    stream: &mut S,
    ctx: &SessionCtx<C>,
    proto: ProtocolVersion,
    _processor: &mut SessionProcessor<C>,
    session_key: &str,
    _session_prg: &crate::routing::PrgId,
    now: Instant,
) -> Result<()>
where
    S: AsyncWrite + Unpin,
    C: Clock + Clone + Send + Sync + 'static,
{
    let mut to_resend = Vec::new();
    {
        let mut guard = ctx.sessions.lock().await;
        if let Some(state) = guard.get_mut(session_key) {
            for track in state.outbound.values_mut() {
                if now >= track.retry_at {
                    to_resend.push((track.mid, track.msg.clone(), track.stage));
                    track.retry_delay =
                        (track.retry_delay.saturating_mul(2)).min(OUTBOUND_RETRY_MAX);
                    track.retry_at = now + track.retry_delay;
                }
            }
        }
    }
    for (mid, msg, stage) in to_resend {
        match stage {
            crate::workloads::mqtt::session::OutboundStage::WaitPubAck
            | crate::workloads::mqtt::session::OutboundStage::WaitPubRec => {
                write_publish(
                    stream,
                    proto,
                    &msg.topic,
                    &msg.payload,
                    msg.qos,
                    msg.retain,
                    Some(mid),
                )
                .await?;
            }
            crate::workloads::mqtt::session::OutboundStage::WaitPubComp => {
                write_pubrel(stream, proto, mid).await?;
            }
        }
    }
    Ok(())
}

async fn publish_will<C: Clock + Clone>(
    ctx: &SessionCtx<C>,
    tenant_id: &str,
    will: Option<&crate::mqtt::Will>,
) {
    if let Some(w) = will {
        let prg = ctx.prg_manager.topic_prg(tenant_id, &w.topic);
        if let Err(err) = ctx
            .prg_manager
            .commit_publish(&prg, &w.topic, &w.payload, w.qos, w.retain)
            .await
        {
            tracing::warn!("failed to publish will for tenant {}: {err:?}", tenant_id);
        }
    }
    ctx.security.release_session(tenant_id);
}

#[allow(clippy::too_many_arguments)]
async fn send_outbound<S, C>(
    stream: &mut S,
    ctx: &SessionCtx<C>,
    tenant_id: &str,
    client_id: &str,
    proto: ProtocolVersion,
    processor: &mut SessionProcessor<C>,
    msg: crate::workloads::mqtt::session::OutboundMessage,
    session_prg: &crate::routing::PrgId,
) -> Result<()>
where
    S: AsyncWrite + Unpin,
    C: Clock + Clone + Send + Sync + 'static,
{
    let mid = match msg.qos {
        Qos::AtMostOnce => None,
        _ => Some(processor.next_mid()),
    };
    if let Some(id) = mid {
        let mut guard = ctx.sessions.lock().await;
        if let Some(state) = guard.get_mut(&format!("{tenant_id}:{client_id}")) {
            processor.record_inflight(state, id);
            let stage = match msg.qos {
                Qos::AtMostOnce => crate::workloads::mqtt::session::OutboundStage::WaitPubAck,
                Qos::AtLeastOnce => crate::workloads::mqtt::session::OutboundStage::WaitPubAck,
                Qos::ExactlyOnce => crate::workloads::mqtt::session::OutboundStage::WaitPubRec,
            };
            let retry_delay = OUTBOUND_RETRY_BASE;
            let tracking = crate::workloads::mqtt::session::OutboundTracking {
                msg: msg.clone(),
                stage,
                mid: id,
                retry_at: ctx.clock.now() + retry_delay,
                retry_delay,
            };
            state.outbound.insert(id, tracking);
        }
    }
    write_publish(
        stream,
        proto,
        &msg.topic,
        &msg.payload,
        msg.qos,
        msg.retain,
        mid,
    )
    .await?;
    if msg.qos == Qos::AtMostOnce {
        let _ = ctx
            .prg_manager
            .clear_offline_entries(session_prg, &msg.client_id, msg.enqueue_index)
            .await;
        let mut guard = ctx.sessions.lock().await;
        if let Some(state) = guard.get_mut(&format!("{tenant_id}:{client_id}")) {
            let mut retained = Vec::new();
            while let Some(front) = state.offline.dequeue() {
                if front.enqueue_index > msg.enqueue_index {
                    retained.push(front);
                }
            }
            for entry in retained {
                state.offline.enqueue(entry);
            }
            state.earliest_offline_queue_index = state.offline.earliest_offline_queue_index();
            state.offline_floor_index = state.offline_floor_index.max(msg.enqueue_index);
        }
    }
    Ok(())
}

async fn complete_outbound<C: Clock + Clone>(
    ctx: &SessionCtx<C>,
    session_key: &str,
    session_prg: &crate::routing::PrgId,
    mid: u16,
    remove_inflight: bool,
) {
    let mut guard = ctx.sessions.lock().await;
    if let Some(state) = guard.get_mut(session_key) {
        if let Some(track) = state.outbound.remove(&mid) {
            let msg = track.msg;
            let _ = ctx
                .prg_manager
                .clear_offline_entries(session_prg, &msg.client_id, msg.enqueue_index)
                .await;
            let mut retained = Vec::new();
            while let Some(front) = state.offline.dequeue() {
                if front.enqueue_index > msg.enqueue_index {
                    retained.push(front);
                }
            }
            for entry in retained {
                state.offline.enqueue(entry);
            }
            state.earliest_offline_queue_index = state.offline.earliest_offline_queue_index();
            state.offline_floor_index = state.offline_floor_index.max(msg.enqueue_index);
        }
        if remove_inflight {
            state.inflight.remove(&mid);
        }
    }
}

async fn mark_pubrec<C: Clock + Clone>(ctx: &SessionCtx<C>, session_key: &str, mid: u16) -> bool {
    let mut guard = ctx.sessions.lock().await;
    if let Some(state) = guard.get_mut(session_key) {
        if let Some(track) = state.outbound.get_mut(&mid) {
            track.stage = crate::workloads::mqtt::session::OutboundStage::WaitPubComp;
            track.retry_delay = track
                .retry_delay
                .saturating_mul(2)
                .min(Duration::from_secs(30));
            track.retry_at = ctx.clock.now() + track.retry_delay;
            return true;
        }
    }
    false
}

async fn deliver_offline<S, C>(
    stream: &mut S,
    ctx: &SessionCtx<C>,
    tenant_id: &str,
    client_id: &str,
    proto: ProtocolVersion,
    processor: &mut SessionProcessor<C>,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    C: Clock + Clone + Send + Sync + 'static,
{
    let session_prg = ctx.prg_manager.session_prg(tenant_id, client_id);
    let mut guard = ctx.sessions.lock().await;
    if let Some(state) = guard.get_mut(&format!("{tenant_id}:{client_id}")) {
        while let Some(entry) = state.offline.dequeue() {
            let payload_len = entry.payload.len() as i64;
            let mid = match entry.qos {
                Qos::AtMostOnce => None,
                _ => {
                    let id = processor.next_mid();
                    processor.record_inflight(state, id);
                    Some(id)
                }
            };
            // Track outbound messages for QoS > 0 so we can retry if no ack received.
            if let Some(id) = mid {
                let stage = match entry.qos {
                    Qos::AtMostOnce => crate::workloads::mqtt::session::OutboundStage::WaitPubAck,
                    Qos::AtLeastOnce => crate::workloads::mqtt::session::OutboundStage::WaitPubAck,
                    Qos::ExactlyOnce => crate::workloads::mqtt::session::OutboundStage::WaitPubRec,
                };
                let retry_delay = OUTBOUND_RETRY_BASE;
                let tracking = crate::workloads::mqtt::session::OutboundTracking {
                    msg: crate::workloads::mqtt::session::OutboundMessage {
                        client_id: client_id.to_string(),
                        topic: entry.topic.clone(),
                        payload: entry.payload.clone(),
                        qos: entry.qos,
                        retain: entry.retain,
                        enqueue_index: entry.enqueue_index,
                    },
                    stage,
                    mid: id,
                    retry_at: ctx.clock.now() + retry_delay,
                    retry_delay,
                };
                state.outbound.insert(id, tracking);
            }
            write_publish(
                stream,
                proto,
                &entry.topic,
                &entry.payload,
                entry.qos,
                entry.retain,
                mid,
            )
            .await?;
            // For QoS 0, we can clear immediately since no ack is expected.
            // For QoS 1/2, the ack handler (complete_outbound) will clear when ack received.
            if entry.qos == Qos::AtMostOnce {
                let _ = ctx
                    .security
                    .record_offline_usage(tenant_id, -1, -payload_len);
                ctx.metrics
                    .record_offline_delta(tenant_id, -1, -payload_len);
                if let Err(err) = ctx
                    .prg_manager
                    .clear_offline_entries(&session_prg, client_id, entry.enqueue_index)
                    .await
                {
                    tracing::warn!("failed to clear offline entries {:?}: {err:?}", session_prg);
                    break;
                }
            }
        }
        state.earliest_offline_queue_index = state.offline.earliest_offline_queue_index();
    }
    Ok(())
}

async fn queue_subscribers_offline<C: Clock + Clone>(
    ctx: &SessionCtx<C>,
    topic_prg: &crate::routing::PrgId,
    topic: &str,
    payload: &[u8],
    qos: Qos,
    retain: bool,
) -> Result<(), ControlPlaneError> {
    let subscribers = ctx
        .prg_manager
        .subscribers_for_topic(topic_prg, topic)
        .await?;
    if subscribers.is_empty() {
        return Ok(());
    }
    let mut targets = Vec::new();
    let mut shared: HashMap<String, Vec<crate::prg::Subscription>> = HashMap::new();
    for sub in subscribers {
        if let Some(group) = &sub.shared_group {
            shared.entry(group.clone()).or_default().push(sub.clone());
        } else {
            targets.push(sub.clone());
        }
    }
    let packet_id = ctx.shared_seq.fetch_add(1, Ordering::Relaxed);
    {
        let mut balancer = ctx.shared_balancer.lock().await;
        for (group, subs) in shared {
            let members: Vec<String> = subs.iter().map(|s| s.client_id.clone()).collect();
            if let Some(chosen) = balancer.assign(&group, &members, packet_id) {
                if let Some(selected) = subs.into_iter().find(|s| s.client_id == chosen) {
                    targets.push(selected);
                }
            }
            balancer.release(&group, packet_id);
        }
    }
    let mut sessions = ctx.sessions.lock().await;
    for sub in targets {
        let key = format!("{}:{}", topic_prg.tenant_id, sub.client_id);
        if let Some(state) = sessions.get_mut(&key) {
            let enqueue_index = state.offline.enqueue_index().saturating_add(1);
            let session_prg = ctx
                .prg_manager
                .session_prg(&topic_prg.tenant_id, &sub.client_id);
            let entry = crate::offline::OfflineEntry {
                client_id: sub.client_id.clone(),
                topic: topic.to_string(),
                qos: sub.qos,
                retain,
                enqueue_index,
                payload: payload.to_vec(),
            };
            if let Some(limits) = ctx.security.quotas().limits() {
                if let Some(max_entries) = limits.max_offline_entries {
                    if state.offline.len() as u64 >= max_entries {
                        ctx.metrics.record_offline_drop(&topic_prg.tenant_id);
                        ctx.metrics.inc_tenant_throttle();
                        audit::emit(
                            "offline_throttled",
                            &topic_prg.tenant_id,
                            &sub.client_id,
                            "offline entries exceeded",
                        );
                        continue;
                    }
                }
                if let Some(max_bytes) = limits.max_offline_bytes {
                    if state.offline.bytes().saturating_add(payload.len() as u64) > max_bytes {
                        ctx.metrics.record_offline_drop(&topic_prg.tenant_id);
                        ctx.metrics.inc_tenant_throttle();
                        audit::emit(
                            "offline_throttled",
                            &topic_prg.tenant_id,
                            &sub.client_id,
                            "offline bytes exceeded",
                        );
                        continue;
                    }
                }
            }
            if let Err(err) = ctx.security.record_offline_usage(
                &topic_prg.tenant_id,
                1,
                entry.payload.len() as i64,
            ) {
                ctx.metrics.record_offline_drop(&topic_prg.tenant_id);
                ctx.metrics.inc_tenant_throttle();
                audit::emit(
                    "offline_throttled",
                    &topic_prg.tenant_id,
                    &sub.client_id,
                    &format!("quota exceeded: {err:?}"),
                );
                continue;
            }
            ctx.metrics
                .record_offline_delta(&topic_prg.tenant_id, 1, entry.payload.len() as i64);
            state.offline.enqueue(entry.clone());
            state.earliest_offline_queue_index = state.offline.earliest_offline_queue_index();
            let eff_qos = match (qos, sub.qos) {
                (Qos::AtMostOnce, _) | (_, Qos::AtMostOnce) => Qos::AtMostOnce,
                (Qos::AtLeastOnce, _) | (_, Qos::AtLeastOnce) => Qos::AtLeastOnce,
                _ => Qos::ExactlyOnce,
            };
            let outbound = crate::workloads::mqtt::session::OutboundMessage {
                client_id: sub.client_id.clone(),
                topic: topic.to_string(),
                payload: payload.to_vec(),
                qos: eff_qos,
                retain,
                enqueue_index,
            };
            if let Some(tx) = state.outbound_tx.clone() {
                if tx.try_send(outbound.clone()).is_err() {
                    tracing::debug!(
                        "outbound queue full for {} sending offline to {:?}",
                        sub.client_id,
                        session_prg
                    );
                }
            }
            ctx.prg_manager
                .record_offline_entry(&session_prg, &sub.client_id, entry)
                .await?;
        }
    }
    Ok(())
}

async fn persisted_session_snapshot<C: Clock + Clone>(
    ctx: &SessionCtx<C>,
    session_key: &str,
    client_id: &str,
    forward_chain_floor: Option<u64>,
    processor: &SessionProcessor<C>,
) -> crate::prg::PersistedSessionState {
    let mut guard = ctx.sessions.lock().await;
    if let Some(state) = guard.get_mut(session_key) {
        let inflight = state
            .inflight
            .keys()
            .map(|mid| crate::workloads::mqtt::session::PersistedInflight { message_id: *mid })
            .collect();
        let outbound = state
            .outbound
            .values()
            .map(|track| crate::workloads::mqtt::session::PersistedOutbound {
                mid: track.mid,
                stage: track.stage,
                msg: track.msg.clone(),
                retry_delay_ms: track.retry_delay.as_millis() as u64,
            })
            .collect();
        state.forward_progress = processor.forward_seq_snapshot();
        let forward_floor = forward_chain_floor.unwrap_or_else(|| {
            state
                .forward_progress
                .iter()
                .map(|f| f.last_seq)
                .max()
                .unwrap_or(0)
        });
        state.dedupe_entries = processor.dedupe_snapshot();
        let last_packet_at = state
            .last_packet_at_wall
            .and_then(|ts| ts.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs());
        return crate::prg::PersistedSessionState {
            client_id: client_id.to_string(),
            dedupe_floor: state.dedupe_floor_index,
            offline_floor: state.offline_floor_index,
            earliest_offline: state.earliest_offline_queue_index,
            forward_chain_floor: forward_floor,
            effective_product_floor: 0,
            keep_alive: state.keep_alive,
            session_expiry_interval: state.session_expiry_interval,
            session_epoch: state.session_epoch,
            inflight,
            outbound,
            dedupe: state.dedupe_entries.clone(),
            forward_seq: state.forward_progress.clone(),
            last_packet_at,
        };
    }
    let forward_seq = processor.forward_seq_snapshot();
    crate::prg::PersistedSessionState {
        client_id: client_id.to_string(),
        dedupe_floor: 0,
        offline_floor: 0,
        earliest_offline: 0,
        forward_chain_floor: forward_chain_floor
            .unwrap_or_else(|| forward_seq.iter().map(|f| f.last_seq).max().unwrap_or(0)),
        effective_product_floor: 0,
        keep_alive: 0,
        session_expiry_interval: None,
        session_epoch: 0,
        inflight: Vec::new(),
        outbound: Vec::new(),
        dedupe: processor.dedupe_snapshot(),
        forward_seq,
        last_packet_at: None,
    }
}

async fn handle_publish<S, C>(
    stream: &mut S,
    ctx: &SessionCtx<C>,
    tenant_id: &str,
    client_id: &str,
    proto: ProtocolVersion,
    publish: crate::mqtt::PublishPacket,
    processor: &mut SessionProcessor<C>,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    C: Clock + Clone + Send + Sync + 'static,
{
    if let Err(err) = ctx.security.authorize_acl(tenant_id, &publish.topic) {
        let reason = internal_from_security(&err);
        write_disconnect(stream, proto, reason, None).await?;
        if matches!(err, SecurityError::QuotaExceeded) {
            ctx.metrics.inc_tenant_throttle();
        }
        return Err(anyhow!("acl failure: {err}"));
    }
    let session_key = format!("{tenant_id}:{client_id}");
    let session_prg = ctx.prg_manager.session_prg(tenant_id, client_id);
    let topic_prg = ctx.prg_manager.topic_prg(tenant_id, &publish.topic);
    let is_cross = session_prg != topic_prg;
    if let Err(err) = ctx.prg_manager.validate_prg_epoch(&session_prg).await {
        let reason = internal_from_control_plane(&err);
        write_disconnect(stream, proto, reason, None).await?;
        return Err(anyhow!("routing epoch mismatch"));
    }
    if let Some(mid) = publish.message_id {
        let mut guard = ctx.sessions.lock().await;
        if let Some(state) = guard.get_mut(&session_key) {
            let dup = processor.record_dedupe(state, mid, crate::dedupe::Direction::Publish, 0);
            processor.record_inflight(state, mid);
            state.status = SessionStatus::Connected;
            if dup {
                // Duplicate MID: acknowledge without reprocessing.
                match publish.qos {
                    Qos::AtLeastOnce => {
                        write_puback(stream, proto, mid, InternalCode::Success).await?;
                    }
                    Qos::ExactlyOnce => {
                        write_pubrec(stream, proto, mid, InternalCode::Success).await?;
                    }
                    Qos::AtMostOnce => {}
                }
                return Ok(());
            }
        }
    }
    let snapshot = ctx.metrics.snapshot();
    // In embedded single-node mode, skip replication lag checks since there's no
    // multi-node replication happening. Only check queue depth for backpressure.
    let is_embedded = ctx.control_plane.is_embedded();
    let high_lag = if is_embedded {
        false // Skip lag-based drops in embedded mode
    } else {
        snapshot.apply_lag_seconds > 2 || snapshot.replication_lag_seconds > 3
    };
    if publish.qos == Qos::AtMostOnce
        && (snapshot.queue_depth_delivery >= ctx.thresholds.apply_to_delivery_drop_qos0 || high_lag)
    {
        ctx.metrics.inc_reject_overload();
        audit::emit(
            "qos0_dropped",
            tenant_id,
            client_id,
            &format!(
                "backlog drop depth={} apply_lag={} repl_lag={}",
                snapshot.queue_depth_delivery,
                snapshot.apply_lag_seconds,
                snapshot.replication_lag_seconds
            ),
        );
        return Ok(());
    }
    let backpressure_triggered = if is_embedded {
        // Embedded mode: only check queue depth, not replication lag
        snapshot.queue_depth_apply >= ctx.thresholds.commit_to_apply_pause_ack
    } else {
        // Clustered mode: full backpressure checks
        snapshot.queue_depth_apply >= ctx.thresholds.commit_to_apply_pause_ack
            || snapshot.apply_lag_seconds > 5
            || snapshot.replication_lag_seconds > 5
    };
    if (publish.qos == Qos::AtLeastOnce || publish.qos == Qos::ExactlyOnce)
        && backpressure_triggered
    {
        ctx.metrics.inc_reject_overload();
        write_disconnect(stream, proto, InternalCode::TransientBackpressure, None).await?;
        return Err(anyhow!(
            "backpressure gating QoS1/2 publish lag={} repl_lag={} embedded={}",
            snapshot.apply_lag_seconds,
            snapshot.replication_lag_seconds,
            is_embedded
        ));
    }
    ctx.security.record_publish(tenant_id).map_err(|e| {
        if let SecurityError::QuotaExceeded = e {
            ctx.metrics.inc_tenant_throttle();
        }
        anyhow!(e.to_string())
    })?;
    tracing::info!(
        "MQTT PUBLISH received tenant={} client_id={} topic={} qos={:?} retain={} payload_len={}",
        tenant_id,
        client_id,
        publish.topic,
        publish.qos,
        publish.retain,
        publish.payload.len()
    );
    let routing_epoch = ctx.prg_manager.routing_epoch();
    if is_cross && ctx.control_plane.rebalance_grace_active() {
        ctx.metrics
            .add_forward_backpressure(snapshot.replication_lag_seconds);
    }
    let receipt = match processor
        .forward_publish(
            &ctx.prg_manager,
            &session_prg,
            &topic_prg,
            routing_epoch,
            &publish.topic,
            &publish.payload,
            publish.qos,
            publish.retain,
        )
        .await
    {
        Ok(r) => r,
        Err(ForwardError::Control(err)) => {
            if is_cross {
                ctx.metrics.record_forward_failure();
            }
            let reason = internal_from_control_plane(&err);
            write_disconnect(stream, proto, reason, None).await?;
            return Err(anyhow!("control-plane error: {err:?}"));
        }
        Err(ForwardError::Routing(err)) => {
            if is_cross {
                ctx.metrics.record_forward_failure();
            }
            let reason = internal_from_routing(&err);
            write_disconnect(stream, proto, reason, None).await?;
            return Err(anyhow!("routing error: {err:?}"));
        }
    };
    if let Err(err) = ctx
        .prg_manager
        .wait_for_prg_floor(&topic_prg, receipt.ack_index)
        .await
    {
        write_disconnect(stream, proto, InternalCode::PermanentDurability, None).await?;
        return Err(anyhow!("durability wait failed: {err:?}"));
    }
    if receipt.applied && !receipt.duplicate {
        if let Err(err) = queue_subscribers_offline(
            ctx,
            &topic_prg,
            &publish.topic,
            &publish.payload,
            publish.qos,
            publish.retain,
        )
        .await
        {
            let reason = internal_from_control_plane(&err);
            write_disconnect(stream, proto, reason, None).await?;
            return Err(anyhow!("offline enqueue failed: {err:?}"));
        }
    }
    let persisted_session =
        persisted_session_snapshot(ctx, &session_key, client_id, None, processor).await;
    let effective_floor = match ctx
        .prg_manager
        .persist_session_state(&session_prg, persisted_session)
        .await
    {
        Ok(floor) => floor,
        Err(err) => {
            let reason = internal_from_control_plane(&err);
            write_disconnect(stream, proto, reason, None).await?;
            return Err(anyhow!("control-plane error: {err:?}"));
        }
    };
    if let Some(mid) = publish.message_id {
        let mut guard = ctx.sessions.lock().await;
        if let Some(state) = guard.get_mut(&session_key) {
            processor.update_publish_dedupe_floor(state, mid, receipt.ack_index);
        }
    }
    let receive_max = receive_max_hint(&ctx.metrics, processor, &ctx.thresholds);
    ctx.metrics.record_leader_credit_hint(receive_max as u64);
    if effective_floor > 0 {
        ctx.metrics.set_apply_lag(0);
    }
    tracing::debug!(
        "receive_max hint tenant={} client_id={} value={}",
        tenant_id,
        client_id,
        receive_max
    );
    match publish.qos {
        Qos::AtMostOnce => {
            tracing::info!(
                "MQTT PUBLISH committed tenant={} client_id={} topic={} qos=0",
                tenant_id,
                client_id,
                publish.topic
            );
        }
        Qos::AtLeastOnce => {
            let mid = publish
                .message_id
                .ok_or_else(|| anyhow!("qos1 publish missing message id"))?;
            write_puback(stream, proto, mid, InternalCode::Success).await?;
            let mut guard = ctx.sessions.lock().await;
            if let Some(state) = guard.get_mut(&session_key) {
                state.inflight.remove(&mid);
            }
            tracing::info!(
                "MQTT PUBLISH committed tenant={} client_id={} topic={} qos=1 mid={}",
                tenant_id,
                client_id,
                publish.topic,
                mid
            );
        }
        Qos::ExactlyOnce => {
            let mid = publish
                .message_id
                .ok_or_else(|| anyhow!("qos2 publish missing message id"))?;
            write_pubrec(stream, proto, mid, InternalCode::Success).await?;
            tracing::info!(
                "MQTT PUBLISH pubrec sent tenant={} client_id={} topic={} qos=2 mid={}",
                tenant_id,
                client_id,
                publish.topic,
                mid
            );
        }
    }
    Ok(())
}

async fn handle_subscribe<S, C>(
    stream: &mut S,
    ctx: &SessionCtx<C>,
    tenant_id: &str,
    client_id: &str,
    proto: ProtocolVersion,
    sub: crate::mqtt::SubscribePacket,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    C: Clock + Clone + Send + Sync + 'static,
{
    let mut grants = Vec::new();
    for filter in &sub.filters {
        let (topic_filter, shared_group) = parse_shared_filter(&filter.topic_filter);
        let verdict = ctx.security.authorize_acl(tenant_id, &topic_filter);
        let grant = match verdict {
            Ok(_) => {
                let prg = ctx.prg_manager.topic_prg(tenant_id, &topic_filter);
                match ctx
                    .prg_manager
                    .add_subscription(
                        &prg,
                        &topic_filter,
                        filter.qos,
                        client_id,
                        shared_group.clone(),
                    )
                    .await
                {
                    Ok(_) => {}
                    Err(err) => {
                        tracing::warn!("subscribe rejected due to placement {:?}", err);
                        grants.push(SubAckResult::Error(internal_from_control_plane(&err)));
                        continue;
                    }
                }
                match ctx.prg_manager.retained(&prg, &topic_filter).await {
                    Ok(Some(retained)) => {
                        let _ = write_publish(
                            stream,
                            proto,
                            &filter.topic_filter,
                            &retained,
                            Qos::AtMostOnce,
                            true,
                            None,
                        )
                        .await;
                    }
                    Ok(None) => {}
                    Err(err) => {
                        grants.push(SubAckResult::Error(internal_from_control_plane(&err)));
                        continue;
                    }
                }
                SubAckResult::Granted(filter.qos)
            }
            Err(err) => {
                let reason = internal_from_security(&err);
                SubAckResult::Error(reason)
            }
        };
        grants.push(grant);
    }
    write_suback(stream, proto, sub.packet_id, &grants).await
}

async fn handle_unsubscribe<S, C>(
    stream: &mut S,
    ctx: &SessionCtx<C>,
    tenant_id: &str,
    client_id: &str,
    proto: ProtocolVersion,
    unsub: crate::mqtt::UnsubscribePacket,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    C: Clock + Clone + Send + Sync + 'static,
{
    for topic in &unsub.topics {
        let (topic_filter, shared_group) = parse_shared_filter(topic);
        let _ = ctx.security.authorize_acl(tenant_id, &topic_filter);
        let prg = ctx.prg_manager.topic_prg(tenant_id, &topic_filter);
        if let Err(err) = ctx
            .prg_manager
            .remove_subscription(&prg, &topic_filter, client_id, shared_group.clone())
            .await
        {
            tracing::warn!("unsubscribe blocked for {:?}: {err:?}", prg);
            return Ok(());
        }
    }
    write_unsuback(stream, proto, unsub.packet_id, InternalCode::Success).await
}

fn receive_max_hint<C: Clock + Clone>(
    metrics: &BackpressureState,
    processor: &mut SessionProcessor<C>,
    thresholds: &QueueThresholds,
) -> u16 {
    let snap = metrics.snapshot();
    let lag = snap.replication_lag_seconds.max(snap.apply_lag_seconds);
    let mut headroom =
        if lag > 5 || snap.queue_depth_delivery >= thresholds.apply_to_delivery_drop_qos0 / 2 {
            0.4
        } else if lag > 2 {
            0.6
        } else {
            1.0
        };
    if snap.forward_backpressure_seconds > 0 {
        headroom *= 0.5;
    }
    if snap.forward_latency_ms > 0 {
        headroom *= if snap.forward_latency_ms > 500 {
            0.4
        } else if snap.forward_latency_ms > 200 {
            0.6
        } else {
            0.8
        };
    }
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    if snap.last_forward_failure_epoch > 0
        && now.saturating_sub(snap.last_forward_failure_epoch) < 10
    {
        headroom *= 0.5;
    } else if snap.forward_failures_total > 0 {
        headroom *= 0.8;
    }
    let mut hint = processor.receive_max_hint(headroom);
    if snap.leader_credit_hint > 0 {
        hint = hint.min(snap.leader_credit_hint as u16).max(1);
    }
    hint
}

fn parse_shared_filter(filter: &str) -> (String, Option<String>) {
    let prefix = "$share/";
    if filter.starts_with(prefix) {
        if let Some(rest) = filter.strip_prefix(prefix) {
            let mut parts = rest.splitn(2, '/');
            if let (Some(group), Some(topic)) = (parts.next(), parts.next()) {
                return (topic.to_string(), Some(group.to_string()));
            }
        }
    }
    (filter.to_string(), None)
}

fn internal_from_security(err: &SecurityError) -> InternalCode {
    match err {
        SecurityError::StaleControlPlane => InternalCode::PermanentDurability,
        SecurityError::TenantMismatch => InternalCode::TokenBindingMismatch,
        SecurityError::QuotaExceeded => InternalCode::OverQuota,
        SecurityError::Unauthorized => InternalCode::Unauthorized,
        SecurityError::KmsKeyConflict => InternalCode::TokenBindingMismatch,
        SecurityError::InvalidToken => InternalCode::Unauthorized,
        SecurityError::MtlsBindingMismatch => InternalCode::TokenBindingMismatch,
    }
}

#[allow(dead_code)]
fn internal_from_routing(err: &RoutingError) -> InternalCode {
    InternalCode::from(err)
}

fn internal_from_control_plane(err: &ControlPlaneError) -> InternalCode {
    InternalCode::from(err)
}

fn internal_from_error(err: &anyhow::Error) -> InternalCode {
    if let Some(sec) = err.downcast_ref::<SecurityError>() {
        return internal_from_security(sec);
    }
    InternalCode::ProtocolError
}

async fn register_or_resume<C: Clock + Clone>(
    ctx: &SessionCtx<C>,
    tenant_id: &str,
    client_id: &str,
    clean_start: bool,
    session_prg: &crate::routing::PrgId,
) -> Result<(bool, u64), ControlPlaneError> {
    let key = format!("{tenant_id}:{client_id}");
    let mut guard = ctx.sessions.lock().await;
    let mut existed = guard.contains_key(&key);
    let entry = guard.entry(key.clone()).or_insert_with(|| SessionState {
        client_id: client_id.to_string(),
        tenant_id: tenant_id.to_string(),
        session_epoch: 0,
        inflight: HashMap::new(),
        dedupe_floor_index: 0,
        offline_floor_index: 0,
        status: crate::workloads::mqtt::session::SessionStatus::Connected,
        earliest_offline_queue_index: 0,
        offline: crate::offline::OfflineQueue::default(),
        keep_alive: 0,
        session_expiry_interval: None,
        last_packet_at: Some(ctx.clock.now()),
        last_packet_at_wall: Some(std::time::SystemTime::now()),
        will: None,
        outbound: HashMap::new(),
        outbound_tx: None,
        dedupe_entries: Vec::new(),
        forward_progress: Vec::new(),
    });
    let mut purge = false;
    if let Some(expiry) = entry.session_expiry_interval {
        if let Some(last) = entry.last_packet_at {
            if ctx.clock.now().saturating_duration_since(last).as_secs() >= expiry as u64 {
                entry.offline = crate::offline::OfflineQueue::default();
                entry.earliest_offline_queue_index = 0;
                entry.dedupe_floor_index = 0;
                entry.offline_floor_index = 0;
                entry.outbound.clear();
                entry.session_epoch = entry.session_epoch.saturating_add(1);
                entry.dedupe_entries.clear();
                entry.forward_progress.clear();
                purge = true;
                existed = false;
            }
        }
    }
    // Track whether we need to clear the forwarding engine's sequence tracking
    let clear_forward_state = !existed || clean_start;
    if clean_start && existed {
        entry.session_epoch = entry.session_epoch.saturating_add(1);
        entry.inflight.clear();
        entry.outbound.clear();
        entry.offline = crate::offline::OfflineQueue::default();
        entry.earliest_offline_queue_index = 0;
        entry.dedupe_floor_index = 0;
        entry.offline_floor_index = 0;
        entry.dedupe_entries.clear();
        entry.forward_progress.clear();
        purge = true;
        existed = false;
    }
    // Clear server-side forward sequence tracking when session starts fresh
    if clear_forward_state {
        ctx.prg_manager.clear_session_forward_state(session_prg);
    }
    let session_epoch = entry.session_epoch;
    if !clean_start {
        if let Some((persisted, offline)) = ctx
            .prg_manager
            .session_snapshot(session_prg, client_id)
            .await?
        {
            entry.dedupe_floor_index = persisted.dedupe_floor;
            entry.offline_floor_index = persisted.offline_floor;
            entry.earliest_offline_queue_index = persisted.earliest_offline;
            entry.keep_alive = persisted.keep_alive;
            entry.session_expiry_interval = persisted.session_expiry_interval;
            entry.session_epoch = persisted.session_epoch;
            entry.dedupe_entries = persisted.dedupe.clone();
            entry.forward_progress = persisted.forward_seq.clone();
            entry.inflight = persisted
                .inflight
                .into_iter()
                .map(|p| {
                    (
                        p.message_id,
                        crate::workloads::mqtt::session::InflightRecord {
                            message_id: p.message_id,
                            created_at: Duration::from_secs(0),
                        },
                    )
                })
                .collect();
            entry.outbound.clear();
            for persisted_out in persisted.outbound {
                let retry_delay = Duration::from_millis(
                    persisted_out
                        .retry_delay_ms
                        .max(OUTBOUND_RETRY_BASE.as_millis() as u64),
                );
                entry.outbound.insert(
                    persisted_out.mid,
                    crate::workloads::mqtt::session::OutboundTracking {
                        msg: persisted_out.msg.clone(),
                        stage: persisted_out.stage,
                        mid: persisted_out.mid,
                        retry_at: ctx.clock.now() + retry_delay,
                        retry_delay,
                    },
                );
            }
            entry.last_packet_at_wall = persisted
                .last_packet_at
                .and_then(|secs| std::time::UNIX_EPOCH.checked_add(Duration::from_secs(secs)));
            if let Some(wall) = entry.last_packet_at_wall {
                if let Ok(elapsed) = std::time::SystemTime::now().duration_since(wall) {
                    entry.last_packet_at = ctx
                        .clock
                        .now()
                        .checked_sub(elapsed)
                        .or_else(|| Some(ctx.clock.now()));
                }
            }
            entry.offline = crate::offline::OfflineQueue::default();
            for off in offline {
                entry.offline.enqueue(off);
            }
        }
    }
    entry.status = crate::workloads::mqtt::session::SessionStatus::Connected;
    entry.last_packet_at = Some(ctx.clock.now());
    entry.last_packet_at_wall = Some(std::time::SystemTime::now());
    drop(guard);
    if purge {
        ctx.prg_manager
            .purge_session(session_prg, client_id)
            .await?;
    }
    Ok((existed, session_epoch))
}
