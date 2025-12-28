use super::SessionCtx;
use crate::control::capabilities::ProtocolType;
use crate::time::Clock;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};

pub trait ProtocolStream: AsyncRead + AsyncWrite {}
impl<T: AsyncRead + AsyncWrite> ProtocolStream for T {}

/// Boxed IO stream passed to protocol plugins.
pub type ProtocolIo = Box<dyn ProtocolStream + Unpin + Send>;

#[derive(Debug, Clone)]
pub struct ProtocolDescriptor {
    pub name: &'static str,
    pub protocol: ProtocolType,
    pub default_port: u16,
    pub workload_label: &'static str,
    pub workload_version: u16,
    pub requires_sni: bool,
    pub supported_alpns: &'static [&'static str],
    pub rate_limit_classes: &'static [&'static str],
    pub requires_capabilities: &'static [&'static str],
}

#[derive(Debug, Clone, Copy)]
pub enum ProtocolTransport {
    Tcp,
    Quic,
}

#[derive(Debug, Clone)]
pub struct ProtocolSession {
    pub tenant_id: String,
    pub sni: Option<String>,
    pub alpn: Option<String>,
    pub transport: ProtocolTransport,
    pub peer_addr: Option<std::net::SocketAddr>,
    pub capability_epoch: Option<u64>,
}

#[derive(Debug, Clone)]
pub enum ProtocolError {
    AdmissionDenied,
    CapabilityDisabled,
    UnsupportedProtocol(String),
    FaultInjected(String),
    IoError(String),
    Internal(String),
}

impl From<anyhow::Error> for ProtocolError {
    fn from(err: anyhow::Error) -> Self {
        ProtocolError::Internal(err.to_string())
    }
}

pub(crate) trait ProtocolPlugin<C>: Send + Sync
where
    C: Clock + Clone + Send + Sync + 'static,
{
    fn descriptor(&self) -> &ProtocolDescriptor;
    fn alpns(&self) -> &[&'static str];
    fn handle_connection(
        &self,
        stream: ProtocolIo,
        session: ProtocolSession,
        ctx: SessionCtx<C>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ProtocolError>> + Send + '_>>;
}

pub(crate) struct ProtocolRegistry<C>
where
    C: Clock + Clone + Send + Sync + 'static,
{
    plugins: RwLock<HashMap<String, Arc<dyn ProtocolPlugin<C>>>>,
    alpns: RwLock<HashMap<String, String>>,
}

impl<C> Default for ProtocolRegistry<C>
where
    C: Clock + Clone + Send + Sync + 'static,
{
    fn default() -> Self {
        Self {
            plugins: RwLock::new(HashMap::new()),
            alpns: RwLock::new(HashMap::new()),
        }
    }
}

impl<C> ProtocolRegistry<C>
where
    C: Clock + Clone + Send + Sync + 'static,
{
    pub fn register(&self, plugin: Arc<dyn ProtocolPlugin<C>>) {
        let name = plugin.descriptor().name.to_string();
        {
            let mut guard = self.plugins.write();
            guard.insert(name.clone(), plugin.clone());
        }
        let mut alpns = self.alpns.write();
        for alpn in plugin.alpns() {
            alpns.insert(alpn.to_string(), name.clone());
        }
    }

    pub fn plugin_for_alpn(&self, alpn: &str) -> Option<Arc<dyn ProtocolPlugin<C>>> {
        let name = {
            let alpns = self.alpns.read();
            alpns.get(alpn).cloned()
        }?;
        self.plugins.read().get(&name).cloned()
    }
}
