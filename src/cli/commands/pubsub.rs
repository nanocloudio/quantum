//! Subscribe and Publish command implementations (kcat-style MQTT client).

use crate::cli::args::{OutputFormat, PublishArgs, QosLevel, SubscribeArgs, TlsArgs};
use anyhow::{Context, Result};
use base64::Engine;
use chrono::Utc;
use rumqttc::{
    AsyncClient, ConnectionError, Event, EventLoop, Incoming, MqttOptions, QoS, TlsConfiguration,
    Transport,
};
use rustls::client::{ServerCertVerified, ServerCertVerifier};
use rustls::{Certificate, ClientConfig, Error as TlsError, PrivateKey, RootCertStore, ServerName};
use rustls_pemfile::{certs, ec_private_keys, pkcs8_private_keys, rsa_private_keys};
use serde::Serialize;
use std::fs::File;
use std::io::{BufReader, Cursor, Read as StdRead};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::path::Path;
use std::sync::Arc;
use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncBufReadExt, BufReader as AsyncBufReader};
use tokio::signal::unix::{signal, SignalKind};
use tokio::time::{sleep, Duration};

/// Run the subscribe command - connect to broker and stream messages to stdout.
pub async fn run_subscribe(args: SubscribeArgs) -> Result<()> {
    run_subscribe_async(args).await
}

/// Run the publish command - publish messages from stdin or command line.
pub async fn run_publish(args: PublishArgs) -> Result<()> {
    run_publish_async(args).await
}

/// Wait for shutdown signal (SIGINT or SIGTERM).
async fn shutdown_signal() -> &'static str {
    let mut sigint = signal(SignalKind::interrupt()).expect("register SIGINT handler");
    let mut sigterm = signal(SignalKind::terminate()).expect("register SIGTERM handler");

    tokio::select! {
        _ = sigint.recv() => "SIGINT",
        _ = sigterm.recv() => "SIGTERM",
    }
}

// -----------------------------------------------------------------------------
// Subscribe implementation
// -----------------------------------------------------------------------------

/// Backoff configuration for reconnection attempts.
struct Backoff {
    current_ms: u64,
    max_ms: u64,
}

impl Backoff {
    fn new() -> Self {
        Self {
            current_ms: 100,
            max_ms: 30_000,
        }
    }

    fn next_delay(&mut self) -> Duration {
        let delay = self.current_ms;
        // Exponential backoff with cap
        self.current_ms = (self.current_ms * 2).min(self.max_ms);
        // Add jitter (±25%)
        let jitter = delay / 4;
        let actual = delay + (rand_u64() % (jitter * 2)).saturating_sub(jitter);
        Duration::from_millis(actual)
    }

    fn reset(&mut self) {
        self.current_ms = 100;
    }
}

/// Simple pseudo-random number using time-based seed.
fn rand_u64() -> u64 {
    use std::time::SystemTime;
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1)
}

async fn run_subscribe_async(args: SubscribeArgs) -> Result<()> {
    let (client, mut eventloop) = create_mqtt_client(&args.tls, "quantum-sub")?;

    // Subscribe to all topics
    let qos = args.qos.to_rumqttc();
    for topic in &args.topic {
        client
            .subscribe(topic.clone(), qos)
            .await
            .with_context(|| format!("subscribe to topic '{topic}'"))?;
    }
    eprintln!(
        "subscribed to {} topic(s): {}",
        args.topic.len(),
        args.topic.join(", ")
    );

    let mut disconnecting = false;
    let mut backoff = Backoff::new();
    let mut connected = false;

    loop {
        tokio::select! {
            biased;
            sig = shutdown_signal(), if !disconnecting => {
                eprintln!("received {sig}, shutting down...");
                disconnecting = true;
                let _ = client.disconnect().await;
            }
            res = eventloop.poll() => {
                match res {
                    Ok(Event::Incoming(Incoming::ConnAck(ack))) => {
                        if !connected {
                            eprintln!("connected to {}:{}", args.tls.host, args.tls.port);
                            connected = true;
                        } else {
                            eprintln!("reconnected to {}:{}", args.tls.host, args.tls.port);
                        }
                        backoff.reset();
                        if ack.code != rumqttc::ConnectReturnCode::Success {
                            return Err(anyhow::anyhow!("connection rejected: {:?}", ack.code));
                        }
                    }
                    Ok(Event::Incoming(Incoming::SubAck(_))) => {
                        // Subscription confirmed
                    }
                    Ok(Event::Incoming(Incoming::Publish(publish))) => {
                        output_message(&args.format, &publish.topic, &publish.payload);
                    }
                    Ok(_) => {}
                    Err(ConnectionError::ConnectionRefused(code)) => {
                        eprintln!("connection refused: {code:?}");
                        return Err(anyhow::anyhow!("connection refused: {code:?}"));
                    }
                    Err(ConnectionError::Io(ref io_err))
                        if io_err.kind() == std::io::ErrorKind::UnexpectedEof =>
                    {
                        if disconnecting {
                            break;
                        }
                        connected = false;
                        let delay = backoff.next_delay();
                        eprintln!("connection lost; reconnecting in {}ms...", delay.as_millis());
                        sleep(delay).await;
                    }
                    Err(ConnectionError::Tls(ref tls_err)) => {
                        return Err(anyhow::anyhow!("TLS error: {tls_err}"));
                    }
                    Err(err) => {
                        if disconnecting {
                            break;
                        }
                        connected = false;
                        let delay = backoff.next_delay();
                        eprintln!("connection error: {err:?}; reconnecting in {}ms...", delay.as_millis());
                        sleep(delay).await;
                    }
                }
                if disconnecting {
                    break;
                }
            }
        }
    }

    Ok(())
}

#[derive(Serialize)]
struct MessageOutput<'a> {
    ts: String,
    topic: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload_b64: Option<String>,
}

fn output_message(format: &OutputFormat, topic: &str, payload: &[u8]) {
    match format {
        OutputFormat::Json => {
            let (payload_str, payload_b64) = match std::str::from_utf8(payload) {
                Ok(s) => (Some(s), None),
                Err(_) => (
                    None,
                    Some(base64::engine::general_purpose::STANDARD.encode(payload)),
                ),
            };
            let msg = MessageOutput {
                ts: Utc::now().to_rfc3339(),
                topic,
                payload: payload_str,
                payload_b64,
            };
            if let Ok(json) = serde_json::to_string(&msg) {
                println!("{json}");
            }
        }
        OutputFormat::Raw => {
            if let Ok(text) = std::str::from_utf8(payload) {
                println!("{text}");
            } else {
                // For binary data in raw mode, write to stdout directly
                use std::io::Write;
                let _ = std::io::stdout().write_all(payload);
                let _ = std::io::stdout().write_all(b"\n");
            }
        }
    }
}

// -----------------------------------------------------------------------------
// Publish implementation
// -----------------------------------------------------------------------------

async fn run_publish_async(args: PublishArgs) -> Result<()> {
    let (client, eventloop) = create_mqtt_client(&args.tls, "quantum-pub")?;
    let qos = args.qos.to_rumqttc();

    if let Some(ref topic) = args.topic {
        // Single-topic mode
        let (payload, source) = if let Some(ref msg) = args.message {
            (msg.as_bytes().to_vec(), "argument")
        } else {
            // Read entire stdin as payload (supports binary data)
            let mut buf = Vec::new();
            std::io::stdin()
                .read_to_end(&mut buf)
                .context("read stdin")?;
            (buf, "stdin")
        };

        eprintln!(
            "publishing {} bytes from {} to '{}' (qos={}, retain={})",
            payload.len(),
            source,
            topic,
            qos_name(qos),
            args.retain
        );

        publish_single(
            client,
            eventloop,
            &args.tls,
            topic,
            payload,
            qos,
            args.retain,
        )
        .await
    } else {
        // Multi-topic stdin mode
        publish_multi_stdin(
            client,
            eventloop,
            &args.tls,
            &args.delimiter,
            qos,
            args.retain,
            args.binary,
        )
        .await
    }
}

fn qos_name(qos: QoS) -> &'static str {
    match qos {
        QoS::AtMostOnce => "0",
        QoS::AtLeastOnce => "1",
        QoS::ExactlyOnce => "2",
    }
}

async fn publish_single(
    client: AsyncClient,
    mut eventloop: EventLoop,
    tls: &TlsArgs,
    topic: &str,
    payload: Vec<u8>,
    qos: QoS,
    retain: bool,
) -> Result<()> {
    client
        .publish(topic, qos, retain, payload)
        .await
        .context("queue publish")?;

    // Drive the event loop until we get the ack (for QoS > 0) or the publish is sent
    let mut published = false;
    let mut disconnecting = false;

    loop {
        tokio::select! {
            biased;
            sig = shutdown_signal(), if !disconnecting => {
                eprintln!("received {sig}, aborting...");
                disconnecting = true;
                let _ = client.disconnect().await;
            }
            res = eventloop.poll() => {
                match res {
                    Ok(Event::Incoming(Incoming::ConnAck(ack))) => {
                        if ack.code != rumqttc::ConnectReturnCode::Success {
                            return Err(anyhow::anyhow!("connection rejected: {:?}", ack.code));
                        }
                        eprintln!("connected to {}:{}", tls.host, tls.port);
                    }
                    Ok(Event::Incoming(Incoming::PubAck(_))) => {
                        eprintln!("published (ack received)");
                        published = true;
                        let _ = client.disconnect().await;
                        disconnecting = true;
                    }
                    Ok(Event::Incoming(Incoming::PubComp(_))) => {
                        eprintln!("published (qos2 complete)");
                        published = true;
                        let _ = client.disconnect().await;
                        disconnecting = true;
                    }
                    Ok(Event::Outgoing(rumqttc::Outgoing::Publish(_))) if qos == QoS::AtMostOnce => {
                        eprintln!("published (qos0 sent)");
                        published = true;
                        let _ = client.disconnect().await;
                        disconnecting = true;
                    }
                    Ok(_) => {}
                    Err(ConnectionError::ConnectionRefused(code)) => {
                        return Err(anyhow::anyhow!("connection refused: {code:?}"));
                    }
                    Err(ConnectionError::Tls(ref tls_err)) => {
                        return Err(anyhow::anyhow!("TLS error: {tls_err}"));
                    }
                    Err(ConnectionError::Io(ref io_err))
                        if io_err.kind() == std::io::ErrorKind::UnexpectedEof && disconnecting =>
                    {
                        break;
                    }
                    Err(err) => {
                        if disconnecting {
                            break;
                        }
                        return Err(anyhow::anyhow!("publish failed: {err}"));
                    }
                }
                if disconnecting {
                    // Give a moment for disconnect to complete
                    sleep(Duration::from_millis(50)).await;
                    break;
                }
            }
        }
    }

    if !published && !disconnecting {
        anyhow::bail!("publish was not acknowledged");
    }
    Ok(())
}

async fn publish_multi_stdin(
    client: AsyncClient,
    mut eventloop: EventLoop,
    tls: &TlsArgs,
    delimiter: &str,
    qos: QoS,
    retain: bool,
    binary: bool,
) -> Result<()> {
    eprintln!(
        "reading stdin lines with delimiter '{}' (qos={}, retain={}, binary={})",
        if delimiter == "\t" { "\\t" } else { delimiter },
        qos_name(qos),
        retain,
        binary
    );

    // Set stdin to non-blocking for truly async I/O
    let stdin_fd = std::io::stdin().as_raw_fd();
    set_nonblocking(stdin_fd)?;
    // Safety: we're duplicating stdin's fd to get an owned handle
    let owned_fd = unsafe { OwnedFd::from_raw_fd(libc::dup(stdin_fd)) };
    let async_stdin = AsyncFd::new(owned_fd).context("create async stdin")?;
    let mut reader = AsyncBufReader::new(AsyncStdin(async_stdin)).lines();

    let mut disconnecting = false;
    let mut pending_acks: usize = 0;
    let mut published_count: usize = 0;
    let mut eof_reached = false;

    loop {
        tokio::select! {
            biased;
            sig = shutdown_signal(), if !disconnecting => {
                eprintln!("received {sig}, flushing {} pending messages...", pending_acks);
                disconnecting = true;
                // Don't disconnect yet - wait for pending acks
                if pending_acks == 0 {
                    let _ = client.disconnect().await;
                }
            }
            result = reader.next_line(), if !disconnecting && !eof_reached => {
                match result {
                    Ok(Some(line)) => {
                        if let Some((topic, payload_str)) = line.split_once(delimiter) {
                            if topic.is_empty() {
                                eprintln!("skipping line with empty topic");
                                continue;
                            }
                            // Decode payload: base64 if --binary, otherwise raw bytes
                            let payload = if binary {
                                match base64::engine::general_purpose::STANDARD.decode(payload_str.trim()) {
                                    Ok(bytes) => bytes,
                                    Err(e) => {
                                        eprintln!("skipping line with invalid base64: {e}");
                                        continue;
                                    }
                                }
                            } else {
                                payload_str.as_bytes().to_vec()
                            };
                            if let Err(e) = client
                                .publish(topic.to_string(), qos, retain, payload)
                                .await
                            {
                                eprintln!("failed to queue publish: {e}");
                            } else {
                                pending_acks += 1;
                            }
                        } else {
                            eprintln!("skipping malformed line (no delimiter): {}", truncate_line(&line, 60));
                        }
                    }
                    Ok(None) => {
                        // EOF reached
                        eof_reached = true;
                        eprintln!("EOF reached; waiting for {} pending acks...", pending_acks);
                        if pending_acks == 0 {
                            disconnecting = true;
                            let _ = client.disconnect().await;
                        }
                    }
                    Err(e) => {
                        eprintln!("stdin error: {e}");
                        eof_reached = true;
                        if pending_acks == 0 {
                            disconnecting = true;
                            let _ = client.disconnect().await;
                        }
                    }
                }
            }
            res = eventloop.poll() => {
                match res {
                    Ok(Event::Incoming(Incoming::ConnAck(ack))) => {
                        if ack.code != rumqttc::ConnectReturnCode::Success {
                            return Err(anyhow::anyhow!("connection rejected: {:?}", ack.code));
                        }
                        eprintln!("connected to {}:{}", tls.host, tls.port);
                    }
                    Ok(Event::Incoming(Incoming::PubAck(_))) |
                    Ok(Event::Incoming(Incoming::PubComp(_))) => {
                        pending_acks = pending_acks.saturating_sub(1);
                        published_count += 1;
                        if (eof_reached || disconnecting) && pending_acks == 0 {
                            disconnecting = true;
                            let _ = client.disconnect().await;
                        }
                    }
                    Ok(Event::Outgoing(rumqttc::Outgoing::Publish(_))) if qos == QoS::AtMostOnce => {
                        pending_acks = pending_acks.saturating_sub(1);
                        published_count += 1;
                        if (eof_reached || disconnecting) && pending_acks == 0 {
                            disconnecting = true;
                            let _ = client.disconnect().await;
                        }
                    }
                    Ok(_) => {}
                    Err(ConnectionError::ConnectionRefused(code)) => {
                        return Err(anyhow::anyhow!("connection refused: {code:?}"));
                    }
                    Err(ConnectionError::Tls(ref tls_err)) => {
                        return Err(anyhow::anyhow!("TLS error: {tls_err}"));
                    }
                    Err(ConnectionError::Io(ref io_err))
                        if io_err.kind() == std::io::ErrorKind::UnexpectedEof =>
                    {
                        if disconnecting {
                            break;
                        }
                        eprintln!("connection lost; reconnecting...");
                        sleep(Duration::from_millis(500)).await;
                    }
                    Err(err) => {
                        if disconnecting {
                            break;
                        }
                        return Err(anyhow::anyhow!("connection error: {err}"));
                    }
                }
                if disconnecting && pending_acks == 0 {
                    break;
                }
            }
        }
    }

    eprintln!("published {} message(s)", published_count);
    Ok(())
}

fn truncate_line(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len])
    }
}

// -----------------------------------------------------------------------------
// TLS and MQTT client factory
// -----------------------------------------------------------------------------

fn create_mqtt_client(
    tls_args: &TlsArgs,
    default_prefix: &str,
) -> Result<(AsyncClient, EventLoop)> {
    let tls_config = build_tls_config(&tls_args.cert, &tls_args.key, &tls_args.ca)?;

    let client_id = tls_args.client_id.clone().unwrap_or_else(|| {
        format!(
            "{}-{}",
            default_prefix,
            uuid::Uuid::new_v4()
                .to_string()
                .split('-')
                .next()
                .unwrap_or("xxxx")
        )
    });

    let mut mqtt = MqttOptions::new(client_id, &tls_args.host, tls_args.port);
    mqtt.set_keep_alive(std::time::Duration::from_secs(30));
    mqtt.set_transport(Transport::tls_with_config(TlsConfiguration::Rustls(
        Arc::new(tls_config),
    )));

    let (client, eventloop) = AsyncClient::new(mqtt, 64);
    Ok((client, eventloop))
}

fn build_tls_config(cert_path: &Path, key_path: &Path, ca_path: &Path) -> Result<ClientConfig> {
    // Load CA certificates
    let mut root_store = RootCertStore::empty();
    let ca_reader = &mut BufReader::new(
        File::open(ca_path).with_context(|| format!("open CA cert: {}", ca_path.display()))?,
    );
    let ca_der = certs(ca_reader).context("parse CA certificates")?;
    let ca_certs: Vec<Certificate> = ca_der.into_iter().map(Certificate).collect();
    let (added, _) = root_store.add_parsable_certificates(&ca_certs);
    if added == 0 {
        anyhow::bail!("no CA certificates loaded from {}", ca_path.display());
    }

    // Load client certificate chain
    let chain_reader = &mut BufReader::new(
        File::open(cert_path)
            .with_context(|| format!("open client cert: {}", cert_path.display()))?,
    );
    let chain = certs(chain_reader).context("parse client certificate chain")?;
    if chain.is_empty() {
        anyhow::bail!("no certificates found in {}", cert_path.display());
    }
    let chain: Vec<Certificate> = chain.into_iter().map(Certificate).collect();

    // Load private key
    let key = load_private_key(key_path)?;

    // Build TLS config
    let mut config = ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(root_store)
        .with_client_auth_cert(chain, key)
        .context("build TLS client config")?;

    config
        .dangerous()
        .set_certificate_verifier(Arc::new(NoHostnameVerifier));
    config.alpn_protocols.push(b"mqtt".to_vec());

    Ok(config)
}

fn load_private_key(path: &Path) -> Result<PrivateKey> {
    let bytes =
        std::fs::read(path).with_context(|| format!("read key file: {}", path.display()))?;
    let mut cursor = Cursor::new(&bytes);

    // Try PKCS#8 first
    if let Some(key) = pkcs8_private_keys(&mut cursor)
        .context("parse PKCS#8 private key")?
        .into_iter()
        .next()
    {
        return Ok(PrivateKey(key));
    }

    // Try RSA
    cursor.set_position(0);
    if let Some(key) = rsa_private_keys(&mut cursor)
        .context("parse RSA private key")?
        .into_iter()
        .next()
    {
        return Ok(PrivateKey(key));
    }

    // Try EC (SEC1)
    cursor.set_position(0);
    if let Some(key) = ec_private_keys(&mut cursor)
        .context("parse EC private key")?
        .into_iter()
        .next()
    {
        return Ok(PrivateKey(key));
    }

    anyhow::bail!("no supported private key found in {}", path.display());
}

struct NoHostnameVerifier;

impl ServerCertVerifier for NoHostnameVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &Certificate,
        _intermediates: &[Certificate],
        _server_name: &ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: std::time::SystemTime,
    ) -> Result<ServerCertVerified, TlsError> {
        Ok(ServerCertVerified::assertion())
    }
}

// -----------------------------------------------------------------------------
// Async stdin helper
// -----------------------------------------------------------------------------

fn set_nonblocking(fd: i32) -> Result<()> {
    // Safety: standard fcntl operations on a valid fd
    unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFL);
        if flags < 0 {
            anyhow::bail!("fcntl F_GETFL failed: {}", std::io::Error::last_os_error());
        }
        if libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) < 0 {
            anyhow::bail!("fcntl F_SETFL failed: {}", std::io::Error::last_os_error());
        }
    }
    Ok(())
}

/// Wrapper around AsyncFd that implements tokio::io::AsyncRead
struct AsyncStdin(AsyncFd<OwnedFd>);

impl tokio::io::AsyncRead for AsyncStdin {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        loop {
            let mut guard = match self.0.poll_read_ready(cx) {
                std::task::Poll::Ready(Ok(guard)) => guard,
                std::task::Poll::Ready(Err(e)) => return std::task::Poll::Ready(Err(e)),
                std::task::Poll::Pending => return std::task::Poll::Pending,
            };

            let fd = self.0.get_ref().as_raw_fd();
            let unfilled = buf.initialize_unfilled();
            // Safety: read into valid buffer with valid fd
            let result = unsafe {
                libc::read(
                    fd,
                    unfilled.as_mut_ptr() as *mut libc::c_void,
                    unfilled.len(),
                )
            };

            if result >= 0 {
                buf.advance(result as usize);
                return std::task::Poll::Ready(Ok(()));
            }

            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::WouldBlock {
                guard.clear_ready();
                continue;
            }
            return std::task::Poll::Ready(Err(err));
        }
    }
}

// -----------------------------------------------------------------------------
// QoS helper
// -----------------------------------------------------------------------------

impl QosLevel {
    fn to_rumqttc(&self) -> QoS {
        match self {
            QosLevel::Qos0 => QoS::AtMostOnce,
            QosLevel::Qos1 => QoS::AtLeastOnce,
            QosLevel::Qos2 => QoS::ExactlyOnce,
        }
    }
}
