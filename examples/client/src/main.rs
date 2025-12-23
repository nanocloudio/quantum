use anyhow::{Context, Result};
use clap::Parser;
use rumqttc::{
    AsyncClient, ConnectionError, Event, Incoming, MqttOptions, QoS, Transport, TlsConfiguration,
};
use rustls::client::{ServerCertVerified, ServerCertVerifier};
use rustls::{Certificate, ClientConfig, PrivateKey, RootCertStore, ServerName, Error as TlsError};
use rustls_pemfile::{certs, ec_private_keys, pkcs8_private_keys, rsa_private_keys};
use std::fs::File;
use std::io::{BufReader, Cursor};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader as AsyncBufReader};
use tokio::io::unix::AsyncFd;
use tokio::signal;
use tokio::time::{sleep, Duration};

#[derive(Parser, Debug)]
#[command(author, version, about = "Simple async MQTT echo client for Quantum")]
struct Args {
    /// Broker hostname or IP
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    /// Broker TLS port
    #[arg(long, default_value_t = 8883)]
    port: u16,
    /// MQTT client identifier
    #[arg(long, default_value = "quantum-echo")]
    client_id: String,
    /// Topic to publish to and subscribe on
    #[arg(long, default_value = "test/echo")]
    topic: String,
    /// PEM file containing the client certificate chain
    #[arg(long, value_name = "PATH")]
    client_chain: PathBuf,
    /// PEM file containing the client private key
    #[arg(long, value_name = "PATH")]
    client_key: PathBuf,
    /// PEM file containing the broker CA bundle
    #[arg(long, value_name = "PATH")]
    ca_cert: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let tls = build_tls_config(&args)?;

    let mut mqtt = MqttOptions::new(args.client_id.clone(), args.host.clone(), args.port);
    mqtt.set_keep_alive(std::time::Duration::from_secs(30));
    mqtt.set_transport(Transport::tls_with_config(TlsConfiguration::Rustls(Arc::new(tls))));

    let (client, mut eventloop) = AsyncClient::new(mqtt, 10);
    client
        .subscribe(args.topic.clone(), QoS::AtLeastOnce)
        .await
        .context("subscribe to echo topic")?;

    let topic = args.topic.clone();
    let shutdown_client = client.clone();

    // Set stdin to non-blocking for truly async I/O
    let stdin_fd = std::io::stdin().as_raw_fd();
    set_nonblocking(stdin_fd)?;
    // Safety: we're duplicating stdin's fd to get an owned handle
    let owned_fd = unsafe { OwnedFd::from_raw_fd(libc::dup(stdin_fd)) };
    let async_stdin = AsyncFd::new(owned_fd).context("create async stdin")?;
    let mut reader = AsyncBufReader::new(AsyncStdin(async_stdin)).lines();

    let mut disconnecting = false;
    loop {
        tokio::select! {
            biased;
            res = signal::ctrl_c(), if !disconnecting => {
                match res {
                    Ok(()) => {
                        eprintln!("shutting down...");
                        disconnecting = true;
                        if let Err(err) = shutdown_client.disconnect().await {
                            eprintln!("failed to send disconnect: {err:?}");
                        }
                    }
                    Err(err) => {
                        eprintln!("ctrl-c signal error: {err:?}");
                        break;
                    }
                }
            }
            result = reader.next_line(), if !disconnecting => {
                match result {
                    Ok(Some(line)) => {
                        let payload = line.into_bytes();
                        client
                            .publish(topic.clone(), QoS::AtLeastOnce, false, payload)
                            .await
                            .context("publish echo line")?;
                    }
                    Ok(None) => break,
                    Err(e) => {
                        eprintln!("stdin error: {e}");
                        break;
                    }
                }
            }
            res = eventloop.poll() => {
                match res {
                    Ok(Event::Incoming(Incoming::Publish(publish))) => {
                        if let Ok(text) = String::from_utf8(publish.payload.to_vec()) {
                            println!("{}", text);
                        } else {
                            println!("<binary payload len={}>", publish.payload.len());
                        }
                    }
                    Ok(_) => {}
                    Err(ConnectionError::ConnectionRefused(code)) => {
                        eprintln!("connection refused: {code:?}; backing off");
                        sleep(Duration::from_millis(200)).await;
                    }
                    Err(ConnectionError::Io(ref io_err))
                        if io_err.kind() == std::io::ErrorKind::UnexpectedEof =>
                    {
                        if disconnecting {
                            break;
                        }
                        eprintln!("connection lost; retrying");
                    }
                    Err(err) => {
                        return Err(err.into());
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
                libc::read(fd, unfilled.as_mut_ptr() as *mut libc::c_void, unfilled.len())
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

fn build_tls_config(args: &Args) -> Result<ClientConfig> {
    let mut root_store = RootCertStore::empty();
    let ca_reader = &mut BufReader::new(File::open(&args.ca_cert).context("open ca cert")?);
    let ca_der = certs(ca_reader).context("parse ca certs")?;
    let ca_certs: Vec<Certificate> = ca_der.into_iter().map(Certificate).collect();
    let (added, _) = root_store.add_parsable_certificates(&ca_certs);
    if added == 0 {
        anyhow::bail!("no CA certificates loaded from {}", args.ca_cert.display());
    }

    let chain_reader =
        &mut BufReader::new(File::open(&args.client_chain).context("open client chain")?);
    let chain = certs(chain_reader).context("parse client chain")?;
    if chain.is_empty() {
        anyhow::bail!(
            "client certificate chain {} did not contain certificates",
            args.client_chain.display()
        );
    }
    let chain: Vec<Certificate> = chain.into_iter().map(Certificate).collect();

    let key = load_private_key(&args.client_key)?;

    let mut config = ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(root_store)
        .with_client_auth_cert(chain, key)
        .context("build tls client config")?;

    config
        .dangerous()
        .set_certificate_verifier(Arc::new(NoHostnameVerifier));
    config.alpn_protocols.push(b"mqtt".to_vec());

    Ok(config)
}

fn load_private_key(path: &PathBuf) -> Result<PrivateKey> {
    let bytes =
        std::fs::read(path).with_context(|| format!("read key file {}", path.display()))?;
    let mut cursor = Cursor::new(&bytes);
    if let Some(key) = pkcs8_private_keys(&mut cursor)
        .context("read pkcs8 private key")?
        .into_iter()
        .next()
    {
        return Ok(PrivateKey(key));
    }
    cursor.set_position(0);
    if let Some(key) = rsa_private_keys(&mut cursor)
        .context("read rsa private key")?
        .into_iter()
        .next()
    {
        return Ok(PrivateKey(key));
    }
    cursor.set_position(0);
    if let Some(key) = ec_private_keys(&mut cursor)
        .context("read sec1 (EC) private key")?
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
