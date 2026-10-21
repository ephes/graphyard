//! Connections as httpcore opens them (PLAN.md 6.1): `socket.create_connection`
//! (one `getaddrinfo`, then every address in turn, each bounded by the
//! timeout), the TLS handshake bounded by the timeout, and every later
//! socket read and write bounded separately. A response that arrives in slow
//! pieces is therefore fine as long as no single read waits longer than the
//! timeout; only the total deadline stops it.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::WebPkiSupportedAlgorithms;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::time::Sleep;

use super::error::{ErrorKind, HttpError, os_error_text};

/// A socket whose every read and write is bounded by the timeout, like
/// httpcore's per-operation `read`/`write` timeouts.
pub(crate) struct TimeoutIo<S> {
    inner: S,
    timeout: Duration,
    read_timer: Option<Pin<Box<Sleep>>>,
    write_timer: Option<Pin<Box<Sleep>>>,
}

impl<S> TimeoutIo<S> {
    pub(crate) fn new(inner: S, timeout: Duration) -> Self {
        Self {
            inner,
            timeout,
            read_timer: None,
            write_timer: None,
        }
    }
}

fn poll_timer(
    timer: &mut Option<Pin<Box<Sleep>>>,
    timeout: Duration,
    cx: &mut Context<'_>,
) -> Poll<io::Error> {
    let sleep = timer.get_or_insert_with(|| Box::pin(tokio::time::sleep(timeout)));
    match sleep.as_mut().poll(cx) {
        Poll::Ready(()) => {
            *timer = None;
            Poll::Ready(io::Error::new(io::ErrorKind::TimedOut, "timed out"))
        }
        Poll::Pending => Poll::Pending,
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for TimeoutIo<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = &mut *self;
        match Pin::new(&mut this.inner).poll_read(cx, buf) {
            Poll::Ready(result) => {
                this.read_timer = None;
                Poll::Ready(result)
            }
            Poll::Pending => poll_timer(&mut this.read_timer, this.timeout, cx).map(Err),
        }
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for TimeoutIo<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = &mut *self;
        match Pin::new(&mut this.inner).poll_write(cx, data) {
            Poll::Ready(result) => {
                this.write_timer = None;
                Poll::Ready(result)
            }
            Poll::Pending => poll_timer(&mut this.write_timer, this.timeout, cx).map(Err),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = &mut *self;
        match Pin::new(&mut this.inner).poll_flush(cx) {
            Poll::Ready(result) => {
                this.write_timer = None;
                Poll::Ready(result)
            }
            Poll::Pending => poll_timer(&mut this.write_timer, this.timeout, cx).map(Err),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// A plain or TLS connection.
pub(crate) enum Stream {
    Plain(TcpStream),
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_write(cx, data),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_write(cx, data),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_flush(cx),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_shutdown(cx),
        }
    }
}

fn timed_out(kind: ErrorKind) -> HttpError {
    HttpError::new(kind, "timed out")
}

/// `socket.create_connection((host, port), timeout)` after the lookup:
/// each address in turn, each attempt bounded by `timeout`; the last
/// attempt's error wins (`ConnectTimeout` or `ConnectError`).
pub(crate) async fn connect_tcp(
    addresses: &[SocketAddr],
    timeout: Duration,
) -> Result<TcpStream, HttpError> {
    let mut last = HttpError::new(ErrorKind::ConnectError, "getaddrinfo returns an empty list");
    for address in addresses {
        match tokio::time::timeout(timeout, TcpStream::connect(address)).await {
            Ok(Ok(stream)) => {
                let _ = stream.set_nodelay(true);
                return Ok(stream);
            }
            Ok(Err(err)) => last = HttpError::new(ErrorKind::ConnectError, os_error_text(&err)),
            Err(_) => last = timed_out(ErrorKind::ConnectTimeout),
        }
    }
    Err(last)
}

/// httpcore's `start_tls` with the connect timeout; certificate and
/// handshake failures are `ConnectError` (the rustls text, P2).
pub(crate) async fn start_tls(
    tcp: TcpStream,
    host: &str,
    config: Arc<ClientConfig>,
    timeout: Duration,
) -> Result<Stream, HttpError> {
    let name = ServerName::try_from(host.to_owned())
        .map_err(|err| HttpError::new(ErrorKind::ConnectError, err.to_string()))?;
    let connector = tokio_rustls::TlsConnector::from(config);
    match tokio::time::timeout(timeout, connector.connect(name, tcp)).await {
        Ok(Ok(stream)) => Ok(Stream::Tls(Box::new(stream))),
        Ok(Err(err)) => {
            let message = match err
                .get_ref()
                .and_then(|inner| inner.downcast_ref::<rustls::Error>())
            {
                Some(tls) => tls.to_string(),
                None => os_error_text(&err),
            };
            Err(HttpError::new(ErrorKind::ConnectError, message))
        }
        Err(_) => Err(timed_out(ErrorKind::ConnectTimeout)),
    }
}

/// The two TLS configurations of a context: verifying against the trust
/// store, and `verify_tls: false` (no certificate or host name checks, the
/// handshake signature still checked).
pub(crate) struct TlsConfigs {
    pub(crate) verify: Arc<ClientConfig>,
    pub(crate) no_verify: Arc<ClientConfig>,
}

impl TlsConfigs {
    /// Configurations trusting `roots`; ALPN `http/1.1` as httpx without
    /// HTTP/2.
    pub(crate) fn new(roots: RootCertStore) -> Self {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let builder = || {
            ClientConfig::builder_with_provider(Arc::clone(&provider))
                .with_safe_default_protocol_versions()
                .expect("ring supports the default protocol versions")
        };
        let mut verify = builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        verify.alpn_protocols = vec![b"http/1.1".to_vec()];
        let mut no_verify = builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerifier(
                provider.signature_verification_algorithms,
            )))
            .with_no_client_auth();
        no_verify.alpn_protocols = vec![b"http/1.1".to_vec()];
        Self {
            verify: Arc::new(verify),
            no_verify: Arc::new(no_verify),
        }
    }
}

/// The system trust store (Ubuntu `ca-certificates` on macmini, PLAN.md
/// D-12), loaded once per context; unreadable entries are skipped.
pub(crate) fn system_roots() -> RootCertStore {
    let mut roots = RootCertStore::empty();
    let loaded = rustls_native_certs::load_native_certs();
    let (_added, _ignored) = roots.add_parsable_certificates(loaded.certs);
    roots
}

/// `verify=False`: any certificate for any name. Python's `CERT_NONE`
/// still verifies the handshake signature with the presented key; so does
/// this verifier.
#[derive(Debug)]
struct NoVerifier(WebPkiSupportedAlgorithms);

impl ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.0)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.0)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.supported_schemes()
    }
}
