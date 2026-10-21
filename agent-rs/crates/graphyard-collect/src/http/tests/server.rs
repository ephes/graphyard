//! Fake HTTP servers on loopback (raw HTTP/1.1 over tokio sockets, so tests
//! control every byte and every delay) and resolver stubs.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::http::resolve::{Lookup, LookupError};

/// A request as the server received it.
#[derive(Clone, Debug)]
pub(super) struct Recorded {
    pub method: String,
    pub target: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Recorded {
    /// The values of a header (case-insensitive name).
    pub fn header_values(&self, name: &str) -> Vec<&str> {
        self.headers
            .iter()
            .filter(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
            .collect()
    }

    pub fn line(&self) -> String {
        format!("{} {}", self.method, self.target)
    }
}

/// What the server does with a request.
#[derive(Clone, Debug)]
pub(super) enum Reply {
    /// Write these bytes and close.
    Raw(Vec<u8>),
    /// Write `prefix`, then one byte of `drip` (cycled) per `interval` for
    /// at most `give_up`.
    Drip {
        prefix: Vec<u8>,
        drip: Vec<u8>,
        interval: Duration,
        give_up: Duration,
    },
    /// Close without answering.
    Close,
}

/// A complete response with `Content-Length` and `Connection: close`.
pub(super) fn response(status_line: &str, headers: &[(&str, &str)], body: &[u8]) -> Reply {
    let mut head = format!("HTTP/1.1 {status_line}\r\n");
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str(&format!(
        "Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    ));
    let mut bytes = head.into_bytes();
    bytes.extend_from_slice(body);
    Reply::Raw(bytes)
}

type Handler = dyn Fn(&Recorded) -> Reply + Send + Sync;

/// A server answering every connection with `handler`.
pub(super) struct Server {
    pub addr: SocketAddr,
    requests: Arc<Mutex<Vec<Recorded>>>,
    accepted: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl Server {
    pub async fn start(handler: impl Fn(&Recorded) -> Reply + Send + Sync + 'static) -> Self {
        Self::start_with(handler, None).await
    }

    /// Serve TLS with `acceptor` when given.
    pub async fn start_with(
        handler: impl Fn(&Recorded) -> Reply + Send + Sync + 'static,
        acceptor: Option<tokio_rustls::TlsAcceptor>,
    ) -> Self {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind loopback");
        let addr = listener.local_addr().expect("local address");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let accepted = Arc::new(AtomicUsize::new(0));
        let handler: Arc<Handler> = Arc::new(handler);
        let task = {
            let requests = Arc::clone(&requests);
            let accepted = Arc::clone(&accepted);
            tokio::spawn(async move {
                while let Ok((stream, _)) = listener.accept().await {
                    accepted.fetch_add(1, Ordering::SeqCst);
                    let handler = Arc::clone(&handler);
                    let requests = Arc::clone(&requests);
                    let acceptor = acceptor.clone();
                    tokio::spawn(async move {
                        match acceptor {
                            Some(acceptor) => {
                                if let Ok(tls) = acceptor.accept(stream).await {
                                    serve(tls, &*handler, &requests).await;
                                }
                            }
                            None => serve(stream, &*handler, &requests).await,
                        }
                    });
                }
            })
        };
        Self {
            addr,
            requests,
            accepted,
            task,
        }
    }

    /// `http://127.0.0.1:<port>`.
    pub fn origin(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub fn port(&self) -> u16 {
        self.addr.port()
    }

    pub fn requests(&self) -> Vec<Recorded> {
        self.requests.lock().expect("requests lock").clone()
    }

    pub fn accepted(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    handler: &Handler,
    requests: &Mutex<Vec<Recorded>>,
) {
    let Some(request) = read_request(&mut stream).await else {
        return;
    };
    requests
        .lock()
        .expect("requests lock")
        .push(request.clone());
    match handler(&request) {
        Reply::Raw(bytes) => {
            let _ = stream.write_all(&bytes).await;
            let _ = stream.shutdown().await;
        }
        Reply::Drip {
            prefix,
            drip,
            interval,
            give_up,
        } => {
            if stream.write_all(&prefix).await.is_err() {
                return;
            }
            let started = tokio::time::Instant::now();
            let mut index = 0;
            while started.elapsed() < give_up {
                let byte = [drip[index % drip.len()]];
                if stream.write_all(&byte).await.is_err() || stream.flush().await.is_err() {
                    return;
                }
                index += 1;
                tokio::time::sleep(interval).await;
            }
        }
        Reply::Close => {}
    }
}

async fn read_request<S: AsyncRead + Unpin>(stream: &mut S) -> Option<Recorded> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        if let Some(end) = find(&buffer, b"\r\n\r\n") {
            break end;
        }
        let read = stream.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        buffer.extend_from_slice(&chunk[..read]);
    };
    let head = String::from_utf8_lossy(&buffer[..head_end]).into_owned();
    let mut lines = head.split("\r\n");
    let mut request_line = lines.next()?.split(' ');
    let method = request_line.next()?.to_owned();
    let target = request_line.next()?.to_owned();
    let headers: Vec<(String, String)> = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_owned(), value.trim().to_owned()))
        .collect();
    let length: usize = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.parse().ok())
        .unwrap_or(0);
    let mut body = buffer[head_end + 4..].to_vec();
    while body.len() < length {
        let read = stream.read(&mut chunk).await.ok()?;
        if read == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..read]);
    }
    Some(Recorded {
        method,
        target,
        headers,
        body,
    })
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// A resolver stub: every name is loopback, optionally after a delay or
/// until released; `fail` names fail with a `gaierror` text.
pub(super) struct StubLookup {
    pub delay: Duration,
    pub gate: Option<Arc<Gate>>,
    pub fail: Vec<String>,
    pub calls: AtomicUsize,
}

impl StubLookup {
    pub fn loopback() -> Self {
        Self {
            delay: Duration::ZERO,
            gate: None,
            fail: Vec::new(),
            calls: AtomicUsize::new(0),
        }
    }
}

impl Lookup for StubLookup {
    fn lookup(&self, host: &str) -> Result<Vec<IpAddr>, LookupError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        std::thread::sleep(self.delay);
        if let Some(gate) = &self.gate {
            gate.wait();
        }
        if self.fail.iter().any(|name| name == host) {
            return Err(LookupError(
                "[Errno -2] Name or service not known".to_owned(),
            ));
        }
        Ok(vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
    }
}

/// A gate blocking resolver threads until opened (opened on drop, so a
/// failing test never leaves a blocked thread behind).
#[derive(Default)]
pub(super) struct Gate {
    open: Mutex<bool>,
    changed: Condvar,
}

impl Gate {
    pub fn wait(&self) {
        let mut open = self.open.lock().expect("gate lock");
        while !*open {
            open = self.changed.wait(open).expect("gate wait");
        }
    }

    pub fn open(&self) {
        *self.open.lock().expect("gate lock") = true;
        self.changed.notify_all();
    }
}

/// Opens the gate when dropped.
pub(super) struct GateGuard(pub Arc<Gate>);

impl Drop for GateGuard {
    fn drop(&mut self) {
        self.0.open();
    }
}
