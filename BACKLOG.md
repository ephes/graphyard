# Backlog

## HTTP page probe: total deadline

`http_page_probe` only has httpx's per-operation timeout (`request_timeout_seconds`)
plus a streamed body cap (`max_body_bytes`). An endpoint that drips bytes, including
incomplete response headers, resets that timeout on every read and can hold the
single-threaded agent loop well past `request_timeout_seconds`.

Add a real total deadline for the whole probe (DNS, connect, TLS, headers, body,
redirects) using only public httpx/httpcore APIs, or run probes off the agent loop
(worker with a hard deadline). Avoid patching private httpx attributes such as
`HTTPTransport._pool`. Cover it with a real-socket test that drips header bytes.
