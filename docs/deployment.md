# Web service entrypoint

Required production entrypoint: `config.wsgi_proxy:application`.
The shipped `deploy/systemd/graphyard-web.service` runs Granian with this WSGI
wrapper. It rewrites REMOTE_ADDR from X-Forwarded-For only for trusted loopback
peers (`127.0.0.1` and `::1`); headers from other peers do not replace the address.
Login throttling uses that normalized client address. Keep
`GRAPHYARD_LOGIN_THROTTLE_PROXY_HOPS=0` with this entrypoint so the throttle does
not interpret the forwarded header again. The loopback reverse proxy must
append or replace X-Forwarded-For with the actual client address.
