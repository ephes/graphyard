django: PYTHONUNBUFFERED=true uv run src/django/manage.py runserver 0.0.0.0:8000
agent: PYTHONUNBUFFERED=true uv run src/django/manage.py start_agent
influxdb: sh -c 'if command -v influxd >/dev/null 2>&1; then mkdir -p .dev/influxdb2; exec influxd --bolt-path .dev/influxdb2/influxd.bolt --engine-path .dev/influxdb2/engine; elif command -v influxdb3 >/dev/null 2>&1; then mkdir -p .dev/influxdb3; exec influxdb3 serve --node-id graphyard-dev --object-store file --data-dir .dev/influxdb3 --http-bind 127.0.0.1:8086 --without-auth; else echo "Neither influxd nor influxdb3 found (macOS: brew install influxdb)"; exit 1; fi'
grafana: uv run python scripts/dev_grafana.py
