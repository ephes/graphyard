# Round 85 graphyard slices

All six round 85 slices were re-verified against main (`110dfab`) before work
and remained applicable.

Store boundaries (implemented on the earlier parked branch, reviewed here):

1. `graphyard-registry-sql-variable-budget`: registry lookups and last-seen
   updates are chunked at the connection's SQLite variable limit inside the
   one existing write transaction (late-chunk failure rolls back everything).
2. `graphyard-rust-heartbeat-clock-regression-fence`: heartbeat status/details
   are fenced by observation time under the writer reservation; equal times keep
   the first committed projection and `last_success` keeps the maximum.
3. `graphyard-store-cancelled-caller-transaction-oracle`: a real actor/registry
   transaction is gated, its caller dropped, and complete commit or injected
   rollback plus writer-lock release are asserted.

Collection ownership and identity:

4. `graphyard-collection-claim-binds-current-definition`: the initial due claim
   compares name, type, interval and config in Python and Rust. An edit between
   load and claim takes no claim and sends nothing with the old definition.
5. `graphyard-python-collector-live-lease-parity`: the Python agent uses the
   Rust lease (`min(max(3, interval), 300)` s), renews it every third from a
   background thread, and on a lost renewal refuses publication and completion.
6. `graphyard-unifi-device-identity-ambiguity`: a configured MAC is
   authoritative; a name must match exactly one device; ambiguous matches warn
   without points in both runtimes (shared `unifi.json` fixtures).

Each new regression failed on the previous implementation. Gates: `just lint`,
`just typecheck`, `just schema-check`, full pytest, full Rust workspace tests and
the offline parity harness (`just rs-parity`). The PostgreSQL-only oracles are
unaffected and were not run.
