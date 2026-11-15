# Backlog

No open items.

Completed in scout round 91 (B31): cached public releases now bind to their exact
source/target definition and registry generation; refreshes enforce a five-minute
elapsed admission budget with explicit incomplete/skipped-source reporting.
Regression coverage includes independent database connections and 200 synthetic
sources; no public release fetch or rollout is required for validation.

Implemented in scout round 97 (B13–B18): recorder file authority/session
baselines, bounded probes and controls, atomic bounded shadow preparation with
last-published completion records, bounded evidence admission, finite
qualification inputs, ordered matching and missing-evidence findings. Local
synthetic fixtures cover restarts, real child kills, busy SQLite, helper process
groups, input budgets and alignment work. Cross-family review and merge remain
with the external clerk; no runtime rollout is part of this work.
