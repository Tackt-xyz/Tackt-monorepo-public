# Demo account-health history

For a small demo, PostgreSQL stores a derived display projection of the existing streamed Risk
processor. It is not an accounting source, authorization record, or Tackt credit calculation.
The authoritative Risk streams and their publish/acknowledgement sequence are unchanged.

## Data flow and bounded work

Configure `RISK_ENGINE_HISTORY_ACCOUNTS` with a comma-separated list of Core user UUIDs. An empty
list disables sampling; startup rejects malformed, nil, duplicate, or more than 32 subjects.
`RISK_ENGINE_HISTORY_INTERVAL_SECS` defaults to 60 and must be within 1–3600. This is an explicit
demo opt-in, not an automatic scan of every allocated engine account slot.

At a completed processing boundary, the single engine thread resolves each configured subject
through its existing identity map and copies only its health ratio, classification, identity, and capture time.
It uses a bounded channel (64 samples) with `try_send`. Capture never waits for a consumer,
clones a portfolio, serializes JSON, or performs database I/O. The timer is checked after every
completed event as well as on idle wakeups, so continuous ingress cannot starve sampling.
Recovery corrections finish before sampling starts. Poisoned or draining state is not sampled.

A separate Tokio worker formats the captured values and calls `tackt-postgres` using a dedicated
one-connection pool. Each write has a two-second timeout. Timeout/failure drops that sample;
the next interval captures current state. Full/closed queues also drop samples. Counters expose
successful writes, failures, and drops. Shutdown allows the writer three seconds to drain after
the engine exits, then aborts it. This path deliberately permits gaps and has no durable retry
log. Sampling and querying continue without adding ClickHouse, Redis, or another NATS stream.

`risk_health_samples` uses plain inserts through its typed API, with no retry handling. Sample UUIDs
remain internal row keys; duplicate IDs fail. `(user_id, sampled_at DESC, sample_id DESC)` supports
latest and bounded history reads. Late arrival of an older capture cannot replace the latest
capture. Run one history-producing Risk host for these demo accounts; cross-host clock ordering
and overlapping producers are not coordinated. At two accounts/minute, growth is 2,880 rows/day.
No automatic retention, backfill, or downsampling is introduced.

## Measurement semantics

`metric = pooled_margin_health` means `(pooled collateral - pooled liabilities) / total
maintenance`, using the existing processor's policy. It is not the separate credit evaluator's
`liquidation_capacity / Tackt debt`.

The table stores `sample_id` (internal row key), `user_id`, `sampled_at`, `health NUMERIC(39,6)`,
and `classification TEXT`. There is no JSONB payload, breakdown, threshold, or version metadata.
Finite engine ratios use 1e6 fixed point and are stored exactly as decimal ratios. The API returns
an exact decimal string such as `"1.200000"` to avoid JavaScript precision loss.

A null health with `data_incomplete` means unavailable inputs. A null health with `healthy`
means positive unbounded health (zero maintenance); null with `liquidatable` means negative
unbounded health. Display these as unavailable or unbounded, not zero. Classification is copied
from the engine, so historical labels do not depend on today's thresholds.

The existing price/portfolio wire inputs do not carry source observation timestamps. Therefore
all reads say `inputFreshness: "unknown"`. `sampledAt` is capture time, not price freshness.
A recently captured sample must never be rendered as proof of fresh inputs. An old latest `sampledAt` shows that the
history pipeline itself has fallen behind; a chart should preserve gaps rather than interpolate
them as trusted observations.

## Core read API

- `GET /api/v1/users/me/risk-health`: latest captured sample, or `latest: null`.
- `GET /api/v1/users/me/risk-health/history?fromMs=...&toMs=...&limit=...`: newest-first samples
  in `[fromMs, toMs)`. Defaults to the last 24 hours and 1,440 rows; maximum range is 31 days and
  maximum limit is 2,000. `truncated: true` instructs the caller to narrow the range or raise the
  limit. There is no claim that a truncated response is a complete 30-day chart.

Each API sample contains only `sampledAt`, `health`, and `classification`. Account and sample IDs
stay internal; responses retain the metric, input freshness, and history truncation indicator.

Both use Core's existing user authentication and `no-store` response middleware. User lifecycle
and scoped sample reads execute in one PostgreSQL statement snapshot. Missing and closed users
are rejected. The request has no user-ID selector and accepts no writes or calculation inputs.
The terminal remains a separate frontend integration; its illustrative credit health must not
be silently relabeled with this margin metric.

## Rollout and verification

Migration `0010_risk_health_history` creates the numeric table and its latest/history index directly.
Its down migration drops the derived history table and its samples. This draft uses one migration;
a disposable test database initialized with an earlier draft must be recreated before testing,
because SQLx verifies migration checksums.

With split credentials, the Risk history role needs INSERT and SELECT on this table; Core needs
SELECT. Existing Ledger read permissions stay unchanged. Migrations use the migrating user's
ownership, create no roles, and run through `PgRepository::initialise`.

Use only the disposable repository Compose services for integration tests. Tests cover immutable
inserts and duplicate-ID rejection, latest/range ordering, precision, lifecycle/account isolation, incomplete data,
finite/unbounded storage and migration rollback/reapplication, unknown subjects, and saturated/closed queues.
The benchmark's `RISK_BENCH_HISTORY_SAMPLING=true` samples two accounts after every measured price
update (far more frequently than the demo timer); `RISK_BENCH_HISTORY_QUEUE_FULL=true` holds the
queue full throughout. It also measures warm capture cost and allocations independently of DB
I/O. Results and remaining verification limitations are recorded in the dated benchmark report.
