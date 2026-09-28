# DawnWatcher

DawnWatcher is a deterministic, auditable trading-assistance platform. It will combine
Tencent intraday market monitoring, post-close data workflows, Feishu notifications, and
narrowly scoped language-model agents.

The project is currently at **Phase 2: single-source market data foundation**. In addition to
the durable Phase 1 foundation, it collects A-share snapshots from Tencent, validates them,
archives the exact raw responses, replays archives offline, and persists auditable snapshots
in SQLite. It also provides non-overlapping fixed-interval collection for unattended
operation. Tushare `trade_cal` is cached in SQLite and gates all live collection by trading
day and auction phase. Strategies, external notification delivery, and agents will be
implemented in later phases.

The active collector intentionally uses Tencent only. Historical Sina/Tencent rows remain
readable in SQLite for audit purposes, but new collections do not request Sina, perform
cross-provider reconciliation, or generate source-conflict metrics.

An opt-in diagnostic command can still compare Sina and Tencent concurrently. It is isolated
from the production collector and is intended for time-bounded provider reliability tests;
it does not write market snapshots to the production tables.

## Requirements

- Python 3.12
- A local filesystem for runtime data
- A Tushare token with access to `trade_cal`

## Development setup

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
```

Copy `.env.example` to `.env` only when local overrides are needed. Defaults are safe for
development. Put the Tushare credential alone in a project-root file named `token` and set
its permissions to `0600`. The file is excluded from version control and its contents are
never included in logs or configuration output.

## Commands

```bash
dawnwatcher --help
dawnwatcher doctor
dawnwatcher config
dawnwatcher db upgrade
dawnwatcher db check
dawnwatcher db recover
dawnwatcher db backup
dawnwatcher calendar sync
dawnwatcher calendar status
dawnwatcher quotes collect 600000.SH 000001.SZ
dawnwatcher quotes watch 600000.SH 000001.SZ
dawnwatcher quotes watch 600000.SH 000001.SZ --interval 30 --max-runs 10
dawnwatcher quotes compare 600000.SH 000001.SZ --interval 15 \
  --until 2026-09-28T15:00:00+08:00
dawnwatcher quotes stats --date 2026-09-28
dawnwatcher quotes replay data/raw/quotes/YYYY-MM-DD/tencent/example.json.gz \
  --expected-date 2026-09-24
dawnwatcher monitor check
dawnwatcher monitor watch
```

`calendar sync` downloads the current calendar year from Tushare by default and atomically
upserts all natural dates into SQLite. Explicit `--start-date` and `--end-date` ranges are also
supported. Runtime gates use the local cache and refresh it at most once every 24 hours; a
failed refresh retains known cached dates, while an unknown date fails closed.

`quotes collect` is market-gated by default, archives raw Tencent responses, and persists
normalized records. `--ignore-market-gate` is an explicit diagnostic override. Use a
stable `--idempotency-key` when a scheduler may retry the same logical run. For diagnostics,
`--no-persist` and `--no-archive` can disable either side effect. Security identifiers are
normalized to uppercase Tushare `ts_code` values such as `600000.SH`,
`000001.SZ`, and `920001.BJ`. Legacy numeric or `sh`/`sz`/`bj` aliases remain accepted as
inputs and archive-replay compatibility, but all application output and persisted data use
the Tushare form.

`quotes watch` starts immediately and then collects on a configurable fixed cadence. The
default interval is 15 seconds and can be changed with
`DAWNWATCHER_MARKET_POLL_INTERVAL_SECONDS` or overridden for one process with `--interval`.
Runs never overlap: if collection exceeds the interval, elapsed schedule slots are skipped.
The process handles SIGINT and SIGTERM cleanly. `--max-runs` is useful for bounded smoke tests;
without it, the process continues until a stop signal arrives.

`quotes compare` is the separate Sina/Tencent diagnostic path. Both requests are dispatched
concurrently on each cadence and each source has its own timeout and circuit breaker. The
default interval is also 15 seconds. `--until` accepts a timezone-aware ISO timestamp, which
is convenient for an afternoon test ending at 15:00; `--max-runs` can be used for a bounded
smoke test. Raw responses are archived under `data/raw/quotes/YYYY-MM-DD/{sina,tencent}`.
Each run is appended to `data/reports/afternoon-stability-YYYY-MM-DD/compare.jsonl`, while
every `near`, `conflict`, `degraded`, `stale`, or `blocked` result is appended as an independent
JSON record to `inconsistencies.jsonl`. Critical price tolerances retain the earlier definition:
exact match is `<= max(0.01, reference * 0.0002)`, near is `<= max(0.03, reference * 0.0005)`,
and larger differences are conflicts. The diagnostic path is deliberately not used by the
Tencent-only production watcher.

Both `quotes collect` and `quotes watch` call Tencent only during active auction phases on
dates marked open by Tushare:

- 09:15:00 ≤ t ≤ 09:25:00: `opening_call_auction`
- 09:25:00 < t < 09:30:00: `opening_pause` (no collection)
- 09:30:00 ≤ t ≤ 11:30:00: `morning_continuous`
- 11:30:00 < t < 13:00:00: `midday_break` (no collection)
- 13:00:00 ≤ t < 14:57:00: `afternoon_continuous`
- 14:57:00 ≤ t ≤ 15:00:00: `closing_call_auction`

If a scheduler tick lands exactly at 09:25, 11:30, or 15:00, it remains part of the preceding
active phase. Collections persist `market_phase` and expose `auction_mode` so call-auction
snapshots cannot be mistaken for continuous-auction observations. Non-trading ticks are
reported as scheduler skips and generate no Tencent HTTP traffic.

The collector uses bounded request timeouts, batches of at most 50 symbols by default, and an
in-process Tencent circuit breaker. There are no aggressive automatic HTTP retries. A valid
Tencent quote is `complete`; missing, stale, or invalid values are blocked from downstream
strategy code.

`quotes stats` reports Tencent's complete-run success rate, valid-quote coverage,
average/p50/p95/max collection latency, circuit-open and circuit-suppression counts,
quality-state counts, and within-session collection gaps. Historical dual-source rows are
reported separately and do not contaminate current single-source metrics.

`quotes watch` writes a durable heartbeat on every scheduler tick and after every collection.
Run `monitor watch` as a separate supervised process so a dead or stalled quote watcher can be
detected. The monitor checks quote-watcher heartbeat freshness, usable-collection freshness
during active auction phases, and free space on the runtime data filesystem. Alerts are
stateful: the first observation, severity escalation, and recovery are each enqueued once in
the transactional notification outbox. External Feishu delivery is not implemented yet, so
these alerts remain safely queued until a notification worker is added.

Defaults are a 30-second monitor cadence, a 60-second stale-heartbeat threshold, a 60-second
collection-gap threshold, a 5 GiB disk warning, and a 1 GiB disk critical alert. They can be
changed with the corresponding `DAWNWATCHER_MONITOR_*`, `DAWNWATCHER_HEARTBEAT_*`,
`DAWNWATCHER_COLLECTION_GAP_*`, and `DAWNWATCHER_DISK_*` settings shown in `.env.example`.

## Quality checks

```bash
ruff check .
ruff format --check .
mypy
pytest
```

Runtime files belong under `data/` and are intentionally excluded from version control.
Raw quote archives are checksum-protected gzip JSON envelopes under `data/raw/quotes/`.

SQLite runs in WAL mode with full synchronous durability, foreign-key enforcement, a busy
timeout, and Alembic-managed migrations. Do not place the database on a network filesystem.
Backup destinations are immutable: an existing backup file will never be overwritten.
