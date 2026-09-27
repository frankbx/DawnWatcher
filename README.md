# DawnWatcher

DawnWatcher is a deterministic, auditable trading-assistance platform. It will combine
dual-source intraday market monitoring, post-close data workflows, Feishu notifications,
and narrowly scoped language-model agents.

The project is currently at **Phase 2: dual-source market data foundation**. In addition to
the durable Phase 1 foundation, it can collect A-share snapshots from Sina and Tencent in
parallel, normalize and validate both sources independently, reconcile important fields,
archive the exact raw responses, replay archives offline, and persist auditable snapshots in
SQLite. It also provides non-overlapping fixed-interval collection for unattended operation.
Tushare `trade_cal` is cached in SQLite and gates all live collection by trading day and
auction phase. Strategies, external notification delivery, and agents will be implemented in
later phases.

Phase 2 deliberately keeps provider records separate. It never creates a synthetic quote by
mixing fields from Sina and Tencent. A reconciled record instead selects one complete source,
retains field-by-field comparisons, and assigns one of these quality states: `complete`,
`near`, `degraded`, `conflicted`, `stale`, or `blocked`.

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
dawnwatcher quotes replay data/raw/quotes/YYYY-MM-DD/sina/example.json.gz \
  --expected-date 2026-09-24
```

`calendar sync` downloads the current calendar year from Tushare by default and atomically
upserts all natural dates into SQLite. Explicit `--start-date` and `--end-date` ranges are also
supported. Runtime gates use the local cache and refresh it at most once every 24 hours; a
failed refresh retains known cached dates, while an unknown date fails closed.

`quotes collect` is market-gated by default, archives raw responses, and persists normalized
and reconciled records. `--ignore-market-gate` is an explicit diagnostic override. Use a
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

Both `quotes collect` and `quotes watch` call Sina and Tencent only during active auction
phases on dates marked open by Tushare:

- 09:15:00 ≤ t ≤ 09:25:00: `opening_call_auction`
- 09:25:00 < t < 09:30:00: `opening_pause` (no collection)
- 09:30:00 ≤ t ≤ 11:30:00: `morning_continuous`
- 11:30:00 < t < 13:00:00: `midday_break` (no collection)
- 13:00:00 ≤ t < 14:57:00: `afternoon_continuous`
- 14:57:00 ≤ t ≤ 15:00:00: `closing_call_auction`

If a scheduler tick lands exactly at 09:25, 11:30, or 15:00, it remains part of the preceding
active phase. Collections persist `market_phase` and expose `auction_mode` so call-auction
snapshots cannot be mistaken for continuous-auction observations. Non-trading ticks are
reported as scheduler skips and generate no Sina/Tencent HTTP traffic.

Within every collection cycle, corresponding Sina and Tencent batches use a two-stage start
barrier. Both sides first prepare their request, record dispatch readiness, and reach the final
barrier before either side may enter the HTTP stack. This also prevents a fast provider from
starting the next batch while the other provider is still processing the previous one. Each
result reports `request_dispatch_ready_at`, `request_start_skew_ms`, and
`max_request_start_skew_ms` so application-level launch alignment remains observable. A failed
or circuit-open provider leaves the barrier immediately and cannot block the healthy provider.

The collectors use bounded request timeouts, batches of at most 50 symbols by default, and
an in-process circuit breaker per provider. There are no aggressive automatic HTTP retries.
If one provider is unavailable, a valid quote from the other remains usable but is marked
`degraded`; strategy code in later phases must make an explicit decision about whether that
quality is acceptable.

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
