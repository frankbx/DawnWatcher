# DawnWatcher

DawnWatcher is a deterministic, auditable trading-assistance platform. It combines
Tencent intraday market monitoring and Feishu operational alerts, and will add post-close workflows and
narrowly scoped language-model agents.

The project is currently at **Phase 2: single-source market data and feature foundation**. In addition to
the durable Phase 1 foundation, it collects A-share snapshots from Tencent, validates them,
archives the exact raw responses, replays archives offline, and persists auditable snapshots
in SQLite. Persisted snapshots can be aggregated into auditable one-minute bars with price
trend, incremental turnover, VWAP deviation, relative volume, and market/industry relative
strength. It also provides non-overlapping fixed-interval collection for unattended
operation. Tushare `trade_cal` is cached in SQLite and gates all live collection by trading
day and auction phase. Operational alerts can be delivered through a durable Feishu custom-bot
worker. Strategies, decision notifications, and agents will be implemented in later phases.

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

Install the optional Parquet writer when this host is responsible for minute-data sealing:

```bash
python -m pip install -e '.[dev,parquet]'
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
dawnwatcher features build 600000.SH --date 2026-09-28 \
  --market-benchmark 000001.SH --industry-map industry-benchmarks.json
dawnwatcher features show --date 2026-09-28 --symbol 600000.SH
dawnwatcher features seal --date 2026-09-28 --session morning
dawnwatcher features seal --date 2026-09-28 --session afternoon
dawnwatcher features merge-day --date 2026-09-28
dawnwatcher monitor check
dawnwatcher monitor watch
dawnwatcher notifications deliver --max-items 20
dawnwatcher notifications watch
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

`features build` is an idempotent materialization step over validated Tencent snapshots. It
creates `minute_bar` and `minute_feature` rows; rerunning it updates the same provider/symbol/
minute keys. The formulas are:

- one-minute trend: `(minute close / minute open - 1) * 10,000` basis points;
- incremental volume and amount: last cumulative value in the minute minus the previous
  available snapshot's cumulative value;
- minute VWAP: incremental amount divided by incremental shares, with VWAP deviation as
  `(minute close / minute VWAP - 1) * 10,000` basis points;
- relative volume: current incremental shares divided by the mean for the same local clock
  minute over up to 20 preceding stored dates, emitted only after five dates by default;
- relative strength: the stock's one-minute trend minus the matching market or industry
  benchmark trend.

The market benchmark and every industry benchmark must be included in `quotes watch`, or the
corresponding strength is stored as null with a quality flag. An industry mapping file is a
JSON object such as:

```json
{
  "600000.SH": "512800.SH",
  "000001.SZ": "512800.SH"
}
```

Index and ETF Tushare codes such as `000001.SH`, `399001.SZ`, `510300.SH`, and `159915.SZ`
are accepted. Missing cumulative baselines, resets, low sample coverage, insufficient history,
and absent benchmark bars never become zero-valued signals; they remain null and are recorded
in `quality_flags`. Use `--interval` if the source snapshots were not collected at the default
15-second cadence. The build command may be run after each completed minute for intraday use
and rerun after the close to finalize the day.

`features seal` writes a sealed analytical partition after a trading session has ended.
The wide rows combine the minute OHLC/turnover data, derived features, and stable stock-pool
labels. Output is partitioned as
`data/lake/minute_market/trade_date=YYYY-MM-DD/session={morning,afternoon}`. Each partition
contains `part-000.parquet` plus `manifest.json`, which records the schema version, expected
and actual symbol/minute coverage, missing features, file size, and SHA-256 checksum. Parquet
is written to a temporary file, read back for row-count validation, synced, and atomically
renamed. The manifest is published only after the data file succeeds. A session with gaps is
still retained for audit with `complete: false`, and the CLI returns status 3; an active
session cannot be sealed.

After the afternoon partition is sealed, `features merge-day` validates both session
manifests and checksums and publishes a single
`data/lake/minute_market/trade_date=YYYY-MM-DD/day.parquet` file with
`day-manifest.json`. Morning and afternoon rows retain their `session` and `market_phase`
columns. The two session partitions remain as recovery checkpoints. If either source session
is incomplete, the daily file is still auditable but is also marked `complete: false`.

For unattended session-close operation, run the wrapper below under the same process
supervisor as the collectors. It rebuilds the final minute features, seals the morning
partition at 11:32 and the afternoon partition at 15:02, then publishes `day.parquet`. It
exits non-zero if a session or daily seal fails or is incomplete:

```bash
.venv/bin/python scripts/watch_minute_sealer.py --date 2026-09-29
```

`scripts/watch_market_analysis.py` runs that materialization incrementally: eight seconds after
each wall-clock minute it rebuilds only the just-completed minute, using the preceding minute as
the cumulative volume/amount baseline. On each 15-minute boundary it sends a Feishu Card JSON
2.0 overview containing an auditable intraday market temperature, the 沪深300 rolling return,
daily and rolling breadth for the 320 fixed representative stocks, and ranked sector strength
confirmed by configured industry ETFs. Temperature combines daily and rolling sample breadth
with benchmark/sample returns; same-clock relative volume raises confidence and distinguishes
extreme states but never supplies direction by itself. Around 09:45 the card also labels whether
the opening gap is being confirmed or reversed and treats that result as a risk adjustment, not
an independent buy signal. The 40 dynamic observers are excluded from breadth to avoid selection
bias. Sector warming/cooling compares the current 15-minute window with the preceding window;
missing history, partial ETF proxies, and sample-only breadth remain explicit in the output.

For a bounded intraday run:

```bash
.venv/bin/python scripts/watch_market_analysis.py \
  --until 2026-09-29T15:00:00+08:00
```

`quotes watch` writes a durable heartbeat on every scheduler tick and after every collection.
Run `monitor watch` as a separate supervised process so a dead or stalled quote watcher can be
detected. The monitor checks quote-watcher heartbeat freshness, usable-collection freshness
during active auction phases, and free space on the runtime data filesystem. Alerts are
stateful: the first observation, severity escalation, and recovery are each enqueued once in
the transactional notification outbox.

To deliver those records to a phone, add a custom bot to the target Feishu group and put its
complete V2 webhook URL alone in the project-root `feishu_webhook` file. If the bot enables
signature verification, put the signing secret alone in `feishu_secret`; otherwise leave that
file absent. Both files are excluded from version control and should have mode `0600`. Run
`notifications watch` as a third supervised process. It polls every 5 seconds by default,
leases at most 20 messages at a time, records every delivery attempt, retries with capped
exponential backoff, and dead-letters a notification after its configured maximum attempts.
Outbox alerts and `notifications test` messages are sent as Feishu interactive cards using
Card JSON schema 2.0 (`msg_type: interactive`, with content under `card.body.elements`).
The webhook URL is restricted to approved Feishu/Lark custom-bot HTTPS endpoints and is never
included in logs, public configuration, or database error messages.

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
