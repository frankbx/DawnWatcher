# RegimeBeacon

RegimeBeacon is a deterministic, auditable trading-assistance platform. It combines
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
worker. A calendar-driven runtime supervisor starts and stops all intraday services without
daily operator intervention. Strategies, decision notifications, and agents will be implemented
in later phases.

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

For ad-hoc AkShare/Sina historical-minute downloads, install the separate
`historical-minutes` extra in a non-live environment. It is not required by the
Tencent real-time watcher:

```bash
python -m pip install -e '.[historical-minutes]'
```

Copy `.env.example` to `.env` only when local overrides are needed. Defaults are safe for
development. Put the Tushare credential alone in a project-root file named `token` and set
its permissions to `0600`. The file is excluded from version control and its contents are
never included in logs or configuration output.

## Commands

```bash
regimebeacon --help
regimebeacon doctor
regimebeacon config
regimebeacon db upgrade
regimebeacon db check
regimebeacon db recover
regimebeacon db backup
regimebeacon calendar sync
regimebeacon calendar status
regimebeacon quotes collect 600000.SH 000001.SZ
regimebeacon quotes watch 600000.SH 000001.SZ
regimebeacon quotes watch 600000.SH 000001.SZ --interval 30 --max-runs 10
regimebeacon quotes compare 600000.SH 000001.SZ --interval 15 \
  --until 2026-09-28T15:00:00+08:00
regimebeacon quotes stats --date 2026-09-28
regimebeacon quotes replay data/raw/quotes/YYYY-MM-DD/tencent/example.json.gz \
  --expected-date 2026-09-24
regimebeacon features build 600000.SH --date 2026-09-28 \
  --market-benchmark 000001.SH --industry-map industry-benchmarks.json
regimebeacon features show --date 2026-09-28 --symbol 600000.SH
regimebeacon features seal --date 2026-09-28 --session morning
regimebeacon features seal --date 2026-09-28 --session afternoon
regimebeacon features merge-day --date 2026-09-28
regimebeacon acceptance run --date 2026-09-28
regimebeacon monitor check
regimebeacon monitor watch
regimebeacon notifications deliver --max-items 20
regimebeacon notifications watch
regimebeacon runtime run --project-root "$PWD"
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

`quotes watch` starts immediately when invoked manually and then collects on a configurable fixed cadence; the unattended supervisor starts it at 09:30. The
default interval is 15 seconds and can be changed with
`REGIMEBEACON_MARKET_POLL_INTERVAL_SECONDS` or overridden for one process with `--interval`.
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

Production `quotes collect` and `quotes watch` call Tencent only from the start of
continuous trading and through the official close on dates marked open by Tushare:

- 09:15:00 ≤ t ≤ 09:25:00: `opening_call_auction` (no production collection)
- 09:25:00 < t < 09:30:00: `opening_pause` (no collection)
- 09:30:00 ≤ t ≤ 11:30:00: `morning_continuous`
- 11:30:00 < t < 13:00:00: `midday_break` (no collection)
- 13:00:00 ≤ t < 14:57:00: `afternoon_continuous`
- 14:57:00 ≤ t ≤ 15:00:00: `closing_call_auction` (retain closing prices)
- 15:00:00 < t < 15:00:30: `closing_final_quote` (final-price capture only;
  excluded from continuous-trading statistics)

To check the opening call auction without writing quote snapshots or raw archives, run
`regimebeacon quotes probe-opening 600000.SH 510300.SH` during 09:15–09:25. Outside
that phase the command skips the request. If a scheduler tick lands exactly at 11:30
or 15:00, it remains part of the preceding active phase. The brief post-close
capture window allows a 15-second tick offset from the wall-clock boundary to
record the officially published final quote. Persisted collections carry
`market_phase` so closing-auction snapshots cannot be mistaken for continuous trading.
Non-trading ticks generate no Tencent HTTP traffic.

The collector uses bounded request timeouts, batches of at most 50 symbols by default, and an
in-process Tencent circuit breaker. There are no aggressive automatic HTTP retries. A valid
Tencent quote is `complete`; missing, stale, or invalid values are blocked from downstream
strategy code.

`quotes stats` reports Tencent's continuous-trading complete-run success rate, valid-quote coverage,
average/p50/p95/max collection latency, circuit-open and circuit-suppression counts,
quality-state counts, and within-session collection gaps. Opening and closing call-auction
rows, if present, are excluded from these rates; the close remains available for
the final price and minute-line seal. Historical dual-source rows are reported
separately and do not contaminate current single-source metrics.

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
supervisor as the collectors. It rebuilds only missing or incomplete minute features, seals the morning
partition at 11:32 and the afternoon partition at 15:02, then publishes `day.parquet`. It
exits non-zero if a session or daily seal fails or is incomplete:

```bash
.venv/bin/python scripts/watch_minute_sealer.py --date 2026-09-29
```

## Unattended trading-day runtime

`runtime run` is the single long-lived owner of the daily service lifecycle. It reads the
locally cached Tushare calendar (refreshing it at most once per day), does nothing on known
non-trading days, and reconciles child processes every five seconds on an open date:

- 08:50: start operational monitoring and durable Feishu outbox delivery;
- 09:14:30: start the session sealer, which waits for its post-session deadlines;
- 09:30: start the 15-second Tencent watcher and minute analysis; begin aligned
  15-minute status reports and, if configured, separate holdings review cards;
- 11:32: seal the morning minute partition;
- 15:00:30: stop quote collection after the inclusive 15:00 tick;
- 15:01: stop market-analysis and status-report processes;
- 15:02: seal the afternoon partition and merge the whole-day Parquet file;
- after the sealer finishes: validate the day's collection and sealed minute files, then
  enqueue one daily acceptance card;
- 15:15: stop normal operational support processes; after a late seal or acceptance,
  continue notification delivery for a two-minute drain.

The supervisor uses an advisory lock so two instances cannot collect the same pool. A failed
continuous service is restarted after ten seconds. Daily analysis/report services receive up
to three attempts, and a terminal failure is written to the notification outbox. If the host
or analysis worker restarts during trading, the per-minute analysis resumes without an
immediate full-day SQLite rebuild; the post-session sealer performs catch-up before sealing.
If the host
or supervisor starts late, the minute sealer can catch up until 23:50. Its per-date result
marker preserves retry counts across supervisor restarts: incomplete market data is a final
result, whereas transient sealing failures receive up to three attempts. An unknown trading
calendar fails closed and sends an operational alert through the notification worker; a later
calendar recovery is also reported. An intentional supervisor restart does not consume a
sealing attempt; a sealer still running at its 23:50 deadline is terminated and alerted, with
daily acceptance allowed to catch up until 23:58. A per-date sealer lock also prevents
concurrent Parquet writes if an old child survives an unexpected supervisor exit. Runtime logs are split under
`data/reports/runtime/YYYY-MM-DD/`; the supervisor log remains under `data/reports/`.

`acceptance run` is automatically scheduled once the sealer reaches a terminal result, including
an incomplete result. It checks 948 expected 15-second slots across morning and afternoon
continuous trading only; Tencent
valid-quote coverage, complete-run rate, P95 latency, circuit trips, and the full stock-pool
coverage; and both session Parquet partitions plus the merged day file. New sealed days
have 120 morning and 120 afternoon minute timestamps (240 total), excluding the opening
call auction but retaining the official close. Legacy 250-minute seals remain readable.
The report is written atomically to
`data/reports/daily/YYYY-MM-DD/acceptance.json`, recorded as an idempotent `job_run`, and
queued once as a Feishu Card 2.0 verdict. A failed data-quality verdict is a completed
assessment, not a command crash. The default thresholds are 99.5% valid quotes, 99% complete
runs, 15,000 ms P95 latency, and a 60-second maximum missing collection gap; see
`.env.example` for configuration. The card distinguishes passed, warning, and failed days.

On macOS, install and immediately load the per-user launch agent once:

```bash
.venv/bin/python scripts/install_macos_launch_agent.py \
  --project-root "$PWD"
launchctl print "gui/$(id -u)/com.regimebeacon.runtime"
```

The generated agent uses `RunAtLoad` and `KeepAlive`, so it restarts after a crash and starts
again when the user logs in. The Mac must remain powered on, awake, and logged in. No token or
Feishu secret is embedded in the plist. To stop and unload it deliberately:

```bash
launchctl bootout "gui/$(id -u)/com.regimebeacon.runtime"
```

`scripts/watch_market_analysis.py` runs that materialization incrementally: eight seconds after
each retained trading minute it rebuilds only the just-completed minute, using the preceding minute as
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
The worker skips opening and midday empty minutes, does not replay a full day on startup by
default, and records each feature-build duration. `--rebuild-on-start` explicitly enables the
expensive full-day repair for a controlled maintenance run.

For a bounded intraday run:

```bash
.venv/bin/python scripts/watch_market_analysis.py \
  --until 2026-09-29T15:00:00+08:00
```

`quotes watch` writes a durable heartbeat after each collection and on skipped scheduler ticks.
A temporary SQLite writer lock on the heartbeat does not discard an otherwise collectible quote
round; registration is retried on the next tick. Run `monitor watch` as a separate supervised
process so a dead or stalled quote watcher can be detected. Monitoring ignores orphaned running
heartbeats from prior trading days and allows a bounded startup/restart grace after a clean stop
or when a fresh quote collection proves the watcher is active. It checks collection freshness
only during continuous auction and also checks free space on the runtime data filesystem. Alerts are
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
changed with the corresponding `REGIMEBEACON_MONITOR_*`, `REGIMEBEACON_HEARTBEAT_*`,
`REGIMEBEACON_COLLECTION_GAP_*`, and `REGIMEBEACON_DISK_*` settings shown in `.env.example`.

## Private holdings review

`data/private/holdings.json` is a local, git-ignored file containing Tushare-format
codes, names, per-share cost in yuan, and share counts. Keep it at permission `0600`.
When present at supervisor startup, held symbols are added to the Tencent quote watcher
without duplicating symbols already in the market pool. They remain outside the fixed
market-breadth sample and its Parquet completeness denominator. After a trade, update this
file and restart the supervisor; the system does not infer executions from prices.

`scripts/watch_holdings.py` queues a separate Feishu Card JSON 2.0 review every
five minutes from 09:35 through 11:30 and 13:05 through 15:00, 40 seconds after
each boundary. The opening auction and lunch break are excluded from the window.
The notification outbox deduplicates each trading-date/slot and retries delivery;
cards expire at the next slot so yesterday's advice cannot arrive the next morning.
The card includes current price, day and five-minute moves, relative move against the
沪深300 ETF, cost-based unrealized P/L, and an explicit rule-based action suggestion
for each holding. Quotes
older than 90 seconds or carrying validation issues suppress valuation and guidance;
the opening auction is not compared with continuous trading. P/L excludes fees,
taxes, dividends, and later trades. Prompts do not place orders and are not a substitute
for a user-defined risk plan.
Price-move thresholds use the same window as the card: the original 15-minute
0.5%/1.0% bands become 0.3%/0.6% at five minutes via a rounded square-root-of-time
heuristic. The 5% cost-loss and 1.5× historical same-window volume thresholds stay
unchanged. These are transparent review triggers, not backtested trade signals.

The separate AkShare/Sina history for the five held symbols is under
`data/lake/akshare_sina_full_day/holdings_as_of=2026-09-29/`. Each complete-day file
preserves the provider's original end label, a start/end interval, a distinct
14:57–15:00 closing-auction record, and 09:30/15:00 price points. To refresh in a
historical-minute environment, choose a new output directory:

```bash
python scripts/download_akshare_sina_full_days.py \
  --symbol-file data/private/holdings.json \
  --output data/lake/akshare_sina_full_day/holdings_as_of=YYYY-MM-DD
```

The holdings watcher automatically selects the newest matching `holdings_as_of=*`
directory and compares its last five to eight **prior, complete trading days** with
the current 15-minute slot. Each card shows the same-clock historical median return,
the current return's percentage-point difference, and current Tencent volume divided
by the historical Sina median volume. The 09:30–09:45 opening window omits the volume
ratio because opening-auction volume allocation is not confirmed. A historical window
must be contiguous and contain at least five days; references older than 21 calendar
days are disabled. Missing/old history degrades the card explicitly without stopping
real-time monitoring. Cross-provider volume ratios are approximate, not trade signals.
The 15:00 card requires a quote timestamp at or after 15:00; an auction-period quote
before the final match is marked "收盘价未确认" and suppresses final advice.
The runtime only reads local Parquet files; AkShare is never called during trading.

One unadjusted Sina daily bar per pool/holding symbol can be cached separately from
both the real-time SQLite database and the minute-history files. Install the
`historical-daily` optional dependencies, then run the paced, resumable downloader:

```bash
python scripts/download_sina_daily.py --trade-date YYYY-MM-DD
```

The output is `data/lake/sina_daily/trade_date=YYYY-MM-DD/day.parquet` with a
manifest recording requested, returned, and missing symbols. It uses AkShare's
Sina stock and ETF daily interfaces and does not adjust prices. A complete daily
cache is **not** a substitute for quote snapshots or minute bars; this command
does not delete SQLite data.

To independently verify one cached day against Tushare's unadjusted stock
`daily` and ETF `fund_daily` endpoints, with `token` in the project root:

```bash
python scripts/compare_tushare_sina_daily.py --trade-date YYYY-MM-DD
```

The matched comparison Parquet and JSON summary are written under
`data/lake/tushare_sina_daily_compare/trade_date=YYYY-MM-DD/`. Tushare volume
is converted from hands to shares/ETF units and amount from thousand yuan to yuan
before comparing with Sina. The comparison does not modify SQLite.

### Tushare daily data lake

For the production daily cache, install the DuckDB/Parquet extra and keep the
Tushare credential in the existing `token` file:

```bash
python -m pip install -e '.[daily-lake]'
regimebeacon daily sync --date 2026-09-30
regimebeacon daily backfill --start-date 2026-09-25 --end-date 2026-09-30
regimebeacon daily status --date 2026-09-30
regimebeacon daily history --symbol 002409.SZ --start-date 2026-09-30 --end-date 2026-09-30
regimebeacon daily summary --date 2026-09-30
```

`sync` uses the configured pool plus any private holdings. It makes four
date-wide Tushare requests: stock `daily`, ETF `fund_daily`, stock `adj_factor`,
and ETF `fund_adj`. It requires a completed historical date, or 17:30 local time
for today's date so that the later ETF factor feed can finish. Repeating a
complete date with the same member set is idempotent. Use `--refresh` to pick
up revised factors or prices. An ETF factor permission error fails the sync;
there is no synthetic 1.0 factor fallback.
`backfill` first checks the SSE Tushare trading calendar and skips closed
dates. It is resumable by complete partition and capped at 30 trading days per
invocation by default; increase `--max-trading-days` deliberately for longer
ranges. The unattended runtime starts the same sync at 17:30 on trading days,
retries failures until 23:50, and records the result under `data/reports/runtime/`.

The immutable Parquet objects live in separate
`data/lake/tushare_daily/dataset=price|factor/trade_date=YYYY-MM-DD/` trees.
Prices are **unadjusted**. `vol` remains in Tushare hands and `amount` in
thousand yuan; `adj_factor` is only in the factor dataset. No query silently
applies an adjustment. SQLite `daily_lake_partition` stores the active object
path, SHA-256, row/expected counts, member-set fingerprint and missing symbols.
Only `complete` partitions are visible to DuckDB; partial dates remain
auditable in `daily status`. Query-time checksums detect damaged files. On
refresh, a new content-addressed object is written before the SQLite pointer
changes, so a failed refresh retains the last complete version. Old inactive
objects are retained for audit and are not automatically deleted.

The `daily summary` breadth and turnover figures describe **only this configured
pool and holdings**, not the full A-share market. A missing price row may reflect
a suspension; until explicitly reconciled it is marked partial, never filled
with a guessed bar. These daily files neither replace nor delete real-time
snapshots, minute bars or the earlier Sina cache.
Tushare's stock `pre_close` is the ex-rights reference close and `pct_chg` uses
that reference, so `pct_chg` must not be reconstructed blindly from two raw
`close` values around corporate actions.

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
