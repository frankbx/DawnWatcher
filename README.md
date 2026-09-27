# DawnWatcher

DawnWatcher is a deterministic, auditable trading-assistance platform. It will combine
dual-source intraday market monitoring, post-close data workflows, Feishu notifications,
and narrowly scoped language-model agents.

The project is currently at **Phase 1: durable local foundation**. It includes an
Alembic-managed SQLite schema, workflow job states, a transactional notification outbox,
append-only audit events, crash recovery, and verified online backups. Market data,
strategies, external notification delivery, and agents will be implemented in later phases.

## Requirements

- Python 3.12
- A local filesystem for runtime data

## Development setup

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
```

Copy `.env.example` to `.env` only when local overrides are needed. Defaults are safe for
development.

## Commands

```bash
dawnwatcher --help
dawnwatcher doctor
dawnwatcher config
dawnwatcher db upgrade
dawnwatcher db check
dawnwatcher db recover
dawnwatcher db backup
```

## Quality checks

```bash
ruff check .
ruff format --check .
mypy
pytest
```

Runtime files belong under `data/` and are intentionally excluded from version control.

SQLite runs in WAL mode with full synchronous durability, foreign-key enforcement, a busy
timeout, and Alembic-managed migrations. Do not place the database on a network filesystem.
Backup destinations are immutable: an existing backup file will never be overwritten.
