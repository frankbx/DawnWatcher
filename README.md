# DawnWatcher

DawnWatcher is a deterministic, auditable trading-assistance platform. It will combine
dual-source intraday market monitoring, post-close data workflows, Feishu notifications,
and narrowly scoped language-model agents.

The project is currently at **Phase 0: engineering foundation**. Market data, strategies,
database persistence, notifications, and agents will be implemented in later phases.

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
```

## Quality checks

```bash
ruff check .
ruff format --check .
mypy
pytest
```

Runtime files belong under `data/` and are intentionally excluded from version control.

