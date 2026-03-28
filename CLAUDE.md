# CLAUDE.md — mtg-elt

This file provides guidance for AI assistants working in this repository.

## Project Overview

**mtg-elt** is an ELT (Extract, Load, Transform) pipeline for Magic: The Gathering card data, focused on the Commander format. It fetches card data from the Scryfall API, enriches it with EDHRec commander statistics, and loads everything into DuckDB (local) and MotherDuck (cloud). A dbt project handles downstream transformations.

## Repository Structure

```
mtg-elt/
├── load/                     # Data extraction and loading (dlt pipelines)
│   ├── scryfall.py           # Scryfall bulk API → DuckDB/MotherDuck
│   └── edhrec.py             # EDHRec API enrichment for commanders
├── transform/                # dbt project for data transformation
│   ├── dbt_project.yml       # dbt project config (profile: mtg_elt)
│   ├── models/               # dbt models (currently empty)
│   ├── tests/                # dbt tests (currently empty)
│   ├── seeds/                # Static seed data
│   ├── macros/               # dbt macros
│   ├── snapshots/            # dbt snapshots
│   └── analyses/             # Ad-hoc analyses
├── main.py                   # Entry point placeholder
├── sql_database_pipeline.py  # Reference/template DLT SQL examples (not in use)
├── pyproject.toml            # Project metadata and dependencies
├── uv.lock                   # Locked dependency versions
└── .pre-commit-config.yaml   # Pre-commit hooks
```

## Data Flow

```
Scryfall API
    │
    ▼
load/scryfall.py  ──(dlt)──►  scryfall.oracle_cards  (MotherDuck + local scryfall.db)
                                        │
                                        ▼
                          load/edhrec.py  ──(dlt)──►  edhrec.commanders  (MotherDuck)
                                        │
                                        ▼
                          transform/ (dbt)  ──►  Transformed models (not yet built)
```

**Key data decisions:**
- Scryfall data is filtered to Commander-legal cards with paper printings only
- `oracle_id` is the primary key (deduplicated across printings)
- Scryfall data is fetched once per run via `functools.lru_cache` and shared across both pipelines
- Both pipelines use `write_disposition="replace"` (full refresh on each run)

## Tech Stack

| Tool | Purpose |
|------|---------|
| Python 3.12 | Runtime |
| [dlt](https://dlthub.com/) | Data loading framework |
| DuckDB | Local analytical database |
| MotherDuck | Cloud-hosted DuckDB |
| dbt + dbt-duckdb | SQL transformation layer |
| ruff | Linting and formatting |
| pre-commit | Git hook enforcement |
| tenacity | Retry logic for API calls |
| pyedhrec | EDHRec API client |
| uv | Package and environment management |

## Development Setup

```bash
# Install dependencies
uv sync

# Install pre-commit hooks
uv run pre-commit install

# Set up MotherDuck credentials in .dlt/secrets.toml
# (see dlt docs for secrets management)
```

### Secrets

dlt reads secrets from `.dlt/secrets.toml` (gitignored). MotherDuck credentials must be configured there:

```toml
[destination.motherduck]
credentials = "md:?motherduck_token=<YOUR_TOKEN>"
```

## Running the Pipelines

```bash
# Run the Scryfall load (fetches cards, loads to MotherDuck + local DuckDB)
uv run python load/scryfall.py

# Run the EDHRec enrichment (reads from MotherDuck, enriches commanders)
uv run python load/edhrec.py
```

## dbt (Transform Layer)

The dbt project is named `mtg_elt` and uses the `mtg_elt` profile. No models exist yet — this is the primary area for future development.

```bash
# Run from the transform/ directory
cd transform
dbt run
dbt test
```

## Code Conventions

### Python

- **Formatter/Linter**: `ruff` — enforced via pre-commit. Run manually with `uv run ruff check . && uv run ruff format .`
- **Type hints**: Use throughout. Return types required on all functions.
- **Docstrings**: Required on all public functions and classes.
- **Imports**: Standard library first, then third-party, then local — ruff enforces this.
- **No commits to `main`**: enforced by pre-commit hook `no-commit-to-branch`.

### dlt Patterns

- Sources are decorated with `@dlt.source`, resources with `@dlt.resource`
- Always specify `primary_key` and `write_disposition` on resources
- Use `dlt.secrets.get()` for credential access — never hardcode credentials
- Each pipeline target (MotherDuck, local DuckDB) gets its own `dlt.pipeline()` instance

### dbt Patterns

- Models go in `transform/models/`
- Follow the standard dbt layering: staging → intermediate → marts
- Use snake_case for all model names, column names, and file names
- Default materialization is `view` (set in `dbt_project.yml`); override per model with `{{ config(materialized='table') }}`

## Pre-commit Hooks

The following checks run automatically on `git commit`:

- `ruff` — Python linting
- `ruff-format` — Python formatting
- `no-commit-to-branch` — blocks commits directly to `main`
- `check-yaml`, `check-json`, `check-toml` — config file validation
- `check-ast` — Python syntax validation
- `debug-statements` — blocks accidental `pdb`/`breakpoint()` commits
- `end-of-file-fixer`, `trailing-whitespace` — whitespace hygiene
- `check-merge-conflict` — blocks unresolved merge conflict markers

To run all hooks manually: `uv run pre-commit run --all-files`

## Key Files to Understand

| File | Why it matters |
|------|---------------|
| `load/scryfall.py` | Core data ingestion — understand `_process_scryfall_cards()` and the dual-pipeline pattern |
| `load/edhrec.py` | Enrichment logic — note the dlt resource dependency chain (`get_commander_specific_data` depends on `get_all_cards_data`) |
| `transform/dbt_project.yml` | dbt project root config |
| `pyproject.toml` | All dependencies defined here; use `uv` to manage |

## Known Gaps / TODOs

- `transform/models/` is empty — dbt models have not been written yet
- No tests exist (neither pytest nor dbt tests)
- No CI/CD pipeline configured
- `main.py` is a placeholder with no real logic
- `sql_database_pipeline.py` contains example/template code that is not integrated into the project
- EDHRec enrichment has a TODO to define the source schema using Pydantic

## What NOT to Do

- Do not commit directly to `main` (blocked by pre-commit)
- Do not hardcode MotherDuck tokens or other secrets
- Do not add new dependencies without updating `uv.lock` via `uv add <package>`
- Do not modify `uv.lock` manually
- Do not use `pip install` — always use `uv`
- Do not remove the `lru_cache` from `_process_scryfall_cards()` — it prevents double-fetching the large Scryfall bulk dataset
