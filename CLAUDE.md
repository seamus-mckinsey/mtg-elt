# CLAUDE.md

## Architecture

ELT pipeline for Magic: The Gathering Commander data.

```
Scryfall API → load/scryfall.py (dlt) → DuckDB / MotherDuck (scryfall.oracle_cards)
                                                  ↓
               load/edhrec.py (dlt)  → MotherDuck (edhrec.commanders)
                                                  ↓
               transform/ (dbt)      → Transformed models (not yet built)
```

## Tech Stack

- **dlt** — data loading into DuckDB/MotherDuck
- **DuckDB** — local analytical database; **MotherDuck** — cloud DuckDB
- **dbt** (dbt-duckdb) — SQL transformation layer
- **uv** — package and environment management
- **ruff** — linting and formatting

## Commands

```bash
uv sync                            # install dependencies
uv run pre-commit install          # set up git hooks

uv run python load/scryfall.py     # load cards from Scryfall API
uv run python load/edhrec.py       # enrich commanders with EDHRec data

cd transform && dbt run            # run dbt models
cd transform && dbt test           # run dbt tests

uv run ruff check . && uv run ruff format .   # lint and format
uv run pre-commit run --all-files             # run all pre-commit checks
```
