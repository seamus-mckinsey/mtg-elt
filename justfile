default:
    @just --list

# Install dependencies, git hooks, and dbt packages
setup:
    uv sync
    uv run pre-commit install
    cd transform && uv run dbt deps

# Load Scryfall card data
load-scryfall:
    uv run python load/scryfall.py

# Enrich commanders with EDHRec data (destination: local | cloud | both)
load-edhrec destination="both":
    uv run python load/edhrec.py --destination {{destination}}

# Run the full load pipeline
load: load-scryfall load-edhrec

# Run dbt models
transform:
    cd transform && uv run dbt run

# Test dbt models
transform-test:
    cd transform && uv run dbt test

# Lint and format
lint:
    uv run ruff check .
    uv run ruff format .

# Run unit tests
test:
    uv run pytest

# Run all pre-commit checks
check:
    uv run pre-commit run --all-files
