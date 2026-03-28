# mtg-elt

An ELT pipeline for collecting and transforming Magic: The Gathering card data. Extracts card data from Scryfall and EDHRec, loads it into DuckDB (local) and MotherDuck (cloud), and transforms it with dbt.

## Overview

**Data sources:**
- **Scryfall** — bulk Oracle card data (names, types, mana costs, legalities, sets, pricing ranks, etc.)
- **EDHRec** — Commander-format analytics (synergy cards, top creatures/spells/lands, commander-specific recommendations)

**Pipeline flow:**

```
Scryfall API  ──► load/scryfall.py ──► db/scryfall.db (DuckDB) + MotherDuck (scryfall dataset)
                                                  │
                                       db/scryfall.db
                                                  │
EDHRec API    ──► load/edhrec.py ───► db/edhrec.db (DuckDB) + MotherDuck (edhrec dataset)
                                                  │
                                        transform/ (dbt)
```

Cards are filtered to those **legal in Commander** format and available in **paper** (no digital-only cards).

## Tech Stack

| Layer | Tool |
|---|---|
| Language | Python 3.12 |
| Extract & Load | [dlt](https://dlthub.com/) |
| Transform | [dbt](https://www.getdbt.com/) + [dbt-duckdb](https://github.com/duckdb/dbt-duckdb) |
| Local DB | [DuckDB](https://duckdb.org/) |
| Cloud DB | [MotherDuck](https://motherduck.com/) |
| Package manager | [uv](https://github.com/astral-sh/uv) |

## Setup

**Prerequisites:** Python 3.12+, [uv](https://github.com/astral-sh/uv)

```bash
uv sync
```

**Credentials:** dlt reads secrets from `.dlt/secrets.toml`. Create it with your MotherDuck token:

```toml
[destination.motherduck]
credentials = "md:?motherduck_token=<your_token>"
```

## Running the Pipelines

**1. Load Scryfall card data**

```bash
python load/scryfall.py
```

Fetches the Oracle Cards bulk export from Scryfall, filters to Commander-legal paper cards, and loads them to both `scryfall.db` (local) and MotherDuck (`scryfall.oracle_cards`). The bulk download is cached so it only happens once per process run.

**2. Load EDHRec enrichment data**

```bash
python load/edhrec.py                        # write to both local and MotherDuck (default)
python load/edhrec.py --destination local    # local DuckDB only (no credentials needed)
python load/edhrec.py --destination cloud    # MotherDuck only
```

Reads `oracle_cards` from local `db/scryfall.db`, identifies commanders, and enriches each with EDHRec data: commander stats, high-synergy cards, top cards by type (creatures, instants, sorceries, enchantments, artifacts, lands, planeswalkers), and utility lands. Results load to `db/edhrec.db` and/or MotherDuck (`edhrec.commanders`). API calls use retry logic with exponential backoff. Enrichment runs once and is cached, so writing to both destinations doesn't double the API calls.

**3. Transform with dbt**

```bash
cd transform/
dbt run
dbt test
```

## Project Structure

```
load/
  scryfall.py      # Scryfall bulk data extraction
  edhrec.py        # EDHRec Commander enrichment
transform/         # dbt project (mtg_elt profile)
  models/
  tests/
  seeds/
  macros/
  snapshots/
main.py            # Entry point placeholder
pyproject.toml     # Project dependencies
```

## Data Schema

**`scryfall.oracle_cards`** (one row per unique Oracle card legal in Commander)

| Column | Description |
|---|---|
| oracle_id | Unique identifier (primary key) |
| name | Card name |
| mana_cost / cmc | Mana cost and converted mana cost |
| type_line | Full type line |
| oracle_text | Rules text |
| power / toughness | P/T for creatures |
| rarity | common / uncommon / rare / mythic |
| set / set_name / set_type | Set info |
| released_at | Release date |
| artist | Illustrator |
| edhrec_rank | Community popularity rank |

**`edhrec.commanders`** (one row per Commander-eligible card, enriched)

Includes all base card fields plus nested EDHRec data: `commander_data`, `high_synergy_cards`, `top_creatures`, `top_instants`, `top_sorceries`, `top_enchantments`, `top_artifacts`, `top_mana_artifacts`, `top_planeswalkers`, `top_utility_lands`, `top_lands`, `new_cards`.
