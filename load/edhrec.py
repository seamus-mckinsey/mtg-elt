import argparse
import functools
from pathlib import Path
from typing import Any

import dlt
import duckdb
import requests
from pyedhrec import EDHRec
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

DB_DIR = Path(__file__).parent.parent / "db"
SCRYFALL_DB = DB_DIR / "scryfall.db"
EDHREC_DB = DB_DIR / "edhrec.db"


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(),
    retry=retry_if_exception_type(requests.exceptions.HTTPError),
)
def call_edhrec_api(edhrec_client, func_name, card_name) -> dict[str, Any]:
    """Calls the specified EDHRec API function with retry logic."""
    return getattr(edhrec_client, func_name)(card_name)


@functools.lru_cache(maxsize=1)
def _enrich_commanders() -> tuple[dict, ...]:
    """Read oracle_cards from local DuckDB and enrich commanders via EDHRec API.

    Cached so API calls only happen once when writing to multiple destinations.
    """
    edhrec_client = EDHRec()

    conn = duckdb.connect(str(SCRYFALL_DB), read_only=True)
    result = conn.execute("SELECT * FROM scryfall.oracle_cards")
    col_names = [desc[0] for desc in result.description]
    rows = result.fetchall()
    conn.close()

    oracle_cards = [dict(zip(col_names, row)) for row in rows]

    enrichment_functions = [
        "get_commander_data",
        "get_new_cards",
        "get_high_synergy_cards",
        "get_top_cards",
        "get_top_creatures",
        "get_top_instants",
        "get_top_sorceries",
        "get_top_enchantments",
        "get_top_artifacts",
        "get_top_mana_artifacts",
        "get_top_planeswalkers",
        "get_top_utility_lands",
        "get_top_lands",
    ]

    results = []
    processed = 0

    for card_data in oracle_cards:
        card_details = card_data.get("card_details")
        is_commander = card_details.get("commander", False) if card_details else False

        if is_commander:
            card_name = card_data["name"]
            print(f"Fetching additional commander data for: {card_name}")
            for func_name in enrichment_functions:
                field_name = func_name.replace("get_", "")
                card_data[field_name] = call_edhrec_api(
                    edhrec_client, func_name, card_name
                )
            processed += 1
            print(f"Enriched {card_name}. Processed {processed} commanders")
            results.append(card_data)

    print(f"Finished enriching {processed} commanders.")
    return tuple(results)


@dlt.source
def edhrec():
    """Source that enriches commander cards with EDHRec data."""

    @dlt.resource(
        name="commanders",
        primary_key="oracle_id",
        write_disposition="replace",
    )
    def commanders():
        yield list(_enrich_commanders())

    return commanders


def run_enrichment_pipeline(destination: str = "both") -> None:
    """Run the pipeline that enriches card data."""
    pipelines = []

    if destination in ("local", "both"):
        pipelines.append(
            dlt.pipeline(
                pipeline_name="edhrec_data_local",
                destination=dlt.destinations.duckdb(str(EDHREC_DB)),
                dataset_name="edhrec",
            )
        )

    if destination in ("cloud", "both"):
        pipelines.append(
            dlt.pipeline(
                pipeline_name="edhrec_data_cloud",
                destination="motherduck",
                dataset_name="edhrec",
            )
        )

    for pipeline in pipelines:
        print(f"Running {pipeline.pipeline_name}...")
        load_info = pipeline.run(edhrec())
        print(load_info)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load EDHRec commander enrichment data."
    )
    parser.add_argument(
        "--destination",
        choices=["local", "cloud", "both"],
        default="both",
        help="Where to write: local DuckDB, MotherDuck cloud, or both (default: both)",
    )
    args = parser.parse_args()
    run_enrichment_pipeline(destination=args.destination)
