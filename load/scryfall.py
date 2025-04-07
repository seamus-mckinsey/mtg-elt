"""Load Scryfall cards into DuckDB."""

import functools
from collections.abc import Iterator
from typing import Any

import dlt
import requests


def fetch_bulk_data():
    """Fetch the bulk data information from Scryfall API."""
    response = requests.get("https://api.scryfall.com/bulk-data", timeout=10)
    return response.json()


def get_default_cards_uri(bulk_data):
    """Extract the download URI for default_cards from the bulk data."""
    for item in bulk_data.get("data", []):
        if item.get("name") == "Oracle Cards":
            return item.get("download_uri")
    raise ValueError("Oracle Cards data not found in the bulk data response")


@functools.lru_cache(maxsize=1)
def _process_scryfall_cards() -> list[dict[str, Any]]:
    """Fetch, download, and process Scryfall Oracle cards."""
    # Step 1: Fetch bulk data information
    print("Fetching bulk data information...")
    bulk_data = fetch_bulk_data()

    # Step 2: Get the download URI for default_cards
    download_uri = get_default_cards_uri(bulk_data)
    print(f"Found download URI: {download_uri}")

    # Step 3: Download the JSON data
    print("Downloading card data (this might take a while)...")
    response = requests.get(download_uri, timeout=60)
    cards_data = response.json()
    print(f"Downloaded data for {len(cards_data)} cards")

    # Step 4: extract only fields we need
    cards_data_filtered = [
        {
            "oracle_id": card.get("oracle_id"),
            "name": card.get("name"),
            "scryfall_id": card.get("id"),
            "lang": card.get("lang"),
            "released_at": card.get("released_at"),
            "uri": card.get("uri"),
            "scryfall_uri": card.get("scryfall_uri"),
            "mana_cost": card.get("mana_cost"),
            "cmc": card.get("cmc"),
            "type_line": card.get("type_line"),
            "oracle_text": card.get("oracle_text"),
            "power": card.get("power"),
            "toughness": card.get("toughness"),
            "reserved": card.get("reserved"),
            "game_changer": card.get("game_changer"),
            "foil": card.get("foil"),
            "nonfoil": card.get("nonfoil"),
            "oversized": card.get("oversized"),
            "promo": card.get("promo"),
            "reprint": card.get("reprint"),
            "variation": card.get("variation"),
            "set_id": card.get("set_id"),
            "set": card.get("set"),
            "set_name": card.get("set_name"),
            "set_type": card.get("set_type"),
            "set_uri": card.get("set_uri"),
            "set_search_uri": card.get("set_search_uri"),
            "scryfall_set_uri": card.get("scryfall_set_uri"),
            "rulings_uri": card.get("rulings_uri"),
            "prints_search_uri": card.get("prints_search_uri"),
            "collector_number": card.get("collector_number"),
            "digital": card.get("digital"),
            "rarity": card.get("rarity"),
            "artist": card.get("artist"),
            "story_spotlight": card.get("story_spotlight"),
            "edhrec_rank": card.get("edhrec_rank"),
        }
        for card in cards_data
        if card.get("legalities", {}).get("commander") == "legal"
        and "paper" in card.get("games")
    ]
    return cards_data_filtered


@dlt.source
def scryfall_source():
    """Define the Scryfall data source."""

    @dlt.resource(
        write_disposition="replace",
        primary_key="oracle_id",
        table_name="oracle_cards",
    )
    def oracle_cards_md() -> Iterator[dict[str, Any]]:
        """Resource that extracts Oracle cards from Scryfall bulk data for MotherDuck."""
        yield _process_scryfall_cards()

    @dlt.resource(
        write_disposition="replace",
        primary_key="oracle_id",
        table_name="oracle_cards",
    )
    def oracle_cards_local() -> Iterator[dict[str, Any]]:
        """Resource that extracts Oracle cards from Scryfall bulk data for local."""
        yield _process_scryfall_cards()

    return oracle_cards_md, oracle_cards_local


def run_pipeline():
    """Run the DLT pipeline to load Scryfall card names into DuckDB."""
    # Initialize the pipeline with DuckDB destination
    md_pipeline = dlt.pipeline(
        pipeline_name="scryfall_cards_md",
        destination="motherduck",
        dataset_name="scryfall",
    )

    local_pipeline = dlt.pipeline(
        pipeline_name="scryfall_cards_local",
        destination=dlt.destinations.duckdb("scryfall.db"),
    )

    # Instantiate the source once
    source = scryfall_source()

    # Load the data using the same source instance for both pipelines
    # The caching on _process_scryfall_cards ensures data is fetched only once
    print("Running MotherDuck pipeline...")
    load_info_md = md_pipeline.run(source)
    print("Running Local DuckDB pipeline...")
    load_info_local = local_pipeline.run(source)

    print(f"MotherDuck pipeline run completed. Load info: {load_info_md}")
    print(f"Local pipeline run completed. Load info: {load_info_local}")


if __name__ == "__main__":
    run_pipeline()
