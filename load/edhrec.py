from collections.abc import Iterator
from typing import Any

import dlt
import requests  # Add import for requests
from dlt.sources.sql_database import sql_table
from pyedhrec import EDHRec
from tenacity import (  # Import tenacity
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(),
    retry=retry_if_exception_type(requests.exceptions.HTTPError),
)
def call_edhrec_api(edhrec_client, func_name, card_name) -> dict[str, Any]:
    """Calls the specified EDHRec API function with retry logic."""
    return getattr(edhrec_client, func_name)(card_name)


def get_data_for_one_card(
    card_data: dict[str, str],
    edhrec_client: EDHRec,
    func_names: list[str],
) -> dict[str, Any]:
    """Get data for one card."""
    card_name = card_data["name"]
    oracle_id = card_data["oracle_id"]

    card_data = {
        "name": card_name,
        "oracle_id": oracle_id,
    }

    for func_name in func_names:
        card_data[func_name] = call_edhrec_api(edhrec_client, func_name, card_name)
    return card_data


# TODO(seamus): can we define the source using Pydantic and define duckdb tables in the
# source?
@dlt.source
def edhrec() -> None:
    """Source that enriches card data with additional information."""
    edhrec_client = EDHRec()

    @dlt.resource(primary_key="oracle_id")
    def get_all_cards_data():
        """Resource that fetches basic details for each card."""
        # Use the specific sql_database source connection defined in the secrets file

        print(dlt.secrets.get("destination.motherduck.credentials"))

        oracle_cards = sql_table(
            credentials=dlt.secrets.get("destination.motherduck.credentials"),
            schema="scryfall",
            table="oracle_cards",
        )
        # show what's available in the database
        print(oracle_cards)

        yield oracle_cards

    @dlt.resource(
        name="commanders",
        primary_key="oracle_id",
        write_disposition="replace",
    )
    def get_commander_specific_data(
        card_base_data: Iterator[dict[str, Any]] = get_all_cards_data,
    ) -> Iterator[dict[str, Any]]:
        """Resource that adds commander-specific EDHREC data.

        Filtered to cards identified as commanders.
        """
        print("Processing cards to add commander-specific data...")

        # List of enrichment functions to apply only to commanders
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

        processed_commanders = 0
        # Process data yielded by get_all_cards_data
        for card_data in card_base_data:
            # Check if the card is a commander based on the details fetched previously
            # Safely check if card_details exists and is not None before accessing is_commander
            card_details = card_data.get("card_details")
            is_commander = (
                card_details.get("commander", False) if card_details else False
            )

            if is_commander:
                card_name = card_data["name"]
                print(f"Fetching additional commander data for: {card_name}")
                # For each function, create a field in the card data
                for func_name in enrichment_functions:
                    field_name = func_name.replace("get_", "")
                    # Call the API function using the helper with retry logic
                    api_result = call_edhrec_api(
                        edhrec_client,  # Use the client initialized in the source
                        func_name,
                        card_name,
                    )
                    card_data[field_name] = api_result

                processed_commanders += 1
                print(
                    f"Enriched {card_name}. Processed {processed_commanders} commanders",
                )

        print(
            f"Finished enriching {processed_commanders} commanders.",
        )
        yield card_data

    return (
        get_all_cards_data,
        get_commander_specific_data,
    )


def run_enrichment_pipeline() -> None:
    """Run the pipeline that enriches card data.

    Args:
        run_mode: Specifies which part of the pipeline to run.
                  Options: "all_cards", "commanders", "all". Defaults to "all".

    """
    # Initialize the pipeline
    pipeline = dlt.pipeline(
        pipeline_name="edhrec_data",
        destination="motherduck",
        dataset_name="edhrec",
    )

    # Run the pipeline based on the mode
    print("Running the EDHREC enrichment pipeline...")

    source = edhrec()
    load_info = pipeline.run(source)

    print(load_info)


if __name__ == "__main__":
    run_enrichment_pipeline()
