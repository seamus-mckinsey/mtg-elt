"""Tests for scryfall loader."""

import pytest

from load.scryfall import get_default_cards_uri


def test_get_default_cards_uri_returns_uri():
    bulk_data = {
        "data": [
            {"name": "Oracle Cards", "download_uri": "https://example.com/oracle.json"},
            {"name": "All Cards", "download_uri": "https://example.com/all.json"},
        ]
    }
    assert get_default_cards_uri(bulk_data) == "https://example.com/oracle.json"


def test_get_default_cards_uri_missing_raises():
    bulk_data = {
        "data": [{"name": "All Cards", "download_uri": "https://example.com/all.json"}]
    }
    with pytest.raises(ValueError, match="Oracle Cards data not found"):
        get_default_cards_uri(bulk_data)


def test_get_default_cards_uri_empty_data():
    with pytest.raises(ValueError, match="Oracle Cards data not found"):
        get_default_cards_uri({"data": []})
