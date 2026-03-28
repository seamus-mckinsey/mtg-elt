"""Tests for edhrec loader."""

from unittest.mock import MagicMock, patch

import pytest
import requests
import tenacity

from load.edhrec import _enrich_commanders, call_edhrec_api, run_enrichment_pipeline


@pytest.fixture(autouse=True)
def clear_cache():
    _enrich_commanders.cache_clear()
    yield
    _enrich_commanders.cache_clear()


def _mock_db(rows, col_names):
    """Return a mock duckdb connection yielding the given rows."""
    mock_result = MagicMock()
    mock_result.description = [(col,) for col in col_names]
    mock_result.fetchall.return_value = rows
    mock_conn = MagicMock()
    mock_conn.execute.return_value = mock_result
    return mock_conn


# ---------------------------------------------------------------------------
# call_edhrec_api
# ---------------------------------------------------------------------------


def test_call_edhrec_api_returns_result():
    client = MagicMock()
    client.get_commander_data.return_value = {"top_cards": []}
    result = call_edhrec_api(client, "get_commander_data", "Atraxa")
    assert result == {"top_cards": []}
    client.get_commander_data.assert_called_once_with("Atraxa")


def test_call_edhrec_api_retries_on_http_error():
    client = MagicMock()
    client.get_commander_data.side_effect = [
        requests.exceptions.HTTPError(),
        {"top_cards": []},
    ]
    result = call_edhrec_api(client, "get_commander_data", "Atraxa")
    assert result == {"top_cards": []}
    assert client.get_commander_data.call_count == 2


@patch("time.sleep")
def test_call_edhrec_api_raises_after_max_attempts(mock_sleep):
    client = MagicMock()
    client.get_commander_data.side_effect = requests.exceptions.HTTPError()
    # tenacity wraps exhausted retries in RetryError, not the original exception
    with pytest.raises(tenacity.RetryError):
        call_edhrec_api(client, "get_commander_data", "Atraxa")
    assert client.get_commander_data.call_count == 3


# ---------------------------------------------------------------------------
# _enrich_commanders
# ---------------------------------------------------------------------------

COLS = ["oracle_id", "name", "card_details"]

COMMANDER_ROW = ("abc-123", "Atraxa, Praetors' Voice", {"commander": True})
NON_COMMANDER_ROW = ("def-456", "Lightning Bolt", {"commander": False})
NO_DETAILS_ROW = ("ghi-789", "Forest", None)


@patch("load.edhrec.duckdb.connect")
@patch("load.edhrec.EDHRec")
def test_enrich_commanders_enriches_commanders(mock_edhrec_cls, mock_connect):
    mock_connect.return_value = _mock_db([COMMANDER_ROW], COLS)
    mock_edhrec_cls.return_value = MagicMock()

    result = _enrich_commanders()

    assert len(result) == 1
    assert result[0]["name"] == "Atraxa, Praetors' Voice"


@patch("load.edhrec.duckdb.connect")
@patch("load.edhrec.EDHRec")
def test_enrich_commanders_skips_non_commanders(mock_edhrec_cls, mock_connect):
    mock_connect.return_value = _mock_db(
        [COMMANDER_ROW, NON_COMMANDER_ROW, NO_DETAILS_ROW], COLS
    )
    mock_edhrec_cls.return_value = MagicMock()

    result = _enrich_commanders()

    assert len(result) == 1
    assert result[0]["oracle_id"] == "abc-123"


@patch("load.edhrec.duckdb.connect")
@patch("load.edhrec.EDHRec")
def test_enrich_commanders_adds_all_enrichment_fields(mock_edhrec_cls, mock_connect):
    mock_connect.return_value = _mock_db([COMMANDER_ROW], COLS)
    mock_edhrec_cls.return_value = MagicMock()

    result = _enrich_commanders()

    expected_fields = {
        "commander_data",
        "new_cards",
        "high_synergy_cards",
        "top_cards",
        "top_creatures",
        "top_instants",
        "top_sorceries",
        "top_enchantments",
        "top_artifacts",
        "top_mana_artifacts",
        "top_planeswalkers",
        "top_utility_lands",
        "top_lands",
    }
    assert expected_fields.issubset(result[0].keys())


@patch("load.edhrec.duckdb.connect")
@patch("load.edhrec.EDHRec")
def test_enrich_commanders_reads_correct_table(mock_edhrec_cls, mock_connect):
    mock_connect.return_value = _mock_db([], COLS)
    mock_edhrec_cls.return_value = MagicMock()

    _enrich_commanders()

    mock_connect.return_value.execute.assert_called_once_with(
        "SELECT * FROM scryfall.oracle_cards"
    )


@patch("load.edhrec.duckdb.connect")
@patch("load.edhrec.EDHRec")
def test_enrich_commanders_result_is_cached(mock_edhrec_cls, mock_connect):
    mock_connect.return_value = _mock_db([], COLS)
    mock_edhrec_cls.return_value = MagicMock()

    _enrich_commanders()
    _enrich_commanders()

    assert mock_connect.call_count == 1
    assert mock_edhrec_cls.call_count == 1


# ---------------------------------------------------------------------------
# run_enrichment_pipeline
# ---------------------------------------------------------------------------


@patch("load.edhrec.edhrec")
@patch("load.edhrec.dlt.pipeline")
def test_run_pipeline_local_creates_local_pipeline(mock_pipeline, mock_edhrec_src):
    mock_pipeline.return_value = MagicMock()
    run_enrichment_pipeline("local")

    assert mock_pipeline.call_count == 1
    assert mock_pipeline.call_args.kwargs["pipeline_name"] == "edhrec_data_local"


@patch("load.edhrec.edhrec")
@patch("load.edhrec.dlt.pipeline")
def test_run_pipeline_cloud_creates_cloud_pipeline(mock_pipeline, mock_edhrec_src):
    mock_pipeline.return_value = MagicMock()
    run_enrichment_pipeline("cloud")

    assert mock_pipeline.call_count == 1
    assert mock_pipeline.call_args.kwargs["pipeline_name"] == "edhrec_data_cloud"


@patch("load.edhrec.edhrec")
@patch("load.edhrec.dlt.pipeline")
def test_run_pipeline_both_creates_two_pipelines(mock_pipeline, mock_edhrec_src):
    mock_pipeline.return_value = MagicMock()
    run_enrichment_pipeline("both")

    assert mock_pipeline.call_count == 2
    names = {c.kwargs["pipeline_name"] for c in mock_pipeline.call_args_list}
    assert names == {"edhrec_data_local", "edhrec_data_cloud"}


@patch("load.edhrec.edhrec")
@patch("load.edhrec.dlt.pipeline")
def test_run_pipeline_runs_each_pipeline(mock_pipeline, mock_edhrec_src):
    mock_pipe_instance = MagicMock()
    mock_pipeline.return_value = mock_pipe_instance
    run_enrichment_pipeline("both")

    assert mock_pipe_instance.run.call_count == 2
