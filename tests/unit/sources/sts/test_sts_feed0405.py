import json
from pathlib import Path

from databricks.labs.community_connector.sources.sts import StsLakeflowConnect


CONFIG_PATH = Path(__file__).parent / "configs" / "dev_config.json"
MATCH_ID = "MLS-MAT-00067J"


def _make_connector() -> StsLakeflowConnect:
    return StsLakeflowConnect(json.loads(CONFIG_PATH.read_text()))


def test_feed0405_interval_definitions_are_readable():
    connector = _make_connector()

    records, _ = connector.read_table("speed_interval_definitions", {}, {"match_id": MATCH_ID})
    rows = list(records)

    assert rows
    assert rows[0]["match_id"] == MATCH_ID
    assert rows[0]["stat_type"] == "interval"
    assert rows[0]["interval_id"]
    assert rows[0]["from_value"]
    assert rows[0]["from_interpretation"]
    assert rows[0]["to_value"]
    assert rows[0]["to_interpretation"]
    assert rows[0]["source_service_id"] == "Feed-04.05-SpeedIntervals-Match"


def test_feed0405_player_intervals_are_readable():
    connector = _make_connector()

    records, _ = connector.read_table("speed_interval_player_stats", {}, {"match_id": MATCH_ID})
    rows = list(records)

    assert rows
    assert rows[0]["match_id"] == MATCH_ID
    assert rows[0]["stat_type"] == "interval"
    assert rows[0]["event_time"]
    assert rows[0]["person_id"]
    assert rows[0]["interval_id"]
    assert rows[0]["count"]
    assert rows[0]["distance_covered"]
    assert rows[0]["source_service_id"] == "Feed-04.05-SpeedIntervals-Match"
