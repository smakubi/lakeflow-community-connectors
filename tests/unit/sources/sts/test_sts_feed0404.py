import json
from pathlib import Path

from databricks.labs.community_connector.sources.sts import StsLakeflowConnect


CONFIG_PATH = Path(__file__).parent / "configs" / "dev_config.json"
MATCH_ID = "MLS-MAT-00067J"


def _make_connector() -> StsLakeflowConnect:
    return StsLakeflowConnect(json.loads(CONFIG_PATH.read_text()))


def test_feed0404_distance_match_teams_are_readable():
    connector = _make_connector()

    records, _ = connector.read_table("distance_match_teams", {}, {"match_id": MATCH_ID})
    rows = list(records)

    assert rows
    assert rows[0]["match_id"] == MATCH_ID
    assert rows[0]["stat_type"] == "performance"
    assert rows[0]["event_time"]
    assert rows[0]["team_id"]
    assert rows[0]["distance_covered"]
    assert rows[0]["source_service_id"] == "Feed-04.04-Distance-Match"


def test_feed0404_distance_match_players_are_readable():
    connector = _make_connector()

    records, _ = connector.read_table("distance_match_players", {}, {"match_id": MATCH_ID})
    rows = list(records)

    assert rows
    assert rows[0]["match_id"] == MATCH_ID
    assert rows[0]["stat_type"] == "performance"
    assert rows[0]["event_time"]
    assert rows[0]["team_id"]
    assert rows[0]["person_id"]
    assert rows[0]["distance_covered"]
    assert rows[0]["average_speed"]
    assert rows[0]["maximal_speed"]
    assert rows[0]["source_service_id"] == "Feed-04.04-Distance-Match"
