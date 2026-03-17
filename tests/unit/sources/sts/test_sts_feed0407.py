import json
from pathlib import Path

from databricks.labs.community_connector.sources.sts import StsLakeflowConnect


CONFIG_PATH = Path(__file__).parent / "configs" / "dev_config.json"
MATCH_ID = "MLS-MAT-00067J"


def _make_connector() -> StsLakeflowConnect:
    return StsLakeflowConnect(json.loads(CONFIG_PATH.read_text()))


def test_feed0407_team_statistics_are_readable():
    connector = _make_connector()

    records, _ = connector.read_table("positional_match_team_statistics", {}, {"match_id": MATCH_ID})
    rows = list(records)

    assert rows
    assert rows[0]["match_id"] == MATCH_ID
    assert rows[0]["scope"] == "match"
    assert rows[0]["data_status"] == "postmatch"
    assert rows[0]["team_id"]
    assert rows[0]["team_name"]
    assert rows[0]["distance_covered"]
    assert rows[0]["ball_possession_ratio"]
    assert rows[0]["sprints"]
    assert rows[0]["source_service_id"] == "Feed-04.07-PositionalData-Match-Statistics"


def test_feed0407_player_statistics_are_readable():
    connector = _make_connector()

    records, _ = connector.read_table(
        "positional_match_player_statistics", {}, {"match_id": MATCH_ID}
    )
    rows = list(records)

    assert rows
    assert rows[0]["match_id"] == MATCH_ID
    assert rows[0]["scope"] == "match"
    assert rows[0]["data_status"] == "postmatch"
    assert rows[0]["team_id"]
    assert rows[0]["player_id"]
    assert rows[0]["player_first_name"]
    assert rows[0]["distance_covered"]
    assert rows[0]["average_speed"]
    assert rows[0]["sprints"]
    assert rows[0]["goalkeeper"] in {"true", "false"}
    assert rows[0]["source_service_id"] == "Feed-04.07-PositionalData-Match-Statistics"
