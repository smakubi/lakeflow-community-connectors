import json
from pathlib import Path

from databricks.labs.community_connector.sources.sts import StsLakeflowConnect


CONFIG_PATH = Path(__file__).parent / "configs" / "dev_config.json"
MATCH_ID = "MLS-MAT-00067J"


def _make_connector() -> StsLakeflowConnect:
    return StsLakeflowConnect(json.loads(CONFIG_PATH.read_text()))


def test_feed0604_player_topspeed_alerts_are_readable():
    connector = _make_connector()

    records, _ = connector.read_table("player_topspeed_alerts", {}, {"match_id": MATCH_ID})
    rows = list(records)

    assert rows
    assert rows[0]["match_id"] == MATCH_ID
    assert rows[0]["data_status"] == "postmatch"
    assert rows[0]["change_scope_type"] == "1"
    assert rows[0]["player_id"]
    assert rows[0]["player_first_name"]
    assert rows[0]["team_id"]
    assert rows[0]["maximum_speed"]
    assert rows[0]["competition_record"] in {"true", "false"}
    assert rows[0]["source_service_id"] == "Feed-06.04-Player-Topspeed-Alert"


def test_feed0604_player_topspeed_alert_rankings_are_readable():
    connector = _make_connector()

    records, _ = connector.read_table(
        "player_topspeed_alert_rankings", {}, {"match_id": MATCH_ID}
    )
    rows = list(records)

    assert rows
    assert rows[0]["match_id"] == MATCH_ID
    assert rows[0]["ranking_type"]
    assert rows[0]["rank"]
    assert rows[0]["absolute"]
    assert rows[0]["player_id"]
    assert rows[0]["player_first_name"]
    assert rows[0]["team_id"]
    assert rows[0]["minute_of_play"]
    assert rows[0]["source_service_id"] == "Feed-06.04-Player-Topspeed-Alert"
