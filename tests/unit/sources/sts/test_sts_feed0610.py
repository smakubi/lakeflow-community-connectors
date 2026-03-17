import json
from pathlib import Path

from databricks.labs.community_connector.sources.sts import StsLakeflowConnect


CONFIG_PATH = Path(__file__).parent / "configs" / "dev_config.json"
MATCH_ID = "MLS-MAT-00067J"


def _make_connector() -> StsLakeflowConnect:
    return StsLakeflowConnect(json.loads(CONFIG_PATH.read_text()))


def test_feed0610_win_probability_by_minute_is_readable():
    connector = _make_connector()

    records, _ = connector.read_table("win_probability_by_minute", {}, {"match_id": MATCH_ID})
    rows = list(records)

    assert rows
    assert rows[0]["match_id"] == MATCH_ID
    assert rows[0]["data_status"] == "postmatch"
    assert rows[0]["scope"] == "Match"
    assert rows[0]["minute_of_play"]
    assert rows[0]["home_team_win_probability"]
    assert rows[0]["guest_team_win_probability"]
    assert rows[0]["draw_probability"]
    assert rows[0]["source_service_id"] == "Feed-06.10-WinProbability"
