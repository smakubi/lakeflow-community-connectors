import json
from pathlib import Path

from databricks.labs.community_connector.sources.sts import StsLakeflowConnect


CONFIG_PATH = Path(__file__).parent / "configs" / "dev_config.json"
MATCH_ID = "MLS-MAT-00067J"


def _make_connector() -> StsLakeflowConnect:
    return StsLakeflowConnect(json.loads(CONFIG_PATH.read_text()))


def test_feed0605_attacking_zone_entries_are_readable():
    connector = _make_connector()

    records, _ = connector.read_table("attacking_zone_entries", {}, {"match_id": MATCH_ID})
    rows = list(records)

    assert rows
    assert rows[0]["match_id"] == MATCH_ID
    assert rows[0]["data_status"] == "postmatch"
    assert rows[0]["team_id"]
    assert rows[0]["team_name"]
    assert rows[0]["team_role"] in {"home", "guest"}
    assert rows[0]["team_entries"]
    assert rows[0]["zone_id"] in {"1", "2", "3", "4"}
    assert rows[0]["entries"]
    assert rows[0]["ratio"]
    assert rows[0]["source_service_id"] == "Feed-06.05-AttackingZones"
