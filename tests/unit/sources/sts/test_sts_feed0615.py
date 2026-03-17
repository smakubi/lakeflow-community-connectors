import json
from pathlib import Path

from databricks.labs.community_connector.sources.sts import StsLakeflowConnect


CONFIG_PATH = Path(__file__).parent / "configs" / "dev_config.json"
MATCH_ID = "MLS-MAT-00067J"


def _make_connector() -> StsLakeflowConnect:
    return StsLakeflowConnect(json.loads(CONFIG_PATH.read_text()))


def test_feed0615_advanced_events_raw_are_readable():
    connector = _make_connector()

    records, _ = connector.read_table("advanced_events_raw", {}, {"match_id": MATCH_ID})
    rows = list(records)

    assert rows
    assert rows[0]["match_id"] == MATCH_ID
    assert rows[0]["data_status"] == "postmatch"
    assert rows[0]["event_type"]
    assert rows[0]["event_id"]
    assert rows[0]["team_id"]
    assert rows[0]["event_attributes_json"]
    assert rows[0]["event_xml"]
    assert rows[0]["source_service_id"] == "Feed-06.15-AdvancedEvents"
