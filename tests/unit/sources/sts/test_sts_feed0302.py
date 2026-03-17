import json
from pathlib import Path

from databricks.labs.community_connector.sources.sts import StsLakeflowConnect


CONFIG_PATH = Path(__file__).parent / "configs" / "dev_config.json"
MATCH_ID = "MLS-MAT-000532"


def _make_connector() -> StsLakeflowConnect:
    return StsLakeflowConnect(json.loads(CONFIG_PATH.read_text()))


def test_feed0302_raw_messages_are_readable():
    connector = _make_connector()

    records, _ = connector.read_table("event_raw_messages", {}, {"match_id": MATCH_ID})
    rows = list(records)

    assert rows
    assert rows[0]["match_id"] == MATCH_ID
    assert rows[0]["event_id"]
    assert rows[0]["event_time"]
    assert rows[0]["event_type"]
    assert rows[0]["data_status"]
    assert rows[0]["event_attributes_json"]
    assert rows[0]["event_xml"]
    assert rows[0]["source_service_id"] == "Feed-03.02-EventData-Match-Raw_Postmatch"


def test_feed0302_delete_events_are_readable():
    connector = _make_connector()

    records, _ = connector.read_table("event_raw_delete_events", {}, {"match_id": MATCH_ID})
    rows = list(records)

    assert rows
    assert rows[0]["match_id"] == MATCH_ID
    assert rows[0]["event_id"]
    assert rows[0]["event_time"]
    assert rows[0]["data_status"]
    assert rows[0]["event_xml"]
    assert rows[0]["source_service_id"] == "Feed-03.02-EventData-Match-Raw_Postmatch"
