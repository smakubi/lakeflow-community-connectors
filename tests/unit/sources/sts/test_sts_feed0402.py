import json
from pathlib import Path

from databricks.labs.community_connector.sources.sts import StsLakeflowConnect


CONFIG_PATH = Path(__file__).parent / "configs" / "dev_config.json"
MATCH_ID = "MLS-MAT-000532"


def _make_connector() -> StsLakeflowConnect:
    return StsLakeflowConnect(json.loads(CONFIG_PATH.read_text()))


def test_feed0402_pitch_metadata_is_readable():
    connector = _make_connector()

    records, _ = connector.read_table("tracking_pitch_metadata", {}, {"match_id": MATCH_ID})
    record = next(records)

    assert record["match_id"] == MATCH_ID
    assert record["metadata_type"] == "pitch-size"
    assert record["pitch_x"]
    assert record["pitch_y"]
    assert record["source_service_id"] == "Feed-04.02-PositionalData-Match-Raw_Postmatch"


def test_feed0402_postmatch_framesets_are_readable():
    connector = _make_connector()

    records, _ = connector.read_table(
        "tracking_postmatch_framesets",
        {},
        {"match_id": MATCH_ID, "max_records_per_batch": "1"},
    )
    record = next(records)

    assert record["match_id"] == MATCH_ID
    assert record["game_section"]
    assert record["team_id"]
    assert record["person_id"]
    assert record["event_time"]
    assert record["frame_count"]
    assert record["frames_json"]
    assert record["source_service_id"] == "Feed-04.02-PositionalData-Match-Raw_Postmatch"
