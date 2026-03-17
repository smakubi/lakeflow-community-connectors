import json
from pathlib import Path

from databricks.labs.community_connector.sources.sts import StsLakeflowConnect


CONFIG_PATH = Path(__file__).parent / "configs" / "dev_config.json"
MATCH_ID = "MLS-MAT-000532"


def _make_connector() -> StsLakeflowConnect:
    return StsLakeflowConnect(json.loads(CONFIG_PATH.read_text()))


def test_feed0302_var_notifications_are_readable():
    connector = _make_connector()

    records, _ = connector.read_table("event_raw_var_notifications", {}, {"match_id": MATCH_ID})
    rows = list(records)

    assert rows
    assert rows[0]["match_id"] == MATCH_ID
    assert rows[0]["event_id"]
    assert rows[0]["incident"]
    assert rows[0]["review"]
    assert rows[0]["decision"]
    assert rows[0]["status"]
    assert rows[0]["source_service_id"] == "Feed-03.02-EventData-Match-Raw_Postmatch"


def test_feed0302_play_events_are_readable():
    connector = _make_connector()

    records, _ = connector.read_table("event_raw_play_events", {}, {"match_id": MATCH_ID})
    rows = list(records)

    assert rows
    assert rows[0]["match_id"] == MATCH_ID
    assert rows[0]["team_id"]
    assert rows[0]["player_id"]
    assert rows[0]["ball_possession_phase"]
    assert rows[0]["evaluation"]
    assert rows[0]["play_subtype"]
    assert rows[0]["play_subtype_attributes_json"]
    assert rows[0]["source_service_id"] == "Feed-03.02-EventData-Match-Raw_Postmatch"


def test_feed0302_foul_events_are_readable():
    connector = _make_connector()

    records, _ = connector.read_table("event_raw_foul_events", {}, {"match_id": MATCH_ID})
    rows = list(records)

    assert rows
    assert rows[0]["match_id"] == MATCH_ID
    assert rows[0]["team_fouler_id"]
    assert rows[0]["fouler_player_id"]
    assert rows[0]["foul_type"]
    assert rows[0]["source_service_id"] == "Feed-03.02-EventData-Match-Raw_Postmatch"


def test_feed0302_shot_at_goal_events_are_readable():
    connector = _make_connector()

    records, _ = connector.read_table("event_raw_shot_at_goal_events", {}, {"match_id": MATCH_ID})
    rows = list(records)

    assert rows
    assert rows[0]["match_id"] == MATCH_ID
    assert rows[0]["team_id"]
    assert rows[0]["player_id"]
    assert rows[0]["type_of_shot"]
    assert rows[0]["shot_outcome_type"]
    assert rows[0]["shot_outcome_attributes_json"]
    assert rows[0]["x_position"]
    assert rows[0]["source_service_id"] == "Feed-03.02-EventData-Match-Raw_Postmatch"


def test_feed0302_substitution_events_are_readable():
    connector = _make_connector()

    records, _ = connector.read_table("event_raw_substitution_events", {}, {"match_id": MATCH_ID})
    rows = list(records)

    assert rows
    assert rows[0]["match_id"] == MATCH_ID
    assert rows[0]["team_id"]
    assert rows[0]["player_in_id"]
    assert rows[0]["player_out_id"]
    assert rows[0]["infringement"]
    assert rows[0]["source_service_id"] == "Feed-03.02-EventData-Match-Raw_Postmatch"


def test_feed0302_caution_events_are_readable():
    connector = _make_connector()

    records, _ = connector.read_table("event_raw_caution_events", {}, {"match_id": MATCH_ID})
    rows = list(records)

    assert rows
    assert rows[0]["match_id"] == MATCH_ID
    assert rows[0]["team_id"]
    assert rows[0]["player_id"]
    assert rows[0]["card_color"]
    assert rows[0]["card_rating"]
    assert rows[0]["source_service_id"] == "Feed-03.02-EventData-Match-Raw_Postmatch"


def test_feed0302_offside_events_are_readable():
    connector = _make_connector()

    records, _ = connector.read_table("event_raw_offside_events", {}, {"match_id": MATCH_ID})
    rows = list(records)

    assert rows
    assert rows[0]["match_id"] == MATCH_ID
    assert rows[0]["team_id"]
    assert rows[0]["player_id"]
    assert rows[0]["source_service_id"] == "Feed-03.02-EventData-Match-Raw_Postmatch"


def test_feed0302_corner_kick_events_are_readable():
    connector = _make_connector()

    records, _ = connector.read_table("event_raw_corner_kick_events", {}, {"match_id": MATCH_ID})
    rows = list(records)

    assert rows
    assert rows[0]["match_id"] == MATCH_ID
    assert rows[0]["team_id"]
    assert rows[0]["side"]
    assert rows[0]["placing"]
    assert rows[0]["set_piece_result_type"]
    assert rows[0]["set_piece_result_attributes_json"]
    assert rows[0]["source_service_id"] == "Feed-03.02-EventData-Match-Raw_Postmatch"


def test_feed0302_free_kick_events_are_readable():
    connector = _make_connector()

    records, _ = connector.read_table("event_raw_free_kick_events", {}, {"match_id": MATCH_ID})
    rows = list(records)

    assert rows
    assert rows[0]["match_id"] == MATCH_ID
    assert rows[0]["team_id"]
    assert rows[0]["execution_mode"]
    assert rows[0]["decision_timestamp"]
    assert rows[0]["set_piece_result_type"]
    assert rows[0]["set_piece_result_attributes_json"]
    assert rows[0]["source_service_id"] == "Feed-03.02-EventData-Match-Raw_Postmatch"
