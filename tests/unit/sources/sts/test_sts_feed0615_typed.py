import json
from pathlib import Path

from databricks.labs.community_connector.sources.sts import StsLakeflowConnect


CONFIG_PATH = Path(__file__).parent / "configs" / "dev_config.json"
MATCH_ID = "MLS-MAT-00067J"


def _make_connector() -> StsLakeflowConnect:
    return StsLakeflowConnect(json.loads(CONFIG_PATH.read_text()))


def test_feed0615_advanced_event_plays_are_readable():
    connector = _make_connector()

    records, _ = connector.read_table("advanced_event_plays", {}, {"match_id": MATCH_ID})
    rows = list(records)

    assert rows
    assert rows[0]["match_id"] == MATCH_ID
    assert rows[0]["data_status"] == "postmatch"
    assert rows[0]["event_id"]
    assert rows[0]["team_id"]
    assert rows[0]["player_id"]
    assert rows[0]["evaluation"]
    assert rows[0]["is_pass"] in {"true", "false"}
    assert rows[0]["receiver_id"]
    assert any(row["game_time"] for row in rows)
    assert rows[0]["source_service_id"] == "Feed-06.15-AdvancedEvents"


def test_feed0615_advanced_event_receptions_are_readable():
    connector = _make_connector()

    records, _ = connector.read_table("advanced_event_receptions", {}, {"match_id": MATCH_ID})
    rows = list(records)

    assert rows
    assert rows[0]["match_id"] == MATCH_ID
    assert rows[0]["data_status"] == "postmatch"
    assert rows[0]["event_id"]
    assert rows[0]["play_id"]
    assert rows[0]["team_id"]
    assert rows[0]["player_id"]
    assert rows[0]["is_interception"] in {"true", "false"}
    assert any(row["game_time"] for row in rows)
    assert rows[0]["source_service_id"] == "Feed-06.15-AdvancedEvents"


def test_feed0615_advanced_event_carries_are_readable():
    connector = _make_connector()

    records, _ = connector.read_table("advanced_event_carries", {}, {"match_id": MATCH_ID})
    rows = list(records)

    assert rows
    assert rows[0]["match_id"] == MATCH_ID
    assert rows[0]["data_status"] == "postmatch"
    assert rows[0]["event_id"]
    assert rows[0]["team_id"]
    assert rows[0]["player_id"]
    assert rows[0]["distance"]
    assert rows[0]["defensive_state_start"]
    assert rows[0]["defensive_state_end"]
    assert rows[0]["end_game_time"]
    assert rows[0]["source_service_id"] == "Feed-06.15-AdvancedEvents"


def test_feed0615_advanced_event_team_possessions_are_readable():
    connector = _make_connector()

    records, _ = connector.read_table("advanced_event_team_possessions", {}, {"match_id": MATCH_ID})
    rows = list(records)

    assert rows
    assert rows[0]["match_id"] == MATCH_ID
    assert rows[0]["data_status"] == "postmatch"
    assert rows[0]["event_id"]
    assert rows[0]["in_game_section"]
    assert rows[0]["sync_successful"] in {"true", "false"}
    assert any(row["vertical_gain_carries"] for row in rows)
    assert any(row["sum_x_g_con"] for row in rows)
    assert any(row["end_game_time"] for row in rows)
    assert rows[0]["source_service_id"] == "Feed-06.15-AdvancedEvents"


def test_feed0615_advanced_event_other_ball_actions_are_readable():
    connector = _make_connector()

    records, _ = connector.read_table(
        "advanced_event_other_ball_actions", {}, {"match_id": MATCH_ID}
    )
    rows = list(records)

    assert rows
    assert rows[0]["match_id"] == MATCH_ID
    assert rows[0]["data_status"] == "postmatch"
    assert rows[0]["event_id"]
    assert rows[0]["team_id"]
    assert rows[0]["player_id"]
    assert rows[0]["in_game_section"]
    assert rows[0]["is_defensive_clearance"] in {"true", "false"}
    assert any(row["game_time"] for row in rows)
    assert rows[0]["source_service_id"] == "Feed-06.15-AdvancedEvents"


def test_feed0615_advanced_event_tackling_games_are_readable():
    connector = _make_connector()

    records, _ = connector.read_table(
        "advanced_event_tackling_games", {}, {"match_id": MATCH_ID}
    )
    rows = list(records)

    assert rows
    assert rows[0]["match_id"] == MATCH_ID
    assert rows[0]["data_status"] == "postmatch"
    assert rows[0]["event_id"]
    assert rows[0]["winner_team_id"]
    assert rows[0]["winner_player_id"]
    assert rows[0]["loser_team_id"]
    assert rows[0]["loser_player_id"]
    assert rows[0]["is_foul"] in {"true", "false"}
    assert rows[0]["type"]
    assert rows[0]["source_service_id"] == "Feed-06.15-AdvancedEvents"


def test_feed0615_advanced_event_fouls_are_readable():
    connector = _make_connector()

    records, _ = connector.read_table("advanced_event_fouls", {}, {"match_id": MATCH_ID})
    rows = list(records)

    assert rows
    assert rows[0]["match_id"] == MATCH_ID
    assert rows[0]["data_status"] == "postmatch"
    assert rows[0]["event_id"]
    assert rows[0]["fouler_team_id"]
    assert rows[0]["fouler_player_id"]
    assert rows[0]["foul_type"]
    assert rows[0]["in_game_section"]
    assert any(row["game_time"] for row in rows)
    assert rows[0]["source_service_id"] == "Feed-06.15-AdvancedEvents"


def test_feed0615_advanced_event_shots_at_goal_are_readable():
    connector = _make_connector()

    records, _ = connector.read_table("advanced_event_shots_at_goal", {}, {"match_id": MATCH_ID})
    rows = list(records)

    assert rows
    assert rows[0]["match_id"] == MATCH_ID
    assert rows[0]["data_status"] == "postmatch"
    assert rows[0]["event_id"]
    assert rows[0]["player_id"]
    assert rows[0]["team_id"]
    assert rows[0]["shot_result"]
    assert rows[0]["is_blocked_shot"] in {"true", "false"}
    assert rows[0]["x_g"]
    assert rows[0]["source_service_id"] == "Feed-06.15-AdvancedEvents"
