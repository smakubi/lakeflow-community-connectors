import json
from pathlib import Path

import pytest

from databricks.labs.community_connector.sources.sts import StsLakeflowConnect


CONFIG_PATH = Path(__file__).parent / "configs" / "dev_config.json"


def _make_connector() -> StsLakeflowConnect:
    return StsLakeflowConnect(json.loads(CONFIG_PATH.read_text()))


def _sample_fixture(connector: StsLakeflowConnect) -> dict:
    fixtures, _ = connector.read_table(
        "fixtures_schedule",
        {},
        {"competition_id": "MLS-COM-000001", "season_id": "MLS-SEA-0001JG"},
    )
    return next(fixtures)


def _assert_feed07_record_shape(
    rows: list[dict],
    fixture: dict,
    service_id: str,
    feed_type: str,
    time_scope: str,
    data_scope: str,
    team_id: str | None = None,
) -> None:
    if not rows:
        return

    record = rows[0]
    assert record["competition_id"] == fixture["competition_id"]
    assert record["season_id"] == fixture["season_id"]
    assert record["matchday_id"] == fixture["matchday_id"]
    assert record["source_service_id"] == service_id
    assert record["feed_type"].lower() == feed_type.lower()
    assert record["time_scope"] == time_scope
    assert record["data_scope"] == data_scope
    if team_id is not None:
        assert record["team_id"] == team_id
    assert record["ranking_type"]
    assert record["list_entries_json"]


MATCH_PLAYER_CASES = [
    ("rankings_match_player_goal", "Feed-07.01.01-Rankings-Match_Player_Goal", "Goals"),
    (
        "rankings_match_player_ballaction",
        "Feed-07.01.01-Rankings-Match_Player_BallAction",
        "BallAction",
    ),
    (
        "rankings_match_player_tackling",
        "Feed-07.01.01-Rankings-Match_Player_Tackling",
        "Tackling",
    ),
    ("rankings_match_player_play", "Feed-07.01.01-Rankings-Match_Player_Play", "Play"),
    ("rankings_match_player_foul", "Feed-07.01.01-Rankings-Match_Player_Foul", "Foul"),
]

MATCHDAY_TEAM_CASES = [
    ("rankings_matchday_team_goal", "Feed-07.04-Rankings-Matchday_Team_Goal", "Goals"),
    (
        "rankings_matchday_team_ballaction",
        "Feed-07.04-Rankings-Matchday_Team_BallAction",
        "BallAction",
    ),
    (
        "rankings_matchday_team_tackling",
        "Feed-07.04-Rankings-Matchday_Team_Tackling",
        "Tackling",
    ),
    ("rankings_matchday_team_play", "Feed-07.04-Rankings-Matchday_Team_Play", "Play"),
    ("rankings_matchday_team_foul", "Feed-07.04-Rankings-Matchday_Team_Foul", "Foul"),
    (
        "rankings_matchday_team_positionaldata",
        "Feed-07.04-Rankings-Matchday_Team_Positionaldata",
        "Positiondata",
    ),
]

SEASON_TEAM_CASES = [
    ("rankings_season_team_goal", "Feed-07.05-Rankings-Season_Team_Goal", "Goals"),
    (
        "rankings_season_team_ballaction",
        "Feed-07.05-Rankings-Season_Team_BallAction",
        "BallAction",
    ),
    (
        "rankings_season_team_tackling",
        "Feed-07.05-Rankings-Season_Team_Tackling",
        "Tackling",
    ),
    ("rankings_season_team_play", "Feed-07.05-Rankings-Season_Team_Play", "Play"),
    ("rankings_season_team_foul", "Feed-07.05-Rankings-Season_Team_Foul", "Foul"),
    (
        "rankings_season_team_positionaldata",
        "Feed-07.05-Rankings-Season_Team_Positionaldata",
        "Positiondata",
    ),
]

MATCH_PLAYER_TEAM_CASES = [
    ("rankings_match_player_team_goal", "Feed-07.01.02-Rankings-Match_Player_Team_Goal", "Goals"),
    (
        "rankings_match_player_team_ballaction",
        "Feed-07.01.02-Rankings-Match_Player_Team_BallAction",
        "Ballaction",
    ),
    (
        "rankings_match_player_team_tackling",
        "Feed-07.01.02-Rankings-Match_Player_Team_Tackling",
        "Tackling",
    ),
    ("rankings_match_player_team_play", "Feed-07.01.02-Rankings-Match_Player_Team_Play", "Play"),
    ("rankings_match_player_team_foul", "Feed-07.01.02-Rankings-Match_Player_Team_Foul", "Foul"),
]

MATCHDAY_PLAYER_CASES = [
    ("rankings_matchday_player_goal", "Feed-07.02.01-Rankings-Matchday_Player_Goal", "Goals"),
    (
        "rankings_matchday_player_ballaction",
        "Feed-07.02.01-Rankings-Matchday_Player_BallAction",
        "Ballaction",
    ),
    (
        "rankings_matchday_player_tackling",
        "Feed-07.02.01-Rankings-Matchday_Player_Tackling",
        "Tackling",
    ),
    ("rankings_matchday_player_play", "Feed-07.02.01-Rankings-Matchday_Player_Play", "Play"),
    ("rankings_matchday_player_foul", "Feed-07.02.01-Rankings-Matchday_Player_Foul", "Foul"),
    (
        "rankings_matchday_player_positionaldata",
        "Feed-07.02.01-Rankings-Matchday_Player_Positionaldata",
        "Positiondata",
    ),
]

MATCHDAY_PLAYER_TOP50_CASES = [
    (
        "rankings_matchday_player_top50_goal",
        "Feed-07.02.02-Rankings-Matchday_Player_Top50_Goal",
        "Goals",
    ),
    (
        "rankings_matchday_player_top50_ballaction",
        "Feed-07.02.02-Rankings-Matchday_Player_Top50_BallAction",
        "Ballaction",
    ),
    (
        "rankings_matchday_player_top50_positionaldata",
        "Feed-07.02.02-Rankings-Matchday_Player_Top50_Positionaldata",
        "Positiondata",
    ),
]

SEASON_PLAYER_CASES = [
    ("rankings_season_player_goal", "Feed-07.03.01-Rankings-Season_Player_Goal", "Goals"),
    (
        "rankings_season_player_ballaction",
        "Feed-07.03.01-Rankings-Season_Player_BallAction",
        "Ballaction",
    ),
    (
        "rankings_season_player_tackling",
        "Feed-07.03.01-Rankings-Season_Player_Tackling",
        "Tackling",
    ),
    ("rankings_season_player_play", "Feed-07.03.01-Rankings-Season_Player_Play", "Play"),
    ("rankings_season_player_foul", "Feed-07.03.01-Rankings-Season_Player_Foul", "Foul"),
    (
        "rankings_season_player_positionaldata",
        "Feed-07.03.01-Rankings-Season_Player_Positionaldata",
        "Positiondata",
    ),
]

SEASON_PLAYER_TOP50_CASES = [
    (
        "rankings_season_player_top50_goal",
        "Feed-07.03.02-Rankings-Season_Player_Top50_Goal",
        "Goals",
    ),
    (
        "rankings_season_player_top50_ballaction",
        "Feed-07.03.02-Rankings-Season_Player_Top50_BallAction",
        "Ballaction",
    ),
    (
        "rankings_season_player_top50_tackling",
        "Feed-07.03.02-Rankings-Season_Player_Top50_Tackling",
        "Tackling",
    ),
    (
        "rankings_season_player_top50_play",
        "Feed-07.03.02-Rankings-Season_Player_Top50_Play",
        "Play",
    ),
    (
        "rankings_season_player_top50_positionaldata",
        "Feed-07.03.02-Rankings-Season_Player_Top50_Positionaldata",
        "Positiondata",
    ),
]

SEASON_PLAYER_TEAM_CASES = [
    (
        "rankings_season_player_team_goal",
        "Feed-07.03.03-Rankings-Season_Player_Team_Goal",
        "Goals",
    ),
    (
        "rankings_season_player_team_ballaction",
        "Feed-07.03.03-Rankings-Season_Player_Team_BallAction",
        "Ballaction",
    ),
    (
        "rankings_season_player_team_tackling",
        "Feed-07.03.03-Rankings-Season_Player_Team_Tackling",
        "Tackling",
    ),
    (
        "rankings_season_player_team_play",
        "Feed-07.03.03-Rankings-Season_Player_Team_Play",
        "Play",
    ),
    (
        "rankings_season_player_team_foul",
        "Feed-07.03.03-Rankings-Season_Player_Team_Foul",
        "Foul",
    ),
    (
        "rankings_season_player_team_positionaldata",
        "Feed-07.03.03-Rankings-Season_Player_Team_Positionaldata",
        "Positiondata",
    ),
]


@pytest.mark.parametrize(("table_name", "service_id", "feed_type"), MATCH_PLAYER_CASES)
def test_feed07_match_player_rankings_are_readable(table_name: str, service_id: str, feed_type: str):
    connector = _make_connector()
    fixture = _sample_fixture(connector)

    records, _ = connector.read_table(
        table_name,
        {},
        {"match_id": fixture["match_id"]},
    )
    rows = list(records)
    if rows:
        assert rows[0]["match_id"] == fixture["match_id"]
    _assert_feed07_record_shape(rows, fixture, service_id, feed_type, "Match", "MatchInternal")


@pytest.mark.parametrize(("table_name", "service_id", "feed_type"), MATCHDAY_TEAM_CASES)
def test_feed07_matchday_team_rankings_are_readable(
    table_name: str, service_id: str, feed_type: str
):
    connector = _make_connector()
    fixture = _sample_fixture(connector)

    records, _ = connector.read_table(
        table_name,
        {},
        {
            "competition_id": fixture["competition_id"],
            "season_id": fixture["season_id"],
            "matchday_id": fixture["matchday_id"],
        },
    )
    rows = list(records)
    _assert_feed07_record_shape(rows, fixture, service_id, feed_type, "MatchDay", "Competition")


@pytest.mark.parametrize(("table_name", "service_id", "feed_type"), SEASON_TEAM_CASES)
def test_feed07_season_team_rankings_are_readable(table_name: str, service_id: str, feed_type: str):
    connector = _make_connector()
    fixture = _sample_fixture(connector)

    records, _ = connector.read_table(
        table_name,
        {},
        {
            "competition_id": fixture["competition_id"],
            "season_id": fixture["season_id"],
            "matchday_id": fixture["matchday_id"],
        },
    )
    rows = list(records)
    _assert_feed07_record_shape(rows, fixture, service_id, feed_type, "Season", "Competition")


@pytest.mark.parametrize(("table_name", "service_id", "feed_type"), MATCH_PLAYER_TEAM_CASES)
def test_feed07_match_player_team_rankings_are_readable(
    table_name: str, service_id: str, feed_type: str
):
    connector = _make_connector()
    fixture = _sample_fixture(connector)

    records, _ = connector.read_table(
        table_name,
        {},
        {"match_id": fixture["match_id"], "team_id": fixture["home_team_id"]},
    )
    rows = list(records)
    if rows:
        assert rows[0]["match_id"] == fixture["match_id"]
    _assert_feed07_record_shape(
        rows,
        fixture,
        service_id,
        feed_type,
        "Match",
        "TeamInternal",
        fixture["home_team_id"],
    )


@pytest.mark.parametrize(("table_name", "service_id", "feed_type"), MATCHDAY_PLAYER_CASES)
def test_feed07_matchday_player_rankings_are_readable(
    table_name: str, service_id: str, feed_type: str
):
    connector = _make_connector()
    fixture = _sample_fixture(connector)

    records, _ = connector.read_table(
        table_name,
        {},
        {
            "competition_id": fixture["competition_id"],
            "season_id": fixture["season_id"],
            "matchday_id": fixture["matchday_id"],
        },
    )
    rows = list(records)
    _assert_feed07_record_shape(rows, fixture, service_id, feed_type, "MatchDay", "Competition")


@pytest.mark.parametrize(("table_name", "service_id", "feed_type"), MATCHDAY_PLAYER_TOP50_CASES)
def test_feed07_matchday_player_top50_rankings_are_readable(
    table_name: str, service_id: str, feed_type: str
):
    connector = _make_connector()
    fixture = _sample_fixture(connector)

    records, _ = connector.read_table(
        table_name,
        {},
        {
            "competition_id": fixture["competition_id"],
            "season_id": fixture["season_id"],
            "matchday_id": fixture["matchday_id"],
        },
    )
    rows = list(records)
    _assert_feed07_record_shape(rows, fixture, service_id, feed_type, "MatchDay", "Top50")


@pytest.mark.parametrize(("table_name", "service_id", "feed_type"), SEASON_PLAYER_CASES)
def test_feed07_season_player_rankings_are_readable(
    table_name: str, service_id: str, feed_type: str
):
    connector = _make_connector()
    fixture = _sample_fixture(connector)

    records, _ = connector.read_table(
        table_name,
        {},
        {
            "competition_id": fixture["competition_id"],
            "season_id": fixture["season_id"],
            "matchday_id": fixture["matchday_id"],
        },
    )
    rows = list(records)
    _assert_feed07_record_shape(rows, fixture, service_id, feed_type, "Season", "Competition")


@pytest.mark.parametrize(("table_name", "service_id", "feed_type"), SEASON_PLAYER_TOP50_CASES)
def test_feed07_season_player_top50_rankings_are_readable(
    table_name: str, service_id: str, feed_type: str
):
    connector = _make_connector()
    fixture = _sample_fixture(connector)

    records, _ = connector.read_table(
        table_name,
        {},
        {
            "competition_id": fixture["competition_id"],
            "season_id": fixture["season_id"],
            "matchday_id": fixture["matchday_id"],
        },
    )
    rows = list(records)
    _assert_feed07_record_shape(rows, fixture, service_id, feed_type, "Season", "Top50")


@pytest.mark.parametrize(("table_name", "service_id", "feed_type"), SEASON_PLAYER_TEAM_CASES)
def test_feed07_season_player_team_rankings_are_readable(
    table_name: str, service_id: str, feed_type: str
):
    connector = _make_connector()
    fixture = _sample_fixture(connector)

    records, _ = connector.read_table(
        table_name,
        {},
        {
            "competition_id": fixture["competition_id"],
            "season_id": fixture["season_id"],
            "matchday_id": fixture["matchday_id"],
            "team_id": fixture["home_team_id"],
        },
    )
    rows = list(records)
    _assert_feed07_record_shape(
        rows,
        fixture,
        service_id,
        feed_type,
        "Season",
        "TeamInternal",
        fixture["home_team_id"],
    )
