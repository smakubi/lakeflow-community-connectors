import json
from pathlib import Path

from databricks.labs.community_connector.sources.sts import StsLakeflowConnect


CONFIG_PATH = Path(__file__).parent / "configs" / "dev_config.json"
COMPETITION_ID = "MLS-COM-000001"
SEASON_ID = "MLS-SEA-0001K9"


def _make_connector() -> StsLakeflowConnect:
    return StsLakeflowConnect(json.loads(CONFIG_PATH.read_text()))


def test_feed0606_xg_rankings_season_is_readable():
    connector = _make_connector()

    records, _ = connector.read_table(
        "xg_rankings_season",
        {},
        {"competition_id": COMPETITION_ID, "season_id": SEASON_ID},
    )
    rows = list(records)

    assert rows
    assert rows[0]["competition_id"] == COMPETITION_ID
    assert rows[0]["season_id"] == SEASON_ID
    assert rows[0]["ranking_type"] == "xGEfficiency"
    assert rows[0]["time_scope"] in {"Season", "Last5matches"}
    assert rows[0]["scope"] in {"Team", "Player"}
    assert rows[0]["rank"]
    assert rows[0]["absolute"]
    assert rows[0]["x_goals"]
    assert rows[0]["goals"]
    assert rows[0]["source_service_id"] == "Feed-06.06-xGRankings_Season"
    assert any(row["scope"] == "Team" and row["team_id"] for row in rows)
    assert any(row["scope"] == "Player" and row["player_id"] for row in rows)
