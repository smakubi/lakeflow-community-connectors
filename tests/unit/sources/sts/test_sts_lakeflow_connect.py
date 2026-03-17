import json
import importlib.util
from pathlib import Path

from databricks.labs.community_connector.sources.sts import (
    SUPPORTED_TABLES,
    StsLakeflowConnect,
)
from databricks.labs.community_connector.sources.sts.sts import (
    FEED0302_TABLES,
    FEED0402_TABLES,
    FEED07_MATCH_ID_TABLES,
    FEED07_MATCH_TEAM_TABLES,
    FEED07_SEASON_MATCHDAY_TABLES,
    FEED07_TEAM_SCOPED_SEASON_TABLES,
    MATCH_ID_TABLES,
    SEASON_STATISTICS_CLUB_TABLES,
    SEASON_STATISTICS_COMPETITION_TABLES,
)


CONFIG_PATH = Path(__file__).parent / "configs" / "dev_config.json"
TABLE_CONFIG_PATH = Path(__file__).parent / "configs" / "dev_table_config.json"
TEST_SUITE_PATH = Path(__file__).resolve().parents[1] / "test_suite.py"
PRIMARY_COMPETITION_ID = "MLS-COM-000001"
PRIMARY_SEASON_ID = "MLS-SEA-0001JG"
XG_SEASON_ID = "MLS-SEA-0001K9"
STANDARD_MATCH_ID = "MLS-MAT-00067J"
RAW_MATCH_ID = "MLS-MAT-000532"
CONTRACT_TABLES = [
    "competitions",
    "fixtures_schedule",
    "tracking_postmatch_framesets",
    "rankings_match_player_goal",
]


_TEST_SUITE_SPEC = importlib.util.spec_from_file_location("sts_test_suite", TEST_SUITE_PATH)
assert _TEST_SUITE_SPEC and _TEST_SUITE_SPEC.loader, "Unable to load shared Lakeflow test suite."
_TEST_SUITE_MODULE = importlib.util.module_from_spec(_TEST_SUITE_SPEC)
_TEST_SUITE_SPEC.loader.exec_module(_TEST_SUITE_MODULE)
LakeflowConnectTests = _TEST_SUITE_MODULE.LakeflowConnectTests


class TestStsLakeflowConnect(LakeflowConnectTests):
    connector_class = StsLakeflowConnect
    sample_records = 1

    def _tables(self):
        available_tables = set(super()._tables())
        return [table_name for table_name in CONTRACT_TABLES if table_name in available_tables]

    @classmethod
    def setup_class(cls):
        cls.config = json.loads(CONFIG_PATH.read_text())
        cls.table_configs = cls._build_table_configs(cls.config)
        if TABLE_CONFIG_PATH.exists():
            overrides = json.loads(TABLE_CONFIG_PATH.read_text())
            for table_name, override in overrides.items():
                cls.table_configs.setdefault(table_name, {}).update(
                    {key: str(value) for key, value in override.items()}
                )
        super().setup_class()

    @classmethod
    def _build_table_configs(cls, config: dict) -> dict[str, dict[str, str]]:
        connector = cls.connector_class(config)
        club_id = cls._first_club_id(connector)
        feed07_scope = cls._feed07_scope(connector)
        table_configs: dict[str, dict[str, str]] = {}

        for table_name in SUPPORTED_TABLES:
            if table_name in FEED0302_TABLES | FEED0402_TABLES:
                table_configs[table_name] = {"match_id": RAW_MATCH_ID}
            elif table_name in MATCH_ID_TABLES:
                table_configs[table_name] = {"match_id": STANDARD_MATCH_ID}
            elif table_name == "matchdays":
                table_configs[table_name] = {"competition_id": PRIMARY_COMPETITION_ID}
            elif table_name == "clubs":
                table_configs[table_name] = {
                    "competition_id": PRIMARY_COMPETITION_ID,
                    "season_id": PRIMARY_SEASON_ID,
                }
            elif table_name in {"players", "team_officials"}:
                table_configs[table_name] = {
                    "club_id": club_id,
                    "season_id": PRIMARY_SEASON_ID,
                }
            elif table_name == "suspensions":
                table_configs[table_name] = {"season_id": PRIMARY_SEASON_ID}
            elif table_name == "fixtures_schedule":
                table_configs[table_name] = {
                    "competition_id": PRIMARY_COMPETITION_ID,
                    "season_id": PRIMARY_SEASON_ID,
                }
            elif table_name == "seasons":
                table_configs[table_name] = {"competition_id": PRIMARY_COMPETITION_ID}
            elif table_name == "season_tables":
                table_configs[table_name] = {"competition_id": PRIMARY_COMPETITION_ID}
            elif table_name in SEASON_STATISTICS_CLUB_TABLES:
                table_configs[table_name] = {
                    "competition_id": PRIMARY_COMPETITION_ID,
                    "season_id": PRIMARY_SEASON_ID,
                    "club_id": club_id,
                }
            elif table_name in SEASON_STATISTICS_COMPETITION_TABLES:
                table_configs[table_name] = {
                    "competition_id": PRIMARY_COMPETITION_ID,
                    "season_id": XG_SEASON_ID if table_name == "xg_rankings_season" else PRIMARY_SEASON_ID,
                }
            elif table_name in FEED07_MATCH_ID_TABLES:
                table_configs[table_name] = {"match_id": feed07_scope["match_id"]}
            elif table_name in FEED07_MATCH_TEAM_TABLES:
                table_configs[table_name] = {
                    "match_id": feed07_scope["match_id"],
                    "team_id": feed07_scope["team_id"],
                }
            elif table_name in FEED07_TEAM_SCOPED_SEASON_TABLES:
                table_configs[table_name] = {
                    "competition_id": feed07_scope["competition_id"],
                    "season_id": feed07_scope["season_id"],
                    "matchday_id": feed07_scope["matchday_id"],
                    "team_id": feed07_scope["team_id"],
                }
            elif table_name in FEED07_SEASON_MATCHDAY_TABLES:
                table_configs[table_name] = {
                    "competition_id": feed07_scope["competition_id"],
                    "season_id": feed07_scope["season_id"],
                    "matchday_id": feed07_scope["matchday_id"],
                }
            else:
                table_configs[table_name] = {}

        return table_configs

    @staticmethod
    def _first_club_id(connector: StsLakeflowConnect) -> str:
        records, _ = connector.read_table(
            "clubs",
            {},
            {
                "competition_id": PRIMARY_COMPETITION_ID,
                "season_id": PRIMARY_SEASON_ID,
            },
        )
        first_row = next(records, None)
        assert first_row and first_row.get("club_id"), "Unable to discover an STS club_id for tests."
        return str(first_row["club_id"])

    @staticmethod
    def _feed07_scope(connector: StsLakeflowConnect) -> dict[str, str]:
        records, _ = connector.read_table(
            "fixtures_schedule",
            {},
            {
                "competition_id": PRIMARY_COMPETITION_ID,
                "season_id": PRIMARY_SEASON_ID,
            },
        )
        rows = list(records)
        fixture = next((row for row in rows if row.get("match_id") == STANDARD_MATCH_ID), None)
        if fixture is None and rows:
            fixture = rows[0]

        assert fixture, "Unable to discover an STS fixture for Feed-07 tests."
        assert fixture.get("competition_id"), "Fixture discovery did not return competition_id."
        assert fixture.get("season_id"), "Fixture discovery did not return season_id."
        assert fixture.get("matchday_id"), "Fixture discovery did not return matchday_id."
        assert fixture.get("home_team_id"), "Fixture discovery did not return home_team_id."
        assert fixture.get("match_id"), "Fixture discovery did not return match_id."

        return {
            "competition_id": str(fixture["competition_id"]),
            "season_id": str(fixture["season_id"]),
            "matchday_id": str(fixture["matchday_id"]),
            "team_id": str(fixture["home_team_id"]),
            "match_id": str(fixture["match_id"]),
        }
