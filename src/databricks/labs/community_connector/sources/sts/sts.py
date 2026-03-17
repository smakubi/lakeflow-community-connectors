"""Lakeflow connector implementation for Sportec Solutions feeds."""

from __future__ import annotations

import hashlib
import itertools
import json
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Iterator

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.sts.sts_schemas import (
    FEED0407_MATCH_STATISTIC_ATTRIBUTES,
    FEED0407_PLAYER_STATISTIC_ATTRIBUTES,
    FEED0407_TEAM_STATISTIC_ATTRIBUTES,
    FEED0604_ALERT_ATTRIBUTES,
    FEED0604_ALERT_FLAG_ATTRIBUTES,
    FEED0604_FASTEST_PLAYER_ATTRIBUTES,
    FEED0604_RANKING_ENTRY_ATTRIBUTES,
    FEED0605_ATTACKING_ZONES_ATTRIBUTES,
    FEED0605_TEAM_ATTRIBUTES,
    FEED0605_ZONE_ATTRIBUTES,
    FEED0606_RANKING_ENTRY_ATTRIBUTES,
    FEED0606_XG_RANKINGS_ATTRIBUTES,
    FEED0610_PROBABILITY_ATTRIBUTES,
    FEED0610_WIN_PROBABILITY_ATTRIBUTES,
    FEED0615_ADVANCED_EVENTS_ATTRIBUTES,
    FEED0615_CARRY_ATTRIBUTES,
    FEED0615_FOUL_ATTRIBUTES,
    FEED0615_OTHER_BALL_ACTION_ATTRIBUTES,
    FEED0615_PLAY_ATTRIBUTES,
    FEED0615_RECEPTION_ATTRIBUTES,
    FEED0615_SHOT_AT_GOAL_ATTRIBUTES,
    FEED0615_TACKLING_GAME_ATTRIBUTES,
    FEED0615_TEAM_POSSESSION_ATTRIBUTES,
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
    camel_to_snake,
)

REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 3
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
TOKEN_REFRESH_SKEW_SECONDS = 60

TABLE_CONFIG: dict[str, dict[str, Any]] = {
    "competitions": {
        "service_id": "Feed-01.01-BaseData-Competition",
        "record_tags": ("Competition",),
    },
    "matchdays": {
        "service_id": "Feed-01.02-BaseData-Matchdays",
        "record_tags": ("MatchDay", "Matchday"),
    },
    "stadiums": {
        "service_id": "Feed-01.03-BaseData-Stadiums",
        "record_tags": ("Stadium",),
    },
    "clubs": {
        "service_id": "Feed-01.04-BaseData-Clubs",
        "record_tags": ("Club",),
    },
    "players": {
        "service_id": "Feed-01.05-BaseData-Individuals_Players",
        "record_tags": ("Object", "Person", "Individual", "Player"),
    },
    "team_officials": {
        "service_id": "Feed-01.05-BaseData-Individuals_Teamofficials",
        "record_tags": ("Object", "Person", "Individual", "TeamOfficial", "Official"),
    },
    "referees": {
        "service_id": "Feed-01.05-BaseData-Individuals_Referees",
        "record_tags": ("Object", "Person", "Individual", "Referee"),
    },
    "suspensions": {
        "service_id": "Feed-01.05.01-BaseData-Individuals_Suspensions",
        "record_tags": ("Suspension",),
    },
    "fixtures_schedule": {
        "service_id": "Feed-01.06-BaseData-Schedule",
        "record_tags": ("Fixture", "Match"),
    },
    "seasons": {
        "service_id": "Feed-01.07-BaseData-Season",
        "record_tags": ("Season",),
    },
    "match_information": {
        "service_id": "Feed-02.01-Match-Information",
        "record_tags": ("MatchInformation",),
    },
    "eventdata_match_statistics_match": {
        "service_id": "Feed-03.03-EventData-Match-Statistics",
        "record_tags": ("MatchStatistic",),
    },
    "eventdata_match_statistics_intervals": {
        "service_id": "Feed-03.03-EventData-Match-Statistics_Intervals",
        "record_tags": ("MatchStatistic",),
    },
    "eventdata_match_statistics_periods": {
        "service_id": "Feed-03.03-EventData-Match-Statistics_Periods",
        "record_tags": ("MatchStatistic",),
    },
    "eventdata_match_basic": {
        "service_id": "Feed-03.04-EventData-Match-Basic",
        "record_tags": ("EventsBasic",),
    },
    "eventdata_match_basic_extended": {
        "service_id": "Feed-03.05-EventData-Match-Basic_Extended",
        "record_tags": ("EventsBasicExtended",),
    },
    "season_tables": {
        "service_id": "Feed-05.01-Tables",
        "record_tags": ("Table",),
    },
    "eventdata_season_statistics_competition": {
        "service_id": "Feed-05.02-EventData-Season-Statistics_Competition",
        "record_tags": ("SeasonStatistic",),
    },
    "eventdata_season_statistics_club": {
        "service_id": "Feed-05.02-EventData-Season-Statistics_Club",
        "record_tags": ("SeasonStatistic",),
    },
    "positional_season_statistics_competition": {
        "service_id": "Feed-05.03-PositionalData-Season-Statistics_Competition",
        "record_tags": ("SeasonStatistic",),
    },
    "positional_season_statistics_club": {
        "service_id": "Feed-05.03-PositionalData-Season-Statistics_Club",
        "record_tags": ("SeasonStatistic",),
    },
    "video_assist_events": {
        "service_id": "Feed-06.02-VideoAssist-Info",
        "record_tags": ("Event",),
    },
    "player_topspeed_alerts": {
        "service_id": "Feed-06.04-Player-Topspeed-Alert",
        "record_tags": ("PlayerTopSpeedAlert",),
    },
    "player_topspeed_alert_rankings": {
        "service_id": "Feed-06.04-Player-Topspeed-Alert",
        "record_tags": ("ListEntry",),
    },
    "attacking_zone_entries": {
        "service_id": "Feed-06.05-AttackingZones",
        "record_tags": ("Zone",),
    },
    "xg_rankings_season": {
        "service_id": "Feed-06.06-xGRankings_Season",
        "record_tags": ("ListEntry",),
    },
    "win_probability_by_minute": {
        "service_id": "Feed-06.10-WinProbability",
        "record_tags": ("Probability",),
    },
    "advanced_events_raw": {
        "service_id": "Feed-06.15-AdvancedEvents",
        "record_tags": ("Event",),
    },
    "advanced_event_plays": {
        "service_id": "Feed-06.15-AdvancedEvents",
        "record_tags": ("Play",),
    },
    "advanced_event_receptions": {
        "service_id": "Feed-06.15-AdvancedEvents",
        "record_tags": ("Reception",),
    },
    "advanced_event_carries": {
        "service_id": "Feed-06.15-AdvancedEvents",
        "record_tags": ("Carry",),
    },
    "advanced_event_team_possessions": {
        "service_id": "Feed-06.15-AdvancedEvents",
        "record_tags": ("TeamPossession",),
    },
    "advanced_event_other_ball_actions": {
        "service_id": "Feed-06.15-AdvancedEvents",
        "record_tags": ("OtherBallAction",),
    },
    "advanced_event_tackling_games": {
        "service_id": "Feed-06.15-AdvancedEvents",
        "record_tags": ("TacklingGame",),
    },
    "advanced_event_fouls": {
        "service_id": "Feed-06.15-AdvancedEvents",
        "record_tags": ("Foul",),
    },
    "advanced_event_shots_at_goal": {
        "service_id": "Feed-06.15-AdvancedEvents",
        "record_tags": ("ShotAtGoal",),
    },
    "event_raw_messages": {
        "service_id": "Feed-03.02-EventData-Match-Raw_Postmatch",
        "record_tags": ("Event",),
    },
    "event_raw_delete_events": {
        "service_id": "Feed-03.02-EventData-Match-Raw_Postmatch",
        "record_tags": ("Event",),
    },
    "event_raw_var_notifications": {
        "service_id": "Feed-03.02-EventData-Match-Raw_Postmatch",
        "record_tags": ("Event",),
    },
    "event_raw_play_events": {
        "service_id": "Feed-03.02-EventData-Match-Raw_Postmatch",
        "record_tags": ("Event",),
    },
    "event_raw_foul_events": {
        "service_id": "Feed-03.02-EventData-Match-Raw_Postmatch",
        "record_tags": ("Event",),
    },
    "event_raw_shot_at_goal_events": {
        "service_id": "Feed-03.02-EventData-Match-Raw_Postmatch",
        "record_tags": ("Event",),
    },
    "event_raw_substitution_events": {
        "service_id": "Feed-03.02-EventData-Match-Raw_Postmatch",
        "record_tags": ("Event",),
    },
    "event_raw_caution_events": {
        "service_id": "Feed-03.02-EventData-Match-Raw_Postmatch",
        "record_tags": ("Event",),
    },
    "event_raw_offside_events": {
        "service_id": "Feed-03.02-EventData-Match-Raw_Postmatch",
        "record_tags": ("Event",),
    },
    "event_raw_corner_kick_events": {
        "service_id": "Feed-03.02-EventData-Match-Raw_Postmatch",
        "record_tags": ("Event",),
    },
    "event_raw_free_kick_events": {
        "service_id": "Feed-03.02-EventData-Match-Raw_Postmatch",
        "record_tags": ("Event",),
    },
    "distance_match_teams": {
        "service_id": "Feed-04.04-Distance-Match",
        "record_tags": ("Team",),
    },
    "distance_match_players": {
        "service_id": "Feed-04.04-Distance-Match",
        "record_tags": ("Person",),
    },
    "speed_interval_definitions": {
        "service_id": "Feed-04.05-SpeedIntervals-Match",
        "record_tags": ("Interval",),
    },
    "speed_interval_player_stats": {
        "service_id": "Feed-04.05-SpeedIntervals-Match",
        "record_tags": ("Interval",),
    },
    "positional_match_team_statistics": {
        "service_id": "Feed-04.07-PositionalData-Match-Statistics",
        "record_tags": ("TeamStatistic",),
    },
    "positional_match_player_statistics": {
        "service_id": "Feed-04.07-PositionalData-Match-Statistics",
        "record_tags": ("PlayerStatistic",),
    },
    "tracking_pitch_metadata": {
        "service_id": "Feed-04.02-PositionalData-Match-Raw_Postmatch",
        "record_tags": ("MetaData",),
    },
    "tracking_postmatch_framesets": {
        "service_id": "Feed-04.02-PositionalData-Match-Raw_Postmatch",
        "record_tags": ("FrameSet",),
    },
    "rankings_match_player_goal": {
        "service_id": "Feed-07.01.01-Rankings-Match_Player_Goal",
        "record_tags": ("Ranking",),
    },
    "rankings_matchday_team_goal": {
        "service_id": "Feed-07.04-Rankings-Matchday_Team_Goal",
        "record_tags": ("Ranking",),
    },
}

TABLE_CONFIG.update(
    {
        "rankings_match_player_ballaction": {
            "service_id": "Feed-07.01.01-Rankings-Match_Player_BallAction",
            "record_tags": ("Ranking",),
        },
        "rankings_match_player_tackling": {
            "service_id": "Feed-07.01.01-Rankings-Match_Player_Tackling",
            "record_tags": ("Ranking",),
        },
        "rankings_match_player_play": {
            "service_id": "Feed-07.01.01-Rankings-Match_Player_Play",
            "record_tags": ("Ranking",),
        },
        "rankings_match_player_foul": {
            "service_id": "Feed-07.01.01-Rankings-Match_Player_Foul",
            "record_tags": ("Ranking",),
        },
        "rankings_match_player_team_goal": {
            "service_id": "Feed-07.01.02-Rankings-Match_Player_Team_Goal",
            "record_tags": ("Ranking",),
        },
        "rankings_match_player_team_ballaction": {
            "service_id": "Feed-07.01.02-Rankings-Match_Player_Team_BallAction",
            "record_tags": ("Ranking",),
        },
        "rankings_match_player_team_tackling": {
            "service_id": "Feed-07.01.02-Rankings-Match_Player_Team_Tackling",
            "record_tags": ("Ranking",),
        },
        "rankings_match_player_team_play": {
            "service_id": "Feed-07.01.02-Rankings-Match_Player_Team_Play",
            "record_tags": ("Ranking",),
        },
        "rankings_match_player_team_foul": {
            "service_id": "Feed-07.01.02-Rankings-Match_Player_Team_Foul",
            "record_tags": ("Ranking",),
        },
        "rankings_matchday_player_goal": {
            "service_id": "Feed-07.02.01-Rankings-Matchday_Player_Goal",
            "record_tags": ("Ranking",),
        },
        "rankings_matchday_player_ballaction": {
            "service_id": "Feed-07.02.01-Rankings-Matchday_Player_BallAction",
            "record_tags": ("Ranking",),
        },
        "rankings_matchday_player_tackling": {
            "service_id": "Feed-07.02.01-Rankings-Matchday_Player_Tackling",
            "record_tags": ("Ranking",),
        },
        "rankings_matchday_player_play": {
            "service_id": "Feed-07.02.01-Rankings-Matchday_Player_Play",
            "record_tags": ("Ranking",),
        },
        "rankings_matchday_player_foul": {
            "service_id": "Feed-07.02.01-Rankings-Matchday_Player_Foul",
            "record_tags": ("Ranking",),
        },
        "rankings_matchday_player_positionaldata": {
            "service_id": "Feed-07.02.01-Rankings-Matchday_Player_Positionaldata",
            "record_tags": ("Ranking",),
        },
        "rankings_matchday_player_top50_goal": {
            "service_id": "Feed-07.02.02-Rankings-Matchday_Player_Top50_Goal",
            "record_tags": ("Ranking",),
        },
        "rankings_matchday_player_top50_ballaction": {
            "service_id": "Feed-07.02.02-Rankings-Matchday_Player_Top50_BallAction",
            "record_tags": ("Ranking",),
        },
        "rankings_matchday_player_top50_positionaldata": {
            "service_id": "Feed-07.02.02-Rankings-Matchday_Player_Top50_Positionaldata",
            "record_tags": ("Ranking",),
        },
        "rankings_season_player_goal": {
            "service_id": "Feed-07.03.01-Rankings-Season_Player_Goal",
            "record_tags": ("Ranking",),
        },
        "rankings_season_player_ballaction": {
            "service_id": "Feed-07.03.01-Rankings-Season_Player_BallAction",
            "record_tags": ("Ranking",),
        },
        "rankings_season_player_tackling": {
            "service_id": "Feed-07.03.01-Rankings-Season_Player_Tackling",
            "record_tags": ("Ranking",),
        },
        "rankings_season_player_play": {
            "service_id": "Feed-07.03.01-Rankings-Season_Player_Play",
            "record_tags": ("Ranking",),
        },
        "rankings_season_player_foul": {
            "service_id": "Feed-07.03.01-Rankings-Season_Player_Foul",
            "record_tags": ("Ranking",),
        },
        "rankings_season_player_positionaldata": {
            "service_id": "Feed-07.03.01-Rankings-Season_Player_Positionaldata",
            "record_tags": ("Ranking",),
        },
        "rankings_season_player_top50_goal": {
            "service_id": "Feed-07.03.02-Rankings-Season_Player_Top50_Goal",
            "record_tags": ("Ranking",),
        },
        "rankings_season_player_top50_ballaction": {
            "service_id": "Feed-07.03.02-Rankings-Season_Player_Top50_BallAction",
            "record_tags": ("Ranking",),
        },
        "rankings_season_player_top50_tackling": {
            "service_id": "Feed-07.03.02-Rankings-Season_Player_Top50_Tackling",
            "record_tags": ("Ranking",),
        },
        "rankings_season_player_top50_play": {
            "service_id": "Feed-07.03.02-Rankings-Season_Player_Top50_Play",
            "record_tags": ("Ranking",),
        },
        "rankings_season_player_top50_positionaldata": {
            "service_id": "Feed-07.03.02-Rankings-Season_Player_Top50_Positionaldata",
            "record_tags": ("Ranking",),
        },
        "rankings_season_player_team_goal": {
            "service_id": "Feed-07.03.03-Rankings-Season_Player_Team_Goal",
            "record_tags": ("Ranking",),
        },
        "rankings_season_player_team_ballaction": {
            "service_id": "Feed-07.03.03-Rankings-Season_Player_Team_BallAction",
            "record_tags": ("Ranking",),
        },
        "rankings_season_player_team_tackling": {
            "service_id": "Feed-07.03.03-Rankings-Season_Player_Team_Tackling",
            "record_tags": ("Ranking",),
        },
        "rankings_season_player_team_play": {
            "service_id": "Feed-07.03.03-Rankings-Season_Player_Team_Play",
            "record_tags": ("Ranking",),
        },
        "rankings_season_player_team_foul": {
            "service_id": "Feed-07.03.03-Rankings-Season_Player_Team_Foul",
            "record_tags": ("Ranking",),
        },
        "rankings_season_player_team_positionaldata": {
            "service_id": "Feed-07.03.03-Rankings-Season_Player_Team_Positionaldata",
            "record_tags": ("Ranking",),
        },
        "rankings_matchday_team_ballaction": {
            "service_id": "Feed-07.04-Rankings-Matchday_Team_BallAction",
            "record_tags": ("Ranking",),
        },
        "rankings_matchday_team_tackling": {
            "service_id": "Feed-07.04-Rankings-Matchday_Team_Tackling",
            "record_tags": ("Ranking",),
        },
        "rankings_matchday_team_play": {
            "service_id": "Feed-07.04-Rankings-Matchday_Team_Play",
            "record_tags": ("Ranking",),
        },
        "rankings_matchday_team_foul": {
            "service_id": "Feed-07.04-Rankings-Matchday_Team_Foul",
            "record_tags": ("Ranking",),
        },
        "rankings_matchday_team_positionaldata": {
            "service_id": "Feed-07.04-Rankings-Matchday_Team_Positionaldata",
            "record_tags": ("Ranking",),
        },
        "rankings_season_team_goal": {
            "service_id": "Feed-07.05-Rankings-Season_Team_Goal",
            "record_tags": ("Ranking",),
        },
        "rankings_season_team_ballaction": {
            "service_id": "Feed-07.05-Rankings-Season_Team_BallAction",
            "record_tags": ("Ranking",),
        },
        "rankings_season_team_tackling": {
            "service_id": "Feed-07.05-Rankings-Season_Team_Tackling",
            "record_tags": ("Ranking",),
        },
        "rankings_season_team_play": {
            "service_id": "Feed-07.05-Rankings-Season_Team_Play",
            "record_tags": ("Ranking",),
        },
        "rankings_season_team_foul": {
            "service_id": "Feed-07.05-Rankings-Season_Team_Foul",
            "record_tags": ("Ranking",),
        },
        "rankings_season_team_positionaldata": {
            "service_id": "Feed-07.05-Rankings-Season_Team_Positionaldata",
            "record_tags": ("Ranking",),
        },
    }
)

MATCH_ID_TABLES = {
    "match_information",
    "eventdata_match_statistics_match",
    "eventdata_match_statistics_intervals",
    "eventdata_match_statistics_periods",
    "eventdata_match_basic",
    "eventdata_match_basic_extended",
    "event_raw_messages",
    "event_raw_delete_events",
    "event_raw_var_notifications",
    "event_raw_play_events",
    "event_raw_foul_events",
    "event_raw_shot_at_goal_events",
    "event_raw_substitution_events",
    "event_raw_caution_events",
    "event_raw_offside_events",
    "event_raw_corner_kick_events",
    "event_raw_free_kick_events",
    "distance_match_teams",
    "distance_match_players",
    "speed_interval_definitions",
    "speed_interval_player_stats",
    "positional_match_team_statistics",
    "positional_match_player_statistics",
    "video_assist_events",
    "player_topspeed_alerts",
    "player_topspeed_alert_rankings",
    "attacking_zone_entries",
    "win_probability_by_minute",
    "advanced_events_raw",
    "advanced_event_plays",
    "advanced_event_receptions",
    "advanced_event_carries",
    "advanced_event_team_possessions",
    "advanced_event_other_ball_actions",
    "advanced_event_tackling_games",
    "advanced_event_fouls",
    "advanced_event_shots_at_goal",
    "tracking_pitch_metadata",
    "tracking_postmatch_framesets",
}

SEASON_STATISTICS_COMPETITION_TABLES = {
    "eventdata_season_statistics_competition",
    "positional_season_statistics_competition",
    "xg_rankings_season",
}

SEASON_STATISTICS_CLUB_TABLES = {
    "eventdata_season_statistics_club",
    "positional_season_statistics_club",
}

FEED07_MATCH_ID_TABLES = {
    "rankings_match_player_goal",
    "rankings_match_player_ballaction",
    "rankings_match_player_tackling",
    "rankings_match_player_play",
    "rankings_match_player_foul",
}

FEED07_MATCH_TEAM_TABLES = {
    "rankings_match_player_team_goal",
    "rankings_match_player_team_ballaction",
    "rankings_match_player_team_tackling",
    "rankings_match_player_team_play",
    "rankings_match_player_team_foul",
}

FEED07_COMPETITION_SCOPE_TABLES = {
    "rankings_matchday_player_goal",
    "rankings_matchday_player_ballaction",
    "rankings_matchday_player_tackling",
    "rankings_matchday_player_play",
    "rankings_matchday_player_foul",
    "rankings_matchday_player_positionaldata",
    "rankings_matchday_player_top50_goal",
    "rankings_matchday_player_top50_ballaction",
    "rankings_matchday_player_top50_positionaldata",
    "rankings_season_player_goal",
    "rankings_season_player_ballaction",
    "rankings_season_player_tackling",
    "rankings_season_player_play",
    "rankings_season_player_foul",
    "rankings_season_player_positionaldata",
    "rankings_season_player_top50_goal",
    "rankings_season_player_top50_ballaction",
    "rankings_season_player_top50_tackling",
    "rankings_season_player_top50_play",
    "rankings_season_player_top50_positionaldata",
}

FEED07_TEAM_SCOPED_SEASON_TABLES = {
    "rankings_season_player_team_goal",
    "rankings_season_player_team_ballaction",
    "rankings_season_player_team_tackling",
    "rankings_season_player_team_play",
    "rankings_season_player_team_foul",
    "rankings_season_player_team_positionaldata",
}

FEED07_MATCHDAY_TEAM_TABLES = {
    "rankings_matchday_team_goal",
    "rankings_matchday_team_ballaction",
    "rankings_matchday_team_tackling",
    "rankings_matchday_team_play",
    "rankings_matchday_team_foul",
    "rankings_matchday_team_positionaldata",
}

FEED07_SEASON_TEAM_TABLES = {
    "rankings_season_team_goal",
    "rankings_season_team_ballaction",
    "rankings_season_team_tackling",
    "rankings_season_team_play",
    "rankings_season_team_foul",
    "rankings_season_team_positionaldata",
}

FEED07_SEASON_MATCHDAY_TABLES = (
    FEED07_COMPETITION_SCOPE_TABLES | FEED07_MATCHDAY_TEAM_TABLES | FEED07_SEASON_TEAM_TABLES
)

FEED07_TABLES = (
    FEED07_MATCH_ID_TABLES
    | FEED07_MATCH_TEAM_TABLES
    | FEED07_SEASON_MATCHDAY_TABLES
    | FEED07_TEAM_SCOPED_SEASON_TABLES
)

FEED0402_TABLES = {"tracking_pitch_metadata", "tracking_postmatch_framesets"}
FEED0404_TABLES = {"distance_match_teams", "distance_match_players"}
FEED0405_TABLES = {"speed_interval_definitions", "speed_interval_player_stats"}
FEED0407_TABLES = {"positional_match_team_statistics", "positional_match_player_statistics"}
FEED0604_TABLES = {"player_topspeed_alerts", "player_topspeed_alert_rankings"}
FEED0605_TABLES = {"attacking_zone_entries"}
FEED0606_TABLES = {"xg_rankings_season"}
FEED0610_TABLES = {"win_probability_by_minute"}
FEED0615_TABLES = {
    "advanced_events_raw",
    "advanced_event_plays",
    "advanced_event_receptions",
    "advanced_event_carries",
    "advanced_event_team_possessions",
    "advanced_event_other_ball_actions",
    "advanced_event_tackling_games",
    "advanced_event_fouls",
    "advanced_event_shots_at_goal",
}
FEED0302_TABLES = {
    "event_raw_messages",
    "event_raw_delete_events",
    "event_raw_var_notifications",
    "event_raw_play_events",
    "event_raw_foul_events",
    "event_raw_shot_at_goal_events",
    "event_raw_substitution_events",
    "event_raw_caution_events",
    "event_raw_offside_events",
    "event_raw_corner_kick_events",
    "event_raw_free_kick_events",
}


@dataclass
class HttpResponse:
    status_code: int
    text: str
    headers: dict[str, str]

    def json(self) -> Any:
        return json.loads(self.text or "{}")


class StsLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for STS XML feeds."""

    _RESERVED_OPTION_KEYS = {
        "audience",
        "base_url",
        "client_id",
        "client_secret",
        "databricks.connection",
        "isdeleteflow",
        "sourcename",
        "tablename",
        "tablenamelist",
        "tableconfigs",
        "token_url",
    }

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        self.client_id = self._require_connection_option("client_id")
        self.client_secret = self._require_connection_option("client_secret")
        self.token_url = self._require_connection_option("token_url")
        self.audience = self._require_connection_option("audience")
        self.base_url = self._require_connection_option("base_url").rstrip("/")

        self._default_headers = {
            "Accept": "application/xml, text/xml;q=0.9, */*;q=0.1",
            "User-Agent": "lakeflow-sts-connector/0.1",
        }

        self._access_token: str | None = None
        self._token_expires_at = datetime.now(timezone.utc)

    def list_tables(self) -> list[str]:
        return SUPPORTED_TABLES.copy()

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        self._validate_table_name(table_name)
        self._validate_table_options(table_name, table_options)
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        self._validate_table_name(table_name)
        self._validate_table_options(table_name, table_options)
        return dict(TABLE_METADATA[table_name])

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        self._validate_table_name(table_name)
        normalized_options = self._validate_table_options(table_name, table_options)
        snapshot_token = self._build_snapshot_token(table_name, normalized_options)

        if isinstance(start_offset, dict) and start_offset.get("snapshot_token") == snapshot_token:
            return iter(()), dict(start_offset)

        records = self._read_snapshot_table(table_name, normalized_options)
        batch_limit = self._max_records_per_batch(normalized_options)
        if batch_limit is not None:
            records = itertools.islice(records, batch_limit)
        end_offset = {"snapshot_token": snapshot_token}
        return iter(records), end_offset

    def _read_snapshot_table(
        self, table_name: str, table_options: dict[str, str]
    ) -> Iterable[dict[str, Any]]:
        if table_name in FEED0402_TABLES and (
            table_name == "tracking_pitch_metadata"
            or self._max_records_per_batch(table_options) is not None
        ):
            streaming_records = self._read_feed0402_streaming_table(
                table_name, table_options
            )
            if streaming_records is not None:
                return streaming_records

        xml_text, source_url = self._fetch_table_xml(table_name, table_options)
        if not xml_text.strip():
            return []

        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as exc:
            raise RuntimeError(f"Failed to parse XML for '{table_name}': {exc}") from exc

        service_id = TABLE_CONFIG[table_name]["service_id"]
        if table_name in FEED0302_TABLES:
            return self._read_feed0302_postmatch_table(
                root, table_name, service_id, source_url, table_options
            )
        if table_name in FEED0404_TABLES:
            return self._read_feed0404_distance_match_table(
                root, table_name, service_id, source_url, table_options
            )
        if table_name in FEED0405_TABLES:
            return self._read_feed0405_speed_intervals_table(
                root, table_name, service_id, source_url, table_options
            )
        if table_name in FEED0407_TABLES:
            return self._read_feed0407_positional_match_statistics_table(
                root, table_name, service_id, source_url, table_options
            )
        if table_name in FEED0604_TABLES:
            return self._read_feed0604_player_topspeed_alert_table(
                root, table_name, service_id, source_url, table_options
            )
        if table_name in FEED0605_TABLES:
            return self._read_feed0605_attacking_zones_table(
                root, table_name, service_id, source_url, table_options
            )
        if table_name in FEED0606_TABLES:
            return self._read_feed0606_xg_rankings_table(
                root, table_name, service_id, source_url, table_options
            )
        if table_name in FEED0610_TABLES:
            return self._read_feed0610_win_probability_table(
                root, table_name, service_id, source_url, table_options
            )
        if table_name in FEED0615_TABLES:
            return self._read_feed0615_advanced_events_table(
                root, table_name, service_id, source_url, table_options
            )
        if table_name in FEED0402_TABLES:
            return self._read_feed0402_postmatch_table(
                root, table_name, service_id, source_url, table_options
            )
        if table_name in FEED07_TABLES:
            return self._read_feed07_rankings_table(
                root, table_name, service_id, source_url, table_options
            )
        if table_name == "match_information":
            return [
                self._build_match_information_record(
                    root, service_id, source_url, table_options
                )
            ]

        record_elements = self._extract_record_elements(root, TABLE_CONFIG[table_name]["record_tags"])
        builder = getattr(self, f"_build_{table_name}_record")
        return [builder(element, service_id, source_url, table_options) for element in record_elements]

    def _fetch_table_xml(
        self, table_name: str, table_options: dict[str, str]
    ) -> tuple[str, str]:
        self._ensure_access_token()
        service_id = TABLE_CONFIG[table_name]["service_id"]
        source_url = self._build_source_url(table_name, service_id, table_options)
        response = self._request_with_retry("GET", source_url)

        if response.status_code != 200:
            raise RuntimeError(
                f"STS request failed for '{table_name}' at '{source_url}': "
                f"{response.status_code} {response.text}"
            )
        return response.text, source_url

    def _request_with_retry(self, method: str, url: str) -> HttpResponse:
        force_token_refresh = False
        response: HttpResponse | None = None

        for attempt in range(MAX_RETRIES + 1):
            if force_token_refresh or self._access_token is None:
                self._ensure_access_token(force_refresh=True)
                force_token_refresh = False

            response = self._http_request(method, url)
            if response.status_code == 401 and attempt < MAX_RETRIES:
                force_token_refresh = True
                continue

            if response.status_code not in RETRYABLE_STATUS_CODES:
                return response

            if attempt == MAX_RETRIES:
                return response

            time.sleep(self._retry_delay_seconds(response, attempt))

        if response is None:
            raise RuntimeError(f"STS request never returned a response for '{url}'")
        return response

    def _ensure_access_token(self, force_refresh: bool = False) -> None:
        now = datetime.now(timezone.utc)
        if (
            not force_refresh
            and self._access_token
            and now + timedelta(seconds=TOKEN_REFRESH_SKEW_SECONDS) < self._token_expires_at
        ):
            return

        response = self._http_request(
            "POST",
            self.token_url,
            data=urllib.parse.urlencode(
                {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "audience": self.audience,
                }
            ).encode("utf-8"),
            headers={
                "Accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"STS token request failed: {response.status_code} {response.text}"
            )

        payload = response.json()
        access_token = payload.get("access_token")
        if not access_token:
            raise RuntimeError("STS token response did not include 'access_token'")

        expires_in = int(payload.get("expires_in", 3600))
        self._access_token = access_token
        self._token_expires_at = now + timedelta(seconds=expires_in)

    def _retry_delay_seconds(self, response: HttpResponse, attempt: int) -> int:
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            try:
                return max(1, int(retry_after)) if retry_after else 5
            except ValueError:
                return 5
        return 2 ** (attempt + 1)

    def _http_request(
        self,
        method: str,
        url: str,
        data: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> HttpResponse:
        request_headers = dict(self._default_headers)
        if self._access_token:
            request_headers["Authorization"] = f"Bearer {self._access_token}"
        if headers:
            request_headers.update(headers)

        request = urllib.request.Request(
            url,
            data=data,
            headers=request_headers,
            method=method,
        )

        try:
            with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                body = response.read().decode("utf-8", errors="replace")
                return HttpResponse(
                    status_code=response.status,
                    text=body,
                    headers=dict(response.headers.items()),
                )
        except urllib.error.HTTPError as error:
            body = error.read().decode("utf-8", errors="replace")
            return HttpResponse(
                status_code=error.code,
                text=body,
                headers=dict(error.headers.items()),
            )

    def _build_source_url(
        self, table_name: str, service_id: str, table_options: dict[str, str]
    ) -> str:
        parameter_path = self._build_parameter_path(table_name, table_options)
        if not parameter_path:
            return f"{self.base_url}/{service_id}"
        return f"{self.base_url}/{service_id}/{parameter_path}"

    def _build_parameter_path(self, table_name: str, table_options: dict[str, str]) -> str | None:
        if table_name in {"competitions", "stadiums", "referees"}:
            return None
        if table_name == "matchdays":
            return table_options["competition_id"]
        if table_name == "clubs":
            return f'{table_options["season_id"]}_{table_options["competition_id"]}'
        if table_name in {"players", "team_officials"}:
            return f'{table_options["club_id"]}_{table_options["season_id"]}'
        if table_name == "suspensions":
            return table_options["season_id"]
        if table_name == "fixtures_schedule":
            if "season_id" in table_options:
                return f'{table_options["season_id"]}_{table_options["competition_id"]}'
            if "matchday_id" in table_options:
                return f'{table_options["matchday_id"]}_{table_options["competition_id"]}'
            return table_options["competition_id"]
        if table_name == "seasons":
            return table_options["competition_id"]
        if table_name in MATCH_ID_TABLES:
            return table_options["match_id"]
        if table_name in FEED07_MATCH_ID_TABLES:
            return table_options["match_id"]
        if table_name in FEED07_MATCH_TEAM_TABLES:
            return f'{table_options["match_id"]}_{table_options["team_id"]}'
        if table_name in FEED07_SEASON_MATCHDAY_TABLES:
            return (
                f'{table_options["season_id"]}_{table_options["competition_id"]}'
                f'_{table_options["matchday_id"]}'
            )
        if table_name in FEED07_TEAM_SCOPED_SEASON_TABLES:
            return (
                f'{table_options["season_id"]}_{table_options["competition_id"]}'
                f'_{table_options["matchday_id"]}_{table_options["team_id"]}'
            )
        if table_name == "season_tables":
            return table_options["competition_id"]
        if table_name in SEASON_STATISTICS_COMPETITION_TABLES:
            return f'{table_options["competition_id"]}_{table_options["season_id"]}'
        if table_name in SEASON_STATISTICS_CLUB_TABLES:
            return (
                f'{table_options["competition_id"]}_{table_options["season_id"]}'
                f'_{table_options["club_id"]}'
            )
        raise ValueError(f"Unsupported STS table '{table_name}'")

    def _validate_table_name(self, table_name: str) -> None:
        if table_name not in SUPPORTED_TABLES:
            raise ValueError(
                f"Unsupported STS table '{table_name}'. Supported tables: {SUPPORTED_TABLES}"
            )

    def _validate_table_options(
        self, table_name: str, table_options: dict[str, str] | None
    ) -> dict[str, str]:
        normalized = {
            str(key).strip().lower(): str(value).strip()
            for key, value in (table_options or {}).items()
            if (
                value is not None
                and str(key).strip()
                and str(value).strip()
                and str(key).strip().lower() not in self._RESERVED_OPTION_KEYS
            )
        }

        allowed_by_table = {
            "competitions": set(),
            "matchdays": {"competition_id"},
            "stadiums": set(),
            "clubs": {"season_id", "competition_id"},
            "players": {"club_id", "season_id"},
            "team_officials": {"club_id", "season_id"},
            "referees": set(),
            "suspensions": {"season_id"},
            "fixtures_schedule": {"competition_id", "season_id", "matchday_id"},
            "seasons": {"competition_id"},
            "match_information": {"match_id"},
            "eventdata_match_statistics_match": {"match_id"},
            "eventdata_match_statistics_intervals": {"match_id"},
            "eventdata_match_statistics_periods": {"match_id"},
            "eventdata_match_basic": {"match_id"},
            "eventdata_match_basic_extended": {"match_id"},
            "season_tables": {"competition_id"},
            "eventdata_season_statistics_competition": {"competition_id", "season_id"},
            "eventdata_season_statistics_club": {"competition_id", "season_id", "club_id"},
            "positional_season_statistics_competition": {"competition_id", "season_id"},
            "positional_season_statistics_club": {"competition_id", "season_id", "club_id"},
            "event_raw_messages": {"match_id"},
            "event_raw_delete_events": {"match_id"},
            "event_raw_var_notifications": {"match_id"},
            "event_raw_play_events": {"match_id"},
            "event_raw_foul_events": {"match_id"},
            "event_raw_shot_at_goal_events": {"match_id"},
            "event_raw_substitution_events": {"match_id"},
            "event_raw_caution_events": {"match_id"},
            "event_raw_offside_events": {"match_id"},
            "event_raw_corner_kick_events": {"match_id"},
            "event_raw_free_kick_events": {"match_id"},
            "distance_match_teams": {"match_id"},
            "distance_match_players": {"match_id"},
            "speed_interval_definitions": {"match_id"},
            "speed_interval_player_stats": {"match_id"},
            "positional_match_team_statistics": {"match_id"},
            "positional_match_player_statistics": {"match_id"},
            "video_assist_events": {"match_id"},
            "player_topspeed_alerts": {"match_id"},
            "player_topspeed_alert_rankings": {"match_id"},
            "attacking_zone_entries": {"match_id"},
            "xg_rankings_season": {"competition_id", "season_id"},
            "win_probability_by_minute": {"match_id"},
            "advanced_events_raw": {"match_id"},
            "advanced_event_plays": {"match_id"},
            "advanced_event_receptions": {"match_id"},
            "advanced_event_carries": {"match_id"},
            "advanced_event_team_possessions": {"match_id"},
            "advanced_event_other_ball_actions": {"match_id"},
            "advanced_event_tackling_games": {"match_id"},
            "advanced_event_fouls": {"match_id"},
            "advanced_event_shots_at_goal": {"match_id"},
            "tracking_pitch_metadata": {"match_id"},
            "tracking_postmatch_framesets": {"match_id"},
        }
        allowed_by_table.update({table: {"match_id"} for table in FEED07_MATCH_ID_TABLES})
        allowed_by_table.update({table: {"match_id", "team_id"} for table in FEED07_MATCH_TEAM_TABLES})
        allowed_by_table.update(
            {
                table: {"competition_id", "season_id", "matchday_id"}
                for table in FEED07_SEASON_MATCHDAY_TABLES
            }
        )
        allowed_by_table.update(
            {
                table: {"competition_id", "season_id", "matchday_id", "team_id"}
                for table in FEED07_TEAM_SCOPED_SEASON_TABLES
            }
        )
        allowed = allowed_by_table[table_name]
        control_options = {"max_records_per_batch"}
        effective_options = {
            key: value for key, value in normalized.items() if key not in control_options
        }

        unknown = sorted(set(effective_options) - allowed)
        if unknown:
            raise ValueError(
                f"Unsupported table_options for '{table_name}': {unknown}. "
                f"Allowed: {sorted(allowed)}"
            )

        batch_limit = normalized.get("max_records_per_batch")
        if batch_limit is not None:
            try:
                if int(batch_limit) < 1:
                    raise ValueError
            except ValueError as exc:
                raise ValueError(
                    "table_options['max_records_per_batch'] must be a positive integer"
                ) from exc

        if table_name in {"competitions", "stadiums", "referees"} and effective_options:
            raise ValueError(f"Table '{table_name}' does not accept table_options")

        if table_name == "matchdays" and "competition_id" not in effective_options:
            raise ValueError("Table 'matchdays' requires table_options['competition_id']")
        if table_name == "clubs" and set(effective_options) != {"season_id", "competition_id"}:
            raise ValueError(
                "Table 'clubs' requires both table_options['season_id'] and "
                "table_options['competition_id']"
            )
        if table_name in {"players", "team_officials"} and set(effective_options) != {
            "club_id",
            "season_id",
        }:
            raise ValueError(
                f"Table '{table_name}' requires both table_options['club_id'] and "
                "table_options['season_id']"
            )
        if table_name == "suspensions" and "season_id" not in effective_options:
            raise ValueError("Table 'suspensions' requires table_options['season_id']")
        if table_name == "seasons" and "competition_id" not in effective_options:
            raise ValueError("Table 'seasons' requires table_options['competition_id']")
        if table_name in MATCH_ID_TABLES and "match_id" not in effective_options:
            raise ValueError(f"Table '{table_name}' requires table_options['match_id']")
        if table_name in FEED07_MATCH_ID_TABLES and "match_id" not in effective_options:
            raise ValueError(f"Table '{table_name}' requires table_options['match_id']")
        if table_name in FEED07_MATCH_TEAM_TABLES and set(effective_options) != {
            "match_id",
            "team_id",
        }:
            raise ValueError(
                f"Table '{table_name}' requires table_options['match_id'] and "
                "table_options['team_id']"
            )
        if table_name in FEED07_SEASON_MATCHDAY_TABLES and set(effective_options) != {
            "competition_id",
            "season_id",
            "matchday_id",
        }:
            raise ValueError(
                f"Table '{table_name}' requires table_options['competition_id'], "
                "table_options['season_id'], and table_options['matchday_id']"
            )
        if table_name in FEED07_TEAM_SCOPED_SEASON_TABLES and set(effective_options) != {
            "competition_id",
            "season_id",
            "matchday_id",
            "team_id",
        }:
            raise ValueError(
                f"Table '{table_name}' requires table_options['competition_id'], "
                "table_options['season_id'], table_options['matchday_id'], and "
                "table_options['team_id']"
            )
        if table_name == "season_tables" and "competition_id" not in effective_options:
            raise ValueError("Table 'season_tables' requires table_options['competition_id']")
        if table_name in SEASON_STATISTICS_COMPETITION_TABLES and set(effective_options) != {
            "competition_id",
            "season_id",
        }:
            raise ValueError(
                f"Table '{table_name}' requires both table_options['competition_id'] and "
                "table_options['season_id']"
            )
        if table_name in SEASON_STATISTICS_CLUB_TABLES and set(effective_options) != {
            "competition_id",
            "season_id",
            "club_id",
        }:
            raise ValueError(
                f"Table '{table_name}' requires table_options['competition_id'], "
                "table_options['season_id'], and table_options['club_id']"
            )

        if table_name == "fixtures_schedule":
            has_competition = "competition_id" in effective_options
            has_season = "season_id" in effective_options
            has_matchday = "matchday_id" in effective_options
            valid = (
                (has_competition and not has_season and not has_matchday)
                or (has_competition and has_season and not has_matchday)
                or (has_competition and has_matchday and not has_season)
            )
            if not valid:
                raise ValueError(
                    "Table 'fixtures_schedule' requires one of: "
                    "{competition_id}, {season_id + competition_id}, or "
                    "{matchday_id + competition_id}"
                )

        return normalized

    def _max_records_per_batch(self, table_options: dict[str, str]) -> int | None:
        value = table_options.get("max_records_per_batch")
        if value is None:
            return None
        return int(value)

    def _require_connection_option(self, option_name: str) -> str:
        value = self.options.get(option_name)
        if value is None or not str(value).strip():
            raise ValueError(f"STS connector requires '{option_name}' in options")
        return str(value).strip()

    def _build_snapshot_token(self, table_name: str, table_options: dict[str, str]) -> str:
        key = json.dumps(
            {
                "table_name": table_name,
                "table_options": table_options,
            },
            sort_keys=True,
        )
        return hashlib.sha1(key.encode("utf-8")).hexdigest()

    def _extract_record_elements(self, root: ET.Element, record_tags: tuple[str, ...]) -> list[ET.Element]:
        target_names = {name.lower() for name in record_tags}

        if self._local_name(root.tag).lower() in target_names:
            return [root]

        direct_matches = [
            child for child in list(root) if self._local_name(child.tag).lower() in target_names
        ]
        if direct_matches:
            return direct_matches

        for child in list(root):
            nested_matches = [
                grandchild
                for grandchild in list(child)
                if self._local_name(grandchild.tag).lower() in target_names
            ]
            if nested_matches:
                return nested_matches

        return [
            element
            for element in root.iter()
            if element is not root and self._local_name(element.tag).lower() in target_names
        ]

    def _build_competitions_record(
        self,
        element: ET.Element,
        service_id: str,
        source_url: str,
        table_options: dict[str, str],
    ) -> dict[str, Any]:
        return self._build_record(
            element,
            service_id,
            source_url,
            table_options,
            {
                "competition_id": self._find_text(element, "CompetitionId"),
                "dl_provider_id": self._find_text(element, "DlProviderId"),
                "competition_name": self._find_text(element, "CompetitionName"),
                "country": self._find_text(element, "Country"),
                "host": self._find_text(element, "Host"),
                "sport": self._find_text(element, "Sport"),
                "competition_type": self._find_text(element, "CompetitionType"),
                "valid_to": self._find_text(element, "ValidTo"),
            },
        )

    def _build_matchdays_record(
        self,
        element: ET.Element,
        service_id: str,
        source_url: str,
        table_options: dict[str, str],
    ) -> dict[str, Any]:
        return self._build_record(
            element,
            service_id,
            source_url,
            table_options,
            {
                "competition_id": table_options.get("competition_id"),
                "matchday_id": self._find_text(element, "MatchDayId"),
                "matchday": self._find_text(element, "MatchDay"),
                "season_id": self._find_text(element, "SeasonId"),
                "season": self._find_text(element, "Season"),
            },
        )

    def _build_stadiums_record(
        self,
        element: ET.Element,
        service_id: str,
        source_url: str,
        table_options: dict[str, str],
    ) -> dict[str, Any]:
        return self._build_record(
            element,
            service_id,
            source_url,
            table_options,
            {
                "stadium_id": self._find_text(element, "StadiumId"),
                "dl_provider_id": self._find_text(element, "DlProviderId"),
                "name": self._find_text(element, "Name"),
                "street": self._find_text(element, "Street"),
                "zip_code": self._find_text(element, "ZipCode", "Zip"),
                "city": self._find_text(element, "City"),
                "country": self._find_text(element, "Country"),
                "turf": self._find_text(element, "Turf"),
                "roofed_over": self._find_bool(element, "RoofedOver"),
                "valid_to": self._find_text(element, "ValidTo"),
            },
        )

    def _build_clubs_record(
        self,
        element: ET.Element,
        service_id: str,
        source_url: str,
        table_options: dict[str, str],
    ) -> dict[str, Any]:
        return self._build_record(
            element,
            service_id,
            source_url,
            table_options,
            {
                "club_id": self._find_text(element, "ClubId"),
                "dl_provider_id": self._find_text(element, "DlProviderId"),
                "competition_id": self._find_text(element, "CompetitionId")
                or table_options.get("competition_id"),
                "season_id": self._find_text(element, "SeasonId") or table_options.get("season_id"),
                "stadium_id": self._find_text(element, "StadiumId"),
                "club_name": self._find_text(element, "ClubName", "Name"),
                "short_name": self._find_text(element, "ShortName"),
                "abbreviation": self._find_text(element, "Abbreviation"),
                "city": self._find_text(element, "City"),
                "country": self._find_text(element, "Country"),
                "valid_to": self._find_text(element, "ValidTo"),
            },
        )

    def _build_players_record(
        self,
        element: ET.Element,
        service_id: str,
        source_url: str,
        table_options: dict[str, str],
    ) -> dict[str, Any]:
        return self._build_record(
            element,
            service_id,
            source_url,
            table_options,
            {
                "object_id": self._find_text(element, "ObjectId", "PersonId", "PlayerId"),
                "dl_provider_id": self._find_text(element, "DlProviderId"),
                "club_id": self._find_text(element, "ClubId") or table_options.get("club_id"),
                "club_name": self._find_text(element, "ClubName"),
                "season_id": table_options.get("season_id"),
                "type": self._find_text(element, "Type"),
                "secondary_type": self._find_text(element, "SecondaryType"),
                "tertiary_type": self._find_text(element, "TertiaryType"),
                "first_name": self._find_text(element, "FirstName"),
                "last_name": self._find_text(element, "LastName"),
                "name": self._find_text(element, "Name"),
                "birth_date": self._find_text(element, "BirthDate"),
                "nationality": self._find_text(element, "Nationality"),
                "shirt_number": self._find_text(element, "ShirtNumber"),
                "position_de": self._find_text(element, "PositionDE", "Position_GER"),
                "position_en": self._find_text(element, "PositionEN", "Position_ENG"),
                "position_es": self._find_text(element, "PositionES", "Position_SPA"),
                "primary_pool": self._find_text(element, "PrimaryPool"),
                "local_player": self._find_bool(element, "LocalPlayer"),
                "leave_date": self._find_text(element, "LeaveDate"),
                "valid_to": self._find_text(element, "ValidTo"),
                "canceled": self._find_bool(element, "Canceled"),
            },
        )

    def _build_team_officials_record(
        self,
        element: ET.Element,
        service_id: str,
        source_url: str,
        table_options: dict[str, str],
    ) -> dict[str, Any]:
        return self._build_record(
            element,
            service_id,
            source_url,
            table_options,
            {
                "object_id": self._find_text(element, "ObjectId", "PersonId"),
                "dl_provider_id": self._find_text(element, "DlProviderId"),
                "club_id": self._find_text(element, "ClubId") or table_options.get("club_id"),
                "club_name": self._find_text(element, "ClubName"),
                "season_id": table_options.get("season_id"),
                "type": self._find_text(element, "Type"),
                "secondary_type": self._find_text(element, "SecondaryType"),
                "tertiary_type": self._find_text(element, "TertiaryType"),
                "first_name": self._find_text(element, "FirstName"),
                "last_name": self._find_text(element, "LastName"),
                "name": self._find_text(element, "Name"),
                "birth_date": self._find_text(element, "BirthDate"),
                "nationality": self._find_text(element, "Nationality"),
                "leave_date": self._find_text(element, "LeaveDate"),
                "valid_to": self._find_text(element, "ValidTo"),
                "canceled": self._find_bool(element, "Canceled"),
            },
        )

    def _build_referees_record(
        self,
        element: ET.Element,
        service_id: str,
        source_url: str,
        table_options: dict[str, str],
    ) -> dict[str, Any]:
        return self._build_record(
            element,
            service_id,
            source_url,
            table_options,
            {
                "object_id": self._find_text(element, "ObjectId", "PersonId"),
                "dl_provider_id": self._find_text(element, "DlProviderId"),
                "type": self._find_text(element, "Type"),
                "secondary_type": self._find_text(element, "SecondaryType"),
                "tertiary_type": self._find_text(element, "TertiaryType"),
                "first_name": self._find_text(element, "FirstName"),
                "last_name": self._find_text(element, "LastName"),
                "name": self._find_text(element, "Name"),
                "birth_date": self._find_text(element, "BirthDate"),
                "nationality": self._find_text(element, "Nationality"),
                "leave_date": self._find_text(element, "LeaveDate"),
                "valid_to": self._find_text(element, "ValidTo"),
                "canceled": self._find_bool(element, "Canceled"),
            },
        )

    def _build_suspensions_record(
        self,
        element: ET.Element,
        service_id: str,
        source_url: str,
        table_options: dict[str, str],
    ) -> dict[str, Any]:
        return self._build_record(
            element,
            service_id,
            source_url,
            table_options,
            {
                "id": self._find_text(element, "Id"),
                "season_id": table_options.get("season_id"),
                "object_id": self._find_text(element, "ObjectId", "PersonId"),
                "start_date": self._find_text(element, "StartDate"),
                "end_date": self._find_text(element, "EndDate"),
                "reason": self._find_text(element, "Reason"),
                "matches": self._find_text(element, "Matches"),
                "type": self._find_text(element, "Type"),
                "validity": self._find_text(element, "Validity"),
                "authority": self._find_text(element, "Authority"),
                "annulation_date": self._find_text(element, "AnnulationDate"),
            },
        )

    def _build_fixtures_schedule_record(
        self,
        element: ET.Element,
        service_id: str,
        source_url: str,
        table_options: dict[str, str],
    ) -> dict[str, Any]:
        return self._build_record(
            element,
            service_id,
            source_url,
            table_options,
            {
                "match_id": self._find_text(element, "MatchId"),
                "dl_provider_id": self._find_text(element, "DlProviderId"),
                "competition_id": self._find_text(element, "CompetitionId")
                or table_options.get("competition_id"),
                "matchday_id": self._find_text(element, "MatchDayId")
                or table_options.get("matchday_id"),
                "season_id": self._find_text(element, "SeasonId") or table_options.get("season_id"),
                "planned_kickoff_time": self._find_text(element, "PlannedKickoffTime"),
                "stadium_id": self._find_text(element, "StadiumId"),
                "home_team_id": self._find_text(element, "HomeTeamId"),
                "guest_team_id": self._find_text(element, "GuestTeamId"),
                "match_date_fixed": self._find_text(element, "MatchDateFixed"),
                "date_quality": self._find_text(element, "DateQuality"),
                "official_information": self._find_text(element, "OfficialInformation"),
                "start_date": self._find_text(element, "StartDate"),
                "end_date": self._find_text(element, "EndDate"),
                "sub_league": self._find_text(element, "SubLeague"),
                "group": self._find_text(element, "Group"),
                "valid_to": self._find_text(element, "ValidTo"),
            },
        )

    def _build_seasons_record(
        self,
        element: ET.Element,
        service_id: str,
        source_url: str,
        table_options: dict[str, str],
    ) -> dict[str, Any]:
        return self._build_record(
            element,
            service_id,
            source_url,
            table_options,
            {
                "competition_id": table_options.get("competition_id"),
                "season_id": self._find_text(element, "SeasonId"),
                "season": self._find_text(element, "Season"),
            },
        )

    def _build_match_information_record(
        self,
        element: ET.Element,
        service_id: str,
        source_url: str,
        table_options: dict[str, str],
    ) -> dict[str, Any]:
        match_information_element = self._find_child(element, "MatchInformation") or element
        general = self._find_child(match_information_element, "General") or match_information_element
        return self._build_record(
            match_information_element,
            service_id,
            source_url,
            table_options,
            {
                "match_id": self._find_text(general, "MatchId") or table_options.get("match_id"),
                "competition_id": self._find_text(general, "CompetitionId"),
                "season_id": self._find_text(general, "SeasonId"),
                "matchday": self._find_text(general, "MatchDay"),
                "planned_kickoff_time": self._find_text(general, "PlannedKickoffTime"),
                "kickoff_time": self._find_text(general, "KickoffTime"),
                "home_team_id": self._find_text(general, "HomeTeamId"),
                "guest_team_id": self._find_text(general, "GuestTeamId"),
                "result": self._find_text(general, "Result"),
                "general_json": self._element_json(general),
                "teams_json": self._element_json(
                    self._find_child(match_information_element, "Teams")
                ),
                "environment_json": self._element_json(
                    self._find_child(match_information_element, "Environment")
                ),
                "referees_json": self._element_json(
                    self._find_child(match_information_element, "Referees")
                ),
                "other_game_information_json": self._element_json(
                    self._find_child(match_information_element, "OtherGameInformation")
                ),
            },
        )

    def _build_eventdata_match_statistics_match_record(
        self,
        element: ET.Element,
        service_id: str,
        source_url: str,
        table_options: dict[str, str],
    ) -> dict[str, Any]:
        return self._build_match_statistics_record(
            element,
            service_id,
            source_url,
            table_options,
            default_scope="match",
        )

    def _build_eventdata_match_statistics_intervals_record(
        self,
        element: ET.Element,
        service_id: str,
        source_url: str,
        table_options: dict[str, str],
    ) -> dict[str, Any]:
        return self._build_match_statistics_record(
            element,
            service_id,
            source_url,
            table_options,
        )

    def _build_eventdata_match_statistics_periods_record(
        self,
        element: ET.Element,
        service_id: str,
        source_url: str,
        table_options: dict[str, str],
    ) -> dict[str, Any]:
        return self._build_match_statistics_record(
            element,
            service_id,
            source_url,
            table_options,
        )

    def _build_match_statistics_record(
        self,
        element: ET.Element,
        service_id: str,
        source_url: str,
        table_options: dict[str, str],
        default_scope: str | None = None,
    ) -> dict[str, Any]:
        team_statistics = self._find_children(element, "TeamStatistic") or self._find_descendants(
            element, "TeamStatistic"
        )
        return self._build_record(
            element,
            service_id,
            source_url,
            table_options,
            {
                "match_id": self._find_text(element, "MatchId") or table_options.get("match_id"),
                "competition_id": self._find_text(element, "CompetitionId"),
                "season_id": self._find_text(element, "SeasonId"),
                "matchday_id": self._find_text(element, "MatchDayId"),
                "home_team_id": self._find_text(element, "HomeTeamId"),
                "guest_team_id": self._find_text(element, "GuestTeamId"),
                "creation_date": self._find_text(element, "CreationDate"),
                "data_status": self._find_text(element, "DataStatus"),
                "match_status": self._find_text(element, "MatchStatus"),
                "minute_of_play": self._find_text(element, "MinuteOfPlay"),
                "scope": self._find_text(element, "Scope") or default_scope,
                "result": self._find_text(element, "Result"),
                "team_statistics_json": self._elements_json(team_statistics),
            },
        )

    def _build_eventdata_match_basic_record(
        self,
        element: ET.Element,
        service_id: str,
        source_url: str,
        table_options: dict[str, str],
    ) -> dict[str, Any]:
        return self._build_match_event_payload_record(
            element,
            service_id,
            source_url,
            table_options,
        )

    def _build_eventdata_match_basic_extended_record(
        self,
        element: ET.Element,
        service_id: str,
        source_url: str,
        table_options: dict[str, str],
    ) -> dict[str, Any]:
        return self._build_match_event_payload_record(
            element,
            service_id,
            source_url,
            table_options,
        )

    def _build_match_event_payload_record(
        self,
        element: ET.Element,
        service_id: str,
        source_url: str,
        table_options: dict[str, str],
    ) -> dict[str, Any]:
        game_sections = self._find_descendants(element, "GameSection")
        return self._build_record(
            element,
            service_id,
            source_url,
            table_options,
            {
                "match_id": self._find_text(element, "MatchId") or table_options.get("match_id"),
                "competition_id": self._find_text(element, "CompetitionId"),
                "season_id": self._find_text(element, "SeasonId"),
                "matchday_id": self._find_text(element, "MatchDayId"),
                "home_team_id": self._find_text(element, "HomeTeamId"),
                "guest_team_id": self._find_text(element, "GuestTeamId"),
                "planned_kickoff_time": self._find_text(element, "PlannedKickoffTime"),
                "kickoff_time": self._find_text(element, "KickoffTime"),
                "creation_date": self._find_text(element, "CreationDate"),
                "data_status": self._find_text(element, "DataStatus"),
                "match_status": self._find_text(element, "MatchStatus"),
                "minute_of_play": self._find_text(element, "MinuteOfPlay"),
                "result": self._find_text(element, "Result"),
                "game_sections_json": self._elements_json(game_sections),
            },
        )

    def _build_season_tables_record(
        self,
        element: ET.Element,
        service_id: str,
        source_url: str,
        table_options: dict[str, str],
    ) -> dict[str, Any]:
        entries = self._find_children(element, "Entry") or self._find_descendants(element, "Entry")
        table_type = self._find_text(element, "TableType", "Type") or "unknown"
        return self._build_record(
            element,
            service_id,
            source_url,
            table_options,
            {
                "competition_id": self._find_text(element, "CompetitionId")
                or table_options.get("competition_id"),
                "season_id": self._find_text(element, "SeasonId"),
                "matchday_id": self._find_text(element, "MatchDayId"),
                "table_type": table_type,
                "group_name": self._find_text(element, "Group"),
                "table_name": self._find_text(element, "Name", "Description"),
                "data_status": self._find_text(element, "DataStatus"),
                "creation_date": self._find_text(element, "CreationDate"),
                "entries_json": self._elements_json(entries),
            },
        )

    def _build_video_assist_events_record(
        self,
        element: ET.Element,
        service_id: str,
        source_url: str,
        table_options: dict[str, str],
    ) -> dict[str, Any]:
        event_id = self._find_text(element, "Id", "EventId") or self._stable_identifier(
            table_options.get("match_id"),
            ET.tostring(element, encoding="unicode"),
        )
        return self._build_record(
            element,
            service_id,
            source_url,
            table_options,
            {
                "match_id": self._find_text(element, "MatchId") or table_options.get("match_id"),
                "event_id": event_id,
                "graphics_id": self._find_text(element, "GraphicsId"),
                "status": self._find_text(element, "Status"),
                "minute_of_play": self._find_text(element, "MinuteOfPlay"),
                "event_time": self._find_text(element, "EventTime"),
                "creation_date": self._find_text(element, "CreationDate"),
                "score": self._find_text(element, "Score"),
                "category_json": self._element_json(self._find_child(element, "Category")),
                "referee_decision_json": self._element_json(
                    self._find_child(element, "RefereeDecision")
                ),
                "reason_json": self._element_json(self._find_child(element, "Reason")),
                "result_json": self._element_json(self._find_child(element, "Result")),
                "descriptions_json": self._element_json(self._find_child(element, "Descriptions")),
                "texts_json": self._elements_json(self._find_descendants(element, "Text")),
            },
        )

    def _build_eventdata_season_statistics_competition_record(
        self,
        element: ET.Element,
        service_id: str,
        source_url: str,
        table_options: dict[str, str],
    ) -> dict[str, Any]:
        return self._build_season_statistics_record(
            element,
            service_id,
            source_url,
            table_options,
        )

    def _build_eventdata_season_statistics_club_record(
        self,
        element: ET.Element,
        service_id: str,
        source_url: str,
        table_options: dict[str, str],
    ) -> dict[str, Any]:
        return self._build_season_statistics_record(
            element,
            service_id,
            source_url,
            table_options,
        )

    def _build_positional_season_statistics_competition_record(
        self,
        element: ET.Element,
        service_id: str,
        source_url: str,
        table_options: dict[str, str],
    ) -> dict[str, Any]:
        return self._build_season_statistics_record(
            element,
            service_id,
            source_url,
            table_options,
        )

    def _build_positional_season_statistics_club_record(
        self,
        element: ET.Element,
        service_id: str,
        source_url: str,
        table_options: dict[str, str],
    ) -> dict[str, Any]:
        return self._build_season_statistics_record(
            element,
            service_id,
            source_url,
            table_options,
        )

    def _build_season_statistics_record(
        self,
        element: ET.Element,
        service_id: str,
        source_url: str,
        table_options: dict[str, str],
    ) -> dict[str, Any]:
        team_statistics = self._find_children(element, "TeamStatistic") or self._find_descendants(
            element, "TeamStatistic"
        )
        team_id = self._find_direct_text_or_attr(element, "TeamId")
        return self._build_record(
            element,
            service_id,
            source_url,
            table_options,
            {
                "competition_id": self._find_direct_text_or_attr(element, "CompetitionId")
                or table_options.get("competition_id"),
                "season_id": self._find_direct_text_or_attr(element, "SeasonId")
                or table_options.get("season_id"),
                "club_id": table_options.get("club_id")
                or self._find_direct_text_or_attr(element, "ClubId"),
                "team_id": team_id,
                "matchday_id": self._find_direct_text_or_attr(element, "MatchDayId"),
                "matchday_status": self._find_direct_text_or_attr(element, "MatchDayStatus"),
                "creation_date": self._find_direct_text_or_attr(element, "CreationDate"),
                "team_statistics_json": self._elements_json(team_statistics),
            },
        )

    def _read_feed07_rankings_table(
        self,
        root: ET.Element,
        table_name: str,
        service_id: str,
        source_url: str,
        table_options: dict[str, str],
    ) -> list[dict[str, Any]]:
        ranking_feed = self._find_descendants(root, "RankingFeed")
        if not ranking_feed:
            return []

        feed = ranking_feed[0]
        metadata = self._find_child(feed, "MetaData")
        rankings = self._find_children(feed, "Ranking")
        return [
            self._build_feed07_ranking_record(
                ranking,
                table_name,
                feed,
                metadata,
                service_id,
                source_url,
                table_options,
            )
            for ranking in rankings
        ]

    def _read_feed0402_postmatch_table(
        self,
        root: ET.Element,
        table_name: str,
        service_id: str,
        source_url: str,
        table_options: dict[str, str],
    ) -> Iterable[dict[str, Any]]:
        positions = self._find_child(root, "Positions")
        if positions is None:
            return iter(())

        event_time = self._find_direct_text_or_attr(positions, "EventTime")
        if table_name == "tracking_pitch_metadata":
            metadata_elements = [
                metadata
                for metadata in self._find_children(positions, "MetaData")
                if self._find_direct_text_or_attr(metadata, "Type") == "pitch-size"
            ]
            return iter(
                [
                self._build_record(
                    metadata,
                    service_id,
                    source_url,
                    table_options,
                    {
                        "match_id": self._find_direct_text_or_attr(metadata, "MatchId")
                        or table_options.get("match_id"),
                        "metadata_type": self._find_direct_text_or_attr(metadata, "Type"),
                        "event_time": event_time,
                        "pitch_x": self._find_direct_text_or_attr(
                            self._find_child(metadata, "PitchSize"), "X"
                        ),
                        "pitch_y": self._find_direct_text_or_attr(
                            self._find_child(metadata, "PitchSize"), "Y"
                        ),
                    },
                )
                for metadata in metadata_elements
                ]
            )

        framesets = self._find_children(positions, "FrameSet")
        retrieved_at = datetime.now(timezone.utc).isoformat()
        return (
            {
                "match_id": self._find_direct_text_or_attr(frameset, "MatchId")
                or table_options.get("match_id"),
                "game_section": self._find_direct_text_or_attr(frameset, "GameSection"),
                "team_id": self._find_direct_text_or_attr(frameset, "TeamId"),
                "person_id": self._find_direct_text_or_attr(frameset, "PersonId"),
                "event_time": event_time,
                "frame_count": str(len(frame_objects)),
                "first_frame_number": frame_objects[0].get("N") if frame_objects else None,
                "last_frame_number": frame_objects[-1].get("N") if frame_objects else None,
                "frames_json": json.dumps(frame_objects, sort_keys=True),
                "source_service_id": service_id,
                "source_url": source_url,
                "requested_parameters": dict(table_options),
                "source_record": {key: str(value) for key, value in frameset.attrib.items()},
                "raw_xml": None,
                "retrieved_at": retrieved_at,
            }
            for frameset in framesets
            for frame_objects in [[dict(frame.attrib) for frame in self._find_children(frameset, "Frame")]]
        )

    def _read_feed0402_streaming_table(
        self, table_name: str, table_options: dict[str, str]
    ) -> list[dict[str, Any]] | None:
        self._ensure_access_token()
        service_id = TABLE_CONFIG[table_name]["service_id"]
        source_url = self._build_source_url(table_name, service_id, table_options)
        batch_limit = self._max_records_per_batch(table_options)
        request = urllib.request.Request(
            source_url,
            headers=dict(self._default_headers)
            | {"Authorization": f"Bearer {self._access_token}"},
            method="GET",
        )

        try:
            with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                records: list[dict[str, Any]] = []
                event_time: str | None = None
                retrieved_at = datetime.now(timezone.utc).isoformat()
                for event, element in ET.iterparse(response, events=("start", "end")):
                    local_name = self._local_name(element.tag)
                    if event == "start" and local_name == "Positions":
                        event_time = element.attrib.get("EventTime")
                        continue
                    if event != "end":
                        continue

                    if table_name == "tracking_pitch_metadata" and local_name == "MetaData":
                        if self._find_direct_text_or_attr(element, "Type") != "pitch-size":
                            continue
                        records.append(
                            self._build_record(
                                element,
                                service_id,
                                source_url,
                                table_options,
                                {
                                    "match_id": self._find_direct_text_or_attr(element, "MatchId")
                                    or table_options.get("match_id"),
                                    "metadata_type": self._find_direct_text_or_attr(element, "Type"),
                                    "event_time": event_time,
                                    "pitch_x": self._find_direct_text_or_attr(
                                        self._find_child(element, "PitchSize"), "X"
                                    ),
                                    "pitch_y": self._find_direct_text_or_attr(
                                        self._find_child(element, "PitchSize"), "Y"
                                    ),
                                },
                            )
                        )
                    elif table_name == "tracking_postmatch_framesets" and local_name == "FrameSet":
                        frame_objects = [
                            dict(frame.attrib) for frame in self._find_children(element, "Frame")
                        ]
                        records.append(
                            {
                                "match_id": self._find_direct_text_or_attr(element, "MatchId")
                                or table_options.get("match_id"),
                                "game_section": self._find_direct_text_or_attr(
                                    element, "GameSection"
                                ),
                                "team_id": self._find_direct_text_or_attr(element, "TeamId"),
                                "person_id": self._find_direct_text_or_attr(element, "PersonId"),
                                "event_time": event_time,
                                "frame_count": str(len(frame_objects)),
                                "first_frame_number": (
                                    frame_objects[0].get("N") if frame_objects else None
                                ),
                                "last_frame_number": (
                                    frame_objects[-1].get("N") if frame_objects else None
                                ),
                                "frames_json": json.dumps(frame_objects, sort_keys=True),
                                "source_service_id": service_id,
                                "source_url": source_url,
                                "requested_parameters": dict(table_options),
                                "source_record": {
                                    key: str(value) for key, value in element.attrib.items()
                                },
                                "raw_xml": None,
                                "retrieved_at": retrieved_at,
                            }
                        )

                    if records and (batch_limit is None or len(records) >= batch_limit):
                        return records
        except Exception:
            return None

        return records

    def _read_feed0404_distance_match_table(
        self,
        root: ET.Element,
        table_name: str,
        service_id: str,
        source_url: str,
        table_options: dict[str, str],
    ) -> list[dict[str, Any]]:
        statistical_data = self._find_child(root, "StatisticalData")
        if statistical_data is None:
            return []

        match_id = self._find_direct_text_or_attr(statistical_data, "MatchId") or table_options.get(
            "match_id"
        )
        stat_type = self._find_direct_text_or_attr(statistical_data, "Type")
        event_time = self._find_direct_text_or_attr(statistical_data, "EventTime")
        teams = self._find_children(statistical_data, "Team")

        if table_name == "distance_match_teams":
            return [
                self._build_record(
                    team,
                    service_id,
                    source_url,
                    table_options,
                    {
                        "match_id": match_id,
                        "stat_type": stat_type,
                        "event_time": event_time,
                        "team_id": self._find_direct_text_or_attr(team, "TeamId"),
                        "distance_covered": self._find_direct_text_or_attr(team, "DistanceCovered"),
                    },
                )
                for team in teams
            ]

        return [
            self._build_record(
                person,
                service_id,
                source_url,
                table_options,
                {
                    "match_id": match_id,
                    "stat_type": stat_type,
                    "event_time": event_time,
                    "team_id": self._find_direct_text_or_attr(team, "TeamId"),
                    "person_id": self._find_direct_text_or_attr(person, "PersonId"),
                    "distance_covered": self._find_direct_text_or_attr(person, "DistanceCovered"),
                    "average_speed": self._find_direct_text_or_attr(person, "AverageSpeed"),
                    "maximal_speed": self._find_direct_text_or_attr(person, "MaximalSpeed"),
                },
            )
            for team in teams
            for person in self._find_children(team, "Person")
        ]

    def _read_feed0405_speed_intervals_table(
        self,
        root: ET.Element,
        table_name: str,
        service_id: str,
        source_url: str,
        table_options: dict[str, str],
    ) -> list[dict[str, Any]]:
        metadata = self._find_child(root, "MetaData")
        statistical_data = self._find_child(root, "StatisticalData")
        if metadata is None and statistical_data is None:
            return []

        metadata_source = metadata if metadata is not None else statistical_data
        match_id = self._find_direct_text_or_attr(metadata_source, "MatchId") or table_options.get(
            "match_id"
        )
        stat_type = self._find_direct_text_or_attr(metadata_source, "Type")
        event_time = self._find_direct_text_or_attr(statistical_data, "EventTime")

        if table_name == "speed_interval_definitions":
            intervals = self._find_children(metadata, "Interval")
            return [
                self._build_record(
                    interval,
                    service_id,
                    source_url,
                    table_options,
                    {
                        "match_id": match_id,
                        "stat_type": stat_type,
                        "interval_id": self._find_direct_text_or_attr(interval, "Id"),
                        "from_value": self._find_direct_text_or_attr(interval, "fromValue"),
                        "from_interpretation": self._find_direct_text_or_attr(
                            interval, "fromInterpretation"
                        ),
                        "to_value": self._find_direct_text_or_attr(interval, "toValue"),
                        "to_interpretation": self._find_direct_text_or_attr(
                            interval, "toInterpretation"
                        ),
                    },
                )
                for interval in intervals
            ]

        persons = self._find_children(statistical_data, "Person")
        return [
            self._build_record(
                interval,
                service_id,
                source_url,
                table_options,
                {
                    "match_id": match_id,
                    "stat_type": stat_type,
                    "event_time": event_time,
                    "person_id": self._find_direct_text_or_attr(person, "PersonId"),
                    "interval_id": self._find_direct_text_or_attr(interval, "IntervalId"),
                    "count": self._find_direct_text_or_attr(interval, "Count"),
                    "distance_covered": self._find_direct_text_or_attr(interval, "DistanceCovered"),
                    "amount": self._find_direct_text_or_attr(interval, "Amount"),
                },
            )
            for person in persons
            for interval in self._find_children(person, "Interval")
        ]

    def _read_feed0407_positional_match_statistics_table(
        self,
        root: ET.Element,
        table_name: str,
        service_id: str,
        source_url: str,
        table_options: dict[str, str],
    ) -> list[dict[str, Any]]:
        match_statistic = self._find_child(root, "MatchStatistic")
        if match_statistic is None:
            return []

        match_values = self._feed0407_attribute_values(
            match_statistic, FEED0407_MATCH_STATISTIC_ATTRIBUTES
        )
        teams = self._find_children(match_statistic, "TeamStatistic")

        if table_name == "positional_match_team_statistics":
            return [
                self._build_record(
                    team,
                    service_id,
                    source_url,
                    table_options,
                    {
                        **match_values,
                        **self._feed0407_attribute_values(team, FEED0407_TEAM_STATISTIC_ATTRIBUTES),
                    },
                )
                for team in teams
            ]

        return [
            self._build_record(
                player,
                service_id,
                source_url,
                table_options,
                {
                    **match_values,
                    "team_id": self._find_direct_text_or_attr(team, "TeamId"),
                    **self._feed0407_attribute_values(
                        player, FEED0407_PLAYER_STATISTIC_ATTRIBUTES
                    ),
                },
            )
            for team in teams
            for player in self._find_children(team, "PlayerStatistic")
        ]

    def _feed0407_attribute_values(
        self, element: ET.Element, attribute_names: list[str]
    ) -> dict[str, str | None]:
        return {
            camel_to_snake(attribute_name): self._find_direct_text_or_attr(element, attribute_name)
            for attribute_name in attribute_names
        }

    def _read_feed0604_player_topspeed_alert_table(
        self,
        root: ET.Element,
        table_name: str,
        service_id: str,
        source_url: str,
        table_options: dict[str, str],
    ) -> list[dict[str, Any]]:
        alert = self._find_child(root, "PlayerTopSpeedAlert")
        if alert is None:
            return []

        alert_values = {
            camel_to_snake(attribute_name): self._find_direct_text_or_attr(alert, attribute_name)
            for attribute_name in FEED0604_ALERT_ATTRIBUTES
        }
        change_scope = self._find_child(alert, "ChangeScope")
        fastest_player = self._find_child(self._find_child(alert, "FastestPlayer"), "Player")
        alert_flags = self._find_child(self._find_child(alert, "FastestPlayer"), "Alert")
        rankings = self._find_children(self._find_child(alert, "RecordRankings"), "Ranking")

        if table_name == "player_topspeed_alerts":
            return [
                self._build_record(
                    alert,
                    service_id,
                    source_url,
                    table_options,
                    {
                        **alert_values,
                        "change_scope_type": self._find_direct_text_or_attr(change_scope, "Type"),
                        **{
                            camel_to_snake(attribute_name): self._find_direct_text_or_attr(
                                fastest_player, attribute_name
                            )
                            for attribute_name in FEED0604_FASTEST_PLAYER_ATTRIBUTES
                        },
                        **{
                            camel_to_snake(attribute_name): self._find_direct_text_or_attr(
                                alert_flags, attribute_name
                            )
                            for attribute_name in FEED0604_ALERT_FLAG_ATTRIBUTES
                        },
                    },
                )
            ]

        return [
            self._build_record(
                entry,
                service_id,
                source_url,
                table_options,
                {
                    **alert_values,
                    "ranking_type": self._find_direct_text_or_attr(ranking, "Type"),
                    **{
                        camel_to_snake(attribute_name): self._find_direct_text_or_attr(
                            entry, attribute_name
                        )
                        for attribute_name in FEED0604_RANKING_ENTRY_ATTRIBUTES
                    },
                },
            )
            for ranking in rankings
            for entry in self._find_children(ranking, "ListEntry")
        ]

    def _read_feed0605_attacking_zones_table(
        self,
        root: ET.Element,
        table_name: str,
        service_id: str,
        source_url: str,
        table_options: dict[str, str],
    ) -> list[dict[str, Any]]:
        attacking_zones = self._find_child(root, "AttackingZones")
        if attacking_zones is None:
            return []

        header_values = {
            camel_to_snake(attribute_name): self._find_direct_text_or_attr(
                attacking_zones, attribute_name
            )
            for attribute_name in FEED0605_ATTACKING_ZONES_ATTRIBUTES
        }
        teams = self._find_children(attacking_zones, "Team")

        return [
            self._build_record(
                zone,
                service_id,
                source_url,
                table_options,
                {
                    **header_values,
                    **{
                        camel_to_snake(attribute_name): self._find_direct_text_or_attr(
                            team, attribute_name
                        )
                        for attribute_name in FEED0605_TEAM_ATTRIBUTES
                    },
                    "team_entries": self._find_direct_text_or_attr(zones, "Entries"),
                    **{
                        camel_to_snake(attribute_name): self._find_direct_text_or_attr(
                            zone, attribute_name
                        )
                        for attribute_name in FEED0605_ZONE_ATTRIBUTES
                    },
                },
            )
            for team in teams
            for zones in [self._find_child(team, "Zones")]
            if zones is not None
            for zone in self._find_children(zones, "Zone")
        ]

    def _read_feed0606_xg_rankings_table(
        self,
        root: ET.Element,
        table_name: str,
        service_id: str,
        source_url: str,
        table_options: dict[str, str],
    ) -> list[dict[str, Any]]:
        xg_rankings = self._find_child(root, "xGRankings")
        if xg_rankings is None:
            return []

        header_values = {
            camel_to_snake(attribute_name): self._find_direct_text_or_attr(
                xg_rankings, attribute_name
            )
            for attribute_name in FEED0606_XG_RANKINGS_ATTRIBUTES
        }
        header_values["competition_id"] = header_values.get("competition_id") or table_options.get(
            "competition_id"
        )
        header_values["season_id"] = header_values.get("season_id") or table_options.get("season_id")

        return [
            self._build_record(
                entry,
                service_id,
                source_url,
                table_options,
                {
                    **header_values,
                    "ranking_type": self._find_direct_text_or_attr(ranking, "Type"),
                    "scope": self._find_direct_text_or_attr(ranking, "Scope"),
                    "time_scope": self._find_direct_text_or_attr(ranking, "TimeScope"),
                    **{
                        camel_to_snake(attribute_name): self._find_direct_text_or_attr(
                            entry, attribute_name
                        )
                        for attribute_name in FEED0606_RANKING_ENTRY_ATTRIBUTES
                    },
                },
            )
            for ranking in self._find_children(xg_rankings, "Ranking")
            for entry in self._find_children(ranking, "ListEntry")
        ]

    def _read_feed0610_win_probability_table(
        self,
        root: ET.Element,
        table_name: str,
        service_id: str,
        source_url: str,
        table_options: dict[str, str],
    ) -> list[dict[str, Any]]:
        win_probability = self._find_child(root, "WinProbability")
        if win_probability is None:
            return []

        header_values = {
            camel_to_snake(attribute_name): self._find_direct_text_or_attr(
                win_probability, attribute_name
            )
            for attribute_name in FEED0610_WIN_PROBABILITY_ATTRIBUTES
        }
        by_minute = self._find_child(win_probability, "WinProbabilityByMinute")

        return [
            self._build_record(
                probability,
                service_id,
                source_url,
                table_options,
                {
                    **header_values,
                    "scope": self._find_direct_text_or_attr(by_minute, "Scope"),
                    **{
                        camel_to_snake(attribute_name): self._find_direct_text_or_attr(
                            probability, attribute_name
                        )
                        for attribute_name in FEED0610_PROBABILITY_ATTRIBUTES
                    },
                },
            )
            for probability in self._find_children(by_minute, "Probability")
        ]

    def _read_feed0615_advanced_events_table(
        self,
        root: ET.Element,
        table_name: str,
        service_id: str,
        source_url: str,
        table_options: dict[str, str],
    ) -> list[dict[str, Any]]:
        advanced_events = self._find_child(root, "AdvancedEvents")
        if advanced_events is None:
            return []

        header_values = {
            camel_to_snake(attribute_name): self._find_direct_text_or_attr(
                advanced_events, attribute_name
            )
            for attribute_name in FEED0615_ADVANCED_EVENTS_ATTRIBUTES
        }

        records: list[dict[str, Any]] = []
        for event in self._find_children(advanced_events, "Event"):
            payload = self._first_child(event)
            if payload is None:
                continue

            event_type = self._local_name(payload.tag)
            if table_name == "advanced_event_plays" and event_type != "Play":
                continue
            if table_name == "advanced_event_receptions" and event_type != "Reception":
                continue
            if table_name == "advanced_event_carries" and event_type != "Carry":
                continue
            if (
                table_name == "advanced_event_team_possessions"
                and event_type != "TeamPossession"
            ):
                continue
            if (
                table_name == "advanced_event_other_ball_actions"
                and event_type != "OtherBallAction"
            ):
                continue
            if table_name == "advanced_event_tackling_games" and event_type != "TacklingGame":
                continue
            if table_name == "advanced_event_fouls" and event_type != "Foul":
                continue
            if table_name == "advanced_event_shots_at_goal" and event_type != "ShotAtGoal":
                continue

            if table_name == "advanced_events_raw":
                records.append(
                    self._build_record(
                        payload,
                        service_id,
                        source_url,
                        table_options,
                        {
                            **header_values,
                            "event_type": event_type,
                            "event_id": self._find_direct_text_or_attr(payload, "EventId"),
                            "team_id": self._find_direct_text_or_attr(payload, "TeamId"),
                            "player_id": self._find_direct_text_or_attr(payload, "PlayerId"),
                            "receiver_id": self._find_direct_text_or_attr(payload, "ReceiverId"),
                            "play_id": self._find_direct_text_or_attr(payload, "PlayId"),
                            "in_game_section": self._find_direct_text_or_attr(payload, "InGameSection"),
                            "game_section": self._find_direct_text_or_attr(payload, "GameSection"),
                            "sync_successful": self._find_direct_text_or_attr(
                                payload, "SyncSuccessful"
                            ),
                            "synced_event_time": self._find_direct_text_or_attr(
                                payload, "SyncedEventTime"
                            ),
                            "game_time": self._find_direct_text_or_attr(payload, "GameTime"),
                            "synced_frame_id": self._find_direct_text_or_attr(
                                payload, "SyncedFrameId"
                            ),
                            "event_attributes_json": self._element_json(payload),
                            "event_xml": ET.tostring(payload, encoding="unicode"),
                        },
                    )
                )
                continue

            attribute_names_by_table = {
                "advanced_event_plays": FEED0615_PLAY_ATTRIBUTES,
                "advanced_event_receptions": FEED0615_RECEPTION_ATTRIBUTES,
                "advanced_event_carries": FEED0615_CARRY_ATTRIBUTES,
                "advanced_event_team_possessions": FEED0615_TEAM_POSSESSION_ATTRIBUTES,
                "advanced_event_other_ball_actions": FEED0615_OTHER_BALL_ACTION_ATTRIBUTES,
                "advanced_event_tackling_games": FEED0615_TACKLING_GAME_ATTRIBUTES,
                "advanced_event_fouls": FEED0615_FOUL_ATTRIBUTES,
                "advanced_event_shots_at_goal": FEED0615_SHOT_AT_GOAL_ATTRIBUTES,
            }
            attribute_names = attribute_names_by_table[table_name]

            records.append(
                self._build_record(
                    payload,
                    service_id,
                    source_url,
                    table_options,
                    {
                        **header_values,
                        **{
                            camel_to_snake(attribute_name): self._find_direct_text_or_attr(
                                payload, attribute_name
                            )
                            for attribute_name in attribute_names
                        },
                        "event_attributes_json": self._element_json(payload),
                        "event_xml": ET.tostring(payload, encoding="unicode"),
                    },
                )
            )

        return records

    def _read_feed0302_postmatch_table(
        self,
        root: ET.Element,
        table_name: str,
        service_id: str,
        source_url: str,
        table_options: dict[str, str],
    ) -> list[dict[str, Any]]:
        event_elements = self._find_children(root, "Event")
        request_values = self._feed0302_request_values(root)

        records: list[dict[str, Any]] = []
        for event in event_elements:
            payload = list(event)
            if not payload:
                continue

            event_payload = payload[0]
            event_type = self._local_name(event_payload.tag)
            if table_name == "event_raw_delete_events" and event_type != "Delete":
                continue
            if table_name == "event_raw_messages" and event_type == "Delete":
                continue

            values = self._feed0302_event_base_values(event, table_options, request_values)

            if table_name == "event_raw_messages":
                values.update(
                    {
                        "x_position": self._find_direct_text_or_attr(event, "x-Position", "X-Position"),
                        "y_position": self._find_direct_text_or_attr(event, "y-Position", "Y-Position"),
                        "x_source_position": self._find_direct_text_or_attr(
                            event, "x-Source-Position", "X-Source-Position"
                        ),
                        "y_source_position": self._find_direct_text_or_attr(
                            event, "y-Source-Position", "Y-Source-Position"
                        ),
                        "start_frame": self._find_direct_text_or_attr(event, "StartFrame"),
                        "end_frame": self._find_direct_text_or_attr(event, "EndFrame"),
                        "event_type": event_type,
                        "event_attributes_json": self._element_json(event_payload),
                        "event_xml": ET.tostring(event_payload, encoding="unicode"),
                    }
                )
            elif table_name == "event_raw_var_notifications":
                if event_type != "VarNotification":
                    continue
                values.update(
                    {
                        "test_mode": self._find_direct_text_or_attr(event_payload, "TestMode"),
                        "incident": self._find_direct_text_or_attr(event_payload, "Incident"),
                        "review": self._find_direct_text_or_attr(event_payload, "Review"),
                        "result": self._find_direct_text_or_attr(event_payload, "Result"),
                        "decision": self._find_direct_text_or_attr(event_payload, "Decision"),
                        "reason": self._find_direct_text_or_attr(event_payload, "Reason"),
                        "status": self._find_direct_text_or_attr(event_payload, "Status"),
                        "event_xml": ET.tostring(event_payload, encoding="unicode"),
                    }
                )
            elif table_name == "event_raw_play_events":
                if event_type != "Play":
                    continue
                play_subtype = self._first_child(event_payload)
                values.update(
                    {
                        "team_id": self._find_direct_text_or_attr(event_payload, "Team"),
                        "player_id": self._find_direct_text_or_attr(event_payload, "Player"),
                        "recipient_player_id": self._find_direct_text_or_attr(
                            event_payload, "Recipient"
                        ),
                        "from_open_play": self._find_direct_text_or_attr(
                            event_payload, "FromOpenPlay"
                        ),
                        "ball_possession_phase": self._find_direct_text_or_attr(
                            event_payload, "BallPossessionPhase"
                        ),
                        "evaluation": self._find_direct_text_or_attr(event_payload, "Evaluation"),
                        "height": self._find_direct_text_or_attr(event_payload, "Height"),
                        "distance": self._find_direct_text_or_attr(event_payload, "Distance"),
                        "penalty_box": self._find_direct_text_or_attr(event_payload, "PenaltyBox"),
                        "goalkeeper_action": self._find_direct_text_or_attr(
                            event_payload, "GoalKeeperAction"
                        ),
                        "play_origin": self._find_direct_text_or_attr(event_payload, "PlayOrigin"),
                        "play_angle": self._find_direct_text_or_attr(event_payload, "PlayAngle"),
                        "semi_field": self._find_direct_text_or_attr(event_payload, "SemiField"),
                        "flat_cross": self._find_direct_text_or_attr(event_payload, "FlatCross"),
                        "rotation": self._find_direct_text_or_attr(event_payload, "Rotation"),
                        "play_subtype": self._local_name(play_subtype.tag)
                        if play_subtype is not None
                        else None,
                        "play_subtype_attributes_json": self._element_json(play_subtype),
                        "event_xml": ET.tostring(event_payload, encoding="unicode"),
                    }
                )
            elif table_name == "event_raw_foul_events":
                if event_type != "Foul":
                    continue
                values.update(
                    {
                        "team_fouler_id": self._find_direct_text_or_attr(event_payload, "TeamFouler"),
                        "team_fouled_id": self._find_direct_text_or_attr(event_payload, "TeamFouled"),
                        "fouler_player_id": self._find_direct_text_or_attr(event_payload, "Fouler"),
                        "fouled_player_id": self._find_direct_text_or_attr(event_payload, "Fouled"),
                        "foul_type": self._find_direct_text_or_attr(event_payload, "FoulType"),
                        "committing_player_action": self._find_direct_text_or_attr(
                            event_payload, "CommittingPlayerAction"
                        ),
                        "event_xml": ET.tostring(event_payload, encoding="unicode"),
                    }
                )
            elif table_name == "event_raw_shot_at_goal_events":
                if event_type != "ShotAtGoal":
                    continue
                shot_outcome = self._first_child(event_payload)
                values.update(
                    {
                        "x_position": self._find_direct_text_or_attr(event, "x-Position", "X-Position"),
                        "y_position": self._find_direct_text_or_attr(event, "y-Position", "Y-Position"),
                        "x_source_position": self._find_direct_text_or_attr(
                            event, "x-Source-Position", "X-Source-Position"
                        ),
                        "y_source_position": self._find_direct_text_or_attr(
                            event, "y-Source-Position", "Y-Source-Position"
                        ),
                        "x_position_from_tracking": self._find_direct_text_or_attr(
                            event, "x-PositionFromTracking", "X-PositionFromTracking"
                        ),
                        "y_position_from_tracking": self._find_direct_text_or_attr(
                            event, "y-PositionFromTracking", "Y-PositionFromTracking"
                        ),
                        "calculated_timestamp": self._find_direct_text_or_attr(
                            event, "CalculatedTimestamp"
                        ),
                        "calculated_frame": self._find_direct_text_or_attr(event, "CalculatedFrame"),
                        "start_frame": self._find_direct_text_or_attr(event, "StartFrame"),
                        "end_frame": self._find_direct_text_or_attr(event, "EndFrame"),
                        "team_id": self._find_direct_text_or_attr(event_payload, "Team"),
                        "player_id": self._find_direct_text_or_attr(event_payload, "Player"),
                        "type_of_shot": self._find_direct_text_or_attr(event_payload, "TypeOfShot"),
                        "inside_box": self._find_direct_text_or_attr(event_payload, "InsideBox"),
                        "chance_evaluation": self._find_direct_text_or_attr(
                            event_payload, "ChanceEvaluation"
                        ),
                        "shot_origin": self._find_direct_text_or_attr(event_payload, "ShotOrigin"),
                        "ball_possession_phase": self._find_direct_text_or_attr(
                            event_payload, "BallPossessionPhase"
                        ),
                        "assist_action": self._find_direct_text_or_attr(event_payload, "AssistAction"),
                        "after_free_kick": self._find_direct_text_or_attr(
                            event_payload, "AfterFreeKick"
                        ),
                        "assist_shot_at_goal": self._find_direct_text_or_attr(
                            event_payload, "AssistShotAtGoal"
                        ),
                        "assist_type_shot_at_goal": self._find_direct_text_or_attr(
                            event_payload, "AssistTypeShotAtGoal"
                        ),
                        "taker_ball_control": self._find_direct_text_or_attr(
                            event_payload, "TakerBallControl"
                        ),
                        "taker_setup": self._find_direct_text_or_attr(event_payload, "TakerSetup"),
                        "counter_attack": self._find_direct_text_or_attr(
                            event_payload, "CounterAttack"
                        ),
                        "build_up": self._find_direct_text_or_attr(event_payload, "BuildUp"),
                        "setup_origin": self._find_direct_text_or_attr(event_payload, "SetupOrigin"),
                        "shot_condition": self._find_direct_text_or_attr(
                            event_payload, "ShotCondition"
                        ),
                        "xg": self._find_direct_text_or_attr(event_payload, "xG", "XG"),
                        "distance_to_goal": self._find_direct_text_or_attr(
                            event_payload, "DistanceToGoal"
                        ),
                        "angle_to_goal": self._find_direct_text_or_attr(event_payload, "AngleToGoal"),
                        "pressure": self._find_direct_text_or_attr(event_payload, "Pressure"),
                        "player_speed": self._find_direct_text_or_attr(event_payload, "PlayerSpeed"),
                        "amount_of_defenders": self._find_direct_text_or_attr(
                            event_payload, "AmountOfDefenders"
                        ),
                        "goal_distance_goalkeeper": self._find_direct_text_or_attr(
                            event_payload, "GoalDistanceGoalkeeper"
                        ),
                        "significance_evaluation": self._find_direct_text_or_attr(
                            event_payload, "SignificanceEvaluation"
                        ),
                        "shot_outcome_type": self._local_name(shot_outcome.tag)
                        if shot_outcome is not None
                        else None,
                        "shot_outcome_attributes_json": self._element_json(shot_outcome),
                        "event_xml": ET.tostring(event_payload, encoding="unicode"),
                    }
                )
            elif table_name == "event_raw_substitution_events":
                if event_type != "Substitution":
                    continue
                values.update(
                    {
                        "team_id": self._find_direct_text_or_attr(event_payload, "Team"),
                        "player_in_id": self._find_direct_text_or_attr(event_payload, "PlayerIn"),
                        "player_out_id": self._find_direct_text_or_attr(event_payload, "PlayerOut"),
                        "playing_position": self._find_direct_text_or_attr(
                            event_payload, "PlayingPosition"
                        ),
                        "infringement": self._find_direct_text_or_attr(
                            event_payload, "Infringement"
                        ),
                        "concussion": self._find_direct_text_or_attr(event_payload, "Concussion"),
                        "event_xml": ET.tostring(event_payload, encoding="unicode"),
                    }
                )
            elif table_name == "event_raw_caution_events":
                if event_type != "Caution":
                    continue
                values.update(
                    {
                        "team_id": self._find_direct_text_or_attr(event_payload, "Team"),
                        "player_id": self._find_direct_text_or_attr(event_payload, "Player"),
                        "card_color": self._find_direct_text_or_attr(event_payload, "CardColor"),
                        "card_rating": self._find_direct_text_or_attr(event_payload, "CardRating"),
                        "reason": self._find_direct_text_or_attr(event_payload, "Reason"),
                        "other_reason": self._find_direct_text_or_attr(event_payload, "OtherReason"),
                        "event_xml": ET.tostring(event_payload, encoding="unicode"),
                    }
                )
            elif table_name == "event_raw_offside_events":
                if event_type != "Offside":
                    continue
                values.update(
                    {
                        "team_id": self._find_direct_text_or_attr(event_payload, "Team"),
                        "player_id": self._find_direct_text_or_attr(event_payload, "Player"),
                        "event_xml": ET.tostring(event_payload, encoding="unicode"),
                    }
                )
            elif table_name in {"event_raw_corner_kick_events", "event_raw_free_kick_events"}:
                expected_type = "CornerKick" if table_name == "event_raw_corner_kick_events" else "FreeKick"
                if event_type != expected_type:
                    continue
                set_piece_result = self._first_child(event_payload)
                values.update(
                    {
                        "x_position": self._find_direct_text_or_attr(event, "x-Position", "X-Position"),
                        "y_position": self._find_direct_text_or_attr(event, "y-Position", "Y-Position"),
                        "x_source_position": self._find_direct_text_or_attr(
                            event, "x-Source-Position", "X-Source-Position"
                        ),
                        "y_source_position": self._find_direct_text_or_attr(
                            event, "y-Source-Position", "Y-Source-Position"
                        ),
                        "team_id": self._find_direct_text_or_attr(event_payload, "Team"),
                        "decision_timestamp": self._find_direct_text_or_attr(
                            event_payload, "DecisionTimestamp"
                        ),
                        "side": self._find_direct_text_or_attr(event_payload, "Side"),
                        "placing": self._find_direct_text_or_attr(event_payload, "Placing"),
                        "target_area": self._find_direct_text_or_attr(event_payload, "TargetArea"),
                        "rotation": self._find_direct_text_or_attr(event_payload, "Rotation"),
                        "post_marking": self._find_direct_text_or_attr(event_payload, "PostMarking"),
                        "execution_mode": self._find_direct_text_or_attr(
                            event_payload, "ExecutionMode"
                        ),
                        "set_piece_result_type": self._local_name(set_piece_result.tag)
                        if set_piece_result is not None
                        else None,
                        "set_piece_result_attributes_json": self._element_json(set_piece_result),
                        "event_xml": ET.tostring(event_payload, encoding="unicode"),
                    }
                )
            else:
                values.update(
                    {
                        "delete_reason": self._find_direct_text_or_attr(event_payload, "Reason"),
                        "event_xml": ET.tostring(event_payload, encoding="unicode"),
                    }
                )

            records.append(
                self._build_record(
                    event,
                    service_id,
                    source_url,
                    table_options,
                    values,
                )
            )

        return records

    def _feed0302_request_values(self, root: ET.Element) -> dict[str, str | None]:
        return {
            "request_id": self._find_direct_text_or_attr(root, "RequestId"),
            "message_time": self._find_direct_text_or_attr(root, "MessageTime"),
            "transmission_complete": self._find_direct_text_or_attr(root, "TransmissionComplete"),
            "transmission_suspended": self._find_direct_text_or_attr(root, "TransmissionSuspended"),
            "data_status": self._find_direct_text_or_attr(root, "DataStatus"),
        }

    def _feed0302_event_base_values(
        self,
        event: ET.Element,
        table_options: dict[str, str],
        request_values: dict[str, str | None],
    ) -> dict[str, Any]:
        return {
            "match_id": self._find_direct_text_or_attr(event, "MatchId") or table_options.get("match_id"),
            "event_id": self._find_direct_text_or_attr(event, "EventId")
            or self._stable_identifier(
                table_options.get("match_id"),
                ET.tostring(event, encoding="unicode"),
            ),
            "event_time": self._find_direct_text_or_attr(event, "EventTime"),
            **request_values,
        }

    def _build_feed07_ranking_record(
        self,
        ranking: ET.Element,
        table_name: str,
        feed: ET.Element,
        metadata: ET.Element | None,
        service_id: str,
        source_url: str,
        table_options: dict[str, str],
    ) -> dict[str, Any]:
        list_entries = self._find_children(ranking, "ListEntry")
        metadata_source = metadata if metadata is not None else feed
        values = {
            "match_id": self._find_direct_text_or_attr(metadata_source, "MatchId")
            or table_options.get("match_id"),
            "competition_id": self._find_direct_text_or_attr(metadata_source, "CompetitionId")
            or table_options.get("competition_id"),
            "season_id": self._find_direct_text_or_attr(metadata_source, "SeasonId")
            or table_options.get("season_id"),
            "matchday_id": self._find_direct_text_or_attr(metadata_source, "MatchDayId")
            or table_options.get("matchday_id"),
            "match_title": self._find_direct_text_or_attr(metadata_source, "Match"),
            "time_scope": self._find_direct_text_or_attr(feed, "TimeScope"),
            "data_scope": self._find_direct_text_or_attr(feed, "DataScope"),
            "feed_type": self._find_direct_text_or_attr(feed, "FeedType"),
            "ranking_type": self._find_direct_text_or_attr(ranking, "Type"),
            "metadata_json": self._element_json(metadata),
            "list_entries_json": self._elements_json(list_entries),
        }
        if table_name in FEED07_MATCH_TEAM_TABLES | FEED07_TEAM_SCOPED_SEASON_TABLES:
            values["team_id"] = table_options.get("team_id")
        return self._build_record(
            ranking,
            service_id,
            source_url,
            table_options,
            values,
        )

    def _build_record(
        self,
        element: ET.Element,
        service_id: str,
        source_url: str,
        table_options: dict[str, str],
        values: dict[str, Any],
    ) -> dict[str, Any]:
        values.update(
            {
                "source_service_id": service_id,
                "source_url": source_url,
                "requested_parameters": dict(table_options),
                "source_record": self._flatten_source_record(self._element_to_object(element)),
                "raw_xml": ET.tostring(element, encoding="unicode"),
                "retrieved_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        return values

    def _find_child(self, element: ET.Element | None, child_name: str) -> ET.Element | None:
        if element is None:
            return None
        target = child_name.lower()
        for child in list(element):
            if self._local_name(child.tag).lower() == target:
                return child
        return None

    def _find_children(self, element: ET.Element | None, *child_names: str) -> list[ET.Element]:
        if element is None:
            return []
        targets = {name.lower() for name in child_names}
        return [child for child in list(element) if self._local_name(child.tag).lower() in targets]

    def _first_child(self, element: ET.Element | None) -> ET.Element | None:
        children = list(element) if element is not None else []
        return children[0] if children else None

    def _find_descendants(
        self, element: ET.Element | None, *candidate_names: str
    ) -> list[ET.Element]:
        if element is None:
            return []
        targets = {name.lower() for name in candidate_names}
        return [
            descendant
            for descendant in element.iter()
            if descendant is not element and self._local_name(descendant.tag).lower() in targets
        ]

    def _find_text(self, element: ET.Element | None, *candidate_names: str) -> str | None:
        if element is None:
            return None
        targets = {name.lower() for name in candidate_names}
        for descendant in element.iter():
            for attr_name, attr_value in descendant.attrib.items():
                if attr_name.lower() in targets:
                    text = self._text_or_none(attr_value)
                    if text is not None:
                        return text
            if self._local_name(descendant.tag).lower() not in targets:
                continue
            text = self._text_or_none(descendant.text)
            if text is not None:
                return text
        return None

    def _find_bool(self, element: ET.Element | None, *candidate_names: str) -> bool | None:
        value = self._find_text(element, *candidate_names)
        return self._parse_bool(value)

    def _find_direct_text_or_attr(
        self, element: ET.Element | None, *candidate_names: str
    ) -> str | None:
        if element is None:
            return None

        targets = {name.lower() for name in candidate_names}
        for attr_name, attr_value in element.attrib.items():
            if attr_name.lower() in targets:
                text = self._text_or_none(attr_value)
                if text is not None:
                    return text

        for child in list(element):
            if self._local_name(child.tag).lower() not in targets:
                continue
            text = self._text_or_none(child.text)
            if text is not None:
                return text

        return None

    def _element_json(self, element: ET.Element | None) -> str | None:
        if element is None:
            return None
        return json.dumps(self._element_to_object(element), sort_keys=True)

    def _elements_json(self, elements: list[ET.Element]) -> str | None:
        if not elements:
            return None
        return json.dumps([self._element_to_object(element) for element in elements], sort_keys=True)

    def _element_to_object(self, element: ET.Element) -> Any:
        children = list(element)
        attributes = {name: value for name, value in element.attrib.items()}
        text = self._text_or_none(element.text)

        if not children:
            if attributes:
                if text is not None:
                    attributes["text"] = text
                return attributes
            return text

        grouped: dict[str, Any] = {}
        for child in children:
            key = self._local_name(child.tag)
            value = self._element_to_object(child)
            if key in grouped:
                if not isinstance(grouped[key], list):
                    grouped[key] = [grouped[key]]
                grouped[key].append(value)
            else:
                grouped[key] = value

        if attributes:
            for name, value in attributes.items():
                grouped[f"@{name}"] = value
        if text is not None:
            grouped["text"] = text
        return grouped

    def _flatten_source_record(
        self, value: Any, prefix: str = ""
    ) -> dict[str, str]:
        flattened: dict[str, str] = {}
        if value is None:
            return flattened

        if isinstance(value, dict):
            for key, nested in value.items():
                child_prefix = f"{prefix}.{key}" if prefix else key
                if isinstance(nested, dict):
                    flattened.update(self._flatten_source_record(nested, child_prefix))
                elif isinstance(nested, list):
                    flattened[child_prefix] = json.dumps(nested, sort_keys=True)
                elif nested is not None:
                    flattened[child_prefix] = str(nested)
            return flattened

        if isinstance(value, list):
            flattened[prefix or "value"] = json.dumps(value, sort_keys=True)
            return flattened

        if prefix:
            flattened[prefix] = str(value)
        else:
            flattened["value"] = str(value)
        return flattened

    def _stable_identifier(self, *parts: str | None) -> str:
        payload = json.dumps(list(parts), sort_keys=True)
        return hashlib.sha1(payload.encode("utf-8")).hexdigest()

    def _parse_bool(self, value: str | None) -> bool | None:
        if value is None:
            return None
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes"}:
            return True
        if lowered in {"false", "0", "no"}:
            return False
        return None

    def _local_name(self, tag_name: str) -> str:
        if "}" in tag_name:
            return tag_name.split("}", 1)[1]
        return tag_name

    def _text_or_none(self, value: str | None) -> str | None:
        if value is None:
            return None
        stripped = value.strip()
        return stripped or None

