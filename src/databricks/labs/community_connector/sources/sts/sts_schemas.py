"""Schema and metadata definitions for the STS connector."""

import re

from pyspark.sql.types import (
    BooleanType,
    MapType,
    StringType,
    StructField,
    StructType,
)


SOURCE_FIELDS = [
    StructField("source_service_id", StringType(), False),
    StructField("source_url", StringType(), False),
    StructField("requested_parameters", MapType(StringType(), StringType()), True),
    StructField("source_record", MapType(StringType(), StringType()), True),
    StructField("raw_xml", StringType(), True),
    StructField("retrieved_at", StringType(), False),
]


def dedupe_fields(fields: list[StructField]) -> list[StructField]:
    """Preserve the first occurrence of each field name."""
    seen: set[str] = set()
    deduped: list[StructField] = []
    for field in fields:
        if field.name in seen:
            continue
        seen.add(field.name)
        deduped.append(field)
    return deduped


def with_source_fields(fields: list[StructField]) -> StructType:
    """Append common source lineage fields to a table schema."""
    return StructType(dedupe_fields(fields + SOURCE_FIELDS))


def camel_to_snake(name: str) -> str:
    """Convert STS CamelCase/XML attribute names to snake_case."""
    with_word_breaks = re.sub(r"(?<!^)(?=[A-Z])", "_", name)
    return with_word_breaks.lower()


def string_fields_from_attribute_names(names: list[str]) -> list[StructField]:
    """Build nullable string StructFields from STS XML attribute names."""
    return [StructField(camel_to_snake(name), StringType(), True) for name in names]


FEED0407_MATCH_STATISTIC_ATTRIBUTES = [
    "Scope",
    "MatchId",
    "GameTitle",
    "PlannedKickOff",
    "KickOff",
    "Competition",
    "CompetitionId",
    "MatchDay",
    "MatchDayId",
    "Season",
    "SeasonId",
    "MatchStatus",
    "DataStatus",
    "MinuteOfPlay",
    "CreationDate",
    "Result",
]

FEED0407_TEAM_STATISTIC_ATTRIBUTES = [
    "TeamId",
    "TeamName",
    "TeamRole",
    "ThreeLetterCode",
    "BallPossessionRatio",
    "BallPossessionTime",
    "DistanceCovered",
    "DistanceCoveredBallOut",
    "DistanceCoveredGross",
    "DistanceCoveredNet",
    "DistanceCoveredOpponentBall",
    "DistanceCoveredOpponentBallNet",
    "DistanceCoveredOwnBall",
    "DistanceCoveredOwnBallNet",
    "DistanceCoveredSpeedRange1",
    "DistanceCoveredSpeedRange1BallOut",
    "DistanceCoveredSpeedRange1Gross",
    "DistanceCoveredSpeedRange1Net",
    "DistanceCoveredSpeedRange1OpponentBall",
    "DistanceCoveredSpeedRange1OpponentBallNet",
    "DistanceCoveredSpeedRange1OwnBall",
    "DistanceCoveredSpeedRange1OwnBallNet",
    "DistanceCoveredSpeedRange2",
    "DistanceCoveredSpeedRange2BallOut",
    "DistanceCoveredSpeedRange2Gross",
    "DistanceCoveredSpeedRange2Net",
    "DistanceCoveredSpeedRange2OpponentBall",
    "DistanceCoveredSpeedRange2OpponentBallNet",
    "DistanceCoveredSpeedRange2OwnBall",
    "DistanceCoveredSpeedRange2OwnBallNet",
    "DistanceCoveredSpeedRange3",
    "DistanceCoveredSpeedRange3BallOut",
    "DistanceCoveredSpeedRange3Gross",
    "DistanceCoveredSpeedRange3Net",
    "DistanceCoveredSpeedRange3OpponentBall",
    "DistanceCoveredSpeedRange3OpponentBallNet",
    "DistanceCoveredSpeedRange3OwnBall",
    "DistanceCoveredSpeedRange3OwnBallNet",
    "DistanceCoveredSpeedRange4",
    "DistanceCoveredSpeedRange4BallOut",
    "DistanceCoveredSpeedRange4Gross",
    "DistanceCoveredSpeedRange4Net",
    "DistanceCoveredSpeedRange4OpponentBall",
    "DistanceCoveredSpeedRange4OpponentBallNet",
    "DistanceCoveredSpeedRange4OwnBall",
    "DistanceCoveredSpeedRange4OwnBallNet",
    "DistanceCoveredSpeedRange5",
    "DistanceCoveredSpeedRange5BallOut",
    "DistanceCoveredSpeedRange5Gross",
    "DistanceCoveredSpeedRange5Net",
    "DistanceCoveredSpeedRange5OpponentBall",
    "DistanceCoveredSpeedRange5OpponentBallNet",
    "DistanceCoveredSpeedRange5OwnBall",
    "DistanceCoveredSpeedRange5OwnBallNet",
    "DistanceCoveredSpeedRange6",
    "DistanceCoveredSpeedRange6BallOut",
    "DistanceCoveredSpeedRange6Gross",
    "DistanceCoveredSpeedRange6Net",
    "DistanceCoveredSpeedRange6OpponentBall",
    "DistanceCoveredSpeedRange6OpponentBallNet",
    "DistanceCoveredSpeedRange6OwnBall",
    "DistanceCoveredSpeedRange6OwnBallNet",
    "IntensiveRuns",
    "IntensiveRunsBallOut",
    "IntensiveRunsGross",
    "IntensiveRunsNet",
    "IntensiveRunsOpponentBall",
    "IntensiveRunsOpponentBallNet",
    "IntensiveRunsOppositeDirection",
    "IntensiveRunsOppositeDirectionBallOut",
    "IntensiveRunsOppositeDirectionGross",
    "IntensiveRunsOppositeDirectionNet",
    "IntensiveRunsOppositeDirectionOpponentBall",
    "IntensiveRunsOppositeDirectionOpponentBallNet",
    "IntensiveRunsOppositeDirectionOwnBall",
    "IntensiveRunsOppositeDirectionOwnBallNet",
    "IntensiveRunsOwnBall",
    "IntensiveRunsOwnBallNet",
    "IntensiveRunsPlayingDirection",
    "IntensiveRunsPlayingDirectionBallOut",
    "IntensiveRunsPlayingDirectionGross",
    "IntensiveRunsPlayingDirectionNet",
    "IntensiveRunsPlayingDirectionOpponentBall",
    "IntensiveRunsPlayingDirectionOpponentBallNet",
    "IntensiveRunsPlayingDirectionOwnBall",
    "IntensiveRunsPlayingDirectionOwnBallNet",
    "PlayingTimeGross",
    "PlayingTimeNet",
    "PlayingTimeSpeedRange1",
    "PlayingTimeSpeedRange1BallOut",
    "PlayingTimeSpeedRange1Gross",
    "PlayingTimeSpeedRange1Net",
    "PlayingTimeSpeedRange1OpponentBall",
    "PlayingTimeSpeedRange1OpponentBallNet",
    "PlayingTimeSpeedRange1OwnBall",
    "PlayingTimeSpeedRange1OwnBallNet",
    "PlayingTimeSpeedRange2",
    "PlayingTimeSpeedRange2BallOut",
    "PlayingTimeSpeedRange2Gross",
    "PlayingTimeSpeedRange2Net",
    "PlayingTimeSpeedRange2OpponentBall",
    "PlayingTimeSpeedRange2OpponentBallNet",
    "PlayingTimeSpeedRange2OwnBall",
    "PlayingTimeSpeedRange2OwnBallNet",
    "PlayingTimeSpeedRange3",
    "PlayingTimeSpeedRange3BallOut",
    "PlayingTimeSpeedRange3Gross",
    "PlayingTimeSpeedRange3Net",
    "PlayingTimeSpeedRange3OpponentBall",
    "PlayingTimeSpeedRange3OpponentBallNet",
    "PlayingTimeSpeedRange3OwnBall",
    "PlayingTimeSpeedRange3OwnBallNet",
    "PlayingTimeSpeedRange4",
    "PlayingTimeSpeedRange4BallOut",
    "PlayingTimeSpeedRange4Gross",
    "PlayingTimeSpeedRange4Net",
    "PlayingTimeSpeedRange4OpponentBall",
    "PlayingTimeSpeedRange4OpponentBallNet",
    "PlayingTimeSpeedRange4OwnBall",
    "PlayingTimeSpeedRange4OwnBallNet",
    "PlayingTimeSpeedRange5",
    "PlayingTimeSpeedRange5BallOut",
    "PlayingTimeSpeedRange5Gross",
    "PlayingTimeSpeedRange5Net",
    "PlayingTimeSpeedRange5OpponentBall",
    "PlayingTimeSpeedRange5OpponentBallNet",
    "PlayingTimeSpeedRange5OwnBall",
    "PlayingTimeSpeedRange5OwnBallNet",
    "PlayingTimeSpeedRange6",
    "PlayingTimeSpeedRange6BallOut",
    "PlayingTimeSpeedRange6Gross",
    "PlayingTimeSpeedRange6Net",
    "PlayingTimeSpeedRange6OpponentBall",
    "PlayingTimeSpeedRange6OpponentBallNet",
    "PlayingTimeSpeedRange6OwnBall",
    "PlayingTimeSpeedRange6OwnBallNet",
    "SpeedRuns",
    "SpeedRunsBallOut",
    "SpeedRunsGross",
    "SpeedRunsNet",
    "SpeedRunsOpponentBall",
    "SpeedRunsOpponentBallNet",
    "SpeedRunsOppositeDirection",
    "SpeedRunsOppositeDirectionBallOut",
    "SpeedRunsOppositeDirectionGross",
    "SpeedRunsOppositeDirectionNet",
    "SpeedRunsOppositeDirectionOpponentBall",
    "SpeedRunsOppositeDirectionOpponentBallNet",
    "SpeedRunsOppositeDirectionOwnBall",
    "SpeedRunsOppositeDirectionOwnBallNet",
    "SpeedRunsOwnBall",
    "SpeedRunsOwnBallNet",
    "SpeedRunsPlayingDirection",
    "SpeedRunsPlayingDirectionBallOut",
    "SpeedRunsPlayingDirectionGross",
    "SpeedRunsPlayingDirectionNet",
    "SpeedRunsPlayingDirectionOpponentBall",
    "SpeedRunsPlayingDirectionOpponentBallNet",
    "SpeedRunsPlayingDirectionOwnBall",
    "SpeedRunsPlayingDirectionOwnBallNet",
    "Sprints",
    "SprintsBallOut",
    "SprintsGross",
    "SprintsNet",
    "SprintsOpponentBall",
    "SprintsOpponentBallNet",
    "SprintsOppositeDirection",
    "SprintsOppositeDirectionBallOut",
    "SprintsOppositeDirectionGross",
    "SprintsOppositeDirectionNet",
    "SprintsOppositeDirectionOpponentBall",
    "SprintsOppositeDirectionOpponentBallNet",
    "SprintsOppositeDirectionOwnBall",
    "SprintsOppositeDirectionOwnBallNet",
    "SprintsOwnBall",
    "SprintsOwnBallNet",
    "SprintsPlayingDirection",
    "SprintsPlayingDirectionBallOut",
    "SprintsPlayingDirectionGross",
    "SprintsPlayingDirectionNet",
    "SprintsPlayingDirectionOpponentBall",
    "SprintsPlayingDirectionOpponentBallNet",
    "SprintsPlayingDirectionOwnBall",
    "SprintsPlayingDirectionOwnBallNet",
]

FEED0407_PLAYER_STATISTIC_ATTRIBUTES = [
    "PlayerId",
    "PlayerFirstName",
    "PlayerLastName",
    "Goalkeeper",
    "AverageSpeed",
    "MaximumSpeed",
    "DistanceCovered",
    "DistanceCoveredBallOut",
    "DistanceCoveredGross",
    "DistanceCoveredNet",
    "DistanceCoveredOpponentBall",
    "DistanceCoveredOpponentBallNet",
    "DistanceCoveredOwnBall",
    "DistanceCoveredOwnBallNet",
    "DistanceCoveredSpeedRange1",
    "DistanceCoveredSpeedRange1BallOut",
    "DistanceCoveredSpeedRange1Gross",
    "DistanceCoveredSpeedRange1Net",
    "DistanceCoveredSpeedRange1OpponentBall",
    "DistanceCoveredSpeedRange1OpponentBallNet",
    "DistanceCoveredSpeedRange1OwnBall",
    "DistanceCoveredSpeedRange1OwnBallNet",
    "DistanceCoveredSpeedRange2",
    "DistanceCoveredSpeedRange2BallOut",
    "DistanceCoveredSpeedRange2Gross",
    "DistanceCoveredSpeedRange2Net",
    "DistanceCoveredSpeedRange2OpponentBall",
    "DistanceCoveredSpeedRange2OpponentBallNet",
    "DistanceCoveredSpeedRange2OwnBall",
    "DistanceCoveredSpeedRange2OwnBallNet",
    "DistanceCoveredSpeedRange3",
    "DistanceCoveredSpeedRange3BallOut",
    "DistanceCoveredSpeedRange3Gross",
    "DistanceCoveredSpeedRange3Net",
    "DistanceCoveredSpeedRange3OpponentBall",
    "DistanceCoveredSpeedRange3OpponentBallNet",
    "DistanceCoveredSpeedRange3OwnBall",
    "DistanceCoveredSpeedRange3OwnBallNet",
    "DistanceCoveredSpeedRange4",
    "DistanceCoveredSpeedRange4BallOut",
    "DistanceCoveredSpeedRange4Gross",
    "DistanceCoveredSpeedRange4Net",
    "DistanceCoveredSpeedRange4OpponentBall",
    "DistanceCoveredSpeedRange4OpponentBallNet",
    "DistanceCoveredSpeedRange4OwnBall",
    "DistanceCoveredSpeedRange4OwnBallNet",
    "DistanceCoveredSpeedRange5",
    "DistanceCoveredSpeedRange5BallOut",
    "DistanceCoveredSpeedRange5Gross",
    "DistanceCoveredSpeedRange5Net",
    "DistanceCoveredSpeedRange5OpponentBall",
    "DistanceCoveredSpeedRange5OpponentBallNet",
    "DistanceCoveredSpeedRange5OwnBall",
    "DistanceCoveredSpeedRange5OwnBallNet",
    "DistanceCoveredSpeedRange6",
    "DistanceCoveredSpeedRange6BallOut",
    "DistanceCoveredSpeedRange6Gross",
    "DistanceCoveredSpeedRange6Net",
    "DistanceCoveredSpeedRange6OpponentBall",
    "DistanceCoveredSpeedRange6OpponentBallNet",
    "DistanceCoveredSpeedRange6OwnBall",
    "DistanceCoveredSpeedRange6OwnBallNet",
    "IntensiveRuns",
    "IntensiveRunsBallOut",
    "IntensiveRunsGross",
    "IntensiveRunsNet",
    "IntensiveRunsOpponentBall",
    "IntensiveRunsOpponentBallNet",
    "IntensiveRunsOppositeDirection",
    "IntensiveRunsOppositeDirectionBallOut",
    "IntensiveRunsOppositeDirectionGross",
    "IntensiveRunsOppositeDirectionNet",
    "IntensiveRunsOppositeDirectionOpponentBall",
    "IntensiveRunsOppositeDirectionOpponentBallNet",
    "IntensiveRunsOppositeDirectionOwnBall",
    "IntensiveRunsOppositeDirectionOwnBallNet",
    "IntensiveRunsOwnBall",
    "IntensiveRunsOwnBallNet",
    "IntensiveRunsPlayingDirection",
    "IntensiveRunsPlayingDirectionBallOut",
    "IntensiveRunsPlayingDirectionGross",
    "IntensiveRunsPlayingDirectionNet",
    "IntensiveRunsPlayingDirectionOpponentBall",
    "IntensiveRunsPlayingDirectionOpponentBallNet",
    "IntensiveRunsPlayingDirectionOwnBall",
    "IntensiveRunsPlayingDirectionOwnBallNet",
    "PlayingTimeGross",
    "PlayingTimeNet",
    "PlayingTimeSpeedRange1",
    "PlayingTimeSpeedRange1BallOut",
    "PlayingTimeSpeedRange1Gross",
    "PlayingTimeSpeedRange1Net",
    "PlayingTimeSpeedRange1OpponentBall",
    "PlayingTimeSpeedRange1OpponentBallNet",
    "PlayingTimeSpeedRange1OwnBall",
    "PlayingTimeSpeedRange1OwnBallNet",
    "PlayingTimeSpeedRange2",
    "PlayingTimeSpeedRange2BallOut",
    "PlayingTimeSpeedRange2Gross",
    "PlayingTimeSpeedRange2Net",
    "PlayingTimeSpeedRange2OpponentBall",
    "PlayingTimeSpeedRange2OpponentBallNet",
    "PlayingTimeSpeedRange2OwnBall",
    "PlayingTimeSpeedRange2OwnBallNet",
    "PlayingTimeSpeedRange3",
    "PlayingTimeSpeedRange3BallOut",
    "PlayingTimeSpeedRange3Gross",
    "PlayingTimeSpeedRange3Net",
    "PlayingTimeSpeedRange3OpponentBall",
    "PlayingTimeSpeedRange3OpponentBallNet",
    "PlayingTimeSpeedRange3OwnBall",
    "PlayingTimeSpeedRange3OwnBallNet",
    "PlayingTimeSpeedRange4",
    "PlayingTimeSpeedRange4BallOut",
    "PlayingTimeSpeedRange4Gross",
    "PlayingTimeSpeedRange4Net",
    "PlayingTimeSpeedRange4OpponentBall",
    "PlayingTimeSpeedRange4OpponentBallNet",
    "PlayingTimeSpeedRange4OwnBall",
    "PlayingTimeSpeedRange4OwnBallNet",
    "PlayingTimeSpeedRange5",
    "PlayingTimeSpeedRange5BallOut",
    "PlayingTimeSpeedRange5Gross",
    "PlayingTimeSpeedRange5Net",
    "PlayingTimeSpeedRange5OpponentBall",
    "PlayingTimeSpeedRange5OpponentBallNet",
    "PlayingTimeSpeedRange5OwnBall",
    "PlayingTimeSpeedRange5OwnBallNet",
    "PlayingTimeSpeedRange6",
    "PlayingTimeSpeedRange6BallOut",
    "PlayingTimeSpeedRange6Gross",
    "PlayingTimeSpeedRange6Net",
    "PlayingTimeSpeedRange6OpponentBall",
    "PlayingTimeSpeedRange6OpponentBallNet",
    "PlayingTimeSpeedRange6OwnBall",
    "PlayingTimeSpeedRange6OwnBallNet",
    "SpeedRuns",
    "SpeedRunsBallOut",
    "SpeedRunsGross",
    "SpeedRunsNet",
    "SpeedRunsOpponentBall",
    "SpeedRunsOpponentBallNet",
    "SpeedRunsOppositeDirection",
    "SpeedRunsOppositeDirectionBallOut",
    "SpeedRunsOppositeDirectionGross",
    "SpeedRunsOppositeDirectionNet",
    "SpeedRunsOppositeDirectionOpponentBall",
    "SpeedRunsOppositeDirectionOpponentBallNet",
    "SpeedRunsOppositeDirectionOwnBall",
    "SpeedRunsOppositeDirectionOwnBallNet",
    "SpeedRunsOwnBall",
    "SpeedRunsOwnBallNet",
    "SpeedRunsPlayingDirection",
    "SpeedRunsPlayingDirectionBallOut",
    "SpeedRunsPlayingDirectionGross",
    "SpeedRunsPlayingDirectionNet",
    "SpeedRunsPlayingDirectionOpponentBall",
    "SpeedRunsPlayingDirectionOpponentBallNet",
    "SpeedRunsPlayingDirectionOwnBall",
    "SpeedRunsPlayingDirectionOwnBallNet",
    "Sprints",
    "SprintsBallOut",
    "SprintsGross",
    "SprintsNet",
    "SprintsOpponentBall",
    "SprintsOpponentBallNet",
    "SprintsOppositeDirection",
    "SprintsOppositeDirectionBallOut",
    "SprintsOppositeDirectionGross",
    "SprintsOppositeDirectionNet",
    "SprintsOppositeDirectionOpponentBall",
    "SprintsOppositeDirectionOpponentBallNet",
    "SprintsOppositeDirectionOwnBall",
    "SprintsOppositeDirectionOwnBallNet",
    "SprintsOwnBall",
    "SprintsOwnBallNet",
    "SprintsPlayingDirection",
    "SprintsPlayingDirectionBallOut",
    "SprintsPlayingDirectionGross",
    "SprintsPlayingDirectionNet",
    "SprintsPlayingDirectionOpponentBall",
    "SprintsPlayingDirectionOpponentBallNet",
    "SprintsPlayingDirectionOwnBall",
    "SprintsPlayingDirectionOwnBallNet",
]

FEED0604_ALERT_ATTRIBUTES = [
    "MatchId",
    "GameTitle",
    "Competition",
    "CompetitionId",
    "MatchDay",
    "MatchDayId",
    "Season",
    "SeasonId",
    "MatchStatus",
    "DataStatus",
    "MinuteOfPlay",
    "CreationDate",
    "HomeTeamId",
    "HomeTeamName",
    "GuestTeamId",
    "GuestTeamName",
]

FEED0604_FASTEST_PLAYER_ATTRIBUTES = [
    "PlayerId",
    "PlayerFirstName",
    "PlayerLastName",
    "TeamName",
    "TeamId",
    "ThreeLetterCode",
    "MaximumSpeed",
    "MinuteOfPlay",
    "TimeStamp",
]

FEED0604_ALERT_FLAG_ATTRIBUTES = [
    "CompetitionRecord",
    "SeasonRecord",
    "SeasonTopThree",
    "TeamSeasonRecord",
    "PersonalBest",
]

FEED0604_RANKING_ENTRY_ATTRIBUTES = [
    "Rank",
    "Absolute",
    "PlayerId",
    "PlayerFirstName",
    "PlayerLastName",
    "SeasonId",
    "MatchId",
    "TeamName",
    "TeamId",
    "MinuteOfPlay",
    "TimeStamp",
]

FEED0605_ATTACKING_ZONES_ATTRIBUTES = [
    "MatchId",
    "GameTitle",
    "Competition",
    "CompetitionId",
    "MatchDay",
    "MatchDayId",
    "Season",
    "SeasonId",
    "MatchStatus",
    "DataStatus",
    "MinuteOfPlay",
    "CreationDate",
    "HomeTeamId",
    "HomeTeamName",
    "GuestTeamId",
    "GuestTeamName",
]

FEED0605_TEAM_ATTRIBUTES = [
    "TeamId",
    "TeamName",
    "TeamRole",
    "ThreeLetterCode",
]

FEED0605_ZONE_ATTRIBUTES = ["ZoneId", "Entries", "Ratio"]

FEED0606_XG_RANKINGS_ATTRIBUTES = [
    "Competition",
    "CompetitionId",
    "MatchDay",
    "MatchDayId",
    "Season",
    "SeasonId",
    "CreationDate",
]

FEED0606_RANKING_ENTRY_ATTRIBUTES = [
    "TeamName",
    "TeamId",
    "PlayerFirstName",
    "PlayerId",
    "PlayerLastName",
    "PlayerAlias",
    "Rank",
    "Absolute",
    "xGoals",
    "Goals",
]

FEED0610_WIN_PROBABILITY_ATTRIBUTES = [
    "Competition",
    "CompetitionId",
    "Season",
    "SeasonId",
    "MatchDay",
    "MatchId",
    "HomeTeamId",
    "HomeTeamName",
    "GuestTeamId",
    "GuestTeamName",
    "MatchStatus",
    "DataStatus",
    "CreationDate",
    "MinuteOfPlay",
]

FEED0610_PROBABILITY_ATTRIBUTES = [
    "MinuteOfPlay",
    "HomeTeamWinProbability",
    "GuestTeamWinProbability",
    "DrawProbability",
]

FEED0615_ADVANCED_EVENTS_ATTRIBUTES = [
    "Competition",
    "CompetitionId",
    "CreationDate",
    "DataStatus",
    "GameTitle",
    "KickoffTime",
    "MatchDay",
    "MatchDayId",
    "MatchId",
    "MatchStatus",
    "MinuteOfPlay",
    "PlannedKickoffTime",
    "Result",
    "Season",
    "SeasonId",
]

FEED0615_PLAY_ATTRIBUTES = [
    "EventId",
    "TeamId",
    "PlayerId",
    "Evaluation",
    "IsPass",
    "IsCross",
    "IsCorner",
    "IsFreeKick",
    "IsGoalKick",
    "IsThrowIn",
    "IsKickOff",
    "ThroughBall",
    "BackLineBreak",
    "IsBallLost",
    "IsBlockedPass",
    "DefensiveState",
    "InGameSection",
    "GameSection",
    "DirectionOfPlay",
    "ReceiverId",
    "ReceptionId",
    "SyncSuccessful",
    "SyncedEventTime",
    "GameTime",
    "X-Position",
    "Y-Position",
    "SyncedFrameId",
    "MaxHeight",
    "Distance",
    "X-Direction",
    "Y-Direction",
    "PlayAngle",
    "PressureOnPlayer",
    "PressureOnReceiver",
    "xP",
    "X-PlayerSpeed",
    "Y-PlayerSpeed",
    "DistanceClosestDefenderToPlayer",
    "X-PositionReceiver",
    "Y-PositionReceiver",
    "NumDefendersGoalSide",
    "NumDefendersPassingLane",
    "ByPassedDefenders",
    "PlayNumInPossession",
    "NumAttackingPlayersAhead",
    "NumDefendingPlayersInBox",
    "NumAttackingPlayersInBox",
]

FEED0615_RECEPTION_ATTRIBUTES = [
    "EventId",
    "PlayId",
    "TeamId",
    "PlayerId",
    "SyncSuccessful",
    "SyncedEventTime",
    "GameTime",
    "X-Position",
    "Y-Position",
    "SyncedFrameId",
    "X-ReceiverSpeed",
    "Y-ReceiverSpeed",
    "PressureOnReceiver",
    "DefensiveState",
    "InGameSection",
    "IsInterception",
]

FEED0615_CARRY_ATTRIBUTES = [
    "EventId",
    "TeamId",
    "PlayerId",
    "SyncSuccessful",
    "SyncedEventTime",
    "GameTime",
    "SyncedFrameId",
    "EndSyncedEventTime",
    "EndGameTime",
    "EndSyncedFrameId",
    "X-Position",
    "Y-Position",
    "X-EndPosition",
    "Y-EndPosition",
    "DefensiveStateStart",
    "DefensiveStateEnd",
    "Distance",
    "InGameSection",
]

FEED0615_TEAM_POSSESSION_ATTRIBUTES = [
    "EventId",
    "TeamId",
    "SyncSuccessful",
    "SyncedEventTime",
    "GameTime",
    "SyncedFrameId",
    "EndSyncedEventTime",
    "EndGameTime",
    "EndSyncedFrameId",
    "X-Position",
    "Y-Position",
    "X-EndPosition",
    "Y-EndPosition",
    "InGameSection",
    "IsContested",
    "SumXGCon",
    "SumXGInd",
    "VerticalGainCarries",
    "VerticalGainOverall",
    "VerticalGainPlays",
]

FEED0615_OTHER_BALL_ACTION_ATTRIBUTES = [
    "EventId",
    "TeamId",
    "PlayerId",
    "SyncSuccessful",
    "SyncedEventTime",
    "GameTime",
    "SyncedFrameId",
    "X-Position",
    "Y-Position",
    "DefensiveState",
    "InGameSection",
    "IsBallLost",
    "IsDefensiveClearance",
]

FEED0615_TACKLING_GAME_ATTRIBUTES = [
    "EventId",
    "SyncSuccessful",
    "SyncedEventTime",
    "GameTime",
    "SyncedFrameId",
    "X-Position",
    "Y-Position",
    "InGameSection",
    "WinnerTeamId",
    "WinnerPlayerId",
    "LoserTeamId",
    "LoserPlayerId",
    "IsFoul",
    "Type",
]

FEED0615_FOUL_ATTRIBUTES = [
    "EventId",
    "SyncSuccessful",
    "SyncedEventTime",
    "GameTime",
    "SyncedFrameId",
    "X-Position",
    "Y-Position",
    "InGameSection",
    "DefensiveState",
    "FoulType",
    "FoulerTeamId",
    "FoulerPlayerId",
    "FouledTeamId",
    "FouledPlayerId",
]

FEED0615_SHOT_AT_GOAL_ATTRIBUTES = [
    "EventId",
    "TeamId",
    "PlayerId",
    "SyncSuccessful",
    "SyncedEventTime",
    "GameTime",
    "SyncedFrameId",
    "X-Position",
    "Y-Position",
    "X-PlayerSpeed",
    "Y-PlayerSpeed",
    "X-PositionGoalkeeper",
    "Y-PositionGoalkeeper",
    "InGameSection",
    "DefensiveState",
    "BuildUp",
    "ShotResult",
    "DistanceToGoal",
    "DistanceGoalkeeperToGoal",
    "AngleToGoal",
    "PressureOnPlayer",
    "IsBlockedShot",
    "IsCorner",
    "IsFreeKick",
    "IsPenalty",
    "NumAttackingPlayersInBox",
    "NumDefendingPlayersInBox",
    "NumDefendersInShotLane",
    "xG",
]


COMPETITIONS_SCHEMA = with_source_fields(
    [
        StructField("competition_id", StringType(), False),
        StructField("dl_provider_id", StringType(), True),
        StructField("competition_name", StringType(), True),
        StructField("country", StringType(), True),
        StructField("host", StringType(), True),
        StructField("sport", StringType(), True),
        StructField("competition_type", StringType(), True),
        StructField("valid_to", StringType(), True),
    ]
)

MATCHDAYS_SCHEMA = with_source_fields(
    [
        StructField("competition_id", StringType(), True),
        StructField("matchday_id", StringType(), False),
        StructField("matchday", StringType(), True),
        StructField("season_id", StringType(), True),
        StructField("season", StringType(), True),
    ]
)

STADIUMS_SCHEMA = with_source_fields(
    [
        StructField("stadium_id", StringType(), False),
        StructField("dl_provider_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("street", StringType(), True),
        StructField("zip_code", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("turf", StringType(), True),
        StructField("roofed_over", BooleanType(), True),
        StructField("valid_to", StringType(), True),
    ]
)

CLUBS_SCHEMA = with_source_fields(
    [
        StructField("club_id", StringType(), False),
        StructField("dl_provider_id", StringType(), True),
        StructField("competition_id", StringType(), True),
        StructField("season_id", StringType(), True),
        StructField("stadium_id", StringType(), True),
        StructField("club_name", StringType(), True),
        StructField("short_name", StringType(), True),
        StructField("abbreviation", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("valid_to", StringType(), True),
    ]
)

PLAYERS_SCHEMA = with_source_fields(
    [
        StructField("object_id", StringType(), False),
        StructField("dl_provider_id", StringType(), True),
        StructField("club_id", StringType(), True),
        StructField("club_name", StringType(), True),
        StructField("season_id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("secondary_type", StringType(), True),
        StructField("tertiary_type", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("name", StringType(), True),
        StructField("birth_date", StringType(), True),
        StructField("nationality", StringType(), True),
        StructField("shirt_number", StringType(), True),
        StructField("position_de", StringType(), True),
        StructField("position_en", StringType(), True),
        StructField("position_es", StringType(), True),
        StructField("primary_pool", StringType(), True),
        StructField("local_player", BooleanType(), True),
        StructField("leave_date", StringType(), True),
        StructField("valid_to", StringType(), True),
        StructField("canceled", BooleanType(), True),
    ]
)

TEAM_OFFICIALS_SCHEMA = with_source_fields(
    [
        StructField("object_id", StringType(), False),
        StructField("dl_provider_id", StringType(), True),
        StructField("club_id", StringType(), True),
        StructField("club_name", StringType(), True),
        StructField("season_id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("secondary_type", StringType(), True),
        StructField("tertiary_type", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("name", StringType(), True),
        StructField("birth_date", StringType(), True),
        StructField("nationality", StringType(), True),
        StructField("valid_to", StringType(), True),
        StructField("canceled", BooleanType(), True),
    ]
)

REFEREES_SCHEMA = with_source_fields(
    [
        StructField("object_id", StringType(), False),
        StructField("dl_provider_id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("secondary_type", StringType(), True),
        StructField("tertiary_type", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("name", StringType(), True),
        StructField("birth_date", StringType(), True),
        StructField("nationality", StringType(), True),
        StructField("leave_date", StringType(), True),
        StructField("valid_to", StringType(), True),
        StructField("canceled", BooleanType(), True),
    ]
)

SUSPENSIONS_SCHEMA = with_source_fields(
    [
        StructField("id", StringType(), False),
        StructField("season_id", StringType(), True),
        StructField("object_id", StringType(), True),
        StructField("start_date", StringType(), True),
        StructField("end_date", StringType(), True),
        StructField("reason", StringType(), True),
        StructField("matches", StringType(), True),
        StructField("type", StringType(), True),
        StructField("validity", StringType(), True),
        StructField("authority", StringType(), True),
        StructField("annulation_date", StringType(), True),
    ]
)

FIXTURES_SCHEDULE_SCHEMA = with_source_fields(
    [
        StructField("match_id", StringType(), False),
        StructField("dl_provider_id", StringType(), True),
        StructField("competition_id", StringType(), True),
        StructField("matchday_id", StringType(), True),
        StructField("season_id", StringType(), True),
        StructField("planned_kickoff_time", StringType(), True),
        StructField("stadium_id", StringType(), True),
        StructField("home_team_id", StringType(), True),
        StructField("guest_team_id", StringType(), True),
        StructField("match_date_fixed", StringType(), True),
        StructField("date_quality", StringType(), True),
        StructField("official_information", StringType(), True),
        StructField("start_date", StringType(), True),
        StructField("end_date", StringType(), True),
        StructField("sub_league", StringType(), True),
        StructField("group", StringType(), True),
        StructField("valid_to", StringType(), True),
    ]
)

SEASONS_SCHEMA = with_source_fields(
    [
        StructField("competition_id", StringType(), True),
        StructField("season_id", StringType(), False),
        StructField("season", StringType(), True),
    ]
)

MATCH_INFORMATION_SCHEMA = with_source_fields(
    [
        StructField("match_id", StringType(), False),
        StructField("competition_id", StringType(), True),
        StructField("season_id", StringType(), True),
        StructField("matchday", StringType(), True),
        StructField("planned_kickoff_time", StringType(), True),
        StructField("kickoff_time", StringType(), True),
        StructField("home_team_id", StringType(), True),
        StructField("guest_team_id", StringType(), True),
        StructField("result", StringType(), True),
        StructField("general_json", StringType(), True),
        StructField("teams_json", StringType(), True),
        StructField("environment_json", StringType(), True),
        StructField("referees_json", StringType(), True),
        StructField("other_game_information_json", StringType(), True),
    ]
)

EVENTDATA_MATCH_STATISTICS_MATCH_SCHEMA = with_source_fields(
    [
        StructField("match_id", StringType(), False),
        StructField("competition_id", StringType(), True),
        StructField("season_id", StringType(), True),
        StructField("matchday_id", StringType(), True),
        StructField("home_team_id", StringType(), True),
        StructField("guest_team_id", StringType(), True),
        StructField("creation_date", StringType(), True),
        StructField("data_status", StringType(), True),
        StructField("match_status", StringType(), True),
        StructField("minute_of_play", StringType(), True),
        StructField("scope", StringType(), True),
        StructField("result", StringType(), True),
        StructField("team_statistics_json", StringType(), True),
    ]
)

EVENTDATA_MATCH_STATISTICS_INTERVALS_SCHEMA = with_source_fields(
    [
        StructField("match_id", StringType(), False),
        StructField("competition_id", StringType(), True),
        StructField("season_id", StringType(), True),
        StructField("matchday_id", StringType(), True),
        StructField("home_team_id", StringType(), True),
        StructField("guest_team_id", StringType(), True),
        StructField("creation_date", StringType(), True),
        StructField("data_status", StringType(), True),
        StructField("match_status", StringType(), True),
        StructField("minute_of_play", StringType(), True),
        StructField("scope", StringType(), True),
        StructField("result", StringType(), True),
        StructField("team_statistics_json", StringType(), True),
    ]
)

EVENTDATA_MATCH_STATISTICS_PERIODS_SCHEMA = with_source_fields(
    [
        StructField("match_id", StringType(), False),
        StructField("competition_id", StringType(), True),
        StructField("season_id", StringType(), True),
        StructField("matchday_id", StringType(), True),
        StructField("home_team_id", StringType(), True),
        StructField("guest_team_id", StringType(), True),
        StructField("creation_date", StringType(), True),
        StructField("data_status", StringType(), True),
        StructField("match_status", StringType(), True),
        StructField("minute_of_play", StringType(), True),
        StructField("scope", StringType(), True),
        StructField("result", StringType(), True),
        StructField("team_statistics_json", StringType(), True),
    ]
)

EVENTDATA_MATCH_BASIC_SCHEMA = with_source_fields(
    [
        StructField("match_id", StringType(), False),
        StructField("competition_id", StringType(), True),
        StructField("season_id", StringType(), True),
        StructField("matchday_id", StringType(), True),
        StructField("home_team_id", StringType(), True),
        StructField("guest_team_id", StringType(), True),
        StructField("planned_kickoff_time", StringType(), True),
        StructField("kickoff_time", StringType(), True),
        StructField("creation_date", StringType(), True),
        StructField("data_status", StringType(), True),
        StructField("match_status", StringType(), True),
        StructField("minute_of_play", StringType(), True),
        StructField("result", StringType(), True),
        StructField("game_sections_json", StringType(), True),
    ]
)

EVENTDATA_MATCH_BASIC_EXTENDED_SCHEMA = with_source_fields(
    [
        StructField("match_id", StringType(), False),
        StructField("competition_id", StringType(), True),
        StructField("season_id", StringType(), True),
        StructField("matchday_id", StringType(), True),
        StructField("home_team_id", StringType(), True),
        StructField("guest_team_id", StringType(), True),
        StructField("planned_kickoff_time", StringType(), True),
        StructField("kickoff_time", StringType(), True),
        StructField("creation_date", StringType(), True),
        StructField("data_status", StringType(), True),
        StructField("match_status", StringType(), True),
        StructField("minute_of_play", StringType(), True),
        StructField("result", StringType(), True),
        StructField("game_sections_json", StringType(), True),
    ]
)

SEASON_TABLES_SCHEMA = with_source_fields(
    [
        StructField("competition_id", StringType(), False),
        StructField("season_id", StringType(), True),
        StructField("matchday_id", StringType(), True),
        StructField("table_type", StringType(), False),
        StructField("group_name", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("data_status", StringType(), True),
        StructField("creation_date", StringType(), True),
        StructField("entries_json", StringType(), True),
    ]
)

SEASON_STATISTICS_COMPETITION_SCHEMA = with_source_fields(
    [
        StructField("competition_id", StringType(), False),
        StructField("season_id", StringType(), False),
        StructField("club_id", StringType(), True),
        StructField("team_id", StringType(), True),
        StructField("matchday_id", StringType(), True),
        StructField("matchday_status", StringType(), True),
        StructField("creation_date", StringType(), True),
        StructField("team_statistics_json", StringType(), True),
    ]
)

SEASON_STATISTICS_CLUB_SCHEMA = with_source_fields(
    [
        StructField("competition_id", StringType(), False),
        StructField("season_id", StringType(), False),
        StructField("club_id", StringType(), False),
        StructField("team_id", StringType(), True),
        StructField("matchday_id", StringType(), True),
        StructField("matchday_status", StringType(), True),
        StructField("creation_date", StringType(), True),
        StructField("team_statistics_json", StringType(), True),
    ]
)

VIDEO_ASSIST_EVENTS_SCHEMA = with_source_fields(
    [
        StructField("match_id", StringType(), False),
        StructField("event_id", StringType(), False),
        StructField("graphics_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("minute_of_play", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("creation_date", StringType(), True),
        StructField("score", StringType(), True),
        StructField("category_json", StringType(), True),
        StructField("referee_decision_json", StringType(), True),
        StructField("reason_json", StringType(), True),
        StructField("result_json", StringType(), True),
        StructField("descriptions_json", StringType(), True),
        StructField("texts_json", StringType(), True),
    ]
)

TRACKING_PITCH_METADATA_SCHEMA = with_source_fields(
    [
        StructField("match_id", StringType(), False),
        StructField("metadata_type", StringType(), False),
        StructField("event_time", StringType(), True),
        StructField("pitch_x", StringType(), True),
        StructField("pitch_y", StringType(), True),
    ]
)

TRACKING_POSTMATCH_FRAMESETS_SCHEMA = with_source_fields(
    [
        StructField("match_id", StringType(), False),
        StructField("game_section", StringType(), False),
        StructField("team_id", StringType(), False),
        StructField("person_id", StringType(), False),
        StructField("event_time", StringType(), True),
        StructField("frame_count", StringType(), False),
        StructField("first_frame_number", StringType(), True),
        StructField("last_frame_number", StringType(), True),
        StructField("frames_json", StringType(), True),
    ]
)

DISTANCE_MATCH_TEAMS_SCHEMA = with_source_fields(
    [
        StructField("match_id", StringType(), False),
        StructField("stat_type", StringType(), False),
        StructField("event_time", StringType(), True),
        StructField("team_id", StringType(), False),
        StructField("distance_covered", StringType(), True),
    ]
)

DISTANCE_MATCH_PLAYERS_SCHEMA = with_source_fields(
    [
        StructField("match_id", StringType(), False),
        StructField("stat_type", StringType(), False),
        StructField("event_time", StringType(), True),
        StructField("team_id", StringType(), False),
        StructField("person_id", StringType(), False),
        StructField("distance_covered", StringType(), True),
        StructField("average_speed", StringType(), True),
        StructField("maximal_speed", StringType(), True),
    ]
)

SPEED_INTERVAL_DEFINITIONS_SCHEMA = with_source_fields(
    [
        StructField("match_id", StringType(), False),
        StructField("stat_type", StringType(), False),
        StructField("interval_id", StringType(), False),
        StructField("from_value", StringType(), True),
        StructField("from_interpretation", StringType(), True),
        StructField("to_value", StringType(), True),
        StructField("to_interpretation", StringType(), True),
    ]
)

SPEED_INTERVAL_PLAYER_STATS_SCHEMA = with_source_fields(
    [
        StructField("match_id", StringType(), False),
        StructField("stat_type", StringType(), False),
        StructField("event_time", StringType(), True),
        StructField("person_id", StringType(), False),
        StructField("interval_id", StringType(), False),
        StructField("count", StringType(), True),
        StructField("distance_covered", StringType(), True),
        StructField("amount", StringType(), True),
    ]
)

POSITIONAL_MATCH_TEAM_STATISTICS_SCHEMA = with_source_fields(
    string_fields_from_attribute_names(FEED0407_MATCH_STATISTIC_ATTRIBUTES)
    + string_fields_from_attribute_names(FEED0407_TEAM_STATISTIC_ATTRIBUTES)
)

POSITIONAL_MATCH_PLAYER_STATISTICS_SCHEMA = with_source_fields(
    string_fields_from_attribute_names(FEED0407_MATCH_STATISTIC_ATTRIBUTES)
    + string_fields_from_attribute_names(["TeamId"])
    + string_fields_from_attribute_names(FEED0407_PLAYER_STATISTIC_ATTRIBUTES)
)

PLAYER_TOPSPEED_ALERTS_SCHEMA = with_source_fields(
    string_fields_from_attribute_names(FEED0604_ALERT_ATTRIBUTES)
    + string_fields_from_attribute_names(["ChangeScopeType"])
    + string_fields_from_attribute_names(FEED0604_FASTEST_PLAYER_ATTRIBUTES)
    + string_fields_from_attribute_names(FEED0604_ALERT_FLAG_ATTRIBUTES)
)

PLAYER_TOPSPEED_ALERT_RANKINGS_SCHEMA = with_source_fields(
    string_fields_from_attribute_names(FEED0604_ALERT_ATTRIBUTES)
    + string_fields_from_attribute_names(["RankingType"])
    + string_fields_from_attribute_names(FEED0604_RANKING_ENTRY_ATTRIBUTES)
)

ATTACKING_ZONE_ENTRIES_SCHEMA = with_source_fields(
    string_fields_from_attribute_names(FEED0605_ATTACKING_ZONES_ATTRIBUTES)
    + string_fields_from_attribute_names(FEED0605_TEAM_ATTRIBUTES)
    + string_fields_from_attribute_names(["TeamEntries"])
    + string_fields_from_attribute_names(FEED0605_ZONE_ATTRIBUTES)
)

XG_RANKINGS_SEASON_SCHEMA = with_source_fields(
    string_fields_from_attribute_names(FEED0606_XG_RANKINGS_ATTRIBUTES)
    + string_fields_from_attribute_names(FEED0606_RANKING_ENTRY_ATTRIBUTES)
    + [
        StructField("ranking_type", StringType(), True),
        StructField("scope", StringType(), True),
        StructField("time_scope", StringType(), True),
    ]
)

WIN_PROBABILITY_BY_MINUTE_SCHEMA = with_source_fields(
    string_fields_from_attribute_names(FEED0610_WIN_PROBABILITY_ATTRIBUTES)
    + string_fields_from_attribute_names(["Scope"])
    + string_fields_from_attribute_names(FEED0610_PROBABILITY_ATTRIBUTES)
)

ADVANCED_EVENTS_RAW_SCHEMA = with_source_fields(
    string_fields_from_attribute_names(FEED0615_ADVANCED_EVENTS_ATTRIBUTES)
    + [
        StructField("event_type", StringType(), False),
        StructField("event_id", StringType(), True),
        StructField("team_id", StringType(), True),
        StructField("player_id", StringType(), True),
        StructField("receiver_id", StringType(), True),
        StructField("play_id", StringType(), True),
        StructField("in_game_section", StringType(), True),
        StructField("game_section", StringType(), True),
        StructField("sync_successful", StringType(), True),
        StructField("synced_event_time", StringType(), True),
        StructField("game_time", StringType(), True),
        StructField("synced_frame_id", StringType(), True),
        StructField("event_attributes_json", StringType(), True),
        StructField("event_xml", StringType(), True),
    ]
)

ADVANCED_EVENT_PLAYS_SCHEMA = with_source_fields(
    string_fields_from_attribute_names(FEED0615_ADVANCED_EVENTS_ATTRIBUTES)
    + string_fields_from_attribute_names(FEED0615_PLAY_ATTRIBUTES)
    + [
        StructField("event_attributes_json", StringType(), True),
        StructField("event_xml", StringType(), True),
    ]
)

ADVANCED_EVENT_RECEPTIONS_SCHEMA = with_source_fields(
    string_fields_from_attribute_names(FEED0615_ADVANCED_EVENTS_ATTRIBUTES)
    + string_fields_from_attribute_names(FEED0615_RECEPTION_ATTRIBUTES)
    + [
        StructField("event_attributes_json", StringType(), True),
        StructField("event_xml", StringType(), True),
    ]
)

ADVANCED_EVENT_CARRIES_SCHEMA = with_source_fields(
    string_fields_from_attribute_names(FEED0615_ADVANCED_EVENTS_ATTRIBUTES)
    + string_fields_from_attribute_names(FEED0615_CARRY_ATTRIBUTES)
    + [
        StructField("event_attributes_json", StringType(), True),
        StructField("event_xml", StringType(), True),
    ]
)

ADVANCED_EVENT_TEAM_POSSESSIONS_SCHEMA = with_source_fields(
    string_fields_from_attribute_names(FEED0615_ADVANCED_EVENTS_ATTRIBUTES)
    + string_fields_from_attribute_names(FEED0615_TEAM_POSSESSION_ATTRIBUTES)
    + [
        StructField("event_attributes_json", StringType(), True),
        StructField("event_xml", StringType(), True),
    ]
)

ADVANCED_EVENT_OTHER_BALL_ACTIONS_SCHEMA = with_source_fields(
    string_fields_from_attribute_names(FEED0615_ADVANCED_EVENTS_ATTRIBUTES)
    + string_fields_from_attribute_names(FEED0615_OTHER_BALL_ACTION_ATTRIBUTES)
    + [
        StructField("event_attributes_json", StringType(), True),
        StructField("event_xml", StringType(), True),
    ]
)

ADVANCED_EVENT_TACKLING_GAMES_SCHEMA = with_source_fields(
    string_fields_from_attribute_names(FEED0615_ADVANCED_EVENTS_ATTRIBUTES)
    + string_fields_from_attribute_names(FEED0615_TACKLING_GAME_ATTRIBUTES)
    + [
        StructField("event_attributes_json", StringType(), True),
        StructField("event_xml", StringType(), True),
    ]
)

ADVANCED_EVENT_FOULS_SCHEMA = with_source_fields(
    string_fields_from_attribute_names(FEED0615_ADVANCED_EVENTS_ATTRIBUTES)
    + string_fields_from_attribute_names(FEED0615_FOUL_ATTRIBUTES)
    + [
        StructField("event_attributes_json", StringType(), True),
        StructField("event_xml", StringType(), True),
    ]
)

ADVANCED_EVENT_SHOTS_AT_GOAL_SCHEMA = with_source_fields(
    string_fields_from_attribute_names(FEED0615_ADVANCED_EVENTS_ATTRIBUTES)
    + string_fields_from_attribute_names(FEED0615_SHOT_AT_GOAL_ATTRIBUTES)
    + [
        StructField("event_attributes_json", StringType(), True),
        StructField("event_xml", StringType(), True),
    ]
)

EVENT_RAW_MESSAGES_SCHEMA = with_source_fields(
    [
        StructField("match_id", StringType(), False),
        StructField("event_id", StringType(), False),
        StructField("event_time", StringType(), False),
        StructField("request_id", StringType(), True),
        StructField("message_time", StringType(), True),
        StructField("transmission_complete", StringType(), True),
        StructField("transmission_suspended", StringType(), True),
        StructField("data_status", StringType(), True),
        StructField("x_position", StringType(), True),
        StructField("y_position", StringType(), True),
        StructField("x_source_position", StringType(), True),
        StructField("y_source_position", StringType(), True),
        StructField("start_frame", StringType(), True),
        StructField("end_frame", StringType(), True),
        StructField("event_type", StringType(), False),
        StructField("event_attributes_json", StringType(), True),
        StructField("event_xml", StringType(), True),
    ]
)

EVENT_RAW_DELETE_EVENTS_SCHEMA = with_source_fields(
    [
        StructField("match_id", StringType(), False),
        StructField("event_id", StringType(), False),
        StructField("event_time", StringType(), False),
        StructField("request_id", StringType(), True),
        StructField("message_time", StringType(), True),
        StructField("transmission_complete", StringType(), True),
        StructField("transmission_suspended", StringType(), True),
        StructField("data_status", StringType(), True),
        StructField("delete_reason", StringType(), True),
        StructField("event_xml", StringType(), True),
    ]
)

EVENT_RAW_VAR_NOTIFICATIONS_SCHEMA = with_source_fields(
    [
        StructField("match_id", StringType(), False),
        StructField("event_id", StringType(), False),
        StructField("event_time", StringType(), False),
        StructField("request_id", StringType(), True),
        StructField("message_time", StringType(), True),
        StructField("transmission_complete", StringType(), True),
        StructField("transmission_suspended", StringType(), True),
        StructField("data_status", StringType(), True),
        StructField("test_mode", StringType(), True),
        StructField("incident", StringType(), True),
        StructField("review", StringType(), True),
        StructField("result", StringType(), True),
        StructField("decision", StringType(), True),
        StructField("reason", StringType(), True),
        StructField("status", StringType(), True),
        StructField("event_xml", StringType(), True),
    ]
)

EVENT_RAW_PLAY_EVENTS_SCHEMA = with_source_fields(
    [
        StructField("match_id", StringType(), False),
        StructField("event_id", StringType(), False),
        StructField("event_time", StringType(), False),
        StructField("request_id", StringType(), True),
        StructField("message_time", StringType(), True),
        StructField("transmission_complete", StringType(), True),
        StructField("transmission_suspended", StringType(), True),
        StructField("data_status", StringType(), True),
        StructField("team_id", StringType(), True),
        StructField("player_id", StringType(), True),
        StructField("recipient_player_id", StringType(), True),
        StructField("from_open_play", StringType(), True),
        StructField("ball_possession_phase", StringType(), True),
        StructField("evaluation", StringType(), True),
        StructField("height", StringType(), True),
        StructField("distance", StringType(), True),
        StructField("penalty_box", StringType(), True),
        StructField("goalkeeper_action", StringType(), True),
        StructField("play_origin", StringType(), True),
        StructField("play_angle", StringType(), True),
        StructField("semi_field", StringType(), True),
        StructField("flat_cross", StringType(), True),
        StructField("rotation", StringType(), True),
        StructField("play_subtype", StringType(), True),
        StructField("play_subtype_attributes_json", StringType(), True),
        StructField("event_xml", StringType(), True),
    ]
)

EVENT_RAW_FOUL_EVENTS_SCHEMA = with_source_fields(
    [
        StructField("match_id", StringType(), False),
        StructField("event_id", StringType(), False),
        StructField("event_time", StringType(), False),
        StructField("request_id", StringType(), True),
        StructField("message_time", StringType(), True),
        StructField("transmission_complete", StringType(), True),
        StructField("transmission_suspended", StringType(), True),
        StructField("data_status", StringType(), True),
        StructField("team_fouler_id", StringType(), True),
        StructField("team_fouled_id", StringType(), True),
        StructField("fouler_player_id", StringType(), True),
        StructField("fouled_player_id", StringType(), True),
        StructField("foul_type", StringType(), True),
        StructField("committing_player_action", StringType(), True),
        StructField("event_xml", StringType(), True),
    ]
)

EVENT_RAW_SHOT_AT_GOAL_EVENTS_SCHEMA = with_source_fields(
    [
        StructField("match_id", StringType(), False),
        StructField("event_id", StringType(), False),
        StructField("event_time", StringType(), False),
        StructField("request_id", StringType(), True),
        StructField("message_time", StringType(), True),
        StructField("transmission_complete", StringType(), True),
        StructField("transmission_suspended", StringType(), True),
        StructField("data_status", StringType(), True),
        StructField("x_position", StringType(), True),
        StructField("y_position", StringType(), True),
        StructField("x_source_position", StringType(), True),
        StructField("y_source_position", StringType(), True),
        StructField("x_position_from_tracking", StringType(), True),
        StructField("y_position_from_tracking", StringType(), True),
        StructField("calculated_timestamp", StringType(), True),
        StructField("calculated_frame", StringType(), True),
        StructField("start_frame", StringType(), True),
        StructField("end_frame", StringType(), True),
        StructField("team_id", StringType(), True),
        StructField("player_id", StringType(), True),
        StructField("type_of_shot", StringType(), True),
        StructField("inside_box", StringType(), True),
        StructField("chance_evaluation", StringType(), True),
        StructField("shot_origin", StringType(), True),
        StructField("ball_possession_phase", StringType(), True),
        StructField("assist_action", StringType(), True),
        StructField("after_free_kick", StringType(), True),
        StructField("assist_shot_at_goal", StringType(), True),
        StructField("assist_type_shot_at_goal", StringType(), True),
        StructField("taker_ball_control", StringType(), True),
        StructField("taker_setup", StringType(), True),
        StructField("counter_attack", StringType(), True),
        StructField("build_up", StringType(), True),
        StructField("setup_origin", StringType(), True),
        StructField("shot_condition", StringType(), True),
        StructField("xg", StringType(), True),
        StructField("distance_to_goal", StringType(), True),
        StructField("angle_to_goal", StringType(), True),
        StructField("pressure", StringType(), True),
        StructField("player_speed", StringType(), True),
        StructField("amount_of_defenders", StringType(), True),
        StructField("goal_distance_goalkeeper", StringType(), True),
        StructField("significance_evaluation", StringType(), True),
        StructField("shot_outcome_type", StringType(), True),
        StructField("shot_outcome_attributes_json", StringType(), True),
        StructField("event_xml", StringType(), True),
    ]
)

EVENT_RAW_SUBSTITUTION_EVENTS_SCHEMA = with_source_fields(
    [
        StructField("match_id", StringType(), False),
        StructField("event_id", StringType(), False),
        StructField("event_time", StringType(), False),
        StructField("request_id", StringType(), True),
        StructField("message_time", StringType(), True),
        StructField("transmission_complete", StringType(), True),
        StructField("transmission_suspended", StringType(), True),
        StructField("data_status", StringType(), True),
        StructField("team_id", StringType(), True),
        StructField("player_in_id", StringType(), True),
        StructField("player_out_id", StringType(), True),
        StructField("playing_position", StringType(), True),
        StructField("infringement", StringType(), True),
        StructField("concussion", StringType(), True),
        StructField("event_xml", StringType(), True),
    ]
)

EVENT_RAW_CAUTION_EVENTS_SCHEMA = with_source_fields(
    [
        StructField("match_id", StringType(), False),
        StructField("event_id", StringType(), False),
        StructField("event_time", StringType(), False),
        StructField("request_id", StringType(), True),
        StructField("message_time", StringType(), True),
        StructField("transmission_complete", StringType(), True),
        StructField("transmission_suspended", StringType(), True),
        StructField("data_status", StringType(), True),
        StructField("team_id", StringType(), True),
        StructField("player_id", StringType(), True),
        StructField("card_color", StringType(), True),
        StructField("card_rating", StringType(), True),
        StructField("reason", StringType(), True),
        StructField("other_reason", StringType(), True),
        StructField("event_xml", StringType(), True),
    ]
)

EVENT_RAW_OFFSIDE_EVENTS_SCHEMA = with_source_fields(
    [
        StructField("match_id", StringType(), False),
        StructField("event_id", StringType(), False),
        StructField("event_time", StringType(), False),
        StructField("request_id", StringType(), True),
        StructField("message_time", StringType(), True),
        StructField("transmission_complete", StringType(), True),
        StructField("transmission_suspended", StringType(), True),
        StructField("data_status", StringType(), True),
        StructField("team_id", StringType(), True),
        StructField("player_id", StringType(), True),
        StructField("event_xml", StringType(), True),
    ]
)

EVENT_RAW_SET_PIECE_EVENTS_SCHEMA = with_source_fields(
    [
        StructField("match_id", StringType(), False),
        StructField("event_id", StringType(), False),
        StructField("event_time", StringType(), False),
        StructField("request_id", StringType(), True),
        StructField("message_time", StringType(), True),
        StructField("transmission_complete", StringType(), True),
        StructField("transmission_suspended", StringType(), True),
        StructField("data_status", StringType(), True),
        StructField("x_position", StringType(), True),
        StructField("y_position", StringType(), True),
        StructField("x_source_position", StringType(), True),
        StructField("y_source_position", StringType(), True),
        StructField("team_id", StringType(), True),
        StructField("decision_timestamp", StringType(), True),
        StructField("side", StringType(), True),
        StructField("placing", StringType(), True),
        StructField("target_area", StringType(), True),
        StructField("rotation", StringType(), True),
        StructField("post_marking", StringType(), True),
        StructField("execution_mode", StringType(), True),
        StructField("set_piece_result_type", StringType(), True),
        StructField("set_piece_result_attributes_json", StringType(), True),
        StructField("event_xml", StringType(), True),
    ]
)

RANKINGS_MATCH_PLAYER_GOAL_SCHEMA = with_source_fields(
    [
        StructField("match_id", StringType(), False),
        StructField("team_id", StringType(), True),
        StructField("competition_id", StringType(), True),
        StructField("season_id", StringType(), True),
        StructField("matchday_id", StringType(), True),
        StructField("match_title", StringType(), True),
        StructField("time_scope", StringType(), False),
        StructField("data_scope", StringType(), False),
        StructField("feed_type", StringType(), False),
        StructField("ranking_type", StringType(), False),
        StructField("metadata_json", StringType(), True),
        StructField("list_entries_json", StringType(), True),
    ]
)

RANKINGS_MATCHDAY_TEAM_GOAL_SCHEMA = with_source_fields(
    [
        StructField("match_id", StringType(), True),
        StructField("competition_id", StringType(), False),
        StructField("season_id", StringType(), False),
        StructField("matchday_id", StringType(), False),
        StructField("match_title", StringType(), True),
        StructField("time_scope", StringType(), False),
        StructField("data_scope", StringType(), False),
        StructField("feed_type", StringType(), False),
        StructField("ranking_type", StringType(), False),
        StructField("metadata_json", StringType(), True),
        StructField("list_entries_json", StringType(), True),
    ]
)

RANKINGS_SCOPED_PLAYER_GOAL_SCHEMA = with_source_fields(
    [
        StructField("match_id", StringType(), True),
        StructField("team_id", StringType(), True),
        StructField("competition_id", StringType(), False),
        StructField("season_id", StringType(), False),
        StructField("matchday_id", StringType(), False),
        StructField("match_title", StringType(), True),
        StructField("time_scope", StringType(), False),
        StructField("data_scope", StringType(), False),
        StructField("feed_type", StringType(), False),
        StructField("ranking_type", StringType(), False),
        StructField("metadata_json", StringType(), True),
        StructField("list_entries_json", StringType(), True),
    ]
)


TABLE_SCHEMAS: dict[str, StructType] = {
    "competitions": COMPETITIONS_SCHEMA,
    "matchdays": MATCHDAYS_SCHEMA,
    "stadiums": STADIUMS_SCHEMA,
    "clubs": CLUBS_SCHEMA,
    "players": PLAYERS_SCHEMA,
    "team_officials": TEAM_OFFICIALS_SCHEMA,
    "referees": REFEREES_SCHEMA,
    "suspensions": SUSPENSIONS_SCHEMA,
    "fixtures_schedule": FIXTURES_SCHEDULE_SCHEMA,
    "seasons": SEASONS_SCHEMA,
    "match_information": MATCH_INFORMATION_SCHEMA,
    "eventdata_match_statistics_match": EVENTDATA_MATCH_STATISTICS_MATCH_SCHEMA,
    "eventdata_match_statistics_intervals": EVENTDATA_MATCH_STATISTICS_INTERVALS_SCHEMA,
    "eventdata_match_statistics_periods": EVENTDATA_MATCH_STATISTICS_PERIODS_SCHEMA,
    "eventdata_match_basic": EVENTDATA_MATCH_BASIC_SCHEMA,
    "eventdata_match_basic_extended": EVENTDATA_MATCH_BASIC_EXTENDED_SCHEMA,
    "season_tables": SEASON_TABLES_SCHEMA,
    "eventdata_season_statistics_competition": SEASON_STATISTICS_COMPETITION_SCHEMA,
    "eventdata_season_statistics_club": SEASON_STATISTICS_CLUB_SCHEMA,
    "positional_season_statistics_competition": SEASON_STATISTICS_COMPETITION_SCHEMA,
    "positional_season_statistics_club": SEASON_STATISTICS_CLUB_SCHEMA,
    "distance_match_teams": DISTANCE_MATCH_TEAMS_SCHEMA,
    "distance_match_players": DISTANCE_MATCH_PLAYERS_SCHEMA,
    "speed_interval_definitions": SPEED_INTERVAL_DEFINITIONS_SCHEMA,
    "speed_interval_player_stats": SPEED_INTERVAL_PLAYER_STATS_SCHEMA,
    "positional_match_team_statistics": POSITIONAL_MATCH_TEAM_STATISTICS_SCHEMA,
    "positional_match_player_statistics": POSITIONAL_MATCH_PLAYER_STATISTICS_SCHEMA,
    "player_topspeed_alerts": PLAYER_TOPSPEED_ALERTS_SCHEMA,
    "player_topspeed_alert_rankings": PLAYER_TOPSPEED_ALERT_RANKINGS_SCHEMA,
    "attacking_zone_entries": ATTACKING_ZONE_ENTRIES_SCHEMA,
    "xg_rankings_season": XG_RANKINGS_SEASON_SCHEMA,
    "win_probability_by_minute": WIN_PROBABILITY_BY_MINUTE_SCHEMA,
    "advanced_events_raw": ADVANCED_EVENTS_RAW_SCHEMA,
    "advanced_event_plays": ADVANCED_EVENT_PLAYS_SCHEMA,
    "advanced_event_receptions": ADVANCED_EVENT_RECEPTIONS_SCHEMA,
    "advanced_event_carries": ADVANCED_EVENT_CARRIES_SCHEMA,
    "advanced_event_team_possessions": ADVANCED_EVENT_TEAM_POSSESSIONS_SCHEMA,
    "advanced_event_other_ball_actions": ADVANCED_EVENT_OTHER_BALL_ACTIONS_SCHEMA,
    "advanced_event_tackling_games": ADVANCED_EVENT_TACKLING_GAMES_SCHEMA,
    "advanced_event_fouls": ADVANCED_EVENT_FOULS_SCHEMA,
    "advanced_event_shots_at_goal": ADVANCED_EVENT_SHOTS_AT_GOAL_SCHEMA,
    "event_raw_messages": EVENT_RAW_MESSAGES_SCHEMA,
    "event_raw_delete_events": EVENT_RAW_DELETE_EVENTS_SCHEMA,
    "event_raw_var_notifications": EVENT_RAW_VAR_NOTIFICATIONS_SCHEMA,
    "event_raw_play_events": EVENT_RAW_PLAY_EVENTS_SCHEMA,
    "event_raw_foul_events": EVENT_RAW_FOUL_EVENTS_SCHEMA,
    "event_raw_shot_at_goal_events": EVENT_RAW_SHOT_AT_GOAL_EVENTS_SCHEMA,
    "event_raw_substitution_events": EVENT_RAW_SUBSTITUTION_EVENTS_SCHEMA,
    "event_raw_caution_events": EVENT_RAW_CAUTION_EVENTS_SCHEMA,
    "event_raw_offside_events": EVENT_RAW_OFFSIDE_EVENTS_SCHEMA,
    "event_raw_corner_kick_events": EVENT_RAW_SET_PIECE_EVENTS_SCHEMA,
    "event_raw_free_kick_events": EVENT_RAW_SET_PIECE_EVENTS_SCHEMA,
    "video_assist_events": VIDEO_ASSIST_EVENTS_SCHEMA,
    "tracking_pitch_metadata": TRACKING_PITCH_METADATA_SCHEMA,
    "tracking_postmatch_framesets": TRACKING_POSTMATCH_FRAMESETS_SCHEMA,
    "rankings_match_player_goal": RANKINGS_MATCH_PLAYER_GOAL_SCHEMA,
    "rankings_matchday_team_goal": RANKINGS_MATCHDAY_TEAM_GOAL_SCHEMA,
}

TABLE_SCHEMAS.update(
    {
        "rankings_match_player_ballaction": RANKINGS_MATCH_PLAYER_GOAL_SCHEMA,
        "rankings_match_player_tackling": RANKINGS_MATCH_PLAYER_GOAL_SCHEMA,
        "rankings_match_player_play": RANKINGS_MATCH_PLAYER_GOAL_SCHEMA,
        "rankings_match_player_foul": RANKINGS_MATCH_PLAYER_GOAL_SCHEMA,
        "rankings_match_player_team_goal": RANKINGS_MATCH_PLAYER_GOAL_SCHEMA,
        "rankings_match_player_team_ballaction": RANKINGS_MATCH_PLAYER_GOAL_SCHEMA,
        "rankings_match_player_team_tackling": RANKINGS_MATCH_PLAYER_GOAL_SCHEMA,
        "rankings_match_player_team_play": RANKINGS_MATCH_PLAYER_GOAL_SCHEMA,
        "rankings_match_player_team_foul": RANKINGS_MATCH_PLAYER_GOAL_SCHEMA,
        "rankings_matchday_player_goal": RANKINGS_SCOPED_PLAYER_GOAL_SCHEMA,
        "rankings_matchday_player_ballaction": RANKINGS_SCOPED_PLAYER_GOAL_SCHEMA,
        "rankings_matchday_player_tackling": RANKINGS_SCOPED_PLAYER_GOAL_SCHEMA,
        "rankings_matchday_player_play": RANKINGS_SCOPED_PLAYER_GOAL_SCHEMA,
        "rankings_matchday_player_foul": RANKINGS_SCOPED_PLAYER_GOAL_SCHEMA,
        "rankings_matchday_player_positionaldata": RANKINGS_SCOPED_PLAYER_GOAL_SCHEMA,
        "rankings_matchday_player_top50_goal": RANKINGS_SCOPED_PLAYER_GOAL_SCHEMA,
        "rankings_matchday_player_top50_ballaction": RANKINGS_SCOPED_PLAYER_GOAL_SCHEMA,
        "rankings_matchday_player_top50_positionaldata": RANKINGS_SCOPED_PLAYER_GOAL_SCHEMA,
        "rankings_season_player_goal": RANKINGS_SCOPED_PLAYER_GOAL_SCHEMA,
        "rankings_season_player_ballaction": RANKINGS_SCOPED_PLAYER_GOAL_SCHEMA,
        "rankings_season_player_tackling": RANKINGS_SCOPED_PLAYER_GOAL_SCHEMA,
        "rankings_season_player_play": RANKINGS_SCOPED_PLAYER_GOAL_SCHEMA,
        "rankings_season_player_foul": RANKINGS_SCOPED_PLAYER_GOAL_SCHEMA,
        "rankings_season_player_positionaldata": RANKINGS_SCOPED_PLAYER_GOAL_SCHEMA,
        "rankings_season_player_top50_goal": RANKINGS_SCOPED_PLAYER_GOAL_SCHEMA,
        "rankings_season_player_top50_ballaction": RANKINGS_SCOPED_PLAYER_GOAL_SCHEMA,
        "rankings_season_player_top50_tackling": RANKINGS_SCOPED_PLAYER_GOAL_SCHEMA,
        "rankings_season_player_top50_play": RANKINGS_SCOPED_PLAYER_GOAL_SCHEMA,
        "rankings_season_player_top50_positionaldata": RANKINGS_SCOPED_PLAYER_GOAL_SCHEMA,
        "rankings_season_player_team_goal": RANKINGS_SCOPED_PLAYER_GOAL_SCHEMA,
        "rankings_season_player_team_ballaction": RANKINGS_SCOPED_PLAYER_GOAL_SCHEMA,
        "rankings_season_player_team_tackling": RANKINGS_SCOPED_PLAYER_GOAL_SCHEMA,
        "rankings_season_player_team_play": RANKINGS_SCOPED_PLAYER_GOAL_SCHEMA,
        "rankings_season_player_team_foul": RANKINGS_SCOPED_PLAYER_GOAL_SCHEMA,
        "rankings_season_player_team_positionaldata": RANKINGS_SCOPED_PLAYER_GOAL_SCHEMA,
        "rankings_matchday_team_ballaction": RANKINGS_MATCHDAY_TEAM_GOAL_SCHEMA,
        "rankings_matchday_team_tackling": RANKINGS_MATCHDAY_TEAM_GOAL_SCHEMA,
        "rankings_matchday_team_play": RANKINGS_MATCHDAY_TEAM_GOAL_SCHEMA,
        "rankings_matchday_team_foul": RANKINGS_MATCHDAY_TEAM_GOAL_SCHEMA,
        "rankings_matchday_team_positionaldata": RANKINGS_MATCHDAY_TEAM_GOAL_SCHEMA,
        "rankings_season_team_goal": RANKINGS_MATCHDAY_TEAM_GOAL_SCHEMA,
        "rankings_season_team_ballaction": RANKINGS_MATCHDAY_TEAM_GOAL_SCHEMA,
        "rankings_season_team_tackling": RANKINGS_MATCHDAY_TEAM_GOAL_SCHEMA,
        "rankings_season_team_play": RANKINGS_MATCHDAY_TEAM_GOAL_SCHEMA,
        "rankings_season_team_foul": RANKINGS_MATCHDAY_TEAM_GOAL_SCHEMA,
        "rankings_season_team_positionaldata": RANKINGS_MATCHDAY_TEAM_GOAL_SCHEMA,
    }
)

TABLE_METADATA: dict[str, dict[str, object]] = {
    "competitions": {
        "primary_keys": ["competition_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "matchdays": {
        "primary_keys": ["matchday_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "stadiums": {
        "primary_keys": ["stadium_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "clubs": {
        "primary_keys": ["club_id", "season_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "players": {
        "primary_keys": ["object_id", "club_id", "season_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "team_officials": {
        "primary_keys": ["object_id", "club_id", "season_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "referees": {
        "primary_keys": ["object_id", "leave_date"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "suspensions": {
        "primary_keys": ["id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "fixtures_schedule": {
        "primary_keys": ["match_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "seasons": {
        "primary_keys": ["season_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "match_information": {
        "primary_keys": ["match_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "eventdata_match_statistics_match": {
        "primary_keys": ["match_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "eventdata_match_statistics_intervals": {
        "primary_keys": ["match_id", "scope"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "eventdata_match_statistics_periods": {
        "primary_keys": ["match_id", "scope"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "eventdata_match_basic": {
        "primary_keys": ["match_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "eventdata_match_basic_extended": {
        "primary_keys": ["match_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "season_tables": {
        "primary_keys": ["competition_id", "season_id", "matchday_id", "table_type", "group_name"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "eventdata_season_statistics_competition": {
        "primary_keys": ["competition_id", "season_id", "matchday_id", "creation_date"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "eventdata_season_statistics_club": {
        "primary_keys": ["competition_id", "season_id", "club_id", "matchday_id", "creation_date"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "positional_season_statistics_competition": {
        "primary_keys": ["competition_id", "season_id", "matchday_id", "creation_date"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "positional_season_statistics_club": {
        "primary_keys": ["competition_id", "season_id", "club_id", "matchday_id", "creation_date"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "distance_match_teams": {
        "primary_keys": ["match_id", "stat_type", "team_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "distance_match_players": {
        "primary_keys": ["match_id", "stat_type", "team_id", "person_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "speed_interval_definitions": {
        "primary_keys": ["match_id", "stat_type", "interval_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "speed_interval_player_stats": {
        "primary_keys": ["match_id", "stat_type", "person_id", "interval_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "positional_match_team_statistics": {
        "primary_keys": ["match_id", "team_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "positional_match_player_statistics": {
        "primary_keys": ["match_id", "team_id", "player_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "player_topspeed_alerts": {
        "primary_keys": ["match_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "player_topspeed_alert_rankings": {
        "primary_keys": ["match_id", "ranking_type", "rank"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "attacking_zone_entries": {
        "primary_keys": ["match_id", "team_id", "zone_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "xg_rankings_season": {
        "primary_keys": ["competition_id", "season_id", "scope", "time_scope", "rank"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "win_probability_by_minute": {
        "primary_keys": ["match_id", "scope", "minute_of_play"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "advanced_events_raw": {
        "primary_keys": ["match_id", "event_id", "event_type"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "advanced_event_plays": {
        "primary_keys": ["match_id", "event_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "advanced_event_receptions": {
        "primary_keys": ["match_id", "event_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "advanced_event_carries": {
        "primary_keys": ["match_id", "event_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "advanced_event_team_possessions": {
        "primary_keys": ["match_id", "event_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "advanced_event_other_ball_actions": {
        "primary_keys": ["match_id", "event_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "advanced_event_tackling_games": {
        "primary_keys": ["match_id", "event_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "advanced_event_fouls": {
        "primary_keys": ["match_id", "event_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "advanced_event_shots_at_goal": {
        "primary_keys": ["match_id", "event_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "event_raw_messages": {
        "primary_keys": ["match_id", "event_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "event_raw_delete_events": {
        "primary_keys": ["match_id", "event_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "event_raw_var_notifications": {
        "primary_keys": ["match_id", "event_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "event_raw_play_events": {
        "primary_keys": ["match_id", "event_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "event_raw_foul_events": {
        "primary_keys": ["match_id", "event_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "event_raw_shot_at_goal_events": {
        "primary_keys": ["match_id", "event_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "event_raw_substitution_events": {
        "primary_keys": ["match_id", "event_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "event_raw_caution_events": {
        "primary_keys": ["match_id", "event_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "event_raw_offside_events": {
        "primary_keys": ["match_id", "event_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "event_raw_corner_kick_events": {
        "primary_keys": ["match_id", "event_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "event_raw_free_kick_events": {
        "primary_keys": ["match_id", "event_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "video_assist_events": {
        "primary_keys": ["match_id", "event_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "tracking_pitch_metadata": {
        "primary_keys": ["match_id", "metadata_type"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "tracking_postmatch_framesets": {
        "primary_keys": ["match_id", "game_section", "team_id", "person_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "rankings_match_player_goal": {
        "primary_keys": ["match_id", "ranking_type"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "rankings_matchday_team_goal": {
        "primary_keys": ["competition_id", "season_id", "matchday_id", "ranking_type"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
}

TABLE_METADATA.update(
    {
        "rankings_match_player_ballaction": {
            "primary_keys": ["match_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_match_player_tackling": {
            "primary_keys": ["match_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_match_player_play": {
            "primary_keys": ["match_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_match_player_foul": {
            "primary_keys": ["match_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_match_player_team_goal": {
            "primary_keys": ["match_id", "team_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_match_player_team_ballaction": {
            "primary_keys": ["match_id", "team_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_match_player_team_tackling": {
            "primary_keys": ["match_id", "team_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_match_player_team_play": {
            "primary_keys": ["match_id", "team_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_match_player_team_foul": {
            "primary_keys": ["match_id", "team_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_matchday_player_goal": {
            "primary_keys": ["competition_id", "season_id", "matchday_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_matchday_player_ballaction": {
            "primary_keys": ["competition_id", "season_id", "matchday_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_matchday_player_tackling": {
            "primary_keys": ["competition_id", "season_id", "matchday_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_matchday_player_play": {
            "primary_keys": ["competition_id", "season_id", "matchday_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_matchday_player_foul": {
            "primary_keys": ["competition_id", "season_id", "matchday_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_matchday_player_positionaldata": {
            "primary_keys": ["competition_id", "season_id", "matchday_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_matchday_player_top50_goal": {
            "primary_keys": ["competition_id", "season_id", "matchday_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_matchday_player_top50_ballaction": {
            "primary_keys": ["competition_id", "season_id", "matchday_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_matchday_player_top50_positionaldata": {
            "primary_keys": ["competition_id", "season_id", "matchday_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_season_player_goal": {
            "primary_keys": ["competition_id", "season_id", "matchday_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_season_player_ballaction": {
            "primary_keys": ["competition_id", "season_id", "matchday_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_season_player_tackling": {
            "primary_keys": ["competition_id", "season_id", "matchday_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_season_player_play": {
            "primary_keys": ["competition_id", "season_id", "matchday_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_season_player_foul": {
            "primary_keys": ["competition_id", "season_id", "matchday_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_season_player_positionaldata": {
            "primary_keys": ["competition_id", "season_id", "matchday_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_season_player_top50_goal": {
            "primary_keys": ["competition_id", "season_id", "matchday_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_season_player_top50_ballaction": {
            "primary_keys": ["competition_id", "season_id", "matchday_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_season_player_top50_tackling": {
            "primary_keys": ["competition_id", "season_id", "matchday_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_season_player_top50_play": {
            "primary_keys": ["competition_id", "season_id", "matchday_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_season_player_top50_positionaldata": {
            "primary_keys": ["competition_id", "season_id", "matchday_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_season_player_team_goal": {
            "primary_keys": ["competition_id", "season_id", "matchday_id", "team_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_season_player_team_ballaction": {
            "primary_keys": ["competition_id", "season_id", "matchday_id", "team_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_season_player_team_tackling": {
            "primary_keys": ["competition_id", "season_id", "matchday_id", "team_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_season_player_team_play": {
            "primary_keys": ["competition_id", "season_id", "matchday_id", "team_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_season_player_team_foul": {
            "primary_keys": ["competition_id", "season_id", "matchday_id", "team_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_season_player_team_positionaldata": {
            "primary_keys": ["competition_id", "season_id", "matchday_id", "team_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_matchday_team_ballaction": {
            "primary_keys": ["competition_id", "season_id", "matchday_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_matchday_team_tackling": {
            "primary_keys": ["competition_id", "season_id", "matchday_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_matchday_team_play": {
            "primary_keys": ["competition_id", "season_id", "matchday_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_matchday_team_foul": {
            "primary_keys": ["competition_id", "season_id", "matchday_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_matchday_team_positionaldata": {
            "primary_keys": ["competition_id", "season_id", "matchday_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_season_team_goal": {
            "primary_keys": ["competition_id", "season_id", "matchday_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_season_team_ballaction": {
            "primary_keys": ["competition_id", "season_id", "matchday_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_season_team_tackling": {
            "primary_keys": ["competition_id", "season_id", "matchday_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_season_team_play": {
            "primary_keys": ["competition_id", "season_id", "matchday_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_season_team_foul": {
            "primary_keys": ["competition_id", "season_id", "matchday_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
        "rankings_season_team_positionaldata": {
            "primary_keys": ["competition_id", "season_id", "matchday_id", "ranking_type"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        },
    }
)

SUPPORTED_TABLES = list(TABLE_SCHEMAS.keys())

