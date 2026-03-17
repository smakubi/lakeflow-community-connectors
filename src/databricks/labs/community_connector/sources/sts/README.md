# Lakeflow STS Community Connector

This connector ingests supported Sportec Solutions (STS) MLS Data Hub feeds into Databricks through Lakeflow Community Connectors. The current implementation targets the WebService-based STS surface that is reachable from the authenticated MLS tenant used during development.

The connector currently covers base data, match metadata, match statistics, post-match raw event projections, post-match tracking extracts, season statistics, Feed-06 analytics, and Feed-07 rankings.

## Prerequisites

- An STS / MLS Data Hub account that has access to the feeds you want to ingest.
- OAuth client-credentials values for the tenant:
  - `client_id`
  - `client_secret`
  - `token_url`
  - `audience`
- The STS distribution API base URL for your tenant.
- Network access from Databricks to the STS Auth0 token endpoint and the STS distribution API.
- Match, season, competition, matchday, club, or team identifiers required by the tables you plan to ingest.

## Setup

### Required Connection Parameters

Provide the following connection-level options when creating the Unity Catalog connection for the STS connector:

| Name | Type | Required | Description | Example |
|---|---|---|---|---|
| `client_id` | string | yes | OAuth client ID for the STS/Auth0 client-credentials application. | `abc123...` |
| `client_secret` | string | yes | OAuth client secret for the STS/Auth0 client-credentials application. | `secret-value` |
| `token_url` | string | yes | Auth0 token endpoint used to exchange client credentials for a bearer token. | `https://datahub-mls-customer.eu.auth0.com/oauth/token` |
| `audience` | string | yes | OAuth audience requested for the STS API token. | `https://distribution.mls-datahub.com` |
| `base_url` | string | yes | Base URL for STS distribution API endpoints. | `https://httpget.distribution.production.mls-datahub.com/DeliveryPlatform/REST/` |

The connector supports table-specific options such as `competition_id`, `season_id`, `club_id`, `matchday_id`, `match_id`, `team_id`, and `max_records_per_batch`. If your Unity Catalog connection enforces an external options allowlist, include these keys in `externalOptionsAllowList`.

### Obtaining the Required Parameters

The STS feed PDFs used for connector development describe feed payloads and parameter patterns, but they do not document authentication. The current connector implementation follows the Auth0 client-credentials flow used by the MLS Data Hub tenant during development.

To obtain the required values:

1. Request an STS / MLS Data Hub API client from your STS account team or tenant administrator.
2. Confirm the OAuth token endpoint for your tenant.
3. Confirm the OAuth audience for your tenant.
4. Confirm the distribution API base URL for your tenant.
5. Verify which feed families are enabled for your tenant, since availability can vary by contract and competition.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the Lakeflow Community Connector flow from the **Add Data** page.
2. Create or reuse an STS Lakeflow Community Connector connection.
3. Enter `client_id`, `client_secret`, `token_url`, `audience`, and `base_url`.
4. If your environment requires `externalOptionsAllowList`, include `competition_id,season_id,club_id,matchday_id,match_id,team_id,max_records_per_batch`.

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

All STS tables in the current implementation are read as `snapshot` tables. The connector does not expose an incremental cursor field for any table, and delete synchronization is not implemented as a separate Lakeflow CDC mode. Each read returns the current payload for the requested STS feed and parameter set.

Every table also includes common lineage fields:

- `source_service_id`
- `source_url`
- `requested_parameters`
- `source_record`
- `raw_xml`
- `retrieved_at`

### Supported Tables By Family

- Base data and schedule:
  - `competitions`
  - `matchdays`
  - `stadiums`
  - `clubs`
  - `players`
  - `team_officials`
  - `referees`
  - `suspensions`
  - `fixtures_schedule`
  - `seasons`
- Match metadata and match summaries:
  - `match_information`
  - `eventdata_match_statistics_match`
  - `eventdata_match_statistics_intervals`
  - `eventdata_match_statistics_periods`
  - `eventdata_match_basic`
  - `eventdata_match_basic_extended`
- Season tables and season statistics:
  - `season_tables`
  - `eventdata_season_statistics_competition`
  - `eventdata_season_statistics_club`
  - `positional_season_statistics_competition`
  - `positional_season_statistics_club`
- Post-match raw event feeds and typed projections:
  - `event_raw_messages`
  - `event_raw_delete_events`
  - `event_raw_var_notifications`
  - `event_raw_play_events`
  - `event_raw_foul_events`
  - `event_raw_shot_at_goal_events`
  - `event_raw_substitution_events`
  - `event_raw_caution_events`
  - `event_raw_offside_events`
  - `event_raw_corner_kick_events`
  - `event_raw_free_kick_events`
- Tracking and positional feeds:
  - `tracking_pitch_metadata`
  - `tracking_postmatch_framesets`
  - `distance_match_teams`
  - `distance_match_players`
  - `speed_interval_definitions`
  - `speed_interval_player_stats`
  - `positional_match_team_statistics`
  - `positional_match_player_statistics`
- Feed-06 analytics:
  - `video_assist_events`
  - `player_topspeed_alerts`
  - `player_topspeed_alert_rankings`
  - `attacking_zone_entries`
  - `xg_rankings_season`
  - `win_probability_by_minute`
  - `advanced_events_raw`
  - `advanced_event_plays`
  - `advanced_event_receptions`
  - `advanced_event_carries`
  - `advanced_event_team_possessions`
  - `advanced_event_other_ball_actions`
  - `advanced_event_tackling_games`
  - `advanced_event_fouls`
  - `advanced_event_shots_at_goal`
- Feed-07 rankings:
  - Match player rankings:
    - `rankings_match_player_goal`
    - `rankings_match_player_ballaction`
    - `rankings_match_player_tackling`
    - `rankings_match_player_play`
    - `rankings_match_player_foul`
  - Match player team-internal rankings:
    - `rankings_match_player_team_goal`
    - `rankings_match_player_team_ballaction`
    - `rankings_match_player_team_tackling`
    - `rankings_match_player_team_play`
    - `rankings_match_player_team_foul`
  - Matchday player competition rankings:
    - `rankings_matchday_player_goal`
    - `rankings_matchday_player_ballaction`
    - `rankings_matchday_player_tackling`
    - `rankings_matchday_player_play`
    - `rankings_matchday_player_foul`
    - `rankings_matchday_player_positionaldata`
  - Matchday player top-50 rankings:
    - `rankings_matchday_player_top50_goal`
    - `rankings_matchday_player_top50_ballaction`
    - `rankings_matchday_player_top50_positionaldata`
  - Matchday team rankings:
    - `rankings_matchday_team_goal`
    - `rankings_matchday_team_ballaction`
    - `rankings_matchday_team_tackling`
    - `rankings_matchday_team_play`
    - `rankings_matchday_team_foul`
    - `rankings_matchday_team_positionaldata`
  - Season player competition rankings:
    - `rankings_season_player_goal`
    - `rankings_season_player_ballaction`
    - `rankings_season_player_tackling`
    - `rankings_season_player_play`
    - `rankings_season_player_foul`
    - `rankings_season_player_positionaldata`
  - Season player top-50 rankings:
    - `rankings_season_player_top50_goal`
    - `rankings_season_player_top50_ballaction`
    - `rankings_season_player_top50_tackling`
    - `rankings_season_player_top50_play`
    - `rankings_season_player_top50_positionaldata`
  - Season player team-internal rankings:
    - `rankings_season_player_team_goal`
    - `rankings_season_player_team_ballaction`
    - `rankings_season_player_team_tackling`
    - `rankings_season_player_team_play`
    - `rankings_season_player_team_foul`
    - `rankings_season_player_team_positionaldata`
  - Season team rankings:
    - `rankings_season_team_goal`
    - `rankings_season_team_ballaction`
    - `rankings_season_team_tackling`
    - `rankings_season_team_play`
    - `rankings_season_team_foul`
    - `rankings_season_team_positionaldata`

### Primary Keys and Ingestion Notes

- Base/reference tables are keyed by STS IDs such as `competition_id`, `season_id`, `matchday_id`, `stadium_id`, `club_id`, or `object_id`.
- Most match-scoped analytical tables are keyed by `match_id` plus additional identifiers such as `team_id`, `player_id`, `scope`, `rank`, or `event_id`.
- Feed-07 tables use ranking-specific keys such as `ranking_type`, plus match, matchday, season, and optional `team_id` dimensions depending on the family.
- No table defines a connector-managed incremental cursor. If you schedule recurring syncs, treat these feeds as replaceable snapshots for a given parameter set.

## Table Configurations

### Source and Destination

These are set directly under each `table` object in the pipeline spec:

| Option | Required | Description |
|---|---|---|
| `source_table` | yes | One of the supported STS table names listed above. |
| `destination_catalog` | no | Target Unity Catalog catalog. |
| `destination_schema` | no | Target schema. |
| `destination_table` | no | Target table name. Defaults to `source_table`. |

### Common `table_configuration` Options

These options are supplied inside each table's `table_configuration` map:

| Option | Required | Description |
|---|---|---|
| `scd_type` | no | Standard Lakeflow option for snapshot tables. |
| `primary_keys` | no | Override the connector's default primary keys. |
| `sequence_by` | no | Optional Lakeflow sequencing column if you apply SCD logic downstream. |
| `max_records_per_batch` | no | Positive integer. Caps records returned by a read. Useful for very large match-scoped feeds such as `tracking_postmatch_framesets`. |

### STS-Specific Table Option Patterns

The connector validates table options strictly. Only the parameter combinations below are accepted.

| Table family | Required `table_configuration` |
|---|---|
| `competitions`, `stadiums`, `referees` | none |
| `matchdays` | `competition_id` |
| `seasons` | `competition_id` |
| `season_tables` | `competition_id` |
| `clubs` | `competition_id`, `season_id` |
| `players`, `team_officials` | `club_id`, `season_id` |
| `suspensions` | `season_id` |
| `fixtures_schedule` | exactly one of: `competition_id`; `competition_id` + `season_id`; `competition_id` + `matchday_id` |
| Match-scoped feeds including `match_information`, `eventdata_match_*`, `event_raw_*`, `distance_match_*`, `speed_interval_*`, `positional_match_*`, `tracking_*`, `video_assist_events`, `player_topspeed_*`, `attacking_zone_entries`, `win_probability_by_minute`, and `advanced_event_*` | `match_id` |
| `eventdata_season_statistics_competition`, `positional_season_statistics_competition`, `xg_rankings_season` | `competition_id`, `season_id` |
| `eventdata_season_statistics_club`, `positional_season_statistics_club` | `competition_id`, `season_id`, `club_id` |
| Feed-07 match player rankings | `match_id` |
| Feed-07 match player team-internal rankings | `match_id`, `team_id` |
| Feed-07 matchday and season competition/top-50 rankings | `competition_id`, `season_id`, `matchday_id` |
| Feed-07 season player team-internal rankings | `competition_id`, `season_id`, `matchday_id`, `team_id` |

### Example Pipeline Configuration

This example shows a mix of base, match-scoped, and ranking tables:

```json
{
  "pipeline_spec": {
    "connection_name": "sts_connection",
    "object": [
      {
        "table": {
          "source_table": "clubs",
          "table_configuration": {
            "competition_id": "MLS-COM-000001",
            "season_id": "MLS-SEA-0001JG"
          }
        }
      },
      {
        "table": {
          "source_table": "advanced_event_shots_at_goal",
          "table_configuration": {
            "match_id": "MLS-MAT-00067J"
          }
        }
      },
      {
        "table": {
          "source_table": "rankings_season_team_goal",
          "table_configuration": {
            "competition_id": "MLS-COM-000001",
            "season_id": "MLS-SEA-0001JG",
            "matchday_id": "MLS-DAY-00004Z"
          }
        }
      }
    ]
  }
}
```

## Schema and Data Type Notes

The connector preserves STS XML content with minimal coercion so that downstream consumers can decide how aggressively to normalize metrics.

| STS source shape | Connector representation | Notes |
|---|---|---|
| IDs, codes, timestamps, statuses, and most measures | string columns | Most STS XML attributes are stored as strings, even when they contain numeric-looking values. |
| Boolean-style flags | boolean columns | Used for selected fields such as `roofed_over`, `local_player`, and `canceled`. |
| Nested repeated structures | JSON string columns | Many complex sections are preserved in columns such as `*_json`, `entries_json`, `metadata_json`, `list_entries_json`, and `team_statistics_json`. |
| Raw source payload | `raw_xml` string | Preserves the original XML fragment used to build the record. |
| Request context | lineage columns | `source_service_id`, `source_url`, `requested_parameters`, `source_record`, and `retrieved_at` are added to every table. |

Important schema characteristics:

- Base/master data tables are relatively flat and easy to join on STS IDs.
- Match metadata and season statistics preserve richer nested sections as JSON strings.
- Feed-07 rankings keep the ranking payload in `list_entries_json` rather than flattening every list entry into separate warehouse rows.
- Advanced event and raw event projections expose typed columns for the implemented event families while still keeping `event_attributes_json` and `event_xml`.

## Tenant-Specific Limitations and Availability Notes

The following notes reflect the current authenticated MLS tenant used during connector development and test execution.

- Supported WebService feeds in the current implementation include:
  - Base data (`Feed-01.*`)
  - Match information (`Feed-02.01`)
  - Match statistics and summaries (`Feed-03.03`, `Feed-03.04`, `Feed-03.05`)
  - Post-match raw events (`Feed-03.02`)
  - Post-match tracking and derived tracking feeds (`Feed-04.02`, `Feed-04.04`, `Feed-04.05`, `Feed-04.07`)
  - Season tables and season statistics (`Feed-05.01`, `Feed-05.02`, `Feed-05.03`)
  - Feed-06 analytics currently implemented in code (`Feed-06.02`, `Feed-06.04`, `Feed-06.05`, `Feed-06.06`, `Feed-06.10`, `Feed-06.15`)
  - Feed-07 ranking families implemented in code
- `Feed-06.07-PassingProfile` is documented by STS but is not supported in the current tenant. Repeated WebService probes returned `404`.
- `Feed-03.01-EventData-Match-Raw` is documented as WebSocket-only and is outside the current snapshot WebService connector model.
- `Feed-04.01-PositionalData-Match-Raw` is documented as WebSocket-only and is outside the current snapshot WebService connector model.
- `Feed-06.01-Penalties-Goalmouth` is documented as FTP-only and is not implemented in this connector.
- Some STS docs mix MLS and DFL examples. Feed structure is similar across those docs, but actual tenant availability can differ by competition or contract.
- Some feed attributes described in STS documentation are delivered only after post-match observation windows, often up to about 24 hours after full time.

## How To Run

### Step 1: Add the Connector

Register or reference the `sts` source code in your Lakeflow Community Connector workflow.

### Step 2: Create the Connection

Create the STS Unity Catalog connection with the OAuth and API endpoint values described above.

### Step 3: Configure Tables

Choose the STS tables you want to ingest and provide the exact table parameters required for each table family.

### Step 4: Run and Schedule

Schedule the pipeline according to how frequently your STS tenant refreshes the selected feeds. Because the connector reads snapshots, periodic reruns should be treated as refreshes for the same parameter set rather than cursor-based incremental syncs.

## Best Practices

- Start with low-volume tables such as `competitions`, `seasons`, `fixtures_schedule`, `match_information`, or `xg_rankings_season` before adding large match-scoped feeds.
- Use `fixtures_schedule` to discover valid `match_id`, `competition_id`, `season_id`, and `matchday_id` values for downstream tables.
- Use `max_records_per_batch` when exploring high-volume feeds such as `tracking_postmatch_framesets`.
- Plan for late-arriving updates on feeds that are revised after post-match observation windows.
- Keep STS IDs exactly as delivered. They are the main join keys across base, match, analytics, and ranking feeds.

## Troubleshooting

- Authentication failures:
  - Verify `client_id`, `client_secret`, `token_url`, and `audience`.
  - Confirm the client is provisioned for the STS tenant you are querying.
- `404` responses:
  - Check the table parameters first.
  - Then confirm the feed is enabled for your tenant. Some documented feeds are not available in every account.
- Empty results for match-scoped feeds:
  - Verify the `match_id` belongs to the same tenant and competition scope.
  - Confirm whether the feed is live-only, post-match-only, or updated later after observation.
- Large payloads:
  - Apply `max_records_per_batch` while validating a configuration.

## References

- Connector implementation: `src/databricks/labs/community_connector/sources/sts/sts.py`
- Connector schemas and metadata: `src/databricks/labs/community_connector/sources/sts/sts_schemas.py`
