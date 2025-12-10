CREATE OR REPLACE FUNCTION ntz_to_tz(p_ntz TIMESTAMP_NTZ, p_tz STRING)
    RETURNS TIMESTAMP_TZ
AS
$$
/*
  Function: ntz_to_tz
  Author: Dan Gillis
  Version: 1.0.0
  Description:
    Converts a TIMESTAMP_NTZ to a TIMESTAMP_TZ in the specified timezone.
    Parameters:
    - p_ntz: The input TIMESTAMP_NTZ value.
    - p_tz: The target timezone as a string (e.g., 'UTC', 'America/New_York', etc.).
    Returns:
    - TIMESTAMP_TZ: The converted timestamp in the specified timezone.
  Example Usage:
    SELECT ntz_to_tz(TO_TIMESTAMP_NTZ('2023-10-01 12:00:00'), 'UTC');
  Notes:
    - Ensure that the timezone string is valid and recognized by Snowflake.
    - This function extracts individual components from the TIMESTAMP_NTZ and reconstructs
      a TIMESTAMP_TZ using the specified timezone.
*/
    TIMESTAMP_TZ_FROM_PARTS(EXTRACT(YEAR FROM p_ntz),
                            EXTRACT(MONTH FROM p_ntz),
                            EXTRACT(DAY FROM p_ntz),
                            EXTRACT(HOUR FROM p_ntz),
                            EXTRACT(MINUTE FROM p_ntz),
                            EXTRACT(SECOND FROM p_ntz),
                            EXTRACT(NANOSECOND FROM p_ntz),
                            p_tz)
$$;

WITH event_mapping_cte
         AS (SELECT record_type                                                               AS record_type,
                    record_attributes:"ai.observability.input_id"::VARCHAR                    AS event_ai_observability_input_id,
                    record_attributes:"ai.observability.record_id"::VARCHAR                   AS event_ai_observability_record_id,
                    timestamp                                                                 AS event_record_ts_ntz,
                    ntz_to_tz(timestamp, 'UTC')                                               AS event_record_ts_tz,
                    start_timestamp                                                           AS event_start_ts_ntz,
                    ntz_to_tz(start_timestamp, 'UTC')                                         AS event_start_ts_tz,
                    resource_attributes:"snow.session.id"::NUMBER                             AS event_snowflake_session_id,
                    resource_attributes:"snow.session.role.primary.id"::VARCHAR               AS event_snowflake_primary_role_id,
                    resource_attributes:"snow.session.role.primary.name"::VARCHAR             AS event_snowflake_primary_role_name,
                    resource_attributes:"snow.user.id"::NUMBER                                AS event_snowflake_user_id,
                    resource_attributes:"snow.user.name"::VARCHAR                             AS event_snowflake_user_name,
                    record:name::VARCHAR                                                      AS event_name,
                    record_attributes:"snow.ai.observability.agent.parent_message_id"::NUMBER AS event_agent_parent_message_id,
                    record_attributes:"snow.ai.observability.agent.thread_id"::NUMBER         AS event_agent_thread_id,
                    record_attributes:"snow.ai.observability.database.id"::NUMBER             AS event_database_id,
                    record_attributes:"snow.ai.observability.database.name"::VARCHAR          AS event_database_name,
                    record_attributes:"snow.ai.observability.object.id"::NUMBER               AS event_object_id,
                    record_attributes:"snow.ai.observability.object.name"::VARCHAR            AS event_object_name,
                    record_attributes:"snow.ai.observability.object.type"::VARCHAR            AS event_object_type,
                    record_attributes:"snow.ai.observability.role.id"::NUMBER                 AS event_role_id,
                    record_attributes:"snow.ai.observability.role.name"::VARCHAR              AS event_role_name,
                    record_attributes:"snow.ai.observability.schema.id"::NUMBER               AS event_schema_id,
                    record_attributes:"snow.ai.observability.schema.name"::VARCHAR            AS event_schema_name,
                    record_attributes:"snow.ai.observability.session.id"::NUMBER              AS event_session_id,
                    record_attributes:"snow.ai.observability.user.id"::NUMBER                 AS event_user_id,
                    record_attributes:"snow.ai.observability.user.name"::VARCHAR              AS event_user_name,
                    trace                                                                     AS event_og_trace,
                    resource_attributes                                                       AS event_og_resource_attributes,
                    record                                                                    AS event_og_record,
                    record_attributes                                                         AS event_og_record_attributes,
                    value                                                                     AS event_og_value

             FROM TABLE (
                 snowflake.local.get_ai_observability_events(
                         'SNOWFLAKE_INTELLIGENCE', -- database
                         'AGENTS', -- schema
                         'CLAIMS_AUDIT_AGENT', -- your agent name
                         'CORTEX AGENT') -- object type
                 )
             WHERE record_type = 'EVENT'),

     span_mapping_cte
         AS (SELECT record_type                                                   AS record_type,
                    record_attributes:"ai.observability.input_id"::VARCHAR        AS span_ai_observability_input_id,
                    record_attributes:"ai.observability.record_id"::VARCHAR       AS span_ai_observability_record_id,
                    record_attributes:request_id::VARCHAR                         AS span_request_id,
                    timestamp                                                     AS span_record_ts_ntz,
                    ntz_to_tz(timestamp, 'UTC')                                   AS span_record_ts_tz,
                    start_timestamp                                               AS span_start_ts_ntz,
                    ntz_to_tz(start_timestamp, 'UTC')                             AS span_start_ts_tz,
                    record:status.code::VARCHAR                                   AS span_status,
                    trace:trace_id::VARCHAR                                       AS trace_id,
                    trace:span_id::VARCHAR                                        AS span_id,
                    resource_attributes:"snow.session.id"::NUMBER                 AS span_snowflake_session_id,
                    resource_attributes:"snow.session.role.primary.id"::VARCHAR   AS span_snowflake_primary_role_id,
                    resource_attributes:"snow.session.role.primary.name"::VARCHAR AS span_snowflake_primary_role_name,
                    resource_attributes:"snow.user.id"::NUMBER                    AS span_snowflake_user_id,
                    resource_attributes:"snow.user.name"::VARCHAR                 AS span_snowflake_user_name,
                    trace                                                         AS span_og_trace,
                    resource_attributes                                           AS span_og_resource_attributes,
                    record                                                        AS span_og_record,
                    record_attributes                                             AS span_og_record_attributes,
                    value                                                         AS span_og_value

             FROM TABLE (
                 snowflake.local.get_ai_observability_events(
                         'SNOWFLAKE_INTELLIGENCE', -- database
                         'AGENTS', -- schema
                         'CLAIMS_AUDIT_AGENT', -- your agent name
                         'CORTEX AGENT') -- object type
                 )
             WHERE record_type = 'SPAN'
               AND record:name::VARCHAR = 'Agent')

SELECT emc.event_agent_thread_id                                                                                                   AS thread_id,
       span_request_id                                                                                                             AS span_request_id,
       MIN(span_start_ts_tz)
           OVER (PARTITION BY emc.event_ai_observability_input_id, emc.event_ai_observability_record_id ORDER BY span_start_ts_tz) AS first_span_record_ts,
       IFF(smc.span_status = 'STATUS_CODE_OK', 'Success', smc.span_status)                                                         AS span_status,
       smc.trace_id                                                                                                                AS trace_id,
       smc.span_id                                                                                                                 AS span_id,
       smc.span_record_ts_tz                                                                                                       AS span_record_ts,
       smc.span_start_ts_tz                                                                                                        AS span_start_ts_tz,
       smc.span_snowflake_primary_role_name                                                                                        AS role_name,
       smc.span_snowflake_user_name                                                                                                AS user_name,
       -- everything below is for debugging / validation purposes only. Above is what I think is really needed and maps to what is in the UI
       '------------------>'                                                                                                       AS appendix,
       emc.event_ai_observability_input_id                                                                                         AS ai_observability_record_id,
       emc.event_ai_observability_record_id                                                                                        AS ai_observability_input_id,
       emc.event_record_ts_tz                                                                                                      AS event_record_ts_tz,
       smc.span_record_ts_tz                                                                                                       AS span_record_ts_tz,
       smc.span_snowflake_session_id                                                                                               AS span_snowflake_session_id,
       smc.span_snowflake_primary_role_id                                                                                          AS span_snowflake_primary_role_id,
       smc.span_snowflake_primary_role_name                                                                                        AS span_snowflake_primary_role_name,
       smc.span_snowflake_user_id                                                                                                  AS span_snowflake_user_id,
       smc.span_snowflake_user_name                                                                                                AS span_snowflake_user_name,
       emc.event_snowflake_session_id                                                                                              AS event_snowflake_session_id,
       emc.event_snowflake_primary_role_id                                                                                         AS event_snowflake_primary_role_id,
       emc.event_snowflake_primary_role_name                                                                                       AS event_snowflake_primary_role_name,
       emc.event_snowflake_user_id                                                                                                 AS event_snowflake_user_id,
       emc.event_snowflake_user_name                                                                                               AS event_snowflake_user_name,
       -- everything below is for additional debugging / validation purposes only. These are the original columns from the function.
       '------------------>'                                                                                                       AS original_columns,
       IFF(emc.record_type IS NOT NULL, emc.event_record_ts_ntz,
           smc.span_record_ts_ntz)                                                                                                 AS timestamp,
       IFF(emc.record_type IS NOT NULL, emc.event_start_ts_ntz,
           smc.span_start_ts_ntz)                                                                                                  AS start_timestamp,
       IFF(emc.record_type IS NOT NULL, emc.event_og_trace,
           smc.span_og_trace)                                                                                                      AS trace,
       IFF(emc.record_type IS NOT NULL, emc.event_og_resource_attributes,
           smc.span_og_resource_attributes)                                                                                        AS resource_attributes,
       IFF(emc.record_type IS NOT NULL, emc.record_type, smc.record_type)                                                          AS record_type,
       IFF(emc.record_type IS NOT NULL, emc.event_og_record,
           smc.span_og_record)                                                                                                     AS record,
       IFF(emc.record_type IS NOT NULL, emc.event_og_record_attributes,
           smc.span_og_record_attributes)                                                                                          AS record_attributes,
       IFF(emc.record_type IS NOT NULL, emc.event_og_value,
           smc.span_og_value)                                                                                                      AS value

FROM event_mapping_cte emc

         LEFT JOIN span_mapping_cte smc
                   ON emc.event_ai_observability_input_id = smc.span_ai_observability_input_id AND
                      emc.event_ai_observability_record_id = smc.span_ai_observability_record_id

-- WHERE emc.event_agent_thread_id = 202820504

ORDER BY emc.event_record_ts_tz, smc.span_start_ts_tz
;
