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

SELECT record_attributes:"snow.ai.observability.agent.parent_message_id"::NUMBER                          AS parent_message_id,
       record_attributes:"snow.ai.observability.agent.thread_id"::NUMBER                                  AS thread_id,
       trace:trace_id::VARCHAR                                                                            AS trace_id,
       record:parent_span_id::VARCHAR                                                                     AS parent_span_id,
       trace:span_id::VARCHAR                                                                             AS span_id,
       record_type                                                                                        AS record_type,
       timestamp                                                                                          AS ts_ntz,
       ntz_to_tz(timestamp, 'UTC')                                                                        AS ts_tz,
       start_timestamp                                                                                    AS start_ts_ntz,
       ntz_to_tz(start_timestamp, 'UTC')                                                                  AS start_ts_tz,
       MIN(start_ts_tz)
           OVER (PARTITION BY ai_observability_input_id, ai_observability_record_id ORDER BY start_ts_tz) AS first_span_record_ts,
       IFF(record:status.code::VARCHAR = 'STATUS_CODE_OK', 'Success',
           record:status.code::VARCHAR)                                                                   AS span_status,
       resource_attributes:"snow.session.id"::NUMBER                                                      AS snowflake_session_id,
       resource_attributes:"snow.session.role.primary.id"::VARCHAR                                        AS snowflake_primary_role_id,
       resource_attributes:"snow.session.role.primary.name"::VARCHAR                                      AS snowflake_primary_role_name,
       resource_attributes:"snow.user.id"::NUMBER                                                         AS snowflake_user_id,
       resource_attributes:"snow.user.name"::VARCHAR                                                      AS snowflake_user_name,
       record:name::VARCHAR                                                                               AS name,
       record_attributes:"ai.observability.input_id"::VARCHAR                                             AS ai_observability_input_id,
       record_attributes:"ai.observability.record_id"::VARCHAR                                            AS ai_observability_record_id,
       record_attributes:"snow.ai.observability.object.version.id"::NUMBER                                AS object_version_id,
       record_attributes:"snow.ai.observability.database.id"::NUMBER                                      AS database_id,
       record_attributes:"snow.ai.observability.database.name"::VARCHAR                                   AS database_name,
       record_attributes:"snow.ai.observability.object.id"::NUMBER                                        AS object_id,
       record_attributes:"snow.ai.observability.object.name"::VARCHAR                                     AS object_name,
       record_attributes:"snow.ai.observability.object.type"::VARCHAR                                     AS object_type,
       record_attributes:"snow.ai.observability.role.id"::NUMBER                                          AS role_id,
       record_attributes:"snow.ai.observability.role.name"::VARCHAR                                       AS role_name,
       record_attributes:"snow.ai.observability.schema.id"::NUMBER                                        AS schema_id,
       record_attributes:"snow.ai.observability.schema.name"::VARCHAR                                     AS schema_name,
       record_attributes:"snow.ai.observability.session.id"::NUMBER                                       AS session_id,
       record_attributes:"snow.ai.observability.user.id"::NUMBER                                          AS user_id,
       record_attributes:"snow.ai.observability.user.name"::VARCHAR                                       AS user_name,
       record_attributes:"snow.ai.observability.span_kind"::NUMBER                                        AS span_kind,
       record:kind::VARCHAR                                                                               AS record_kind,
       -- agent planning attributes
       record_attributes:"snow.ai.observability.agent.planning.custom_orchestration_instructions"::VARCHAR AS planning_custom_orchestration_instructions,
       record_attributes:"snow.ai.observability.agent.planning.duration"::VARCHAR                         AS planning_duration,
       record_attributes:"snow.ai.observability.agent.planning.instruction"::VARCHAR                      AS planning_instruction,
       record_attributes:"snow.ai.observability.agent.planning.messages"::VARCHAR                         AS planning_messages,
       record_attributes:"snow.ai.observability.agent.planning.model"::VARCHAR                            AS planning_model,
       record_attributes:"snow.ai.observability.agent.planning.query"::VARCHAR                            AS planning_query,
       record_attributes:"snow.ai.observability.agent.planning.request_id"::VARCHAR                       AS planning_request_id,
       record_attributes:"snow.ai.observability.agent.planning.response"::VARCHAR                         AS planning_response,
       record_attributes:"snow.ai.observability.agent.planning.status"::VARCHAR                           AS planning_status,
       record_attributes:"snow.ai.observability.agent.planning.status.code"::VARCHAR                      AS planning_status_code,
       record_attributes:"snow.ai.observability.agent.planning.thinking_response"::VARCHAR                AS planning_thinking_response,
       -- agent planning token counts
       record_attributes:"snow.ai.observability.agent.planning.token_count.cache_read_input"::VARCHAR     AS planning_token_count_cache_read_input,
       record_attributes:"snow.ai.observability.agent.planning.token_count.cache_write_input"::VARCHAR    AS planning_token_count_cache_write_input,
       record_attributes:"snow.ai.observability.agent.planning.token_count.input"::VARCHAR                AS planning_token_count_input,
       record_attributes:"snow.ai.observability.agent.planning.token_count.output"::VARCHAR               AS planning_token_count_output,
       record_attributes:"snow.ai.observability.agent.planning.token_count.plan"::VARCHAR                 AS planning_token_count_plan,
       record_attributes:"snow.ai.observability.agent.planning.token_count.total"::VARCHAR                AS planning_token_count_total,
       -- agent planning tool definitions
       record_attributes:"snow.ai.observability.agent.planning.tool.description"::VARCHAR                 AS planning_tool_description,
       record_attributes:"snow.ai.observability.agent.planning.tool.name"::VARCHAR                        AS planning_tool_name,
       record_attributes:"snow.ai.observability.agent.planning.tool.parameters"::VARCHAR                  AS planning_tool_parameters,
       record_attributes:"snow.ai.observability.agent.planning.tool.type"::VARCHAR                        AS planning_tool_type,
       -- agent planning tool execution
       record_attributes:"snow.ai.observability.agent.planning.tool_execution.argument.name"::VARCHAR     AS planning_tool_execution_argument_name,
       record_attributes:"snow.ai.observability.agent.planning.tool_execution.argument.value"::VARCHAR    AS planning_tool_execution_argument_value,
       record_attributes:"snow.ai.observability.agent.planning.tool_execution.id"::VARCHAR                AS planning_tool_execution_id,
       record_attributes:"snow.ai.observability.agent.planning.tool_execution.name"::VARCHAR              AS planning_tool_execution_name,
       record_attributes:"snow.ai.observability.agent.planning.tool_execution.results"::VARCHAR           AS planning_tool_execution_results,
       record_attributes:"snow.ai.observability.agent.planning.tool_execution.type"::VARCHAR              AS planning_tool_execution_type,
       -- everything below is for additional debugging / validation purposes only. These are the original columns from the function.
       '------------------>'                                                                              AS original_columns,
       trace                                                                                              AS trace,
       resource_attributes                                                                                AS resource_attributes,
       record                                                                                             AS record,
       record_attributes                                                                                  AS record_attributes,
       value                                                                                              AS value
FROM TABLE (
    snowflake.local.get_ai_observability_events(
            'SNOWFLAKE_INTELLIGENCE', -- database
            'AGENTS', -- schema
            'CLAIMS_AUDIT_AGENT', -- your agent name
            'CORTEX AGENT') -- object type
    )

ORDER BY thread_id
;
