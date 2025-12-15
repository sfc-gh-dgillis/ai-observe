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

CREATE OR REPLACE FUNCTION http_status_phrase(p_code NUMBER)
    RETURNS VARCHAR
    LANGUAGE SQL
AS
$$
/*
  Function: http_status_phrase
  Author: Dan Gillis
  Version: 1.0.0
  Description:
      Returns the standard HTTP status phrase for a given HTTP status code.
  Parameters:
      - p_code: The HTTP status code (e.g., 200, 404, 500).
  Returns:
      - VARCHAR: The corresponding HTTP status phrase.
*/
    CASE
        WHEN p_code IS NULL THEN NULL
        -- Successful responses
        WHEN p_code = 200 THEN 'OK'
        WHEN p_code = 201 THEN 'Created'
        WHEN p_code = 202 THEN 'Accepted'
        WHEN p_code = 203 THEN 'Non-Authoritative Information'
        WHEN p_code = 204 THEN 'No Content'
        WHEN p_code = 205 THEN 'Reset Content'
        WHEN p_code = 206 THEN 'Partial Content'
        WHEN p_code = 207 THEN 'Multi-Status'
        WHEN p_code = 208 THEN 'Already Reported'
        WHEN p_code = 226 THEN 'IM Used'
        -- Redirection messages
        WHEN p_code = 300 THEN 'Multiple Choices'
        WHEN p_code = 301 THEN 'Moved Permanently'
        WHEN p_code = 302 THEN 'Found'
        WHEN p_code = 303 THEN 'See Other'
        WHEN p_code = 304 THEN 'Not Modified'
        WHEN p_code = 305 THEN 'Use Proxy'
        WHEN p_code = 307 THEN 'Temporary Redirect'
        WHEN p_code = 308 THEN 'Permanent Redirect'
        -- Client error responses
        WHEN p_code = 400 THEN 'Bad Request'
        WHEN p_code = 401 THEN 'Unauthorized'
        WHEN p_code = 402 THEN 'Payment Required'
        WHEN p_code = 403 THEN 'Forbidden'
        WHEN p_code = 404 THEN 'Not Found'
        WHEN p_code = 405 THEN 'Method Not Allowed'
        WHEN p_code = 406 THEN 'Not Acceptable'
        WHEN p_code = 407 THEN 'Proxy Authentication Required'
        WHEN p_code = 408 THEN 'Request Timeout'
        WHEN p_code = 409 THEN 'Conflict'
        WHEN p_code = 410 THEN 'Gone'
        WHEN p_code = 411 THEN 'Length Required'
        WHEN p_code = 412 THEN 'Precondition Failed'
        WHEN p_code = 413 THEN 'Payload Too Large'
        WHEN p_code = 414 THEN 'URI Too Long'
        WHEN p_code = 415 THEN 'Unsupported Media Type'
        WHEN p_code = 416 THEN 'Range Not Satisfiable'
        WHEN p_code = 417 THEN 'Expectation Failed'
        WHEN p_code = 418 THEN 'I''m a teapot'
        WHEN p_code = 421 THEN 'Misdirected Request'
        WHEN p_code = 422 THEN 'Unprocessable Entity'
        WHEN p_code = 423 THEN 'Locked'
        WHEN p_code = 424 THEN 'Failed Dependency'
        WHEN p_code = 425 THEN 'Too Early'
        WHEN p_code = 426 THEN 'Upgrade Required'
        WHEN p_code = 428 THEN 'Precondition Required'
        WHEN p_code = 429 THEN 'Too Many Requests'
        WHEN p_code = 431 THEN 'Request Header Fields Too Large'
        WHEN p_code = 451 THEN 'Unavailable For Legal Reasons'
        -- Server error responses
        WHEN p_code = 500 THEN 'Internal Server Error'
        WHEN p_code = 501 THEN 'Not Implemented'
        WHEN p_code = 502 THEN 'Bad Gateway'
        WHEN p_code = 503 THEN 'Service Unavailable'
        WHEN p_code = 504 THEN 'Gateway Timeout'
        WHEN p_code = 505 THEN 'HTTP Version Not Supported'
        WHEN p_code = 506 THEN 'Variant Also Negotiates'
        WHEN p_code = 507 THEN 'Insufficient Storage'
        WHEN p_code = 508 THEN 'Loop Detected'
        WHEN p_code = 510 THEN 'Not Extended'
        WHEN p_code = 511 THEN 'Network Authentication Required'

        WHEN p_code BETWEEN 200 AND 299 THEN 'Success (2xx)'
        WHEN p_code BETWEEN 300 AND 399 THEN 'Redirection (3xx)'
        WHEN p_code BETWEEN 400 AND 499 THEN 'Client Error (4xx)'
        WHEN p_code BETWEEN 500 AND 599 THEN 'Server Error (5xx)'

        ELSE TO_VARCHAR(p_code)
        END
$$;

WITH tool_error_cte AS (SELECT record_attributes:"snow.ai.observability.agent.thread_id"::NUMBER      AS thread_id,
                               tool_result.value[0].json.error.Code::VARCHAR                          AS tool_error_code,
                               tool_result.value[0].json.error.HTTPStatus::NUMBER                     AS tool_error_http_status,
                               http_status_phrase(tool_result.value[0].json.error.HTTPStatus::NUMBER) AS tool_error_http_status_phrase,
                               tool_result.value[0].json.error.Message::VARCHAR                       AS tool_error_message
                        FROM TABLE (
                                    snowflake.local.get_ai_observability_events(
                                            'SNOWFLAKE_INTELLIGENCE', -- database
                                            'AGENTS', -- schema
                                            'CLAIMS_AUDIT_AGENT', -- your agent name
                                            'CORTEX AGENT') -- object type
                            ) ev,
                             LATERAL FLATTEN(INPUT =>
                                             TRY_PARSE_JSON(ev.value:"snow.ai.observability.response"):content) content_flattened,
                             LATERAL FLATTEN(INPUT => content_flattened.value:tool_result) tool_result

                        WHERE tool_result.value[0].json.error IS NOT NULL)


SELECT record_type                                                                                            AS record_type,
       record_attributes:"snow.ai.observability.agent.parent_message_id"::NUMBER                              AS parent_message_id,
       record_attributes:"snow.ai.observability.agent.thread_id"::NUMBER                                      AS thread_id,
       tec.tool_error_code                                                                                    AS tool_error_code,
       tec.tool_error_http_status                                                                             AS tool_error_http_status,
       tec.tool_error_http_status_phrase                                                                      AS tool_error_http_status_phrase,
       tec.tool_error_message                                                                                 AS tool_error_message,
       value:"snow.ai.observability.response_status_code"::NUMBER                                             AS response_status_code,
       http_status_phrase(value:"snow.ai.observability.response_status_code"::NUMBER)                         AS response_status,
       value:"snow.ai.observability.response_time_ms"::NUMBER                                                 AS response_time_ms,
       trace:trace_id::VARCHAR                                                                                AS trace_id,
       record:parent_span_id::VARCHAR                                                                         AS parent_span_id,
       trace:span_id::VARCHAR                                                                                 AS span_id,
       timestamp                                                                                              AS ts_ntz,
       ntz_to_tz(timestamp, 'UTC')                                                                            AS ts_tz,
       start_timestamp                                                                                        AS start_ts_ntz,
       ntz_to_tz(start_timestamp, 'UTC')                                                                      AS start_ts_tz,
       MIN(start_ts_tz)
           OVER (PARTITION BY thread_id, trace_id ORDER BY start_ts_tz)                                       AS first_span_record_ts,
       IFF(record:status.code::VARCHAR = 'STATUS_CODE_OK', 'Success',
           record:status.code::VARCHAR)                                                                       AS span_status,
       resource_attributes:"snow.session.id"::NUMBER                                                          AS snowflake_session_id,
       resource_attributes:"snow.session.role.primary.id"::VARCHAR                                            AS snowflake_primary_role_id,
       resource_attributes:"snow.session.role.primary.name"::VARCHAR                                          AS snowflake_primary_role_name,
       resource_attributes:"snow.user.id"::NUMBER                                                             AS snowflake_user_id,
       resource_attributes:"snow.user.name"::VARCHAR                                                          AS snowflake_user_name,
       record:name::VARCHAR                                                                                   AS name,
       record_attributes:"ai.observability.input_id"::VARCHAR                                                 AS ai_observability_input_id,
       record_attributes:"ai.observability.record_id"::VARCHAR                                                AS ai_observability_record_id,
       record_attributes:"snow.ai.observability.database.id"::NUMBER                                          AS database_id,
       record_attributes:"snow.ai.observability.database.name"::VARCHAR                                       AS database_name,
       record_attributes:"snow.ai.observability.object.id"::NUMBER                                            AS object_id,
       record_attributes:"snow.ai.observability.object.name"::VARCHAR                                         AS object_name,
       record_attributes:"snow.ai.observability.object.type"::VARCHAR                                         AS object_type,
       record_attributes:"snow.ai.observability.object.version.id"::NUMBER                                    AS object_version_id,
       record_attributes:"snow.ai.observability.role.id"::NUMBER                                              AS role_id,
       record_attributes:"snow.ai.observability.role.name"::VARCHAR                                           AS role_name,
       record_attributes:"snow.ai.observability.schema.id"::NUMBER                                            AS schema_id,
       record_attributes:"snow.ai.observability.schema.name"::VARCHAR                                         AS schema_name,
       record_attributes:"snow.ai.observability.session.id"::NUMBER                                           AS session_id,
       record_attributes:"snow.ai.observability.user.id"::NUMBER                                              AS user_id,
       record_attributes:"snow.ai.observability.user.name"::VARCHAR                                           AS user_name,
       record_attributes:"snow.ai.observability.span_kind"::NUMBER                                            AS span_kind,
       record:kind::VARCHAR                                                                                   AS record_kind,
       -- agent planning attributes
       record_attributes:"snow.ai.observability.agent.planning.custom_orchestration_instructions"::VARCHAR    AS planning_custom_orchestration_instructions,
       record_attributes:"snow.ai.observability.agent.planning.duration"::NUMBER                              AS planning_duration_ms,
       record_attributes:"snow.ai.observability.agent.planning.instruction"::VARCHAR                          AS planning_instruction,
       TRY_PARSE_JSON(record_attributes:"snow.ai.observability.agent.planning.messages")                      AS planning_messages,
       record_attributes:"snow.ai.observability.agent.planning.model"::VARCHAR                                AS planning_model,
       record_attributes:"snow.ai.observability.agent.planning.query"::VARCHAR                                AS planning_query,
       record_attributes:"snow.ai.observability.agent.planning.request_id"::VARCHAR                           AS planning_request_id,
       record_attributes:"snow.ai.observability.agent.planning.response"::VARCHAR                             AS planning_response,
       record_attributes:"snow.ai.observability.agent.planning.status"::VARCHAR                               AS planning_status,
       record_attributes:"snow.ai.observability.agent.planning.status.code"::NUMBER                           AS planning_status_code,
       record_attributes:"snow.ai.observability.agent.planning.thinking_response"::VARCHAR                    AS planning_thinking_response,
       -- agent planning token counts
       record_attributes:"snow.ai.observability.agent.planning.token_count.cache_read_input"::NUMBER          AS planning_token_count_cache_read_input,
       record_attributes:"snow.ai.observability.agent.planning.token_count.cache_write_input"::NUMBER         AS planning_token_count_cache_write_input,
       record_attributes:"snow.ai.observability.agent.planning.token_count.input"::NUMBER                     AS planning_token_count_input,
       record_attributes:"snow.ai.observability.agent.planning.token_count.output"::NUMBER                    AS planning_token_count_output,
       record_attributes:"snow.ai.observability.agent.planning.token_count.plan"::NUMBER                      AS planning_token_count_plan,
       record_attributes:"snow.ai.observability.agent.planning.token_count.total"::NUMBER                     AS planning_token_count_total,
       -- agent planning tool definitions
       TRY_PARSE_JSON(record_attributes:"snow.ai.observability.agent.planning.tool.description")              AS planning_tool_description,
       TRY_PARSE_JSON(record_attributes:"snow.ai.observability.agent.planning.tool.name")                     AS planning_tool_name,
       TRY_PARSE_JSON(record_attributes:"snow.ai.observability.agent.planning.tool.parameters")               AS planning_tool_parameters,
       TRY_PARSE_JSON(record_attributes:"snow.ai.observability.agent.planning.tool.type")                     AS planning_tool_type,
       -- agent planning tool execution
       TRY_PARSE_JSON(record_attributes:"snow.ai.observability.agent.planning.tool_execution.argument.name")  AS planning_tool_execution_argument_name,
       TRY_PARSE_JSON(record_attributes:"snow.ai.observability.agent.planning.tool_execution.argument.value") AS planning_tool_execution_argument_value,
       TRY_PARSE_JSON(record_attributes:"snow.ai.observability.agent.planning.tool_execution.id")             AS planning_tool_execution_id,
       TRY_PARSE_JSON(record_attributes:"snow.ai.observability.agent.planning.tool_execution.name")           AS planning_tool_execution_name,
       TRY_PARSE_JSON(record_attributes:"snow.ai.observability.agent.planning.tool_execution.results")        AS planning_tool_execution_results,
       TRY_PARSE_JSON(record_attributes:"snow.ai.observability.agent.planning.tool_execution.type")           AS planning_tool_execution_type,
       -- event value
       value:"snow.ai.observability.request_body"                                                             AS request_body,
       TRY_PARSE_JSON(value:"snow.ai.observability.response")                                                 AS response,

       -- everything below is for additional debugging / validation purposes only. These are the original columns from the function.
       '------------------>'                                                                                  AS original_columns,
       trace                                                                                                  AS trace,
       resource_attributes                                                                                    AS resource_attributes,
       record                                                                                                 AS record,
       record_attributes                                                                                      AS record_attributes,
       value                                                                                                  AS value
FROM TABLE (
            snowflake.local.get_ai_observability_events(
                    'SNOWFLAKE_INTELLIGENCE', -- database
                    'AGENTS', -- schema
                    'CLAIMS_AUDIT_AGENT', -- your agent name
                    'CORTEX AGENT') -- object type
    )

         LEFT JOIN tool_error_cte tec
                   ON tec.thread_id = record_attributes:"snow.ai.observability.agent.thread_id"::NUMBER

ORDER BY thread_id
;
