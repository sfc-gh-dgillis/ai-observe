-- -----------------------------------------------------------------------
-- SET session variables
-- -----------------------------------------------------------------------
-- Replace ai_eval_db.ai_eval_schema with your actual database and schema names
SET ai_eval_db = 'ai_eval_db';
SET ai_eval_schema = 'ai_eval_db.ai_eval_schema';

-- replace ai_eval_poc_role with your actual role name
SET ai_eval_role = 'ai_eval_poc_role';

-- replace ai_eval_poc_agent with the name of the agent being evaluated
SET ai_eval_agent = 'ai_eval_poc_agent';

-- replace dgillis with the actual user name performing the evaluation
SET ai_eval_user = 'dgillis';

-- -----------------------------------------------------------------------
-- Required Role Grants
-- -----------------------------------------------------------------------
GRANT USAGE ON DATABASE IDENTIFIER ($ai_eval_db) TO ROLE IDENTIFIER ($ai_eval_role);
GRANT USAGE ON SCHEMA IDENTIFIER ($ai_eval_schema) TO ROLE IDENTIFIER ($ai_eval_role);

-- Specialized db/application roles
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE IDENTIFIER($ai_eval_role);
GRANT APPLICATION ROLE SNOWFLAKE.AI_OBSERVABILITY_EVENTS_LOOKUP TO ROLE IDENTIFIER($ai_eval_role);

-- Create Datasets
GRANT CREATE FILE FORMAT ON SCHEMA IDENTIFIER($ai_eval_schema) TO ROLE IDENTIFIER($ai_eval_role);
GRANT CREATE DATASET ON SCHEMA IDENTIFIER($ai_eval_schema) TO ROLE IDENTIFIER($ai_eval_role);

-- Create and execute tasks
GRANT CREATE TASK ON IDENTIFIER($ai_eval_schema) TO ROLE IDENTIFIER($ai_eval_role);
GRANT EXECUTE TASK ON ACCOUNT TO ROLE IDENTIFIER($ai_eval_role);

-- Run evaluations
GRANT MONITOR ON AGENT IDENTIFIER($ai_eval_agent) TO ROLE IDENTIFIER($ai_eval_role);

-- File formats and datasets
GRANT CREATE FILE FORMAT ON SCHEMA IDENTIFIER($ai_eval_schema) TO ROLE IDENTIFIER($ai_eval_role);
GRANT CREATE DATASET ON SCHEMA IDENTIFIER($ai_eval_schema) TO ROLE IDENTIFIER($ai_eval_role);

-- -----------------------------------------------------------------------
-- Required User Grants
-- -----------------------------------------------------------------------
GRANT IMPERSONATE ON USER IDENTIFIER($ai_eval_user) TO ROLE IDENTIFIER($ai_eval_role);
