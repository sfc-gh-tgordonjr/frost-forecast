/*
 * Overview of Snowflake Cortex Function Credit Usage Script
 * -------------------------------------------------------
 * This script sets up a Snowflake environment to monitor Cortex function credit usage for the FrostForecast project. Key tasks include:
 * 1. Defining a variable for the initial load lookback period (in days) for flexibility.
 * 2. Creating a table to store aggregated Cortex function usage data from query history, with tags as a VARIANT.
 * 3. Defining a stored procedure to incrementally update the table with new usage data.
 * 4. Scheduling a task to run the stored procedure every 4 hours using the FROSTFORECAST_ETL warehouse.
 * 5. Creating a materialized view for efficient querying, clustered by warehouse and function name.
 * 6. Creating a table, stored procedure, task, and materialized view for Cortex Analyst usage data.
 * 7. Creating a table, stored procedure, task, and materialized view for Cortex Search Serving usage data.
 * 8. Deploying a Streamlit app for visualizing AI/ML consumption data.
 * 9. Tagging all objects with 'FrostForecast Pipeline' for governance.
 *
 * Note: Stream logic for WH_CREDIT_USAGE_STREAM has been removed.
 *
 * Search sections using Command+F with markers:
 * - SECTION: VARIABLES
 * - SECTION: TABLE CREATION
 * - SECTION: STORED PROCEDURE
 * - SECTION: TASK SCHEDULING
 * - SECTION: MATERIALIZED VIEW
 */

-- SECTION: VARIABLES
------------------------------------------------
-- Define a variable for the initial load lookback period (in days)
------------------------------------------------
SET LOOKBACK_DAYS = 90; -- Adjust this value to change the initial load period (e.g., 7, 30, 60)

-- SECTION: TABLE CREATION
------------------------------------------------
-- Create table to store aggregated Cortex function usage data
------------------------------------------------
USE ROLE FROSTFORECAST_ADMIN;
USE WAREHOUSE FROSTFORECAST_ETL;
CREATE OR REPLACE TABLE FROSTFORECAST.SOURCE_DATA.CORTEX_FUNCTION_CREDIT_USAGE (
  HOUR_START                                   TIMESTAMP_LTZ,
  QH_QUERY_TYPE                                VARCHAR,
  QH_WAREHOUSE_NAME                            VARCHAR,
  QH_WAREHOUSE_SIZE                            VARCHAR,
  QH_WAREHOUSE_TYPE                            VARCHAR,
  QH_QUERY_TAG                                 VARCHAR,
  CF_FUNCTION_NAME                             VARCHAR,
  CF_MODEL_NAME                                VARCHAR,
  TAGS                                         VARIANT,
  QH_TOTAL_QUERIES                             NUMBER(38,0),
  QH_TOTAL_ROWS_PRODUCED                       NUMBER(38,0),
  QH_TOTAL_ROWS_UPDATED                        NUMBER(38,0),
  QH_TOTAL_CREDITS_USED_CLOUD_SERVICES         FLOAT,
  CF_TOTAL_TOKENS                              FLOAT,
  CF_TOTAL_TOKEN_CREDITS                       FLOAT
) AS
WITH TAG_AGG AS (
    SELECT 
        OBJECT_NAME AS WAREHOUSE_NAME,
        TO_VARIANT(ARRAY_AGG(OBJECT_CONSTRUCT('tag_name', TAG_NAME, 'tag_value', TAG_VALUE))) AS TAGS
    FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
    GROUP BY OBJECT_NAME
)
SELECT
    DATE_TRUNC('HOUR', QH.START_TIME) AS HOUR_START,
    QH.QUERY_TYPE,
    QH.WAREHOUSE_NAME,
    QH.WAREHOUSE_SIZE,
    QH.WAREHOUSE_TYPE,
    QH.QUERY_TAG,
    CF.FUNCTION_NAME,
    CF.MODEL_NAME,
    COALESCE(ta.TAGS, PARSE_JSON('[]')) AS TAGS,
    COUNT(DISTINCT QH.QUERY_ID) AS TOTAL_QUERIES,
    SUM(QH.ROWS_PRODUCED) AS TOTAL_ROWS_PRODUCED,
    SUM(QH.ROWS_UPDATED) AS TOTAL_ROWS_UPDATED,
    SUM(QH.CREDITS_USED_CLOUD_SERVICES) AS TOTAL_QH_CREDITS_USED_CLOUD_SERVICES,
    SUM(CF.TOKENS) AS TOTAL_TOKENS,
    SUM(CF.TOKEN_CREDITS) AS TOTAL_TOKEN_CREDITS
FROM 
    SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY QH
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_QUERY_USAGE_HISTORY CF
    ON CF.QUERY_ID = QH.QUERY_ID 
    AND CF.WAREHOUSE_ID = QH.WAREHOUSE_ID
LEFT JOIN TAG_AGG ta
    ON QH.WAREHOUSE_NAME = ta.WAREHOUSE_NAME
WHERE
    CF.FUNCTION_NAME IS NOT NULL
    AND QH.START_TIME >= DATEADD('DAY', -$LOOKBACK_DAYS, CURRENT_TIMESTAMP())
GROUP BY
    DATE_TRUNC('HOUR', QH.START_TIME),
    QH.QUERY_TYPE,
    QH.WAREHOUSE_NAME,
    QH.WAREHOUSE_SIZE,
    QH.WAREHOUSE_TYPE,
    QH.QUERY_TAG,
    CF.FUNCTION_NAME,
    CF.MODEL_NAME,
    ta.TAGS;
ALTER TABLE FROSTFORECAST.SOURCE_DATA.CORTEX_FUNCTION_CREDIT_USAGE SET TAG FROSTFORECAST = 'FrostForecast Object';

-- SECTION: STORED PROCEDURE
------------------------------------------------
-- Create stored procedure to incrementally refresh usage data
------------------------------------------------
USE ROLE FROSTFORECAST_ADMIN;
CREATE OR REPLACE PROCEDURE FROSTFORECAST.SOURCE_DATA.SP_REFRESH_CORTEX_FUNCTION_CREDIT_USAGE()
  RETURNS STRING
  LANGUAGE SQL
  EXECUTE AS OWNER
AS
$$
BEGIN
  MERGE INTO FROSTFORECAST.SOURCE_DATA.CORTEX_FUNCTION_CREDIT_USAGE AS target
  USING (
    WITH last_load AS (
      SELECT COALESCE(MAX(QH_START_HOUR_TIMESTAMP), '1970-01-01') AS last_timestamp
      FROM FROSTFORECAST.SOURCE_DATA.CORTEX_FUNCTION_CREDIT_USAGE
    ),
    TAG_AGG AS (
        SELECT 
            OBJECT_NAME AS WAREHOUSE_NAME,
            TO_VARIANT(ARRAY_AGG(OBJECT_CONSTRUCT('tag_name', TAG_NAME, 'tag_value', TAG_VALUE))) AS TAGS
        FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
        GROUP BY OBJECT_NAME
    )
    SELECT
      DATE_TRUNC('HOUR', QH.START_TIME) AS HOUR_START,
      QH.QUERY_TYPE,
      QH.WAREHOUSE_NAME,
      QH.WAREHOUSE_SIZE,
      QH.WAREHOUSE_TYPE,
      QH.QUERY_TAG,
      CF.FUNCTION_NAME,
      CF.MODEL_NAME,
      COALESCE(ta.TAGS, PARSE_JSON('[]')) AS TAGS,
      COUNT(DISTINCT QH.QUERY_ID) AS QH_TOTAL_QUERIES,
      SUM(QH.ROWS_PRODUCED) AS QH_TOTAL_ROWS_PRODUCED,
      SUM(QH.ROWS_UPDATED) AS QH_TOTAL_ROWS_UPDATED,
      CAST(SUM(QH.CREDITS_USED_CLOUD_SERVICES) AS NUMBER(38,2)) AS QH_TOTAL_CREDITS_USED_CLOUD_SERVICES,
      SUM(CF.TOKENS) AS CF_TOTAL_TOKENS,
      CAST(SUM(CF.TOKEN_CREDITS) AS NUMBER(38,2)) AS CF_TOTAL_TOKEN_CREDITS
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY QH
    LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_QUERY_USAGE_HISTORY CF
      ON CF.QUERY_ID = QH.QUERY_ID 
      AND CF.WAREHOUSE_ID = QH.WAREHOUSE_ID
    LEFT JOIN TAG_AGG ta
      ON QH.WAREHOUSE_NAME = ta.WAREHOUSE_NAME
    WHERE CF.FUNCTION_NAME IS NOT NULL
    AND DATE_TRUNC('HOUR', QH.START_TIME) > (SELECT last_timestamp FROM last_load)
    GROUP BY
      DATE_TRUNC('HOUR', QH.START_TIME),
      QH.QUERY_TYPE,
      QH.WAREHOUSE_NAME,
      QH.WAREHOUSE_SIZE,
      QH.WAREHOUSE_TYPE,
      QH.QUERY_TAG,
      CF.FUNCTION_NAME,
      CF.MODEL_NAME,
      ta.TAGS
  ) AS src
  ON target.HOUR_START = src.HOUR_START
  AND target.QH_WAREHOUSE_NAME = src.WAREHOUSE_NAME
  AND target.QH_QUERY_TYPE = src.QUERY_TYPE
  AND COALESCE(target.QH_QUERY_TAG, '') = COALESCE(src.QUERY_TAG, '')
  AND COALESCE(target.CF_FUNCTION_NAME, '') = COALESCE(src.FUNCTION_NAME, '')
  AND COALESCE(target.CF_MODEL_NAME, '') = COALESCE(src.MODEL_NAME, '')
  WHEN NOT MATCHED THEN
    INSERT (
      HOUR_START,
      QH_QUERY_TYPE,
      QH_WAREHOUSE_NAME,
      QH_WAREHOUSE_SIZE,
      QH_WAREHOUSE_TYPE,
      QH_QUERY_TAG,
      CF_FUNCTION_NAME,
      CF_MODEL_NAME,
      TAGS,
      QH_TOTAL_QUERIES,
      QH_TOTAL_ROWS_PRODUCED,
      QH_TOTAL_ROWS_UPDATED,
      QH_TOTAL_CREDITS_USED_CLOUD_SERVICES,
      CF_TOTAL_TOKENS,
      CF_TOTAL_TOKEN_CREDITS
    )
    VALUES (
      src.HOUR_START,
      src.QUERY_TYPE,
      src.WAREHOUSE_NAME,
      src.WAREHOUSE_SIZE,
      src.WAREHOUSE_TYPE,
      src.QUERY_TAG,
      src.FUNCTION_NAME,
      src.MODEL_NAME,
      src.TAGS,
      src.QH_TOTAL_QUERIES,
      src.QH_TOTAL_ROWS_PRODUCED,
      src.QH_TOTAL_ROWS_UPDATED,
      src.QH_TOTAL_CREDITS_USED_CLOUD_SERVICES,
      src.CF_TOTAL_TOKENS,
      src.CF_TOTAL_TOKEN_CREDITS
    );
  RETURN 'OK';
END;
$$;
ALTER PROCEDURE FROSTFORECAST.SOURCE_DATA.SP_REFRESH_CORTEX_FUNCTION_CREDIT_USAGE() SET TAG FROSTFORECAST = 'FrostForecast Object';

-- SECTION: TASK SCHEDULING
------------------------------------------------
-- Schedule task to run stored procedure every 4 hours
------------------------------------------------
USE ROLE FROSTFORECAST_ADMIN;
CREATE OR REPLACE TASK FROSTFORECAST.SOURCE_DATA.REFRESH_CORTEX_FUNCTION_CREDIT_USAGE
  WAREHOUSE = FROSTFORECAST_ETL
  SCHEDULE = 'USING CRON 0 */4 * * * UTC'
AS
  CALL FROSTFORECAST.SOURCE_DATA.SP_REFRESH_CORTEX_FUNCTION_CREDIT_USAGE();
ALTER TASK FROSTFORECAST.SOURCE_DATA.REFRESH_CORTEX_FUNCTION_CREDIT_USAGE SET TAG FROSTFORECAST = 'FrostForecast Object';

-- SECTION: MATERIALIZED VIEW
------------------------------------------------
-- Create materialized view for optimized query performance
------------------------------------------------
USE ROLE FROSTFORECAST_ADMIN;
CREATE OR REPLACE MATERIALIZED VIEW FROSTFORECAST.SOURCE_DATA.MV_CORTEX_FUNCTION_CREDIT_USAGE
  CLUSTER BY (QH_WAREHOUSE_NAME, CF_FUNCTION_NAME)
AS
SELECT *
FROM FROSTFORECAST.SOURCE_DATA.CORTEX_FUNCTION_CREDIT_USAGE;