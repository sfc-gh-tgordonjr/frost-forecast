/*
 * Overview of Snowflake Pipe Usage Script
 * -------------------------------------------------------
 * This script configures a Snowflake environment to monitor pipe usage for the FrostForecast workflow. Key tasks include:
 * 1. Defining a variable for the initial load lookback period (in days) for flexibility.
 * 2. Creating a table to store aggregated pipe usage data, grouped by hour and pipe name, with tags as a VARIANT for easy parsing.
 * 3. Defining a stored procedure to incrementally update the table with new usage data.
 * 4. Scheduling a task to run the stored procedure every 4 hours using the FROSTFORECAST_ETL warehouse.
 * 5. Creating a materialized view for efficient querying, clustered by pipe name.
 * 6. Tagging all objects with 'FrostForecast Pipeline' for governance.
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
-- Create table to store aggregated pipe usage data
------------------------------------------------
USE ROLE FROSTFORECAST_ADMIN;
USE WAREHOUSE FROSTFORECAST_ETL;
CREATE OR REPLACE TABLE FROSTFORECAST.SOURCE_DATA.PIPE_USAGE (
  HOUR_START                           TIMESTAMP_LTZ,
  PIPE_NAME                            VARCHAR,
  TAGS                                 VARIANT,
  TOTAL_CREDITS_USED                   FLOAT,
  TOTAL_BYTES_INSERTED                 NUMBER(38,0)
) AS
WITH TAG_AGG AS (
    SELECT 
        OBJECT_NAME AS PIPE_NAME,
        TO_VARIANT(ARRAY_AGG(OBJECT_CONSTRUCT('tag_name', TAG_NAME, 'tag_value', TAG_VALUE))) AS TAGS
    FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
    GROUP BY OBJECT_NAME
)
SELECT
    DATE_TRUNC('HOUR', puh.START_TIME) AS HOUR_START,
    puh.PIPE_NAME,
    COALESCE(ta.TAGS, PARSE_JSON('[]')) AS TAGS,
    SUM(puh.CREDITS_USED) AS TOTAL_CREDITS_USED,
    SUM(puh.BYTES_INSERTED) AS TOTAL_BYTES_INSERTED
FROM 
    SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY puh
LEFT JOIN TAG_AGG ta
    ON puh.PIPE_NAME = ta.PIPE_NAME
WHERE
    puh.START_TIME >= DATEADD('DAY', -$LOOKBACK_DAYS, CURRENT_TIMESTAMP())
GROUP BY
    DATE_TRUNC('HOUR', puh.START_TIME),
    puh.PIPE_NAME,
    ta.TAGS;
ALTER TABLE FROSTFORECAST.SOURCE_DATA.PIPE_USAGE SET TAG FROSTFORECAST = 'FrostForecast Object';

-- SECTION: STORED PROCEDURE
------------------------------------------------
-- Create stored procedure to incrementally refresh pipe usage data
------------------------------------------------
USE ROLE FROSTFORECAST_ADMIN;
CREATE OR REPLACE PROCEDURE FROSTFORECAST.SOURCE_DATA.SP_REFRESH_PIPE_USAGE()
  RETURNS STRING
  LANGUAGE SQL
  EXECUTE AS OWNER
AS
$$
BEGIN
  MERGE INTO FROSTFORECAST.SOURCE_DATA.PIPE_USAGE AS target
  USING (
    WITH last_load AS (
      SELECT COALESCE(MAX(HOUR_START), '1970-01-01') AS last_timestamp
      FROM FROSTFORECAST.SOURCE_DATA.PIPE_USAGE
    ),
    TAG_AGG AS (
        SELECT 
            OBJECT_NAME AS PIPE_NAME,
            TO_VARIANT(ARRAY_AGG(OBJECT_CONSTRUCT('tag_name', TAG_NAME, 'tag_value', TAG_VALUE))) AS TAGS
        FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
        GROUP BY OBJECT_NAME
    )
    SELECT
        DATE_TRUNC('HOUR', puh.START_TIME) AS HOUR_START,
        puh.PIPE_NAME,
        COALESCE(ta.TAGS, PARSE_JSON('[]')) AS TAGS,
        SUM(puh.CREDITS_USED) AS TOTAL_CREDITS_USED,
        SUM(puh.BYTES_INSERTED) AS TOTAL_BYTES_INSERTED
    FROM 
        SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY puh
    LEFT JOIN TAG_AGG ta
        ON puh.PIPE_NAME = ta.PIPE_NAME
    WHERE 
        puh.START_TIME > (SELECT last_timestamp FROM last_load)
    GROUP BY
        DATE_TRUNC('HOUR', puh.START_TIME),
        puh.PIPE_NAME,
        ta.TAGS
  ) AS src
  ON target.HOUR_START = src.HOUR_START
  AND target.PIPE_NAME = src.PIPE_NAME
  WHEN NOT MATCHED THEN
    INSERT (
      HOUR_START,
      PIPE_NAME,
      TAGS,
      TOTAL_CREDITS_USED,
      TOTAL_BYTES_INSERTED
    )
    VALUES (
      src.HOUR_START,
      src.PIPE_NAME,
      src.TAGS,
      src.TOTAL_CREDITS_USED,
      src.TOTAL_BYTES_INSERTED
    );
  RETURN 'OK';
END;
$$;
ALTER PROCEDURE FROSTFORECAST.SOURCE_DATA.SP_REFRESH_PIPE_USAGE() SET TAG FROSTFORECAST = 'FrostForecast Object';

-- SECTION: TASK SCHEDULING
------------------------------------------------
-- Schedule task to run stored procedure every 4 hours
------------------------------------------------
USE ROLE FROSTFORECAST_ADMIN;
CREATE OR REPLACE TASK FROSTFORECAST.SOURCE_DATA.REFRESH_PIPE_USAGE
  WAREHOUSE = FROSTFORECAST_ETL
  SCHEDULE = 'USING CRON 0 */4 * * * UTC'
AS
  CALL FROSTFORECAST.SOURCE_DATA.SP_REFRESH_PIPE_USAGE();
ALTER TASK FROSTFORECAST.SOURCE_DATA.REFRESH_PIPE_USAGE SET TAG FROSTFORECAST = 'FrostForecast Object';

-- SECTION: MATERIALIZED VIEW
------------------------------------------------
-- Create materialized view for optimized pipe usage query performance
------------------------------------------------
USE ROLE FROSTFORECAST_ADMIN;
CREATE OR REPLACE MATERIALIZED VIEW FROSTFORECAST.SOURCE_DATA.MV_PIPE_USAGE
  CLUSTER BY (PIPE_NAME)
AS
SELECT *
FROM FROSTFORECAST.SOURCE_DATA.PIPE_USAGE;