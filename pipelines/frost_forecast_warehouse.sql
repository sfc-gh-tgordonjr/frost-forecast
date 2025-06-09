/*
 * Overview of Snowflake Warehouse Usage Script
 * -------------------------------------------------------
 * This script configures a Snowflake environment to monitor warehouse usage for the FrostForecast workflow. Key tasks include:
 * 1. Defining a variable for the initial load lookback period (in days) for flexibility.
 * 2. Creating a table to store aggregated warehouse load, metering, and query count data, with tags as a VARIANT for easy parsing.
 * 3. Defining a stored procedure to incrementally update the table with new usage data.
 * 4. Scheduling a task to run the stored procedure every 4 hours using the FROSTFORECAST_ETL warehouse.
 * 5. Creating a materialized view for efficient querying, clustered by warehouse name.
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
-- Create table to store aggregated warehouse usage data
------------------------------------------------
USE ROLE FROSTFORECAST_ADMIN;
USE WAREHOUSE FROSTFORECAST_ETL;
CREATE OR REPLACE TABLE FROSTFORECAST.SOURCE_DATA.WAREHOUSE_USAGE (
  HOUR_START                           TIMESTAMP_LTZ,
  WAREHOUSE_ID                         VARCHAR,
  WAREHOUSE_NAME                       VARCHAR,
  TAGS                                 VARIANT,
  AVG_RUNNING_LOAD                     FLOAT,
  AVG_QUEUED_LOAD                      FLOAT,
  AVG_QUEUED_PROVISIONING              FLOAT,
  AVG_BLOCKED                          FLOAT,
  TOTAL_CREDITS_USED                   FLOAT,
  COMPUTE_CREDITS_USED                 FLOAT,
  CLOUD_SERVICES_CREDITS_USED          FLOAT,
  QUERY_CREDITS_USED                   FLOAT,
  IDLE_CREDITS                         FLOAT,
  QUERY_COUNT                          FLOAT,
  AVG_EXECUTION_TIME_SECONDS           FLOAT,
  AVG_EXECUTION_TIME_MINUTES           FLOAT
) AS
WITH HOURLY_LOAD AS (
    SELECT 
        DATE_TRUNC('HOUR', wlh.START_TIME) AS HOUR_START,
        wlh.WAREHOUSE_ID,
        wlh.WAREHOUSE_NAME,
        AVG(wlh.AVG_RUNNING) AS AVG_RUNNING_LOAD,
        AVG(wlh.AVG_QUEUED_LOAD) AS AVG_QUEUED_LOAD,
        AVG(wlh.AVG_QUEUED_PROVISIONING) AS AVG_QUEUED_PROVISIONING,
        AVG(wlh.AVG_BLOCKED) AS AVG_BLOCKED
    FROM 
        SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY wlh
    WHERE 
        wlh.START_TIME >= DATEADD('DAY', -$LOOKBACK_DAYS, CURRENT_TIMESTAMP())
    GROUP BY 
        DATE_TRUNC('HOUR', wlh.START_TIME),
        wlh.WAREHOUSE_ID,
        wlh.WAREHOUSE_NAME
),
WAREHOUSE_METERING AS (
    SELECT 
        DATE_TRUNC('HOUR', wmh.START_TIME) AS HOUR_START,
        wmh.WAREHOUSE_ID,
        wmh.WAREHOUSE_NAME,
        SUM(wmh.CREDITS_USED) AS TOTAL_CREDITS_USED,
        SUM(wmh.CREDITS_USED_COMPUTE) AS COMPUTE_CREDITS_USED,
        SUM(wmh.CREDITS_USED_CLOUD_SERVICES) AS CLOUD_SERVICES_CREDITS_USED,
        SUM(wmh.CREDITS_ATTRIBUTED_COMPUTE_QUERIES) AS QUERY_CREDITS_USED
    FROM 
        SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY wmh
    WHERE 
        wmh.START_TIME >= DATEADD('DAY', -$LOOKBACK_DAYS, CURRENT_TIMESTAMP())
    GROUP BY 
        DATE_TRUNC('HOUR', wmh.START_TIME),
        wmh.WAREHOUSE_ID,
        wmh.WAREHOUSE_NAME
),
QUERY_COUNTS AS (
    SELECT 
        DATE_TRUNC('HOUR', qh.START_TIME) AS HOUR_START,
        qh.WAREHOUSE_ID,
        qh.WAREHOUSE_NAME,
        COUNT(qh.QUERY_ID) AS QUERY_COUNT,
        AVG(qh.EXECUTION_TIME) / 1000 AS AVG_EXECUTION_TIME_SECONDS,
        AVG(qh.EXECUTION_TIME) / (1000 * 60) AS AVG_EXECUTION_TIME_MINUTES
    FROM 
        SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh
    WHERE 
        qh.START_TIME >= DATEADD('DAY', -$LOOKBACK_DAYS, CURRENT_TIMESTAMP())
        AND qh.EXECUTION_STATUS = 'SUCCESS'
    GROUP BY 
        DATE_TRUNC('HOUR', qh.START_TIME),
        qh.WAREHOUSE_ID,
        qh.WAREHOUSE_NAME
),
TAG_AGG AS (
    SELECT 
        OBJECT_NAME AS WAREHOUSE_NAME,
        TO_VARIANT(ARRAY_AGG(OBJECT_CONSTRUCT('tag_name', TAG_NAME, 'tag_value', TAG_VALUE))) AS TAGS
    FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
    GROUP BY OBJECT_NAME
)
SELECT 
    hl.HOUR_START,
    hl.WAREHOUSE_ID,
    hl.WAREHOUSE_NAME,
    COALESCE(ta.TAGS, PARSE_JSON('[]')) AS TAGS,
    hl.AVG_RUNNING_LOAD,
    hl.AVG_QUEUED_LOAD,
    hl.AVG_QUEUED_PROVISIONING,
    hl.AVG_BLOCKED,
    COALESCE(wm.TOTAL_CREDITS_USED, 0) AS TOTAL_CREDITS_USED,
    COALESCE(wm.COMPUTE_CREDITS_USED, 0) AS COMPUTE_CREDITS_USED,
    COALESCE(wm.CLOUD_SERVICES_CREDITS_USED, 0) AS CLOUD_SERVICES_CREDITS_USED,
    COALESCE(wm.QUERY_CREDITS_USED, 0) AS QUERY_CREDITS_USED,
    (COALESCE(wm.TOTAL_CREDITS_USED, 0) - COALESCE(wm.QUERY_CREDITS_USED, 0)) AS IDLE_CREDITS,
    COALESCE(qc.QUERY_COUNT, 0) AS QUERY_COUNT,
    COALESCE(qc.AVG_EXECUTION_TIME_SECONDS, 0) AS AVG_EXECUTION_TIME_SECONDS,
    COALESCE(qc.AVG_EXECUTION_TIME_MINUTES, 0) AS AVG_EXECUTION_TIME_MINUTES
FROM 
    HOURLY_LOAD hl
LEFT JOIN 
    WAREHOUSE_METERING wm
    ON hl.WAREHOUSE_ID = wm.WAREHOUSE_ID
    AND hl.HOUR_START = wm.HOUR_START
LEFT JOIN 
    QUERY_COUNTS qc
    ON hl.WAREHOUSE_ID = qc.WAREHOUSE_ID
    AND hl.HOUR_START = qc.HOUR_START
LEFT JOIN 
    TAG_AGG ta
    ON hl.WAREHOUSE_NAME = ta.WAREHOUSE_NAME;
ALTER TABLE FROSTFORECAST.SOURCE_DATA.WAREHOUSE_USAGE SET TAG FROSTFORECAST = 'FrostForecast Object';

-- SECTION: STORED PROCEDURE
------------------------------------------------
-- Create stored procedure to incrementally refresh warehouse usage data
------------------------------------------------
USE ROLE FROSTFORECAST_ADMIN;
CREATE OR REPLACE PROCEDURE FROSTFORECAST.SOURCE_DATA.SP_REFRESH_WAREHOUSE_USAGE()
  RETURNS STRING
  LANGUAGE SQL
  EXECUTE AS OWNER
AS
$$
BEGIN
  MERGE INTO FROSTFORECAST.SOURCE_DATA.WAREHOUSE_USAGE AS target
  USING (
    WITH last_load AS (
      SELECT COALESCE(MAX(HOUR_START), '1970-01-01') AS last_timestamp
      FROM FROSTFORECAST.SOURCE_DATA.WAREHOUSE_USAGE
    ),
    HOURLY_LOAD AS (
        SELECT 
            DATE_TRUNC('HOUR', wlh.START_TIME) AS HOUR_START,
            wlh.WAREHOUSE_ID,
            wlh.WAREHOUSE_NAME,
            AVG(wlh.AVG_RUNNING) AS AVG_RUNNING_LOAD,
            AVG(wlh.AVG_QUEUED_LOAD) AS AVG_QUEUED_LOAD,
            AVG(wlh.AVG_QUEUED_PROVISIONING) AS AVG_QUEUED_PROVISIONING,
            AVG(wlh.AVG_BLOCKED) AS AVG_BLOCKED
        FROM 
            SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY wlh
        WHERE 
            wlh.START_TIME > (SELECT last_timestamp FROM last_load)
        GROUP BY 
            DATE_TRUNC('HOUR', wlh.START_TIME),
            wlh.WAREHOUSE_ID,
            wlh.WAREHOUSE_NAME
    ),
    WAREHOUSE_METERING AS (
        SELECT 
            DATE_TRUNC('HOUR', wmh.START_TIME) AS HOUR_START,
            wmh.WAREHOUSE_ID,
            wmh.WAREHOUSE_NAME,
            SUM(wmh.CREDITS_USED) AS TOTAL_CREDITS_USED,
            SUM(wmh.CREDITS_USED_COMPUTE) AS COMPUTE_CREDITS_USED,
            SUM(wmh.CREDITS_USED_CLOUD_SERVICES) AS CLOUD_SERVICES_CREDITS_USED,
            SUM(wmh.CREDITS_ATTRIBUTED_COMPUTE_QUERIES) AS QUERY_CREDITS_USED
        FROM 
            SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY wmh
        WHERE 
            wmh.START_TIME > (SELECT last_timestamp FROM last_load)
        GROUP BY 
            DATE_TRUNC('HOUR', wmh.START_TIME),
            wmh.WAREHOUSE_ID,
            wmh.WAREHOUSE_NAME
    ),
    QUERY_COUNTS AS (
        SELECT 
            DATE_TRUNC('HOUR', qh.START_TIME) AS HOUR_START,
            qh.WAREHOUSE_ID,
            qh.WAREHOUSE_NAME,
            COUNT(qh.QUERY_ID) AS QUERY_COUNT,
            AVG(qh.EXECUTION_TIME) / 1000 AS AVG_EXECUTION_TIME_SECONDS,
            AVG(qh.EXECUTION_TIME) / (1000 * 60) AS AVG_EXECUTION_TIME_MINUTES
        FROM 
            SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh
        WHERE 
            qh.START_TIME > (SELECT last_timestamp FROM last_load)
            AND qh.EXECUTION_STATUS = 'SUCCESS'
        GROUP BY 
            DATE_TRUNC('HOUR', qh.START_TIME),
            qh.WAREHOUSE_ID,
            qh.WAREHOUSE_NAME
    ),
    TAG_AGG AS (
        SELECT 
            OBJECT_NAME AS WAREHOUSE_NAME,
            TO_VARIANT(ARRAY_AGG(OBJECT_CONSTRUCT('tag_name', TAG_NAME, 'tag_value', TAG_VALUE))) AS TAGS
        FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
        GROUP BY OBJECT_NAME
    )
    SELECT 
        hl.HOUR_START,
        hl.WAREHOUSE_ID,
        hl.WAREHOUSE_NAME,
        COALESCE(ta.TAGS, PARSE_JSON('[]')) AS TAGS,
        hl.AVG_RUNNING_LOAD,
        hl.AVG_QUEUED_LOAD,
        hl.AVG_QUEUED_PROVISIONING,
        hl.AVG_BLOCKED,
        COALESCE(wm.TOTAL_CREDITS_USED, 0) AS TOTAL_CREDITS_USED,
        COALESCE(wm.COMPUTE_CREDITS_USED, 0) AS COMPUTE_CREDITS_USED,
        COALESCE(wm.CLOUD_SERVICES_CREDITS_USED, 0) AS CLOUD_SERVICES_CREDITS_USED,
        COALESCE(wm.QUERY_CREDITS_USED, 0) AS QUERY_CREDITS_USED,
        (COALESCE(wm.TOTAL_CREDITS_USED, 0) - COALESCE(wm.QUERY_CREDITS_USED, 0)) AS IDLE_CREDITS,
        COALESCE(qc.QUERY_COUNT, 0) AS QUERY_COUNT,
        COALESCE(qc.AVG_EXECUTION_TIME_SECONDS, 0) AS AVG_EXECUTION_TIME_SECONDS,
        COALESCE(qc.AVG_EXECUTION_TIME_MINUTES, 0) AS AVG_EXECUTION_TIME_MINUTES
    FROM 
        HOURLY_LOAD hl
    LEFT JOIN 
        WAREHOUSE_METERING wm
        ON hl.WAREHOUSE_ID = wm.WAREHOUSE_ID
        AND hl.HOUR_START = wm.HOUR_START
    LEFT JOIN 
        QUERY_COUNTS qc
        ON hl.WAREHOUSE_ID = qc.WAREHOUSE_ID
        AND hl.HOUR_START = qc.HOUR_START
    LEFT JOIN 
        TAG_AGG ta
        ON hl.WAREHOUSE_NAME = ta.WAREHOUSE_NAME
  ) AS src
  ON target.HOUR_START = src.HOUR_START
  AND target.WAREHOUSE_ID = src.WAREHOUSE_ID
  WHEN NOT MATCHED THEN
    INSERT (
      HOUR_START,
      WAREHOUSE_ID,
      WAREHOUSE_NAME,
      TAGS,
      AVG_RUNNING_LOAD,
      AVG_QUEUED_LOAD,
      AVG_QUEUED_PROVISIONING,
      AVG_BLOCKED,
      TOTAL_CREDITS_USED,
      COMPUTE_CREDITS_USED,
      CLOUD_SERVICES_CREDITS_USED,
      QUERY_CREDITS_USED,
      IDLE_CREDITS,
      QUERY_COUNT,
      AVG_EXECUTION_TIME_SECONDS,
      AVG_EXECUTION_TIME_MINUTES
    )
    VALUES (
      src.HOUR_START,
      src.WAREHOUSE_ID,
      src.WAREHOUSE_NAME,
      src.TAGS,
      src.AVG_RUNNING_LOAD,
      src.AVG_QUEUED_LOAD,
      src.AVG_QUEUED_PROVISIONING,
      src.AVG_BLOCKED,
      src.TOTAL_CREDITS_USED,
      src.COMPUTE_CREDITS_USED,
      src.CLOUD_SERVICES_CREDITS_USED,
      src.QUERY_CREDITS_USED,
      src.IDLE_CREDITS,
      src.QUERY_COUNT,
      src.AVG_EXECUTION_TIME_SECONDS,
      src.AVG_EXECUTION_TIME_MINUTES
    );
  RETURN 'OK';
END;
$$;
ALTER PROCEDURE FROSTFORECAST.SOURCE_DATA.SP_REFRESH_WAREHOUSE_USAGE() SET TAG FROSTFORECAST = 'FrostForecast Object';

-- SECTION: TASK SCHEDULING
------------------------------------------------
-- Schedule task to run stored procedure every 4 hours
------------------------------------------------
USE ROLE FROSTFORECAST_ADMIN;
CREATE OR REPLACE TASK FROSTFORECAST.SOURCE_DATA.REFRESH_WAREHOUSE_USAGE
  WAREHOUSE = FROSTFORECAST_ETL
  SCHEDULE = 'USING CRON 0 */4 * * * UTC'
AS
  CALL FROSTFORECAST.SOURCE_DATA.SP_REFRESH_WAREHOUSE_USAGE();
ALTER TASK FROSTFORECAST.SOURCE_DATA.REFRESH_WAREHOUSE_USAGE SET TAG FROSTFORECAST = 'FrostForecast Object';

-- SECTION: MATERIALIZED VIEW
------------------------------------------------
-- Create materialized view for optimized warehouse usage query performance
------------------------------------------------
USE ROLE FROSTFORECAST_ADMIN;
CREATE OR REPLACE MATERIALIZED VIEW FROSTFORECAST.SOURCE_DATA.MV_WAREHOUSE_USAGE
  CLUSTER BY (WAREHOUSE_NAME)
AS
SELECT *
FROM FROSTFORECAST.SOURCE_DATA.WAREHOUSE_USAGE;