/*
 * Overview of Snowflake Environment Setup Script
 * ---------------------------------------------
 * This script configures a Snowflake environment for the Visibility Book project. It performs the following tasks:
 * 1. Creates or replaces an admin role (FROSTFORECAST_ADMIN) with necessary privileges and assigns it to a user.
 * 2. Sets up two compute warehouses (FROSTFORECAST_ETL and FROSTFORECAST_APP) for ETL and app usage.
 * 3. Creates or replaces a database (FROSTFORECAST) and schema (SOURCE_DATA) for storing objects.
 * 4. Configures GitHub integration, including a secret for credentials, API integration, and a Git repository stage.
 * 5. Implements governance by creating or replacing a tag (FROSTFORECAST).
 * 6. Applies tags to relevant objects (warehouses, database, schema) for tracking.
 *
 * Use Command+F to search for sections using the following markers:
 * - SECTION: VARIABLES
 * - SECTION: ADMIN ROLE
 * - SECTION: COMPUTE
 * - SECTION: STORAGE
 * - SECTION: GITHUB INTEGRATION
 * - SECTION: GOVERNANCE TAGS
 * - SECTION: APPLY TAGS
 */

-- SECTION: VARIABLES
------------------------------------------------
-- Define sensitive credentials and derived values here for easy configuration
------------------------------------------------
SET GITHUB_USERNAME = '<USER NAME>';
SET GITHUB_TOKEN = '<TOKEN>';
SET SF_USER = '<SNOWFLAKE USER NAME>';
SET GITHUB_API_PREFIX = 'https://github.com/' || $GITHUB_USERNAME;
SET GITHUB_REPO_URL = 'https://github.com/' || $GITHUB_USERNAME || '/frost-forecast.git';

-- SECTION: ADMIN ROLE
------------------------------------------------
-- Create or replace and configure the FROSTFORECAST_ADMIN role
------------------------------------------------
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE ROLE FROSTFORECAST_ADMIN;
USE ROLE SYSADMIN;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE FROSTFORECAST_ADMIN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE FROSTFORECAST_ADMIN;
GRANT ROLE FROSTFORECAST_ADMIN TO USER IDENTIFIER($SF_USER);
USE ROLE FROSTFORECAST_ADMIN;

------------------------------------------------
-- SECTION: COMPUTE
------------------------------------------------
-- Create or replace compute warehouses for ETL and app processing
------------------------------------------------
CREATE OR REPLACE WAREHOUSE FROSTFORECAST_APP
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    INITIALLY_SUSPENDED = TRUE;
CREATE OR REPLACE WAREHOUSE FROSTFORECAST_ETL
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    INITIALLY_SUSPENDED = TRUE;

------------------------------------------------
-- SECTION: STORAGE
------------------------------------------------
-- Create or replace database and schema for storing objects
------------------------------------------------
CREATE OR REPLACE DATABASE FROSTFORECAST;
CREATE OR REPLACE SCHEMA FROSTFORECAST.SOURCE_DATA;

------------------------------------------------
-- SECTION: GITHUB INTEGRATION
------------------------------------------------
-- Configure GitHub integration for repository access
------------------------------------------------
-- Create or replace a secret to store GitHub credentials
USE ROLE ACCOUNTADMIN;
CREATE ROLE IF NOT EXISTS GITHUB_SECRET_ADMIN;
GRANT CREATE SECRET ON SCHEMA FROSTFORECAST.SOURCE_DATA TO ROLE GITHUB_SECRET_ADMIN;
GRANT ROLE GITHUB_SECRET_ADMIN TO ROLE FROSTFORECAST_ADMIN;
USE ROLE GITHUB_SECRET_ADMIN;

-- Use variables for GitHub credentials
CREATE OR REPLACE SECRET FROSTFORECAST.SOURCE_DATA.GITHUB_SECRET
    TYPE = PASSWORD
    USERNAME = $GITHUB_USERNAME
    PASSWORD = $GITHUB_TOKEN;

-- Grant USAGE on the secret to FROSTFORECAST_ADMIN
USE ROLE ACCOUNTADMIN;
GRANT USAGE ON SECRET FROSTFORECAST.SOURCE_DATA.GITHUB_SECRET TO ROLE FROSTFORECAST_ADMIN;

-- Recreate the API integration with ALLOWED_AUTHENTICATION_SECRETS
USE ROLE ACCOUNTADMIN;
CREATE ROLE IF NOT EXISTS GITHUB_API_ADMIN;
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE GITHUB_API_ADMIN;
GRANT ROLE GITHUB_API_ADMIN TO ROLE FROSTFORECAST_ADMIN;
CREATE API INTEGRATION IF NOT EXISTS GIT_API_INTEGRATION_FROST_FORECAST
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ($GITHUB_API_PREFIX)
  ALLOWED_AUTHENTICATION_SECRETS = (FROSTFORECAST.SOURCE_DATA.GITHUB_SECRET)
  ENABLED = TRUE;

-- Create or replace a Git repository stage
USE ROLE FROSTFORECAST_ADMIN;
CREATE GIT REPOSITORY IF NOT EXISTS FROSTFORECAST.SOURCE_DATA.FROSTFORECAST_GIT
    API_INTEGRATION = GIT_API_INTEGRATION_FROST_FORECAST
    GIT_CREDENTIALS = FROSTFORECAST.SOURCE_DATA.GITHUB_SECRET
    ORIGIN = $GITHUB_REPO_URL;

-- Verify the Git repository
SHOW GIT REPOSITORIES;

------------------------------------------------
-- SECTION: GOVERNANCE TAGS
------------------------------------------------
-- Create or replace tags for governance and tracking
------------------------------------------------
CREATE OR REPLACE TAG FROSTFORECAST
  ALLOWED_VALUES 'FrostForecast Pipeline', 'FrostForecast Object'
  COMMENT = 'Tag to categorize workloads for tracking FrostForecast credit consumption';

------------------------------------------------
-- SECTION: APPLY TAGS
------------------------------------------------
-- Apply tags to objects for governance
------------------------------------------------
ALTER WAREHOUSE FROSTFORECAST_ETL SET TAG FROSTFORECAST = 'FrostForecast Pipeline';
ALTER WAREHOUSE FROSTFORECAST_APP SET TAG FROSTFORECAST = 'FrostForecast Pipeline';
ALTER DATABASE FROSTFORECAST SET TAG FROSTFORECAST = 'FrostForecast Object';
ALTER SCHEMA FROSTFORECAST.SOURCE_DATA SET TAG FROSTFORECAST = 'FrostForecast Object';

------------------------------------------------
-- SECTION: STREAMLIT APP
------------------------------------------------
-- Deploy Streamlit app for visualizing consumption data
------------------------------------------------
USE ROLE FROSTFORECAST_ADMIN;
CREATE STREAMLIT "FROSTFORECAST: Consumption Prediction"
    MAIN_FILE = 'main.py'
    QUERY_WAREHOUSE = FROSTFORECAST_APP;