# FrostForecast: Snowflake Workload Consumption Pipeline

Welcome to **FrostForecast**, a Snowflake pipeline designed to help users quickly understand how workloads consume credits and predict future consumption over time. This tool supports analysis of current workloads, future workloads, and more. This README guides you through setting up the environment in your own Snowflake account by forking this repository and running the provided setup script.

## Prerequisites
Before you begin, ensure you have:
- A **Snowflake account** with `ACCOUNTADMIN` and `SYSADMIN` role access.
- A **GitHub account** to fork this repository.
- A **GitHub Personal Access Token**:
  - Go to GitHub > Settings > Developer settings > Personal access tokens > Tokens (classic).
  - Generate a new token with the `repo` scope.
  - Copy the token and store it securely (do not commit it to version control).
- A **Snowflake user** with an email address for configuration.

## Setup Instructions

### 1. Fork the Repository
- Click the **Fork** button at the top of this GitHub repository page (`https://github.com/sfc-gh-tgordonjr/frost-forecast`).
- This creates a copy of the repo in your GitHub account (e.g., `https://github.com/your-username/frost-forecast`).
- Note: If you prefer to use a custom repository name:
  - Create a new repo in your GitHub account (e.g., `my-frost-forecast`).
  - Clone this repo: `git clone https://github.com/sfc-gh-tgordonjr/frost-forecast.git`
  - Change the remote: `git remote set-url origin https://github.com/your-username/my-frost-forecast.git`
  - Push to your repo: `git push origin main`

### 2. Update the Setup Script
- Open the `setup.sql` file in this repository.
- Modify the following variables at the top of the script:
  - `SET GITHUB_USERNAME = 'your-username';`  
    Replace `'your-username'` with your GitHub username.
  - `SET GITHUB_TOKEN = 'your-token';`  
    Replace `'your-token'` with your GitHub Personal Access Token (keep this secure!).
  - `SET SF_USER = 'your-snowflake-user';`  
    Replace `'your-snowflake-user'` with your Snowflake username (e.g., `JSMITH`).
  - If you renamed the repository:
    - Update `SET GITHUB_REPO_URL = 'https://github.com/' || $GITHUB_USERNAME || '/your-repo-name.git';`  
      Replace `/your-repo-name.git` with your custom repo name (e.g., `/my-frost-forecast.git`).
- Save the changes.

### 3. Run the Setup Script in Snowflake
- Log in to your Snowflake account via the web UI or SnowSQL.
- Ensure you have `ACCOUNTADMIN` privileges initially (for role and integration setup).
- Open a worksheet or SnowSQL session.
- Copy and paste the contents of `setup.sql`.
- Execute the script step-by-step or all at once.
- What the script does:
  - Creates a `FROSTFORECAST_ADMIN` role and assigns it to your user.
  - Sets up two warehouses: `FROSTFORECAST_ETL` (for ETL tasks) and `FROSTFORECAST_APP` (for app usage).
  - Creates a `FROSTFORECAST` database and `SOURCE_DATA` schema.
  - Configures GitHub integration to connect to your forked repo.
  - Applies governance tags for tracking.
  - Deploys a Streamlit app (`FROSTFORECAST: Consumption Prediction`) to visualize consumption data.

### 4. Verify the Setup
- **Check Git Integration**:
  - Run: `SHOW GIT REPOSITORIES;`
  - Confirm the `FROSTFORECAST.SOURCE_DATA.FROSTFORECAST_GIT` repository appears.
  - Test access: `ls @FROSTFORECAST.SOURCE_DATA.FROSTFORECAST_GIT;`
    This lists files in your forked repo.
- **Check Warehouses and Database**:
  - Run: `SHOW WAREHOUSES;`
    Confirm `FROSTFORECAST_ETL` and `FROSTFORECAST_APP` exist.
  - Run: `SHOW DATABASES;`
    Confirm `FROSTFORECAST` exists.
- **Check Streamlit App**:
  - Run: `SHOW STREAMLITS;`
  - Find `FROSTFORECAST: Consumption Prediction` and access it via the Snowflake UI to view consumption data.

### 5. Security Notes
- **GitHub Token**: Never commit your `GITHUB_TOKEN` to version control. The script stores it securely in a Snowflake secret.
- **Privileges**: The `FROSTFORECAST_ADMIN` role is granted necessary permissions. Review grants if sharing with others.
- **Cleanup**: If needed, drop objects with `DROP DATABASE FROSTFORECAST;`, `DROP WAREHOUSE FROSTFORECAST_ETL;`, etc., using `ACCOUNTADMIN`.

## Usage
- Use the `FROSTFORECAST: Consumption Prediction` Streamlit app in Snowflake to monitor workload credit consumption.
- The app queries data from the `FROSTFORECAST` database and uses the `FROSTFORECAST_APP` warehouse.
- It provides predictive insights into credit usage for current and future workloads based on historical patterns.

## Troubleshooting
- **Git Integration Fails**: Verify your `GITHUB_USERNAME`, `GITHUB_TOKEN`, and `GITHUB_REPO_URL`. Ensure your token has `repo` scope and the repo is accessible (public or private with access).
- **Permission Errors**: Confirm you have `ACCOUNTADMIN` and `SYSADMIN` roles for initial setup. Check grants in `setup.sql`.
- **Streamlit Issues**: Ensure `main.py` exists in your forked repo and the `FROSTFORECAST_APP` warehouse is active.

## Contributing
- Found a bug or have a feature idea? Open an issue in this repo.
- Want to sync updates? Pull changes from the original repo (`sfc-gh-tgordonjr/frost-forecast`) via GitHub’s sync feature.

## Contact
For help, reach out to the repository owner or your Snowflake administrator.

Happy forecasting with FrostForecast!