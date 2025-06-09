"""
FrostForecast Consumption Dashboard

This Streamlit application provides a dashboard to monitor Snowflake resource consumption
for the FrostForecast workload. It allows users to filter data by date range, pricing, and
specific resources, view consumption details, save filter settings, and predict future costs.

Code Structure:
- Imports and Setup: Libraries, Streamlit configuration, and Snowflake session initialization.  # imports-and-setup
- Global Config: Constants like CACHE_TTL, plot dimensions.  # global-config
- Helper Functions: Data fetching, formatting, and utility functions.  # helper-functions
  - fetch_materialized_view_data  # fetch-materialized-view-data
  - fetch_warehouse_tags  # fetch-warehouse-tags
  - format_bytes  # format-bytes
  - format_number  # format-number
- UI Component Functions: Functions for rendering sidebar, tabs, and consumption sections.  # ui-component-functions
  - setup_snowflake  # setup-snowflake
  - initialize_session_state  # initialize-session-state
  - render_sidebar  # render-sidebar
  - render_welcome_tab  # render-welcome-tab
  - render_resource_submission_tab  # render-resource-submission-tab
  - render_consumption_tab  # render-consumption-tab
  - render_predictions_tab  # render-predictions-tab
- Main Function: Orchestrates the application flow.  # main-function

Table of Contents (Ctrl+F to search):
1. Imports and Setup                    # imports-and-setup
2. Global Config                        # global-config
3. Helper Functions                     # helper-functions
   - fetch_materialized_view_data       # fetch-materialized-view-data
   - fetch_warehouse_tags               # fetch-warehouse-tags
   - format_bytes                       # format-bytes
   - format_number                      # format-number
4. UI Component Functions               # ui-component-functions
   - setup_snowflake                    # setup-snowflake
   - initialize_session_state           # initialize-session-state
   - render_sidebar                     # render-sidebar
   - render_welcome_tab                 # render-welcome-tab
   - render_resource_submission_tab     # render-resource-submission-tab
   - render_consumption_tab             # render-consumption-tab
   - render_predictions_tab             # render-predictions-tab
5. Main Function                        # main-function

Note: To style plots (e.g., color, radius), search for 'render-consumption-tab' where Altair charts are defined.
      Look for mark_line(), mark_bar(), and mark_arc() calls to adjust properties like color, strokeWidth, etc.
"""

# imports-and-setup
import streamlit as st
import datetime
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, lit
import pandas as pd
import json
import altair as alt

st.set_page_config(
    page_title="FrostForecast Consumption",
    layout="wide",
    initial_sidebar_state="expanded"
)

# global-config
CACHE_TTL = 600  # seconds
PLOT_WIDTH = 600  # pixels, global width for all plots
PLOT_HEIGHT = 400  # pixels, global height for all plots

# Custom CSS for metric tiles with darker drop shadow
st.markdown("""
<style>
/* Target Streamlit metric tiles */
div[data-testid="stMetric"] {
    transition: all 0.3s ease;
    border-radius: 8px;
    padding: 10px;
    box-shadow: 0 4px 8px rgba(0, 0, 0, 0.2);
}

/* Hover effect: slight scale and enhanced shadow */
div[data-testid="stMetric"]:hover {
    transform: scale(1.05),
    box-shadow: 0 6px 12px rgba(0, 0, 0, 0.25);
    background-color: #f8f9fa;
}
</style>
""", unsafe_allow_html=True)

# helper-functions
# fetch-materialized-view-data
@st.cache_data(ttl=CACHE_TTL)
def fetch_materialized_view_data(_session, view_name, filter_dict=None, start_ts=None, end_ts=None):
    """Fetch data from a materialized view with optional filters."""
    table = _session.table(f"FROSTFORECAST.SOURCE_DATA.{view_name}")
    
    # Check if TAGS column exists in the view
    columns = [col.name.upper() for col in table.schema.fields]
    has_tags_column = "TAGS" in columns
    
    if filter_dict:
        # Apply resource-specific filters, but only fetch data if the filter input is non-empty
        if view_name == "MV_WAREHOUSE_USAGE":
            if not filter_dict.get("warehouses"):  # Skip if no warehouses provided
                return pd.DataFrame()  # Return empty DataFrame
            warehouses = [w.strip() for w in filter_dict["warehouses"].split(",") if w.strip()]
            table = table.filter(col("WAREHOUSE_NAME").isin(warehouses))
        elif view_name == "MV_PIPE_USAGE":
            if not filter_dict.get("pipes"):  # Skip if no pipes provided
                return pd.DataFrame()
            pipes = [p.strip() for p in filter_dict["pipes"].split(",") if p.strip()]
            table = table.filter(col("PIPE_NAME").isin(pipes))
        elif view_name == "MV_SERVERLESS_TASK_USAGE":
            if not filter_dict.get("serverless_tasks"):  # Skip if no serverless tasks provided
                return pd.DataFrame()
            tasks = [t.strip() for t in filter_dict["serverless_tasks"].split(",") if t.strip()]
            table = table.filter(col("TASK_NAME").isin(tasks))
        elif view_name == "MV_SPCS_USAGE":
            if not filter_dict.get("compute_pools"):  # Skip if no compute pools provided
                return pd.DataFrame()
            pools = [p.strip() for p in filter_dict["compute_pools"].split(",") if p.strip()]
            if pools:  # Only apply filter if compute_pools list is not empty
                table = table.filter(col("COMPUTE_POOL_NAME").isin(pools))  # Direct match from form input
    
        # Filter by tag_value in the TAGS VARIANT column, only if TAGS exists and not for MV_SPCS_USAGE or MV_CORTEX_FUNCTION_CREDIT_USAGE
        if has_tags_column and filter_dict.get("tags") and view_name not in ["MV_SPCS_USAGE"]:
            tags = [t.strip() for t in filter_dict["tags"].split(",") if t.strip()]
            if tags:
                # Use FLATTEN to filter rows where at least one tag_value matches, and preserve only matching tags
                resource_col = table.columns[1]  # Second column (e.g., WAREHOUSE_NAME, PIPE_NAME)
                flattened_tags = _session.sql(f"""
                    SELECT t."HOUR_START" AS tag_hour_start, t."{resource_col}" AS resource_name,
                           ARRAY_CONSTRUCT_COMPACT(OBJECT_CONSTRUCT('tag_name', f.value:tag_name, 'tag_value', f.value:tag_value))
                           AS filtered_tags
                    FROM FROSTFORECAST.SOURCE_DATA.{view_name} t,
                    LATERAL FLATTEN(input => t."TAGS") f
                    WHERE GET_PATH(f.value, 'tag_value') IN ({', '.join([f"'{tag}'" for tag in tags])})
                """)
                # Join with the original table and replace the TAGS column with filtered_tags
                table = table.join(
                    flattened_tags,
                    (table["HOUR_START"] == flattened_tags["tag_hour_start"]) & 
                    (table[resource_col] == flattened_tags["resource_name"]),
                    how="left"
                ).drop("TAGS").select(
                    [col(c) for c in table.columns if c != "TAGS"] + [flattened_tags["filtered_tags"].alias("TAGS")]
                )

    if start_ts and end_ts:
        if view_name in ["MV_WAREHOUSE_USAGE", "MV_PIPE_USAGE", "MV_SERVERLESS_TASK_USAGE", "MV_SPCS_USAGE"]:
            table = table.filter((col("HOUR_START") >= lit(start_ts)) & (col("HOUR_START") <= lit(end_ts)))
        elif view_name in ["MV_CORTEX_FUNCTION_CREDIT_USAGE"]:
            table = table.filter((col("HOUR_START") >= lit(start_ts)) & (col("HOUR_START") <= lit(end_ts)))
    
    # Debugging: Log the query for all views
    queries = table.queries
    st.session_state[f"debug_{view_name.lower()}_query"] = queries.get("queries", ["No query available"])[0] if queries else "No query available"
    
    df = table.to_pandas()
    df.columns = [c.lower() for c in df.columns]
    return df
    
# fetch-warehouse-tags
@st.cache_data(ttl=CACHE_TTL)
def fetch_warehouse_tags(_session):
    """Fetch distinct tags from the warehouse usage table."""
    tags = _session.table("FROSTFORECAST.SOURCE_DATA.MV_WAREHOUSE_USAGE")\
        .select(col("TAGS")).distinct().to_pandas()
    tags_list = []
    for tags_variant in tags["TAGS"]:
        if tags_variant:
            tags_json = json.loads(tags_variant)
            for tag in tags_json:
                tag_name = tag.get("tag_name")
                if tag_name and tag_name not in tags_list:
                    tags_list.append(tag_name)
    return [""] + sorted(tags_list)

# format-bytes
def format_bytes(bytes_value):
    """Convert bytes to MB, GB, or TB based on size."""
    if bytes_value < 1024**3:  # Less than 1 GB
        return f"{round(bytes_value / 1024**2, 2)} MB"
    elif bytes_value < 1024**4:  # Less than 1 TB
        return f"{round(bytes_value / 1024**3, 2)} GB"
    else:
        return f"{round(bytes_value / 1024**4, 2)} TB"

# format-number
def format_number(value):
    """Format numbers with commas and scale to k/M/B/T."""
    if value >= 1_000_000_000_000:  # Trillions
        return f"{round(value / 1_000_000_000_000, 1):,g}T"
    elif value >= 1_000_000_000:  # Billions
        return f"{round(value / 1_000_000_000, 1):,g}B"
    elif value >= 1_000_000:  # Millions
        return f"{round(value / 1_000_000, 1):,g}M"
    elif value >= 1_000:  # Thousands
        return f"{round(value / 1_000, 1):,g}k"
    else:
        return f"{int(value):,d}"

# ui-component-functions
# setup-snowflake
def setup_snowflake():
    """Initialize Snowflake session and create necessary database objects."""
    session = get_active_session()
    session.sql("CREATE DATABASE IF NOT EXISTS FROSTFORECAST").collect()
    session.sql("CREATE SCHEMA IF NOT EXISTS FROSTFORECAST.APP_USAGE").collect()
    session.sql("""
    CREATE TABLE IF NOT EXISTS FROSTFORECAST.APP_USAGE.FILTER_SETTINGS (
        FILTER_ID VARCHAR,
        FILTER_DATA VARIANT
    )
    """).collect()
    return session

# initialize-session-state
def initialize_session_state():
    """Initialize session state variables with default values."""
    defaults = {
        "start_date": datetime.date.today() - datetime.timedelta(days=7),
        "end_date": datetime.date.today(),
        "price_per_credit": 3.0,
        "filter_submitted": False,
        "filter_data": None,
        "selected_tag": None,
        "selected_filter": "",
        "warehouse_input": "",
        "tags_input": "",
        "pipe_input": "",
        "serverless_task_input": "",
        "compute_pool_input": "",
        "prediction_data": None,
        "growth_rates": {
            "warehouse": 0.5,
            "compute_pool": 0.5,
            "pipe": 0.5,
            "serverless_task": 0.5,
            "cortex_function": 0.5
        }
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

# render-sidebar
def render_sidebar():
    """Render the sidebar with basic filters."""
    st.sidebar.header("Filters")
    st.sidebar.markdown("Adjust settings in the Resource Submission tab to filter data.")

# render-welcome-tab
def render_welcome_tab():
    """Render the Welcome tab with instructions."""
    with st.container():
        st.header("Welcome to the Snowflake FrostForecast Consumption Dashboard")
        st.markdown("""
        ### Purpose
        This app tracks Snowflake resource consumption for your workloads, providing clear cost insights for current and future workloads.

        ### Monitored Resources
        The app monitors the following resource types:
        - **Warehouses** – Compute engines for queries
        - **Databases** – Data repositories
        - **Schemas** – Organizational folders within databases
        - **Objects** – Tables, views, or other database objects
        - **Pipes** – Automated data loaders (Snowpipes)
        - **Serverless Tasks** – Automated tasks running serverless compute
        - **Compute Pools** – Snowpark Container Services compute pools

        ### How to Use
        1. **Configure Filters (Consumption Tab)**
           - Set the **Date Range** for your workload analysis.
           - Adjust **Pricing** for credits (default: $3.00/credit).

        2. **Submit Resources**
           - Go to the **Resource Submission** tab.
           - Enter comma-separated names for the resources used in your workload.
           - Select a warehouse tag and submit to filter data.
           - Submit resource settings to save, or use the "Query All Data" button to view unfiltered data.

        3. **View Consumption**
           - In the **Consumption** tab, see tables summarizing usage for each resource type, filtered by your submitted resources, tag, and date range.

        4. **Generate Predictions**
           - At the bottom of the **Consumption** tab, click "Generate Prediction" to forecast costs.
           - In the **Predictions** tab, adjust growth rates and view projected costs for 30, 60, 90, and 120 days.

        ### Getting Started
        Head to the **Resource Submission** tab to input your workload resources or query all available data.
        """)
        
# render-resource-submission-tab
def render_resource_submission_tab(session):
    """Render the Resource Submission tab for inputting and saving filters."""
    with st.container():
        st.header("Resource Submission")
        
        if st.button("Query All Data"):
            st.session_state["filter_submitted"] = True
            st.session_state["filter_data"] = None
            st.session_state["selected_tag"] = None
            st.session_state["selected_filter"] = ""
            st.success("Querying all available data from materialized views.")
        st.markdown("---")

        # Create two columns for filter selection and tag selection
        col1, col2 = st.columns(2)

        with col1:
            saved_filters = session.sql("SELECT FILTER_ID FROM FROSTFORECAST.APP_USAGE.FILTER_SETTINGS").to_pandas()
            filter_ids = saved_filters["FILTER_ID"].tolist()
            selected_filter = st.selectbox(
                "Select a saved filter setting",
                [""] + filter_ids,
                index=filter_ids.index(st.session_state["selected_filter"]) + 1 if st.session_state["selected_filter"] in filter_ids else 0,
                key="filter_select"
            )
            if selected_filter != st.session_state["selected_filter"]:
                st.session_state["selected_filter"] = selected_filter
                # Load filter data immediately upon selection
                if selected_filter:
                    filter_data = session.sql(f"SELECT FILTER_DATA FROM FROSTFORECAST.APP_USAGE.FILTER_SETTINGS WHERE FILTER_ID = '{selected_filter}'").to_pandas()
                    if not filter_data.empty:
                        filter_dict = json.loads(filter_data.iloc[0]["FILTER_DATA"])
                        st.session_state["warehouse_input"] = filter_dict.get("warehouses", "")
                        st.session_state["tags_input"] = filter_dict.get("tags", "")
                        st.session_state["pipe_input"] = filter_dict.get("pipes", "")
                        st.session_state["serverless_task_input"] = filter_dict.get("serverless_tasks", "")
                        st.session_state["compute_pool_input"] = filter_dict.get("compute_pools", "")
                        st.session_state["selected_tag"] = filter_dict.get("selected_tag", None)
                        st.session_state["filter_id_input"] = selected_filter
                        st.session_state["last_loaded_filter"] = selected_filter

        with col2:
            warehouse_tags = fetch_warehouse_tags(session)
            selected_tag = st.selectbox(
                "Select a warehouse tag",
                warehouse_tags,
                index=warehouse_tags.index(st.session_state["selected_tag"]) if st.session_state["selected_tag"] in warehouse_tags else 0,
                key="warehouse_tag_input"
            )
            if st.button("Submit Tag"):
                st.session_state["selected_tag"] = selected_tag if selected_tag else None
                st.session_state["filter_submitted"] = True
                st.success(f"Filtering data by tag: {selected_tag}" if selected_tag else "Cleared tag filter.")

        st.markdown("---")

        # Form spans the full width of the container
        with st.form("resource_submission_form"):
            filter_id = st.text_input("Filter Setting Name", value=st.session_state.get("filter_id_input", ""), key="filter_id_input")
            st.markdown("---")
            warehouses = st.text_input("Warehouses Used (comma-separated)", value=st.session_state.get("warehouse_input", ""), key="warehouse_input")
            st.markdown("---")
            tags = st.text_input("Tag Values Used (comma-separated)", value=st.session_state.get("tags_input", ""), key="tags_input")
            st.markdown("---")
            pipes = st.text_input("Pipes Used (comma-separated)", value=st.session_state.get("pipe_input", ""), key="pipe_input")
            st.markdown("---")
            serverless_tasks = st.text_input("Serverless Tasks Used (comma-separated)", value=st.session_state.get("serverless_task_input", ""), key="serverless_task_input")
            st.markdown("---")
            compute_pools = st.text_input("Compute Pools Used (comma-separated)", value=st.session_state.get("compute_pool_input", ""), key="compute_pool_input")
            st.markdown("---")
            submit_button = st.form_submit_button("Submit")

            if submit_button and filter_id:
                filter_data = {
                    "warehouses": warehouses,
                    "tags": tags,
                    "pipes": pipes,
                    "serverless_tasks": serverless_tasks,
                    "compute_pools": compute_pools,
                    "selected_tag": st.session_state.get("selected_tag")
                }
                filter_data_json = json.dumps(filter_data)
                session.sql(f"""
                MERGE INTO FROSTFORECAST.APP_USAGE.FILTER_SETTINGS AS target
                USING (SELECT '{filter_id}' AS FILTER_ID, PARSE_JSON('{filter_data_json}') AS FILTER_DATA) AS source
                ON target.FILTER_ID = source.FILTER_ID
                WHEN MATCHED THEN
                    UPDATE SET FILTER_DATA = source.FILTER_DATA
                WHEN NOT MATCHED THEN
                    INSERT (FILTER_ID, FILTER_DATA)
                    VALUES (source.FILTER_ID, source.FILTER_DATA)
                """).collect()
                st.session_state["filter_submitted"] = True
                st.session_state["filter_data"] = filter_data
                st.session_state["selected_filter"] = filter_id
                st.session_state["last_loaded_filter"] = filter_id

# render-consumption-tab
def render_consumption_tab(session, start_ts, end_ts, price_per_credit):
    import altair as alt

    # Custom CSS for styled metric tiles with hover effect
    st.markdown("""
    <style>
    .metric-tile {
        background-color: #f8f9fa;
        border-radius: 8px;
        padding: 15px;
        margin: 5px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        transition: all 0.3s ease;
        text-align: center;
    }
    .metric-tile:hover {
        transform: translateY(-5px);
        box-shadow: 0 8px 12px rgba(0,0,0,0.2);
    }
    .metric-title {
        font-size: 1.1em;
        font-weight: bold;
        margin-bottom: 8px;
    }
    .metric-value {
        font-size: 1.3em;
        color: #2c3e50;
    }
    </style>
    """, unsafe_allow_html=True)

    st.markdown("""
        <style>
        div[role="listbox"] {
            width: 300px !important;
        }
        </style>
    """, unsafe_allow_html=True)

    with st.container():
        st.header("FrostForecast Consumption")
        col1, col2 = st.columns(2)
        with col1:
            start_date, end_date = st.date_input(
                "Date Range",
                value=[st.session_state["start_date"], st.session_state["end_date"]],
                min_value=datetime.date(2000, 1, 1),
                max_value=datetime.date.today()
            )
            if start_date != st.session_state["start_date"] or end_date != st.session_state["end_date"]:
                st.session_state["start_date"] = start_date
                st.session_state["end_date"] = end_date
            start_ts = datetime.datetime.combine(start_date, datetime.time.min)
            end_ts = datetime.datetime.combine(end_date, datetime.time.max)
        with col2:
            price_per_credit = st.number_input(
                "Price per credit ($)",
                min_value=0.0,
                value=st.session_state["price_per_credit"],
                step=0.01,
                key="price_input"
            )
            if price_per_credit != st.session_state["price_per_credit"]:
                st.session_state["price_per_credit"] = price_per_credit
        st.markdown("---")

        if not st.session_state["filter_submitted"]:
            st.warning("Please submit resources or a tag in the Resource Submission tab to view filtered data, or use the 'Query All Data' button to view unfiltered data.")
        else:
            filter_dict = st.session_state.get("filter_data")
            selected_tag = st.session_state.get("selected_tag")
            tag_message = f"Data filtered by tag: {selected_tag}" if selected_tag else "No tag filter applied."

            wh_df = fetch_materialized_view_data(session, "MV_WAREHOUSE_USAGE", filter_dict, start_ts, end_ts)
            spcs_df = fetch_materialized_view_data(session, "MV_SPCS_USAGE", filter_dict, start_ts, end_ts)
            pipe_df = fetch_materialized_view_data(session, "MV_PIPE_USAGE", filter_dict, start_ts, end_ts)
            task_df = fetch_materialized_view_data(session, "MV_SERVERLESS_TASK_USAGE", filter_dict, start_ts, end_ts)
            cortex_func_df = fetch_materialized_view_data(session, "MV_CORTEX_FUNCTION_CREDIT_USAGE", filter_dict, start_ts, end_ts)

            total_credits = (
                (wh_df["total_credits_used"].sum() if "total_credits_used" in wh_df.columns and not wh_df.empty else 0) +
                (spcs_df["total_credits_used"].sum() if "total_credits_used" in spcs_df.columns and not spcs_df.empty else 0) +
                (pipe_df["total_credits_used"].sum() if "total_credits_used" in pipe_df.columns and not pipe_df.empty else 0) +
                (task_df["total_credits_used"].sum() if "total_credits_used" in task_df.columns and not task_df.empty else 0) +
                (cortex_func_df["cf_total_token_credits"].sum() if "cf_total_token_credits" in cortex_func_df.columns and not cortex_func_df.empty else 0)
            )
            total_cost = round(total_credits * price_per_credit, 2)

            # Add Total Usage title and tiles in two columns
            st.subheader("Total Usage")
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(f"""
                <div class="metric-tile">
                    <div class="metric-title">Total Credits Consumed</div>
                    <div class="metric-value">{total_credits:,.2f}</div>
                </div>
                """, unsafe_allow_html=True)
            with col2:
                st.markdown(f"""
                <div class="metric-tile">
                    <div class="metric-title">Total Cost ($)</div>
                    <div class="metric-value">${total_cost:,.2f}</div>
                </div>
                """, unsafe_allow_html=True)
            st.markdown("---")

            if not wh_df.empty:
                st.subheader("Warehouse Usage")
                cols = st.columns(4)
                total_credits_wh = wh_df["total_credits_used"].sum() if "total_credits_used" in wh_df.columns else 0
                with cols[0]:
                    st.markdown(f"""
                    <div class="metric-tile">
                        <div class="metric-title">Total Cost ($)</div>
                        <div class="metric-value">${total_credits_wh * price_per_credit:,.2f}</div>
                    </div>
                    """, unsafe_allow_html=True)
                with cols[1]:
                    st.markdown(f"""
                    <div class="metric-tile">
                        <div class="metric-title">Total Credits</div>
                        <div class="metric-value">{total_credits_wh:,.2f}</div>
                    </div>
                    """, unsafe_allow_html=True)
                with cols[2]:
                    st.markdown(f"""
                    <div class="metric-tile">
                        <div class="metric-title">Compute Credits</div>
                        <div class="metric-value">{round(wh_df['compute_credits_used'].sum(), 2):,}</div>
                    </div>
                    """, unsafe_allow_html=True)
                with cols[3]:
                    st.markdown(f"""
                    <div class="metric-tile">
                        <div class="metric-title">Cloud Services Credits</div>
                        <div class="metric-value">{round(wh_df['cloud_services_credits_used'].sum(), 2):,}</div>
                    </div>
                    """, unsafe_allow_html=True)
                with st.expander("Warehouse Usage (Data Available)"):
                    st.dataframe(wh_df, use_container_width=True)

                wh_df['hour_start'] = pd.to_datetime(wh_df['hour_start'])
                daily_df = wh_df.groupby(pd.Grouper(key='hour_start', freq='D')).agg({'total_credits_used': 'sum'}).reset_index()

                credits_chart = alt.Chart(daily_df).mark_area(
                    line={'color': '#1565C0'},
                    color=alt.Gradient(
                        gradient='linear',
                        stops=[
                            alt.GradientStop(color='#E3F2FD', offset=0),
                            alt.GradientStop(color='#1565C0', offset=1)
                        ],
                        x1=1,
                        x2=1,
                        y1=1,
                        y2=0
                    )
                ).encode(
                    x=alt.X('yearmonthdate(hour_start):T', title='Date', axis=alt.Axis(format='%Y-%m-%d')),
                    y=alt.Y('total_credits_used:Q', title='Total Credits Used', axis=alt.Axis(format='.2f')),
                    tooltip=[
                        alt.Tooltip('yearmonthdate(hour_start):T', title='Date', format='%Y-%m-%d'),
                        alt.Tooltip('total_credits_used:Q', title='Total Credits', format='.2f')
                    ]
                ).properties(
                    title='Total Credits Used Per Day',
                    width=PLOT_WIDTH,
                    height=PLOT_HEIGHT
                )
                st.altair_chart(credits_chart, use_container_width=True)
                st.markdown("---")
                
            if not spcs_df.empty:
                st.subheader("Compute Pool Usage")
                cols = st.columns(3)
                total_credits_spcs = spcs_df["total_credits_used"].sum() if "total_credits_used" in spcs_df.columns else 0
                with cols[0]:
                    st.markdown(f"""
                    <div class="metric-tile">
                        <div class="metric-title">Total Cost ($)</div>
                        <div class="metric-value">${total_credits_spcs * price_per_credit:,.2f}</div>
                    </div>
                    """, unsafe_allow_html=True)
                with cols[1]:
                    st.markdown(f"""
                    <div class="metric-tile">
                        <div class="metric-title">Distinct Pools</div>
                        <div class="metric-value">{len(spcs_df['compute_pool_name'].unique()) if 'compute_pool_name' in spcs_df.columns else 0}</div>
                    </div>
                    """, unsafe_allow_html=True)
                with cols[2]:
                    st.markdown(f"""
                    <div class="metric-tile">
                        <div class="metric-title">Total Credits</div>
                        <div class="metric-value">{total_credits_spcs:,.2f}</div>
                    </div>
                    """, unsafe_allow_html=True)
                with st.expander("Compute Pool Usage (Data Available)"):
                    st.dataframe(spcs_df, use_container_width=True)
            
                spcs_df['hour_start'] = pd.to_datetime(spcs_df['hour_start'])
                daily_spcs_df = spcs_df.groupby([pd.Grouper(key='hour_start', freq='D'), 'compute_pool_name']).agg({'total_credits_used': 'sum'}).reset_index()
            
                bar_chart = alt.Chart(daily_spcs_df).mark_bar(cornerRadius=5).encode(
                    x=alt.X('yearmonthdate(hour_start):T', title='Date', axis=alt.Axis(format='%Y-%m-%d', labelAngle=0)),
                    y=alt.Y('total_credits_used:Q', title='Total Credits Used', axis=alt.Axis(format='.2f')),
                    color=alt.Color('compute_pool_name:N', legend=alt.Legend(title='Compute Pool'), scale=alt.Scale(scheme='magma')),
                    tooltip=[
                        alt.Tooltip('yearmonthdate(hour_start):T', title='Date', format='%Y-%m-%d'),
                        alt.Tooltip('compute_pool_name:N', title='Compute Pool'),
                        alt.Tooltip('total_credits_used:Q', title='Total Credits', format='.2f')
                    ]
                ).properties(
                    title='Total Credits Used Per Day by Compute Pool',
                    width=PLOT_WIDTH,
                    height=PLOT_HEIGHT
                ).configure_view(
                    strokeWidth=0
                )
                st.altair_chart(bar_chart, use_container_width=True)
                st.markdown("---")
            
            if not pipe_df.empty:
                st.subheader("Pipe Usage")
                cols = st.columns(4)
                total_credits_pipe = pipe_df["total_credits_used"].sum() if "total_credits_used" in pipe_df.columns else 0
                with cols[0]:
                    st.markdown(f"""
                    <div class="metric-tile">
                        <div class="metric-title">Total Cost ($)</div>
                        <div class="metric-value">${total_credits_pipe * price_per_credit:,.2f}</div>
                    </div>
                    """, unsafe_allow_html=True)
                with cols[1]:
                    st.markdown(f"""
                    <div class="metric-tile">
                        <div class="metric-title">Distinct Pipes</div>
                        <div class="metric-value">{len(pipe_df['pipe_name'].unique()) if 'pipe_name' in pipe_df.columns else 0}</div>
                    </div>
                    """, unsafe_allow_html=True)
                with cols[2]:
                    st.markdown(f"""
                    <div class="metric-tile">
                        <div class="metric-title">Total Credits</div>
                        <div class="metric-value">{total_credits_pipe:,.2f}</div>
                    </div>
                    """, unsafe_allow_html=True)
                with cols[3]:
                    st.markdown(f"""
                    <div class="metric-tile">
                        <div class="metric-title">Total Data Inserted</div>
                        <div class="metric-value">{format_bytes(pipe_df['total_bytes_inserted'].sum()) if 'total_bytes_inserted' in pipe_df.columns else '0 MB'}</div>
                    </div>
                    """, unsafe_allow_html=True)
                with st.expander("Pipe Usage (Data Available)"):
                    st.dataframe(pipe_df, use_container_width=True)
            
                pipe_df['hour_start'] = pd.to_datetime(pipe_df['hour_start'])
                daily_pipe_df = pipe_df.groupby([pd.Grouper(key='hour_start', freq='D'), 'pipe_name']).agg({
                    'total_credits_used': 'sum',
                    'total_bytes_inserted': 'sum'
                }).reset_index()
            
                daily_pipe_df['formatted_bytes'] = daily_pipe_df['total_bytes_inserted'].apply(format_bytes)
            
                bar_chart = alt.Chart(daily_pipe_df).mark_bar(cornerRadius=5).encode(
                    x=alt.X('yearmonthdate(hour_start):T', title='Date', axis=alt.Axis(format='%Y-%m-%d', labelAngle=0)),
                    y=alt.Y('total_credits_used:Q', title='Total Credits Used', axis=alt.Axis(format='.2f')),
                    color=alt.Color('pipe_name:N', legend=alt.Legend(title='Pipe'), scale=alt.Scale(scheme='magma')),
                    tooltip=[
                        alt.Tooltip('yearmonthdate(hour_start):T', title='Date', format='%Y-%m-%d'),
                        alt.Tooltip('pipe_name:N', title='Pipe'),
                        alt.Tooltip('total_credits_used:Q', title='Total Credits', format='.2f'),
                        alt.Tooltip('formatted_bytes:N', title='Data Inserted')
                    ]
                ).properties(
                    title='Total Credits Used Per Day by Pipe',
                    width=PLOT_WIDTH,
                    height=PLOT_HEIGHT
                ).configure_view(
                    strokeWidth=0
                )
                st.altair_chart(bar_chart, use_container_width=True)
                st.markdown("---")
            
            if not task_df.empty:
                st.subheader("Serverless Task Usage")
                cols = st.columns(3)
                total_credits_task = task_df["total_credits_used"].sum() if "total_credits_used" in task_df.columns else 0
                with cols[0]:
                    st.markdown(f"""
                    <div class="metric-tile">
                        <div class="metric-title">Total Cost ($)</div>
                        <div class="metric-value">${total_credits_task * price_per_credit:,.2f}</div>
                    </div>
                    """, unsafe_allow_html=True)
                with cols[1]:
                    st.markdown(f"""
                    <div class="metric-tile">
                        <div class="metric-title">Distinct Tasks</div>
                        <div class="metric-value">{len(task_df['task_name'].unique()) if 'task_name' in task_df.columns else 0}</div>
                    </div>
                    """, unsafe_allow_html=True)
                with cols[2]:
                    st.markdown(f"""
                    <div class="metric-tile">
                        <div class="metric-title">Total Credits</div>
                        <div class="metric-value">{total_credits_task:,.2f}</div>
                    </div>
                    """, unsafe_allow_html=True)
                with st.expander("Serverless Task Usage (Data Available)"):
                    st.dataframe(task_df, use_container_width=True)
            
                task_df['hour_start'] = pd.to_datetime(task_df['hour_start'])
                daily_task_df = task_df.groupby([pd.Grouper(key='hour_start', freq='D'), 'task_name']).agg({
                    'total_credits_used': 'sum'
                }).reset_index()
            
                bar_chart = alt.Chart(daily_task_df).mark_bar(cornerRadius=5).encode(
                    x=alt.X('yearmonthdate(hour_start):T', title='Date', axis=alt.Axis(format='%Y-%m-%d', labelAngle=0)),
                    y=alt.Y('total_credits_used:Q', title='Total Credits Used', axis=alt.Axis(format='.2f')),
                    color=alt.Color('task_name:N', legend=alt.Legend(title='Task'), scale=alt.Scale(scheme='magma')),
                    tooltip=[
                        alt.Tooltip('yearmonthdate(hour_start):T', title='Date', format='%Y-%m-%d'),
                        alt.Tooltip('task_name:N', title='Task'),
                        alt.Tooltip('total_credits_used:Q', title='Total Credits', format='.2f')
                    ]
                ).properties(
                    title='Total Credits Used Per Day by Task',
                    width=PLOT_WIDTH,
                    height=PLOT_HEIGHT
                ).configure_view(
                    strokeWidth=0
                )
                st.altair_chart(bar_chart, use_container_width=True)
                st.markdown("---")
            

            if not cortex_func_df.empty:
                st.subheader("Cortex Analytics Function Usage")
                cols = st.columns(5)
                total_credits_func = cortex_func_df["cf_total_token_credits"].sum() if "cf_total_token_credits" in cortex_func_df.columns else 0
                with cols[0]:
                    st.markdown(f"""
                    <div class="metric-tile">
                        <div class="metric-title">Total Cost ($)</div>
                        <div class="metric-value">${total_credits_func * price_per_credit:,.2f}</div>
                    </div>
                    """, unsafe_allow_html=True)
                with cols[1]:
                    st.markdown(f"""
                    <div class="metric-tile">
                        <div class="metric-title">Total Queries</div>
                        <div class="metric-value">{format_number(cortex_func_df['qh_total_queries'].sum()) if 'qh_total_queries' in cortex_func_df.columns else '0'}</div>
                    </div>
                    """, unsafe_allow_html=True)
                with cols[2]:
                    st.markdown(f"""
                    <div class="metric-tile">
                        <div class="metric-title">Total Rows Returned</div>
                        <div class="metric-value">{format_number(cortex_func_df['qh_total_rows_produced'].sum()) if 'qh_total_rows_produced' in cortex_func_df.columns else '0'}</div>
                    </div>
                    """, unsafe_allow_html=True)
                with cols[3]:
                    st.markdown(f"""
                    <div class="metric-tile">
                        <div class="metric-title">Total Tokens</div>
                        <div class="metric-value">{format_number(cortex_func_df['cf_total_tokens'].sum()) if 'cf_total_tokens' in cortex_func_df.columns else '0'}</div>
                    </div>
                    """, unsafe_allow_html=True)
                with cols[4]:
                    st.markdown(f"""
                    <div class="metric-tile">
                        <div class="metric-title">Total Token Credits</div>
                        <div class="metric-value">{total_credits_func:,.2f}</div>
                    </div>
                    """, unsafe_allow_html=True)
                with st.expander("Cortex Analytics Function Usage (Data Available)"):
                    st.dataframe(cortex_func_df, use_container_width=True)
                st.markdown("---")

                filtered_cortex_df = cortex_func_df
                filtered_cortex_df['hour_start'] = pd.to_datetime(filtered_cortex_df['hour_start'])
                daily_cortex_df = filtered_cortex_df.groupby([pd.Grouper(key='hour_start', freq='D'), 'cf_function_name']).agg({
                    'cf_total_token_credits': 'sum',
                    'qh_total_queries': 'sum',
                    'qh_total_rows_produced': 'sum',
                    'qh_total_rows_updated': 'sum',
                    'qh_total_credits_used_cloud_services': 'sum',
                    'cf_total_tokens': 'sum',
                    'cf_model_name': 'first',
                    'qh_query_type': 'first',
                    'qh_warehouse_name': 'first'
                }).reset_index()

                daily_cortex_df['formatted_queries'] = daily_cortex_df['qh_total_queries'].apply(format_number)
                daily_cortex_df['formatted_rows_produced'] = daily_cortex_df['qh_total_rows_produced'].apply(format_number)
                daily_cortex_df['formatted_rows_updated'] = daily_cortex_df['qh_total_rows_updated'].apply(format_number)
                daily_cortex_df['formatted_tokens'] = daily_cortex_df['cf_total_tokens'].apply(format_number)
                daily_cortex_df['formatted_cloud_credits'] = daily_cortex_df['qh_total_credits_used_cloud_services'].round(2).astype(str)
                daily_cortex_df['formatted_token_credits'] = daily_cortex_df['cf_total_token_credits'].round(2).astype(str)
                daily_cortex_df['formatted_total_cost'] = (daily_cortex_df['cf_total_token_credits'] * price_per_credit).round(2).apply(lambda x: f"${x:,.2f}")

                bar_chart = alt.Chart(daily_cortex_df).mark_bar(cornerRadius=5).encode(
                    x=alt.X('yearmonthdate(hour_start):T', title='Date', axis=alt.Axis(format='%Y-%m-%d', labelAngle=0)),
                    y=alt.Y('cf_total_token_credits:Q', title='Token Credits', axis=alt.Axis(format='.2f')),
                    color=alt.Color('cf_function_name:N', legend=alt.Legend(title='Function'), scale=alt.Scale(scheme='magma')),
                    tooltip=[
                        alt.Tooltip('yearmonthdate(hour_start):T', title='Date', format='%Y-%m-%d'),
                        alt.Tooltip('cf_function_name:N', title='Function'),
                        alt.Tooltip('formatted_queries:N', title='Total Queries'),
                        alt.Tooltip('formatted_rows_produced:N', title='Rows Produced'),
                        alt.Tooltip('formatted_rows_updated:N', title='Rows Updated'),
                        alt.Tooltip('formatted_cloud_credits:N', title='Cloud Credits'),
                        alt.Tooltip('formatted_tokens:N', title='Total Tokens'),
                        alt.Tooltip('formatted_token_credits:N', title='Token Credits'),
                        alt.Tooltip('formatted_total_cost:N', title='Total Cost'),
                        alt.Tooltip('cf_model_name:N', title='Model'),
                        alt.Tooltip('qh_query_type:N', title='Query Type'),
                        alt.Tooltip('qh_warehouse_name:N', title='Warehouse')
                    ]
                ).properties(
                    title='Token Credits Per Day by Function',
                    width=PLOT_WIDTH,
                    height=PLOT_HEIGHT
                ).configure_view(
                    strokeWidth=0
                )
                st.altair_chart(bar_chart, use_container_width=True)
                st.markdown("---")

            # Button to generate prediction and switch to Predictions tab
            if st.button("Generate Prediction"):
                prediction_data = {
                    "warehouse_cost": total_credits_wh * price_per_credit if not wh_df.empty else 0,
                    "compute_pool_cost": total_credits_spcs * price_per_credit if not spcs_df.empty else 0,
                    "pipe_cost": total_credits_pipe * price_per_credit if not pipe_df.empty else 0,
                    "serverless_task_cost": total_credits_task * price_per_credit if not task_df.empty else 0,
                    "cortex_function_cost": total_credits_func * price_per_credit if not cortex_func_df.empty else 0
                }
                st.session_state["prediction_data"] = prediction_data
                st.session_state["active_tab"] = "FrostForecast Consumption Predictions"
                st.experimental_rerun()

# render-predictions-tab
def render_predictions_tab():
    """Render the Consumption Predictions tab with interactive growth rate adjustments and receipt-style table."""
    with st.container():
        st.header("FrostForecast Consumption Predictions")
        if not st.session_state["filter_submitted"]:
            st.warning("Please submit resources or a tag in the Resource Submission tab to view predictions, or use the 'Query All Data' button.")
        elif not st.session_state["prediction_data"]:
            st.warning("Please click 'Generate Prediction' in the Consumption tab to view cost projections.")
        else:
            st.markdown("Adjust growth rates (%) to predict future costs based on current consumption.")
            prediction_data = st.session_state["prediction_data"]
            
            # Interactive growth rate inputs
            col1, col2, col3, col4, col5 = st.columns(5)
            with col1:
                if prediction_data["warehouse_cost"] > 0:
                    st.session_state["growth_rates"]["warehouse"] = st.number_input(
                        "Warehouse Growth %",
                        min_value=0.0,
                        value=st.session_state["growth_rates"]["warehouse"],
                        step=0.1,
                        key="growth_warehouse"
                    )
            with col2:
                if prediction_data["compute_pool_cost"] > 0:
                    st.session_state["growth_rates"]["compute_pool"] = st.number_input(
                        "Compute Pool Growth %",
                        min_value=0.0,
                        value=st.session_state["growth_rates"]["compute_pool"],
                        step=0.1,
                        key="growth_compute_pool"
                    )
            with col3:
                if prediction_data["pipe_cost"] > 0:
                    st.session_state["growth_rates"]["pipe"] = st.number_input(
                        "Pipe Growth %",
                        min_value=0.0,
                        value=st.session_state["growth_rates"]["pipe"],
                        step=0.1,
                        key="growth_pipe"
                    )
            with col4:
                if prediction_data["serverless_task_cost"] > 0:
                    st.session_state["growth_rates"]["serverless_task"] = st.number_input(
                        "Serverless Task Growth %",
                        min_value=0.0,
                        value=st.session_state["growth_rates"]["serverless_task"],
                        step=0.1,
                        key="growth_serverless_task"
                    )
            with col5:
                if prediction_data["cortex_function_cost"] > 0:
                    st.session_state["growth_rates"]["cortex_function"] = st.number_input(
                        "Cortex Function Growth %",
                        min_value=0.0,
                        value=st.session_state["growth_rates"]["cortex_function"],
                        step=0.1,
                        key="growth_cortex_function"
                    )
            st.markdown("---")

            # Calculate projected costs
            growth_rates = st.session_state["growth_rates"]
            timeframes = [30, 60, 90, 120]
            receipt_data = []
            total_current = 0
            total_projected = {30: 0, 60: 0, 90: 0, 120: 0}

            for section, current_cost in prediction_data.items():
                if current_cost > 0:
                    section_name = section.replace("_cost", "").replace("_", " ").title()
                    row = {"Section": section_name, "Current Cost ($)": f"${current_cost:,.2f}"}
                    for days in timeframes:
                        growth_key = section.replace("_cost", "")
                        growth_rate = growth_rates.get(growth_key, 0.5) / 100
                        projected_cost = current_cost * (1 + growth_rate) ** (days / 30)  # Monthly compounding
                        row[f"{days} Days ($)"] = f"${projected_cost:,.2f}"
                        total_projected[days] += projected_cost
                    total_current += current_cost
                    receipt_data.append(row)

            # Add totals row
            total_row = {"Section": "Total", "Current Cost ($)": f"${total_current:,.2f}"}
            for days in timeframes:
                total_row[f"{days} Days ($)"] = f"${total_projected[days]:,.2f}"
            receipt_data.append(total_row)

            # Display receipt-style table
            st.subheader("Cost Projection Receipt")
            st.markdown("""
            <style>
            .receipt-table {
                width: 100%;
                border-collapse: collapse;
                font-family: Arial, sans-serif;
                margin: 20px 0;
                box-shadow: 0 4px 8px rgba(0,0,0,0.1);
            }
            .receipt-table th, .receipt-table td {
                padding: 12px;
                text-align: right;
                border-bottom: 1px solid #ddd;
            }
            .receipt-table th {
                background-color: #f2f2f2;
                font-weight: bold;
            }
            .receipt-table tr:last-child {
                font-weight: bold;
                background-color: #f8f9fa;
            }
            .receipt-table td:first-child {
                text-align: left;
            }
            </style>
            """, unsafe_allow_html=True)
            st.table(receipt_data)

# main-function
def main():
    """Main function to orchestrate the FrostForecast Consumption Dashboard."""
    initialize_session_state()
    session = setup_snowflake()
    start_ts = datetime.datetime.combine(st.session_state["start_date"], datetime.time.min)
    end_ts = datetime.datetime.combine(st.session_state["end_date"], datetime.time.max)
    price_per_credit = st.session_state["price_per_credit"]
    render_sidebar()
    
    tab0, tab1, tab2, tab3 = st.tabs([
        "Welcome",
        "Resource Submission",
        "FrostForecast Consumption",
        "FrostForecast Consumption Predictions"
    ])
    
    with tab0:
        render_welcome_tab()
    with tab1:
        render_resource_submission_tab(session)
    with tab2:
        render_consumption_tab(session, start_ts, end_ts, price_per_credit)
    with tab3:
        render_predictions_tab()

if __name__ == "__main__":
    main()