import streamlit as st
import pandas as pd
import snowflake.connector

# ==========================================
# 1. PAGE CONFIGURATION & STYLING
# ==========================================
st.set_page_config(
    page_title="Germany Tech-Stack Radar",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Germany Jobmarket Analyzer & Tech-Radar")
st.markdown("### Strategic Market Analysis for Data & Tech Roles (Germany-wide)")
st.markdown("---")

# ==========================================
# 2. SNOWFLAKE CONNECTION & CACHING
# ==========================================
@st.cache_resource
def init_connection():
    """Establishes connection to Snowflake using Streamlit secrets."""
    return snowflake.connector.connect(**st.secrets["snowflake"])

try:
    conn = init_connection()
except Exception as e:
    st.error(f"Connection to Snowflake failed: {e}")
    st.stop()

@st.cache_data
def load_gold_layer(query):
    """Efficiently loads data from the Gold Layer and caches it."""
    with conn.cursor() as cur:
        cur.execute(query)
        columns = [col[0] for col in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=columns)

# Fetch data from Snowflake Gold Layer
df_jobs = load_gold_layer("SELECT * FROM GOLD.FACT_JOB_POSTINGS;")
df_tech = load_gold_layer("SELECT * FROM GOLD.DIM_JOB_TECHNOLOGIES;")
df_req = load_gold_layer("SELECT * FROM GOLD.DIM_JOB_REQUIREMENTS;")

# Ensure all column names are uppercase to match Snowflake standards uniformly
df_jobs.columns = [col.upper() for col in df_jobs.columns]
df_tech.columns = [col.upper() for col in df_tech.columns]
df_req.columns = [col.upper() for col in df_req.columns]

# ==========================================
# DATA PROCESSING: DATETIME CONVERSIONS & CALCULATIONS
# ==========================================
posted_col = next((c for c in ["PUBLICATION_DATE", "FIRST_SEEN", "POSTED_AT", "CREATED_AT"] if c in df_jobs.columns), None)
last_seen_col = next((c for c in ["LAST_SEEN", "LAST_SEEN_AT", "SCRAPED_AT"] if c in df_jobs.columns), None)

if posted_col:
    df_jobs[posted_col] = pd.to_datetime(df_jobs[posted_col], errors='coerce')
    # Extract weekday name and number for proper ordering (0 = Monday, 6 = Sunday)
    df_jobs["WEEKDAY_NAME"] = df_jobs[posted_col].dt.day_name()
    df_jobs["WEEKDAY_NUM"] = df_jobs[posted_col].dt.dayofweek

if last_seen_col:
    df_jobs[last_seen_col] = pd.to_datetime(df_jobs[last_seen_col], errors='coerce')

# Calculate the duration a job posting has been active/online
if posted_col and last_seen_col:
    df_jobs["DAYS_ONLINE"] = (df_jobs[last_seen_col] - df_jobs[posted_col]).dt.days
    # Clean anomalies (e.g., negative days due to scrapers running across timezones)
    df_jobs["DAYS_ONLINE"] = df_jobs["DAYS_ONLINE"].apply(lambda x: x if x >= 0 else 0)

# ==========================================
# 3. SIDEBAR / GLOBAL FILTERS
# ==========================================
st.sidebar.header("🎯 Filter Center")
st.sidebar.markdown("Filter the visualization layers based on specific job roles.")

# Generate dynamic role list from the dataset
available_roles = ["All Tech Roles"] + list(df_jobs["JOB_ROLE"].dropna().unique())
selected_role = st.sidebar.selectbox("Select Focus Role:", available_roles)

# Filter dataset based on user selection
if selected_role != "All Tech Roles":
    filtered_jobs = df_jobs[df_jobs["JOB_ROLE"] == selected_role]
    filtered_tech = df_tech[df_tech["JOB_ID"].isin(filtered_jobs["JOB_ID"])]
    filtered_req = df_req[df_req["JOB_ID"].isin(filtered_jobs["JOB_ID"])]
else:
    filtered_jobs = df_jobs
    filtered_tech = df_tech
    filtered_req = df_req

# ==========================================
# 4. KPI TILES (HIGHLIGHTS)
# ==========================================
total_jobs = len(filtered_jobs)
top_tech = filtered_tech["TECH_NAME"].value_counts().idxmax() if not filtered_tech.empty else "N/A"
avg_lifespan = f"{int(filtered_jobs['DAYS_ONLINE'].mean())} Days" if "DAYS_ONLINE" in filtered_jobs.columns and not filtered_jobs.empty else "N/A"

kpi1, kpi2, kpi3, kpi4 = st.columns(4)
with kpi1:
    st.metric(label="Analyzed Vacancies Total", value=f"{total_jobs} Jobs")
with kpi2:
    st.metric(label="Top In-Demand Technology", value=top_tech)
with kpi3:
    st.metric(label="Avg. Posting Lifespan", value=avg_lifespan)
with kpi4:
    st.metric(label="Active Role Filter", value=selected_role)

st.markdown("---")

# ==========================================
# 5. TIME-BASED MARKET INSIGHTS
# ==========================================
st.subheader("📅 Time-Based Market Insights")
col_time1, col_time2 = st.columns(2)

with col_time1:
    st.markdown("#### 📆 Job Posting Distribution by Weekday")
    st.markdown("Which days of the week do companies publish new tech jobs most frequently?")
    
    if "WEEKDAY_NAME" in filtered_jobs.columns and not filtered_jobs.empty:
        # Standard sorted list to ensure Monday -> Sunday sequence
        weekday_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        
        # Group, count and reindex
        weekday_counts = filtered_jobs["WEEKDAY_NAME"].value_counts().reindex(weekday_order).fillna(0).reset_index()
        weekday_counts.columns = ["Weekday", "New Postings"]
        
        # 🔥 FIX: Enforce categorical ordering so Streamlit stops sorting alphabetically
        weekday_counts["Weekday"] = pd.Categorical(
            weekday_counts["Weekday"], 
            categories=weekday_order, 
            ordered=True
        )
        
        st.bar_chart(data=weekday_counts, x="Weekday", y="New Postings", color="#4ff0af")
    else:
        st.info("Posting date column not found or empty. Skipping weekday distribution matrix.")

with col_time2:
    st.markdown("#### ⏳ Lifespan Analysis (How long do jobs stay online?)")
    st.markdown("Distribution of how many days job ads remain active before being removed.")
    
    if "DAYS_ONLINE" in filtered_jobs.columns and not filtered_jobs.empty:
        # Bin continuous day counts into logical historical categories
        duration_bins = pd.cut(
            filtered_jobs["DAYS_ONLINE"], 
            bins=[-1, 3, 7, 14, 30, 60, 999], 
            labels=["0-3 Days", "4-7 Days", "1-2 Weeks", "2-4 Weeks", "1-2 Months", "2+ Months"]
        )
        duration_counts = duration_bins.value_counts().reset_index()
        duration_counts.columns = ["Active Time Range", "Jobs Volume"]
        
        st.bar_chart(data=duration_counts, x="Active Time Range", y="Jobs Volume", color="#ff6b6b")
    else:
        st.warning("Lifespan calculation requires both a creation/posting date and a last seen column in your data.")
        st.info(f"Available tables columns for diagnostic: {list(df_jobs.columns)}")

st.markdown("---")

# ==========================================
# 6. CORE ANALYSES (TWO-COLUMN LAYOUT)
# ==========================================
col_left, col_right = st.columns(2)

with col_left:
    # --- ANALYSIS 1: TECH RADAR ---
    st.subheader("💡 Technology Radar (Top Skills)")
    st.markdown("Which tools, libraries, and frameworks are requested most frequently?")
    
    if not filtered_tech.empty:
        tech_counts = filtered_tech["TECH_NAME"].value_counts().reset_index()
        tech_counts.columns = ["Technology", "Mentions"]
        
        st.bar_chart(data=tech_counts.head(15), x="Technology", y="Mentions", color="#29b5e8")
    else:
        st.info("No technology data found for the current selection.")

with col_right:
    # --- ANALYSIS 2: MARKET DYNAMICS (TOP EMPLOYERS) ---
    st.subheader("🏢 Top Employers in Germany")
    st.markdown("Which companies are publishing the highest volume of tech vacancies?")
    
    if not filtered_jobs.empty:
        company_counts = filtered_jobs["COMPANY"].value_counts().reset_index()
        company_counts.columns = ["Company", "Open Positions"]
        
        st.bar_chart(data=company_counts.head(10), x="Company", y="Open Positions", color="#7952b3")
    else:
        st.info("No company recruitment data available.")

st.markdown("---")

# ==========================================
# 7. DEEP DIVE: REQUIREMENTS & BARRIERS
# ==========================================
st.subheader("🛡️ Market Barriers: Requirements & Job Frameworks")
col_req1, col_req2 = st.columns(2)

with col_req1:
    # --- ANALYSIS 3: LANGUAGE BARRIER ---
    st.markdown("#### 🗣️ Required Language Skills")
    if "LANGUAGE" in filtered_jobs.columns:
        lang_counts = filtered_jobs["LANGUAGE"].value_counts().reset_index()
        lang_counts.columns = ["Language", "Count"]
        st.dataframe(lang_counts, use_container_width=True, hide_index=True)
    elif "REQUIREMENT_CATEGORY" in filtered_req.columns:
        lang_data = filtered_req[filtered_req["REQUIREMENT_CATEGORY"].str.upper().str.contains("LANG", na=False)]
        if not lang_data.empty:
            lang_counts = lang_data["REQUIREMENT_NAME"].value_counts().reset_index()
            lang_counts.columns = ["Language", "Mentions"]
            st.dataframe(lang_counts, use_container_width=True, hide_index=True)
        else:
            st.info("No specific language entries found in the dataset.")
    else:
        st.info("Language tracking column missing from dataset architecture.")

with col_req2:
    # --- ANALYSIS 4: EDUCATION DEGREE VS EXPERIENCE ---
    st.markdown("#### 🎓 Education & Qualification Matrix")
    if "REQUIREMENT_CATEGORY" in filtered_req.columns:
        edu_data = filtered_req[filtered_req["REQUIREMENT_CATEGORY"].str.upper().isin(["EDUCATION", "DEGREE", "QUALIFICATION"])]
        if not edu_data.empty:
            edu_counts = edu_data["REQUIREMENT_NAME"].value_counts().reset_index()
            edu_counts.columns = ["Requirement", "Mentions"]
            st.bar_chart(data=edu_counts.head(5), x="Requirement", y="Mentions", color="#ff9900")
        else:
            st.info("No educational metrics found matching target classification strings.")
    else:
        st.info("Unable to generate education matrix due to missing schema definitions.")

# ==========================================
# 8. RAW DATA INSIGHT (FOR VALIDATION)
# ==========================================
st.markdown("---")
with st.expander("🔍 Sample Data: View Live Job Postings"):
    possible_cols = ["TITLE", "JOB_TITLE", "COMPANY", "COMPANY_NAME", "CITY", "LOCATION", "JOB_ROLE", "POSTED_AT", "CREATED_AT", "LAST_SEEN", "LAST_SEEN_AT"]
    columns_to_show = [col for col in possible_cols if col in filtered_jobs.columns]
    
    if columns_to_show:
        st.dataframe(filtered_jobs[columns_to_show].head(20), use_container_width=True, hide_index=True)
    else:
        st.dataframe(filtered_jobs.iloc[:, :5].head(20), use_container_width=True, hide_index=True)