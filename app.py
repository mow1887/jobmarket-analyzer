import streamlit as st
import pandas as pd
import snowflake.connector
import altair as alt

# ==========================================
# 1. PAGE CONFIGURATION & DESIGN SYSTEM
# ==========================================
st.set_page_config(
    page_title="Germany Tech-Stack Radar",
    page_icon="📊",
    layout="wide"
)

# Global Theme Colors
MAIN_THEME_COLOR = "#1f77b4"  # Unified Tech Blue for standard bars
CONCEPT_THEME_COLOR = "#00dbc2" # Teal for Methodologies/Concepts
REGIONAL_THEME_COLOR = "#ff9f43" # Warm Orange for Regional Hotspots

st.title("📊 Germany Jobmarket Analyzer & Tech-Radar")
st.markdown("### Strategic Market Analysis for Data & Tech Roles (Germany-wide)")
st.markdown("---")

# ==========================================
# 2. SNOWFLAKE CONNECTION & CACHING
# ==========================================
@st.cache_resource
def init_connection():
    return snowflake.connector.connect(**st.secrets["snowflake"])

try:
    conn = init_connection()
except Exception as e:
    st.error(f"Connection to Snowflake failed: {e}")
    st.stop()

@st.cache_data
def load_gold_layer(query):
    with conn.cursor() as cur:
        cur.execute(query)
        columns = [col[0] for col in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=columns)

df_jobs = load_gold_layer("SELECT * FROM GOLD.FACT_JOB_POSTINGS;")
df_tech = load_gold_layer("SELECT * FROM GOLD.DIM_JOB_TECHNOLOGIES;")
df_req = load_gold_layer("SELECT * FROM GOLD.DIM_JOB_REQUIREMENTS;")

df_jobs.columns = [col.upper() for col in df_jobs.columns]
df_tech.columns = [col.upper() for col in df_tech.columns]
df_req.columns = [col.upper() for col in df_req.columns]

# ==========================================
# DATE & COLUMN PROCESSING
# ==========================================
if "PUBLICATION_DATE" in df_jobs.columns:
    df_jobs["PUBLICATION_DATE"] = pd.to_datetime(df_jobs["PUBLICATION_DATE"], errors='coerce')
if "FIRST_SEEN" in df_jobs.columns:
    df_jobs["FIRST_SEEN"] = pd.to_datetime(df_jobs["FIRST_SEEN"], errors='coerce')

last_seen_col = next((c for c in ["LAST_SEEN", "LAST_SEEN_AT", "SCRAPED_AT"] if c in df_jobs.columns), None)
if last_seen_col:
    df_jobs[last_seen_col] = pd.to_datetime(df_jobs[last_seen_col], errors='coerce')

# Safely detect title mapping to prevent KeyError breaks
title_col = next((c for c in ["TITLE", "JOB_TITLE"] if c in df_jobs.columns), None)

# ==========================================
# 3. SIDEBAR / GLOBAL FILTERS
# ==========================================
st.sidebar.header("🎯 Filter Center")

available_roles = ["All Tech Roles"] + list(df_jobs["JOB_ROLE"].dropna().unique())
selected_role = st.sidebar.selectbox("Select Focus Role:", available_roles)

exclude_unspecified = st.sidebar.checkbox(
    "Hide Unspecified Tech Jobs", value=False,
    help="Filters out vacancies that only contain generic terms (Classic Tools)."
)

posting_status = st.sidebar.radio(
    "Posting Status:",
    options=["All Postings", "Only Active (Seen Today)", "Only Inactive (Archived)"], index=0
)

top_20_cities = list(df_jobs["LOCATION"].dropna().value_counts().head(20).index)
selected_cities = st.sidebar.multiselect("Filter by Location (Top 20 Cities):", options=top_20_cities, default=[])

st.sidebar.markdown("---")
st.sidebar.subheader("📅 Timeline Analytics Engine")

timeline_baseline = st.sidebar.radio(
    "Lifespan Calculation Baseline:",
    options=["Publication Date to Last Seen", "First Seen (Scraper) to Last Seen"],
    index=0
)

hide_missing_pub_date = st.sidebar.checkbox(
    "Hide Missing Publication Dates", value=False
)

# --- APPLY SIDEBAR FILTERS IN CASCADING ARCHITECTURE ---
filtered_jobs = df_jobs.copy()

if hide_missing_pub_date and "PUBLICATION_DATE" in filtered_jobs.columns:
    filtered_jobs = filtered_jobs[filtered_jobs["PUBLICATION_DATE"].notna()]

if selected_role != "All Tech Roles":
    filtered_jobs = filtered_jobs[filtered_jobs["JOB_ROLE"] == selected_role]

if exclude_unspecified:
    fallback_labels = ["Classic Tools / Open Research", "Klassische Tools / Open Research", "Klassische Tools / Offene Recherche"]
    specific_tech_job_ids = df_tech[~df_tech["TECH_NAME"].isin(fallback_labels)]["JOB_ID"].unique()
    filtered_jobs = filtered_jobs[filtered_jobs["JOB_ID"].isin(specific_tech_job_ids)]

if selected_cities and "LOCATION" in filtered_jobs.columns:
    filtered_jobs = filtered_jobs[filtered_jobs["LOCATION"].isin(selected_cities)]

if last_seen_col and not filtered_jobs.empty:
    latest_pipeline_run = df_jobs[last_seen_col].max()
    if posting_status == "Only Active (Seen Today)":
        filtered_jobs = filtered_jobs[filtered_jobs[last_seen_col] == latest_pipeline_run]
    elif posting_status == "Only Inactive (Archived)":
        filtered_jobs = filtered_jobs[filtered_jobs[last_seen_col] < latest_pipeline_run]

# --- DYNAMIC TIMELINE COMPUTATION ---
if timeline_baseline == "Publication Date to Last Seen":
    start_date_col = "PUBLICATION_DATE" if "PUBLICATION_DATE" in df_jobs.columns else "FIRST_SEEN"
else:
    start_date_col = "FIRST_SEEN" if "FIRST_SEEN" in df_jobs.columns else "PUBLICATION_DATE"

if not filtered_jobs.empty and last_seen_col and start_date_col in filtered_jobs.columns:
    calc_start = filtered_jobs[start_date_col]
    if start_date_col == "PUBLICATION_DATE" and "FIRST_SEEN" in filtered_jobs.columns:
        calc_start = calc_start.fillna(filtered_jobs["FIRST_SEEN"])
    
    filtered_jobs["DAYS_ONLINE"] = (filtered_jobs[last_seen_col] - calc_start).dt.days
    filtered_jobs["DAYS_ONLINE"] = filtered_jobs["DAYS_ONLINE"].apply(lambda x: x if x >= 0 else 0)
    filtered_jobs["WEEKDAY_NAME"] = calc_start.dt.day_name()
    filtered_jobs["CLEAN_START_DATE"] = calc_start

    # GLOBAL SAFEGUARD: Strictly exclude any data from "today" to prevent partial-day bias
    today_floor = pd.Timestamp.now().floor('D')
    filtered_jobs = filtered_jobs[filtered_jobs["CLEAN_START_DATE"] < today_floor]

# Sync dependent Dimension layers uniformly based on filtered keys
filtered_tech = df_tech[df_tech["JOB_ID"].isin(filtered_jobs["JOB_ID"])]
filtered_req = df_req[df_req["JOB_ID"].isin(filtered_jobs["JOB_ID"])]

# GLOBAL BASELINE METRIC
total_jobs = len(filtered_jobs)

# ==========================================
# GLOBAL TAB NAVIGATION
# ==========================================
tab_overview, tab_comparison, tab_advanced = st.tabs([
    "📈 Market Overview", 
    "🔄 Weekly Trend Comparison", 
    "🔬 Advanced Deep-Dive Analytics"
])

# ==========================================
# TAB 1: MARKET OVERVIEW
# ==========================================
with tab_overview:
    if not filtered_tech.empty:
        fallback_labels = ["Classic Tools / Open Research", "Klassische Tools / Open Research", "Klassische Tools / Offene Recherche"]
        tech_counts_kpi = filtered_tech[~filtered_tech["TECH_NAME"].isin(fallback_labels)]
        top_tech = tech_counts_kpi["TECH_NAME"].value_counts().idxmax() if not tech_counts_kpi.empty else "N/A"
    else:
        top_tech = "N/A"

    avg_lifespan = f"{int(filtered_jobs['DAYS_ONLINE'].mean())} Days" if "DAYS_ONLINE" in filtered_jobs.columns and not filtered_jobs.empty else "N/A"

    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    with kpi1: st.metric(label="Analyzed Vacancies Total", value=f"{total_jobs} Jobs")
    with kpi2: st.metric(label="Top In-Demand Technology", value=top_tech)
    with kpi3: st.metric(label="Avg. Posting Lifespan", value=avg_lifespan)
    with kpi4: st.metric(label="Active Role Filter", value=selected_role)

    st.markdown("<br>", unsafe_allow_html=True)

    # GROUP 1: TIME-BASED MARKET INSIGHTS
    with st.container(border=True):
        st.markdown("### 📅 Section 1: Time & Lifespan Dynamics")
        col_time1, col_time2 = st.columns(2)
        with col_time1:
            st.markdown("#### 📆 Job Posting Distribution by Weekday")
            if "CLEAN_START_DATE" in filtered_jobs.columns and not filtered_jobs.empty:
                first_ingest_date = df_jobs["FIRST_SEEN"].min().date()
                df_weekday_clean = filtered_jobs[filtered_jobs["FIRST_SEEN"].dt.date != first_ingest_date]
                
                if not df_weekday_clean.empty:
                    daily_volumes = df_weekday_clean.groupby(df_weekday_clean["CLEAN_START_DATE"].dt.date).size().reset_index(name="DAILY_COUNT")
                    daily_volumes["Weekday"] = pd.to_datetime(daily_volumes["CLEAN_START_DATE"]).dt.day_name()
                    
                    weekday_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
                    weekday_avgs = daily_volumes.groupby("Weekday")["DAILY_COUNT"].mean().reindex(weekday_order).fillna(0).reset_index()
                    weekday_avgs.columns = ["Weekday", "Average Postings"]
                    
                    chart_week = alt.Chart(weekday_avgs).mark_bar(color=MAIN_THEME_COLOR).encode(
                        x=alt.X("Weekday:N", sort=weekday_order, axis=alt.Axis(labelAngle=-45, labelOverlap=False)),
                        y=alt.Y("Average Postings:Q", title="Average Postings per Day"),
                        tooltip=["Weekday", alt.Tooltip("Average Postings:Q", format=".1f")]
                    ).properties(height=280)
                    st.altair_chart(chart_week, use_container_width=True)
                else:
                    st.info("No data available after excluding the initial load setup day.")
            else:
                st.info("No timeline data found to generate weekday distribution.")
                
        with col_time2:
            st.markdown("#### ⏳ Lifespan Analysis (How long do jobs stay online?)")
            if "DAYS_ONLINE" in filtered_jobs.columns and not filtered_jobs.empty:
                duration_bins = pd.cut(filtered_jobs["DAYS_ONLINE"], bins=[-1, 3, 7, 14, 30, 60, 999], labels=["0-3 Days", "4-7 Days", "1-2 Weeks", "2-4 Weeks", "1-2 Months", "2+ Months"])
                duration_counts = duration_bins.value_counts().reset_index()
                duration_counts.columns = ["Active Time Range", "Jobs Volume"]
                
                chart_life = alt.Chart(duration_counts).mark_bar(color=MAIN_THEME_COLOR).encode(
                    x=alt.X("Active Time Range:N", sort=["0-3 Days", "4-7 Days", "1-2 Weeks", "2-4 Weeks", "1-2 Months", "2+ Months"], axis=alt.Axis(labelAngle=-45, labelOverlap=False)),
                    y=alt.Y("Jobs Volume:Q", title="Volume of Jobs")
                ).properties(height=280)
                st.altair_chart(chart_life, use_container_width=True)

    # GROUP 2: TECHNOLOGY & TOOLING DEEP DIVES
    with st.container(border=True):
        st.markdown("### 💡 Section 2: Tech-Radar & Tool Ecosystems")
        col_tech_left, col_tech_right = st.columns([1.1, 0.9])
        with col_tech_left:
            st.markdown("#### 🛠️ Core Skills Breakdown")
            if not filtered_tech.empty:
                tech_counts = filtered_tech["TECH_NAME"].value_counts().reset_index()
                tech_counts.columns = ["Technology", "Mentions"]
                fallback_labels = ["Classic Tools / Open Research", "Klassische Tools / Open Research", "Klassische Tools / Offene Recherche"]
                unspecified_count = tech_counts[tech_counts["Technology"].isin(fallback_labels)]["Mentions"].sum()
                tech_counts_clean = tech_counts[~tech_counts["Technology"].isin(fallback_labels)]
                
                conceptual_skills = ["ETL/ELT", "Machine Learning", "Deep Learning", "NLP", "Time Series", "LLM & Prompting", "AI Agents", "RAG & Vector DBs"]
                df_concepts = tech_counts_clean[tech_counts_clean["Technology"].isin(conceptual_skills)].copy()
                df_tools = tech_counts_clean[~tech_counts_clean["Technology"].isin(fallback_labels)].copy()
                df_tools = df_tools[~df_tools["Technology"].isin(conceptual_skills)]
                
                # Convert absolute mentions to market penetration rate (%)
                df_concepts["Market Penetration (%)"] = (df_concepts["Mentions"] / total_jobs) * 100 if total_jobs > 0 else 0
                df_tools["Market Penetration (%)"] = (df_tools["Mentions"] / total_jobs) * 100 if total_jobs > 0 else 0
                
                tab_tools, tab_concepts = st.tabs(["⚙️ Tools & Platforms", "🧠 Methodologies & Concepts"])
                with tab_tools:
                    chart_tools = alt.Chart(df_tools.head(15)).mark_bar(color=MAIN_THEME_COLOR).encode(
                        x=alt.X("Technology:N", sort="-y", axis=alt.Axis(labelAngle=-45, labelOverlap=False)),
                        y=alt.Y("Market Penetration (%):Q", title="% of All Filtered Jobs"),
                        tooltip=["Technology", alt.Tooltip("Market Penetration (%):Q", format=".1f")]
                    ).properties(height=300)
                    st.altair_chart(chart_tools, use_container_width=True)
                with tab_concepts:
                    chart_concepts = alt.Chart(df_concepts.head(15)).mark_bar(color=CONCEPT_THEME_COLOR).encode(
                        x=alt.X("Technology:N", sort="-y", axis=alt.Axis(labelAngle=-45, labelOverlap=False)),
                        y=alt.Y("Market Penetration (%):Q", title="% of All Filtered Jobs"),
                        tooltip=["Technology", alt.Tooltip("Market Penetration (%):Q", format=".1f")]
                    ).properties(height=300)
                    st.altair_chart(chart_concepts, use_container_width=True)
        with col_tech_right:
            st.markdown("#### ☁️ Cloud Provider Market Share")
            if not filtered_tech.empty:
                cloud_platforms = ["AWS", "Azure", "GCP"]
                df_cloud = tech_counts[tech_counts["Technology"].isin(cloud_platforms)]
                if not df_cloud.empty:
                    chart_cloud = alt.Chart(df_cloud).mark_arc(innerRadius=55, stroke="#1e1e1e").encode(
                        theta=alt.Theta(field="Mentions", type="quantitative"),
                        color=alt.Color("Technology:N", scale=alt.Scale(domain=cloud_platforms, range=["#ff9900", "#0078d4", "#34a853"]), legend=alt.Legend(title="Ecosystem")),
                        tooltip=["Technology", "Mentions"]
                    ).properties(height=320)
                    st.altair_chart(chart_cloud, use_container_width=True)
                else:
                    st.info("No cloud infrastructure metrics found.")

        st.markdown("---")
        col_bi_left, col_bi_right = st.columns(2)
        with col_bi_left:
            st.markdown("#### 📊 Business Intelligence & Data Visualization Demand")
            if not filtered_tech.empty:
                bi_tools = ["Power BI", "Tableau", "Looker", "Metabase", "Qlik", "Excel"]
                df_bi = tech_counts[tech_counts["Technology"].isin(bi_tools)].copy()
                
                # Market penetration rate for BI ecosystem
                df_bi["Market Penetration (%)"] = (df_bi["Mentions"] / total_jobs) * 100 if total_jobs > 0 else 0
                
                if not df_bi.empty:
                    chart_bi = alt.Chart(df_bi).mark_bar(color=MAIN_THEME_COLOR).encode(
                        y=alt.Y("Technology:N", sort="-x", title=None),
                        x=alt.X("Market Penetration (%):Q", title="% of All Filtered Jobs"),
                        tooltip=["Technology", alt.Tooltip("Market Penetration (%):Q", format=".1f")]
                    ).properties(height=220)
                    st.altair_chart(chart_bi, use_container_width=True)
        with col_bi_right:
            if not filtered_tech.empty and unspecified_count > 0 and not exclude_unspecified:
                st.markdown("<div style='padding-top:25px;'></div>", unsafe_allow_html=True)
                st.info(f"ℹ️ **Tech Depth Alert:** In **{unspecified_count:,}** job advertisements, no specialized data tech architecture keyword was matched.")

    # GROUP 3: MARKET PLAYERS & REGIONAL HOTSPOTS
    with st.container(border=True):
        st.markdown("### 🏢 Section 3: Market Players & Geographic Hotspots")
        col_mp_left, col_mp_right = st.columns(2)
        with col_mp_left:
            st.markdown("#### 🏢 Top Recruiting Employers")
            if not filtered_jobs.empty:
                company_counts = filtered_jobs["COMPANY"].value_counts().reset_index()
                company_counts.columns = ["Company", "Open Positions"]
                chart_emp = alt.Chart(company_counts.head(10)).mark_bar(color=MAIN_THEME_COLOR).encode(
                    x=alt.X("Company:N", sort="-y", axis=alt.Axis(labelAngle=-45, labelOverlap=False)),
                    y=alt.Y("Open Positions:Q", title="Open Positions")
                ).properties(height=300)
                st.altair_chart(chart_emp, use_container_width=True)
        with col_mp_right:
            st.markdown("#### 📍 Regional Density (Top Cities)")
            if "LOCATION" in filtered_jobs.columns and not filtered_jobs.empty:
                city_counts = filtered_jobs["LOCATION"].value_counts().reset_index()
                city_counts.columns = ["City", "Open Vacancies"]
                chart_city = alt.Chart(city_counts.head(10)).mark_bar(color=REGIONAL_THEME_COLOR).encode(
                    x=alt.X("City:N", sort="-y", axis=alt.Axis(labelAngle=-45, labelOverlap=False)),
                    y=alt.Y("Open Vacancies:Q", title="Vacancies")
                ).properties(height=300)
                st.altair_chart(chart_city, use_container_width=True)

    # GROUP 4: MARKET BARRIERS & REQUIREMENTS
    with st.container(border=True):
        st.markdown("### 🛡️ Section 4: Market Entry Barriers & Qualifications")
        col_req1, col_req2 = st.columns(2)
        with col_req1:
            st.markdown("#### 🗣️ Required Language Skills")
            if "REQUIREMENT_CATEGORY" in filtered_req.columns:
                lang_data = filtered_req[filtered_req["REQUIREMENT_CATEGORY"].str.upper().str.contains("LANG", na=False)]
                if not lang_data.empty:
                    lang_counts = lang_data["REQUIREMENT_NAME"].value_counts().reset_index()
                    lang_counts.columns = ["Language", "Mentions"]
                    st.dataframe(lang_counts, use_container_width=True, hide_index=True)
                else: st.info("No specific language criteria mapped.")
        with col_req2:
            st.markdown("#### 🎓 Academic Education & Degree Requirements")
            if "REQUIREMENT_CATEGORY" in filtered_req.columns:
                edu_data = filtered_req[filtered_req["REQUIREMENT_CATEGORY"].str.upper().isin(["EDUCATION", "DEGREE", "QUALIFICATION"])]
                if not edu_data.empty:
                    edu_counts = edu_data["REQUIREMENT_NAME"].value_counts().reset_index()
                    edu_counts.columns = ["Requirement", "Mentions"]
                    chart_edu = alt.Chart(edu_counts.head(5)).mark_bar(color=MAIN_THEME_COLOR).encode(
                        x=alt.X("Requirement:N", sort="-y", axis=alt.Axis(labelAngle=-45, labelOverlap=False)),
                        y=alt.Y("Mentions:Q", title="Mentions")
                    ).properties(height=240)
                    st.altair_chart(chart_edu, use_container_width=True)

    # 8. RAW DATA INSIGHT
    st.markdown("---")
    with st.expander("🔍 Sample Data: View Live Job Postings"):
        possible_cols = ["TITLE", "JOB_TITLE", "COMPANY", "COMPANY_NAME", "CITY", "LOCATION", "JOB_ROLE", "POSTED_AT", "CREATED_AT", "LAST_SEEN", "LAST_SEEN_AT"]
        columns_to_show = [col for col in possible_cols if col in filtered_jobs.columns]
        if columns_to_show: st.dataframe(filtered_jobs[columns_to_show].head(20), use_container_width=True, hide_index=True)


# ==========================================
# TAB 2: WEEKLY TREND COMPARISON
# ==========================================
with tab_comparison:
    st.markdown("### 🔄 Last 2 Weeks Market Delta Analysis")
    st.markdown("Comparing vacancies published or discovered in the **Current 7 Days** window vs the **Previous 7 Days** window.")

    if filtered_jobs.empty or "CLEAN_START_DATE" not in filtered_jobs.columns:
        st.info("No timeline data found for the current selection to generate a weekly split.")
    else:
        max_date = filtered_jobs["CLEAN_START_DATE"].max()
        current_week_start = max_date - pd.Timedelta(days=6)
        previous_week_start = max_date - pd.Timedelta(days=13)
        
        cw_start_str = current_week_start.strftime('%d.%m.%Y')
        cw_end_str = max_date.strftime('%d.%m.%Y')
        pw_start_str = previous_week_start.strftime('%d.%m.%Y')
        pw_end_str = (current_week_start - pd.Timedelta(days=1)).strftime('%d.%m.%Y')
        
        st.info(f"📅 **Analysis Periods:** &nbsp;&nbsp; "
                f"**Current Week (CW):** {cw_start_str} to {cw_end_str} &nbsp;&nbsp;|&nbsp;&nbsp; "
                f"**Previous Week (PW):** {pw_start_str} to {pw_end_str}")
        
        jobs_cw = filtered_jobs[filtered_jobs["CLEAN_START_DATE"] >= current_week_start]
        jobs_pw = filtered_jobs[(filtered_jobs["CLEAN_START_DATE"] >= previous_week_start) & (filtered_jobs["CLEAN_START_DATE"] < current_week_start)]
        
        # --- SUB-SECTION 1: DELTA METRICS CARD ---
        with st.container(border=True):
            st.markdown("#### 📊 Dynamic Volume Shifts")
            c_meta1, c_meta2, c_meta3 = st.columns(3)
            
            with c_meta1:
                cw_len = len(jobs_cw)
                pw_len = len(jobs_pw)
                delta_vol = cw_len - pw_len
                st.metric(label="Current Week Volume", value=f"{cw_len} Jobs", delta=f"{delta_vol} vs Pre-Week")
                
            with c_meta2:
                cw_life = jobs_cw["DAYS_ONLINE"].mean() if not jobs_cw.empty else 0
                pw_life = jobs_pw["DAYS_ONLINE"].mean() if not jobs_pw.empty else 0
                delta_life = f"{cw_life - pw_life:.1f} Days"
                st.metric(label="Current Week Avg Lifespan", value=f"{int(cw_life)} Days", delta=delta_life)
                
            with c_meta3:
                cw_emp = jobs_cw["COMPANY"].nunique() if not jobs_cw.empty else 0
                pw_emp = jobs_pw["COMPANY"].nunique() if not jobs_pw.empty else 0
                delta_emp = cw_emp - pw_emp
                st.metric(label="Active Recruiting Companies", value=f"{cw_emp} Firms", delta=f"{delta_emp} vs Pre-Week")

        # --- SUB-SECTION 2: SKILLS DEMAND SHIFT ---
        with st.container(border=True):
            st.markdown("#### 💡 Tech-Stack Demand: Current Week vs Previous Week")
            
            tech_cw = filtered_tech[filtered_tech["JOB_ID"].isin(jobs_cw["JOB_ID"])]["TECH_NAME"].value_counts().reset_index()
            tech_cw.columns = ["Technology", "Current Week"]
            
            tech_pw = filtered_tech[filtered_tech["JOB_ID"].isin(jobs_pw["JOB_ID"])]["TECH_NAME"].value_counts().reset_index()
            tech_pw.columns = ["Technology", "Previous Week"]
            
            df_tech_comp = pd.merge(tech_cw, tech_pw, on="Technology", how="outer").fillna(0)
            fallback_labels = ["Classic Tools / Open Research", "Klassische Tools / Open Research", "Klassische Tools / Offene Recherche"]
            df_tech_comp = df_tech_comp[~df_tech_comp["Technology"].isin(fallback_labels)]
            
            df_tech_comp["Total_Weight"] = df_tech_comp["Current Week"] + df_tech_comp["Previous Week"]
            df_tech_comp = df_tech_comp.sort_values(by="Total_Weight", ascending=False).head(12).drop(columns=["Total_Weight"])
            
            if not df_tech_comp.empty:
                df_tech_melted = df_tech_comp.melt(id_vars=["Technology"], value_vars=["Current Week", "Previous Week"], var_name="Period", value_name="Mentions")
                
                chart_tech_comp = alt.Chart(df_tech_melted).mark_bar().encode(
                    x=alt.X("Technology:N", sort="-y", title="Detected Technology"),
                    y=alt.Y("Mentions:Q", title="Number of Mentions"),
                    xOffset="Period:N",
                    color=alt.Color("Period:N", scale=alt.Scale(domain=["Current Week", "Previous Week"], range=[MAIN_THEME_COLOR, CONCEPT_THEME_COLOR])),
                    tooltip=["Technology", "Period", "Mentions"]
                ).properties(height=340)
                st.altair_chart(chart_tech_comp, use_container_width=True)
            else:
                st.info("Insufficient technology instances to compute cross-week analytics.")

        # --- SUB-SECTION 3: GEOGRAPHIC DISTRIBUTION SHIFT ---
        with st.container(border=True):
            st.markdown("#### 📍 Regional Hotspots: Current Week vs Previous Week")
            
            city_cw = jobs_cw["LOCATION"].value_counts().reset_index()
            city_cw.columns = ["City", "Current Week"]
            
            city_pw = jobs_pw["LOCATION"].value_counts().reset_index()
            city_pw.columns = ["City", "Previous Week"]
            
            df_city_comp = pd.merge(city_cw, city_pw, on="City", how="outer").fillna(0)
            df_city_comp["Total_Weight"] = df_city_comp["Current Week"] + df_city_comp["Previous Week"]
            df_city_comp = df_city_comp.sort_values(by="Total_Weight", ascending=False).head(8).drop(columns=["Total_Weight"])
            
            if not df_city_comp.empty:
                df_city_melted = df_city_comp.melt(id_vars=["City"], value_vars=["Current Week", "Previous Week"], var_name="Period", value_name="Vacancies")
                
                chart_city_comp = alt.Chart(df_city_melted).mark_bar().encode(
                    x=alt.X("City:N", sort="-y", title="Location / City"),
                    y=alt.Y("Vacancies:Q", title="Open Vacancies"),
                    xOffset="Period:N",
                    color=alt.Color("Period:N", scale=alt.Scale(domain=["Current Week", "Previous Week"], range=[MAIN_THEME_COLOR, REGIONAL_THEME_COLOR])),
                    tooltip=["City", "Period", "Vacancies"]
                ).properties(height=340)
                st.altair_chart(chart_city_comp, use_container_width=True)
            else:
                st.info("Insufficient location entries to display geographic trends.")


# ==========================================
# 🔥 TAB 3: ADVANCED DEEP-DIVE ANALYTICS
# ==========================================
with tab_advanced:
    st.markdown("### 🔬 Advanced Multi-Dimensional Insights")
    st.markdown("Deep analytical correlations uncovering cross-tool dependencies, work environments, and career seniority demands.")

    if filtered_jobs.empty or filtered_tech.empty or title_col is None:
        st.info("Insufficient filtered data baseline or missing required title mappings to generate advanced deep-dives.")
    else:
        fallback_labels = ["Classic Tools / Open Research", "Klassische Tools / Open Research", "Klassische Tools / Offene Recherche"]
        
        # ------------------------------------------
        # SECTION 3.1: CORRELATIONS & SENIORITY
        # ------------------------------------------
        with st.container(border=True):
            st.markdown("#### 🧠 Section 1: Tech-Stack Co-Occurrences & Career Track Demands")
            col_adv1, col_adv2 = st.columns(2)
            
            # Heatmap Matrix (Conditional Probability)
            with col_adv1:
                st.markdown("##### 💡 Technology Co-Occurrence Probability Matrix")
                st.markdown("Read as: *'If a job posting requires Tech A, what is the probability (%) that it also demands Tech B?'*")
                
                tech_clean_matrix = filtered_tech[~filtered_tech["TECH_NAME"].isin(fallback_labels)]
                top_10_heatmap_techs = tech_clean_matrix["TECH_NAME"].value_counts().head(10).index.tolist()
                df_heatmap_subset = tech_clean_matrix[tech_clean_matrix["TECH_NAME"].isin(top_10_heatmap_techs)]
                
                if len(top_10_heatmap_techs) > 1:
                    df_pairs = pd.merge(df_heatmap_subset, df_heatmap_subset, on="JOB_ID")
                    df_matrix_coords = df_pairs.groupby(["TECH_NAME_x", "TECH_NAME_y"]).size().reset_index(name="Joint Mentions")
                    df_matrix_coords.columns = ["Tech A", "Tech B", "Joint Mentions"]
                    
                    tech_totals = tech_clean_matrix["TECH_NAME"].value_counts().to_dict()
                    df_matrix_coords["Co-Occurrence Probability (%)"] = df_matrix_coords.apply(
                        lambda r: (r["Joint Mentions"] / tech_totals.get(r["Tech A"], 1)) * 100, axis=1
                    )
                    
                    chart_heatmap = alt.Chart(df_matrix_coords).mark_rect().encode(
                        x=alt.X("Tech B:N", title="Tech B", axis=alt.Axis(labelAngle=-45)),
                        y=alt.Y("Tech A:N", title="If Job Demands: Tech A"),
                        color=alt.Color("Co-Occurrence Probability (%):Q", scale=alt.Scale(scheme="blues"), title="Probability %"),
                        tooltip=["Tech A", "Tech B", alt.Tooltip("Co-Occurrence Probability (%):Q", format=".1f")]
                    ).properties(height=340)
                    st.altair_chart(chart_heatmap, use_container_width=True)
                else:
                    st.info("Not enough distinct technology pairs found to render co-occurrence patterns.")
            
            # Tech Demand by Seniority Tiers
            with col_adv2:
                st.markdown("##### 🎓 Technology Penetration Rate by Seniority Track")
                st.markdown("What percentage of jobs **within each specific seniority level** explicitly demands these top technologies?")
                
                def parse_seniority_tier(title):
                    t = str(title).lower()
                    if any(x in t for x in ["senior", "lead", "principal", "head", "erfahren"]): return "Senior / Lead"
                    elif any(x in t for x in ["junior", "trainee", "einsteiger", "entry"]): return "Junior"
                    else: return "Regular / Mid-Level"
                
                df_jobs_exp = filtered_jobs.copy()
                df_jobs_exp["SENIORITY_LEVEL"] = df_jobs_exp[title_col].apply(parse_seniority_tier)
                
                seniority_baseline_totals = df_jobs_exp["SENIORITY_LEVEL"].value_counts().to_dict()
                
                df_tech_exp = filtered_tech.copy()
                df_tech_exp["SENIORITY_LEVEL"] = df_tech_exp["JOB_ID"].map(df_jobs_exp.set_index("JOB_ID")["SENIORITY_LEVEL"])
                df_tech_exp_clean = df_tech_exp[~df_tech_exp["TECH_NAME"].isin(fallback_labels)].dropna(subset=["SENIORITY_LEVEL"])
                
                top_5_exp_techs = df_tech_exp_clean["TECH_NAME"].value_counts().head(5).index.tolist()
                df_exp_final = df_tech_exp_clean[df_tech_exp_clean["TECH_NAME"].isin(top_5_exp_techs)]
                
                if not df_exp_final.empty:
                    df_grouped_exp = df_exp_final.groupby(["TECH_NAME", "SENIORITY_LEVEL"]).size().reset_index(name="Mentions")
                    
                    df_grouped_exp["Penetration Rate (%)"] = df_grouped_exp.apply(
                        lambda r: (r["Mentions"] / seniority_baseline_totals.get(r["SENIORITY_LEVEL"], 1)) * 100, axis=1
                    )
                    
                    chart_exp = alt.Chart(df_grouped_exp).mark_bar().encode(
                        x=alt.X("TECH_NAME:N", title="Core Technology Stack", axis=alt.Axis(labelAngle=-45)),
                        y=alt.Y("Penetration Rate (%):Q", title="% of Postings Within Tier"),
                        xOffset="SENIORITY_LEVEL:N",
                        color=alt.Color("SENIORITY_LEVEL:N", scale=alt.Scale(
                            domain=["Junior", "Regular / Mid-Level", "Senior / Lead"], 
                            range=["#00dbc2", "#1f77b4", "#ff9f43"]
                        ), legend=alt.Legend(title="Seniority Level")),
                        tooltip=["TECH_NAME", "SENIORITY_LEVEL", alt.Tooltip("Penetration Rate (%):Q", format=".1f")]
                    ).properties(height=340)
                    st.altair_chart(chart_exp, use_container_width=True)
                else:
                    st.info("Seniority context could not be confidently mapped for current technology subset.")

        # ------------------------------------------
        # SECTION 3.2: ENVIRONMENT & STREAM VELOCITY
        # ------------------------------------------
        with st.container(border=True):
            st.markdown("#### 🏠 Section 2: Workplace Flexibility & Ingestion Pipeline Velocity")
            col_adv3, col_adv4 = st.columns(2)
            
            # Workplace Mode
            with col_adv3:
                st.markdown("##### 🏠 Workplace Mode Distribution (Top Cities)")
                st.markdown("100% Normalized representation of on-site vs remote flexibility ratios across major hubs.")
                
                def parse_workplace_flexibility(title):
                    t = str(title).lower()
                    if any(x in t for x in ["remote", "homeoffice", "home office", "telearbeit", "vollremote"]): return "Full Remote"
                    elif any(x in t for x in ["hybrid", "teilzeit ho", "flexibel", "vor ort/ho"]): return "Hybrid"
                    else: return "On-Site / Unspecified"
                
                df_jobs_remote = filtered_jobs.copy()
                df_jobs_remote["WORKPLACE_MODE"] = df_jobs_remote[title_col].apply(parse_workplace_flexibility)
                
                top_8_cities = df_jobs_remote["LOCATION"].value_counts().head(8).index.tolist()
                df_remote_cities = df_jobs_remote[df_jobs_remote["LOCATION"].isin(top_8_cities)]
                
                if not df_remote_cities.empty:
                    df_remote_grouped = df_remote_cities.groupby(["LOCATION", "WORKPLACE_MODE"]).size().reset_index(name="Volume")
                    
                    chart_remote = alt.Chart(df_remote_grouped).mark_bar().encode(
                        x=alt.X("LOCATION:N", title="Geographic Ingestion Hubs", sort="-y", axis=alt.Axis(labelAngle=-45)),
                        y=alt.Y("Volume:Q", title="Percentage Share Matrix", stack="normalize"),
                        color=alt.Color("WORKPLACE_MODE:N", scale=alt.Scale(
                            domain=["Full Remote", "Hybrid", "On-Site / Unspecified"], 
                            range=["#00dbc2", "#1f77b4", "#ff6b6b"]
                        ), legend=alt.Legend(title="Workplace Mode")),
                        tooltip=["LOCATION", "WORKPLACE_MODE", "Volume"]
                    ).properties(height=340)
                    st.altair_chart(chart_remote, use_container_width=True)
                else:
                    st.info("Location geographic dimensions missing from current selection context.")
            
            # 🔥 RECONCILED RE-ENGINEERING: Direct state derivation guarantees 100% mathematical sync
            with col_adv4:
                st.markdown("##### 📈 Cumulative Market Volume vs. Net Daily Change")
                st.markdown("Tracing the **Total Active Cumulative Pool** against the daily **Net Market Change** (Delta of active postings over yesterday).")
                
                if "CLEAN_START_DATE" in filtered_jobs.columns and last_seen_col in filtered_jobs.columns:
                    first_ingest_date = df_jobs["FIRST_SEEN"].min().date()
                    start_timeline = first_ingest_date + pd.Timedelta(days=1)
                    end_timeline = filtered_jobs["CLEAN_START_DATE"].max().date()
                    
                    if start_timeline <= end_timeline:
                        # Step 1: Pre-calculate active totals across the entire history range to set the baseline
                        full_range = pd.date_range(start=first_ingest_date, end=end_timeline, freq='D')
                        daily_active_pool = {}
                        
                        for single_day in full_range:
                            t_date = single_day.date()
                            daily_active_pool[t_date] = filtered_jobs[
                                (filtered_jobs["CLEAN_START_DATE"].dt.date <= t_date) & 
                                (filtered_jobs[last_seen_col].dt.date >= t_date)
                            ].shape[0]
                        
                        # Step 2: Extract perfect deltas from start_timeline onwards
                        display_range = pd.date_range(start=start_timeline, end=end_timeline, freq='D')
                        timeline_records = []
                        
                        for single_day in display_range:
                            target_date = single_day.date()
                            prev_date = (single_day - pd.Timedelta(days=1)).date()
                            
                            active_total = daily_active_pool.get(target_date, 0)
                            prev_total = daily_active_pool.get(prev_date, 0)
                            
                            # Flow Net Change is now the exact mathematical derivative of Stock Active Total
                            net_change_volume = active_total - prev_total
                            
                            timeline_records.append({
                                "Timeline_Date": single_day, 
                                "Active_Total": active_total,
                                "Net_Change": net_change_volume
                            })
                        
                        df_timeline_stream = pd.DataFrame(timeline_records)
                        
                        if not df_timeline_stream.empty:
                            chart_active = alt.Chart(df_timeline_stream).mark_area(
                                opacity=0.15, 
                                color=MAIN_THEME_COLOR, 
                                line={'color': MAIN_THEME_COLOR, 'strokeWidth': 1.5}
                            ).encode(
                                x=alt.X("Timeline_Date:T", title="Timeline (Calendar Logs)"),
                                y=alt.Y(
                                    "Active_Total:Q", 
                                    title="Total Active Cumulative Pool",
                                    axis=alt.Axis(titleColor=MAIN_THEME_COLOR, grid=True)
                                ),
                                tooltip=["Timeline_Date:T", alt.Tooltip("Active_Total:Q", title="Total Active Pool")]
                            )
                            
                            chart_net_base = alt.Chart(df_timeline_stream).encode(
                                x=alt.X("Timeline_Date:T", title="Timeline (Calendar Logs)"),
                                y=alt.Y(
                                    "Net_Change:Q", 
                                    title="Net Daily Market Change (Flow Delta)",
                                    axis=alt.Axis(titleColor=CONCEPT_THEME_COLOR, grid=False)
                                )
                            )
                            
                            line_net = chart_net_base.mark_line(color=CONCEPT_THEME_COLOR, strokeWidth=2.5).encode(
                                tooltip=["Timeline_Date:T", alt.Tooltip("Net_Change:Q", title="Net Market Change")]
                            )
                            
                            zero_rule = alt.Chart(pd.DataFrame([{"zero": 0}])).mark_rule(
                                color="#555555", 
                                strokeDash=[4, 4],
                                strokeWidth=1.5
                            ).encode(y="zero:Q")
                            
                            chart_net_combined = alt.layer(line_net, zero_rule)
                            chart_timeline = alt.layer(chart_active, chart_net_combined).resolve_scale(
                                y='independent'
                            ).properties(height=340).interactive()
                            
                            st.altair_chart(chart_timeline, use_container_width=True)
                        else:
                            st.info("Insufficient operational timeline logs available.")
                    else:
                        st.info("Timeline range calculation underflow (Not enough days after full load setup).")
                else:
                    st.info("Timeline computation columns missing from the target Gold Schema layers.")