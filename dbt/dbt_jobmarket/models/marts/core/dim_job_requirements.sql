{{ config(materialized='table') }}

-- Clean bridge table for formal requirements using pre-calculated flags from your scraper
with staging as (
    select * from {{ ref('stg_jobs') }}
),

unpivoted_requirements as (
    -- Education Degree Levels
    select job_id, 'Wirtschaftsinformatik' as requirement_name, 'Education' as requirement_category from staging where matches_wirtschaftsinformatik = true
    union all
    select job_id, 'Informatik' as requirement_name, 'Education' as requirement_category from staging where matches_informatik = true
    union all
    select job_id, 'Mathematik/Statistik' as requirement_name, 'Education' as requirement_category from staging where matches_mathematik_statistik = true
    union all
    select job_id, 'Abgeschlossenes Studium' as requirement_name, 'Education' as requirement_category from staging where verlangt_studium = true
    union all
    select job_id, 'Promotion/PhD' as requirement_name, 'Education' as requirement_category from staging where requires_phd = true
    
    -- Governance, Privacy & Compliance
    union all
    select job_id, 'EU AI Act Compliance' as requirement_name, 'Governance & Quality' as requirement_category from staging where eu_ai_act_relevant = true
    union all
    select job_id, 'Data Governance & Privacy' as requirement_name, 'Governance & Quality' as requirement_category from staging where data_governance_required = true
    union all
    select job_id, 'Data Quality & QA' as requirement_name, 'Governance & Quality' as requirement_category from staging where focuses_on_data_quality = true
)

select * from unpivoted_requirements