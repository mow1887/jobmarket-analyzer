{{ config(materialized='table') }}

with staging as (
    select * from {{ ref('stg_jobs') }}
)

select
    job_id,
    job_title,
    company,
    clean_city as location,
    country,
    first_seen,
    publication_date,
    last_seen, -- Added to track posting lifespans in downstream BI tools
    experience_level,
    
    -- Automated clustering of target data job roles
    case
        when lower(job_title) like '%analytics engineer%' then 'Analytics Engineer'
        when lower(job_title) like '%data engineer%' or lower(job_title) like '%dateningenieur%' or lower(job_title) like '%cloud engineer%' then 'Data Engineer'
        when lower(job_title) like '%data scientist%' or lower(job_title) like '%data science%' or lower(job_title) like '%machine learning%' or lower(job_title) like '%ml%' then 'Data Scientist'
        when lower(job_title) like '%analyst%' or lower(job_title) like '%analytics%' or lower(job_title) like '%bi%' or lower(job_title) like '%business intelligence%' then 'Data Analyst / BI Specialist'
        else 'Other Data Professional'
    end as job_role,
    
    -- Baseline metric for easier counting in BI tools
    1 as posting_count
from staging