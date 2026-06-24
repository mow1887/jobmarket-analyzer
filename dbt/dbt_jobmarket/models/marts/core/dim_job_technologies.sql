{{ config(materialized='table') }}

-- Clean bridge table leveraging the pre-extracted technologies array from your scraper
with staging as (
    select 
        job_id,
        technologies
    from {{ ref('stg_jobs') }}
    where technologies is not null
)

select
    job_id,
    -- Flattens the JSON array and extracts clean string values
    -- - Handles both native ARRAY types and stringified JSON arrays safely
    trim(flat.value::string) as tech_name
from staging,
lateral flatten(input => parse_json(technologies)) flat