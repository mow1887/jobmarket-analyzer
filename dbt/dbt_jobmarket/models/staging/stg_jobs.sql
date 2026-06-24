WITH raw_jobs AS (
    SELECT * FROM jobmarket_db.silver.v_stg_jobs
)

SELECT 
    job_id,
    job_title,
    company,
    location AS original_location,
    experience_level,
    salary,
    first_seen,
    publication_date,
    last_seen,
    job_link,
    technologies,

    -- ALLE Anforderungen aus dem Scraper freischalten:
    matches_wirtschaftsinformatik,
    matches_informatik,
    matches_mathematik_statistik,
    verlangt_studium,
    requires_phd,
    eu_ai_act_relevant,
    data_governance_required,
    focuses_on_data_quality,

    -- Geo-Bereinigung
    REGEXP_SUBSTR(location, '\\b\\d{4,5}\\b') AS postal_code,
    TRIM(REGEXP_REPLACE(location, '\\b\\d{4,5}\\b', '')) AS clean_city,
    
    CASE 
        WHEN LENGTH(REGEXP_SUBSTR(location, '\\b\\d{4,5}\\b')) = 5 THEN 'Deutschland'
        WHEN LENGTH(REGEXP_SUBSTR(location, '\\b\\d{4,5}\\b')) = 4 AND REGEXP_SUBSTR(location, '\\b\\d{4,5}\\b') LIKE ANY ('1%', '2%', '3%', '4%', '5%', '6%', '7%') THEN 'Österreich'
        WHEN LENGTH(REGEXP_SUBSTR(location, '\\b\\d{4,5}\\b')) = 4 THEN 'Schweiz'
        ELSE 'Deutschland'
    END AS country

FROM raw_jobs
QUALIFY ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY last_seen DESC) = 1