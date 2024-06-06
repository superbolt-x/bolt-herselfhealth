{{ config (
    alias = target.database + '_salesforce_performance'
)}}

WITH data as
    (SELECT create_date::date as date, preferred_clinic, lead_source, COUNT(DISTINCT lead_id) as leads, 0 as appointments
    FROM {{ source('gsheet_raw','salesforce_lead_generation') }}
    GROUP BY 1,2,3
    UNION ALL
    SELECT created_date::date as date, preferred_clinic, person_account_lead_source as lead_source, 0 as leads, COUNT(*) as appointments
    FROM {{ source('gsheet_raw','salesforce_scheduled_visits') }}
    GROUP BY 1,2,3)
    
SELECT DATE_TRUNC('day', date) as date, 'day' as date_granularity, 
    preferred_clinic,
    lead_source,
    COALESCE(SUM(leads),0) as leads,
    COALESCE(SUM(appointments),0) as appointments
FROM data
GROUP BY 1,2,3,4

UNION ALL

SELECT DATE_TRUNC('week', date) as date, 'week' as date_granularity, 
    preferred_clinic,
    lead_source,
    COALESCE(SUM(leads),0) as leads,
    COALESCE(SUM(appointments),0) as appointments
FROM data
GROUP BY 1,2,3,4

UNION ALL

SELECT DATE_TRUNC('month', date) as date, 'month' as date_granularity, 
    preferred_clinic,
    lead_source,
    COALESCE(SUM(leads),0) as leads,
    COALESCE(SUM(appointments),0) as appointments
FROM data
GROUP BY 1,2,3,4

UNION ALL

SELECT DATE_TRUNC('quarter', date) as date, 'quarter' as date_granularity, 
    preferred_clinic,
    lead_source,
    COALESCE(SUM(leads),0) as leads,
    COALESCE(SUM(appointments),0) as appointments
FROM data
GROUP BY 1,2,3,4

UNION ALL

SELECT DATE_TRUNC('year', date) as date, 'year' as date_granularity, 
    preferred_clinic,
    lead_source,
    COALESCE(SUM(leads),0) as leads,
    COALESCE(SUM(appointments),0) as appointments
FROM data
GROUP BY 1,2,3,4
