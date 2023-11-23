{{ config (
    alias = target.database + '_salesforce_performance'
)}}

SELECT date_trunc('day', create_date :: date) :: date as date, 
        'day' as date_granularity, 
        preferred_clinic,
        lead_source,
        Lead_status,
        disqualification_reason,
        coalesce(count(distinct lead_id),0) as leads
FROM {{ source('gsheet_raw','salesforce_performance') }}
group by 1,2,3,4,5,6

UNION ALL

SELECT date_trunc('week', create_date :: date) :: date as date, 
        'week' as date_granularity, 
        preferred_clinic,
        lead_source,
        Lead_status,
        disqualification_reason,
        coalesce(count(distinct lead_id),0) as leads
FROM {{ source('gsheet_raw','salesforce_performance') }}
group by 1,2,3,4,5,6

UNION ALL

SELECT date_trunc('month', create_date :: date) :: date as date, 
        'month' as date_granularity, 
        preferred_clinic,
        lead_source,
        Lead_status,
        disqualification_reason,
        coalesce(count(distinct lead_id),0) as leads
FROM {{ source('gsheet_raw','salesforce_performance') }}
group by 1,2,3,4,5,6

UNION ALL

SELECT date_trunc('quarter', create_date :: date) :: date as date, 
        'quarter' as date_granularity, 
        preferred_clinic,
        lead_source,
        Lead_status,
        disqualification_reason,
        coalesce(count(distinct lead_id),0) as leads
FROM {{ source('gsheet_raw','salesforce_performance') }}
group by 1,2,3,4,5,6

UNION ALL

SELECT date_trunc('year', create_date :: date) :: date as date, 
        'year' as date_granularity, 
        preferred_clinic,
        lead_source,
        Lead_status,
        disqualification_reason,
        coalesce(count(distinct lead_id),0) as leads
FROM {{ source('gsheet_raw','salesforce_performance') }}
group by 1,2,3,4,5,6
