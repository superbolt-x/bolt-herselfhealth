{{ config (
    alias = target.database + '_yelp_performance'
)}}

SELECT DATE_TRUNC('day', date::date) as date, 
    'day' as date_granularity, 
    COALESCE(SUM(spend),0) as spend,
    COALESCE(SUM(clicks),0) as clicks,
    COALESCE(SUM(leads),0) as leads
FROM {{ source('gsheet_raw','yelp_daily_spend') }}
GROUP BY 1,2

UNION ALL

SELECT DATE_TRUNC('week', date::date) as date, 
    'week' as date_granularity, 
    COALESCE(SUM(spend),0) as spend,
    COALESCE(SUM(clicks),0) as clicks,
    COALESCE(SUM(leads),0) as leads
FROM {{ source('gsheet_raw','yelp_daily_spend') }}
GROUP BY 1,2

UNION ALL

SELECT DATE_TRUNC('month', date::date) as date, 
    'month' as date_granularity, 
    COALESCE(SUM(spend),0) as spend,
    COALESCE(SUM(clicks),0) as clicks,
    COALESCE(SUM(leads),0) as leads
FROM {{ source('gsheet_raw','yelp_daily_spend') }}
GROUP BY 1,2

UNION ALL

SELECT DATE_TRUNC('quarter', date::date) as date, 
    'quarter' as date_granularity, 
    COALESCE(SUM(spend),0) as spend,
    COALESCE(SUM(clicks),0) as clicks,
    COALESCE(SUM(leads),0) as leads
FROM {{ source('gsheet_raw','yelp_daily_spend') }}
GROUP BY 1,2

UNION ALL

SELECT DATE_TRUNC('year', date::date) as date, 
    'year' as date_granularity, 
    COALESCE(SUM(spend),0) as spend,
    COALESCE(SUM(clicks),0) as clicks,
    COALESCE(SUM(leads),0) as leads
FROM {{ source('gsheet_raw','yelp_daily_spend') }}
GROUP BY 1,2
