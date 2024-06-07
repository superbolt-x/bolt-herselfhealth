{{ config (
    alias = target.database + '_blended_performance'
)}}

WITH data as
    (SELECT 'Facebook' as channel, date::date, date_granularity, 
      COALESCE(SUM(spend),0) as spend, COALESCE(SUM(link_clicks),0) as clicks, 
      0 as sf_leads, 0 as sf_appointments
    FROM {{ source('reporting','facebook_ad_performance') }}
    GROUP BY 1,2,3
    UNION ALL
    SELECT 'Google Ads' as channel, date::date, date_granularity, 
      COALESCE(SUM(spend),0) as spend, COALESCE(SUM(clicks),0) as clicks, 
      0 as sf_leads, 0 as sf_appointments
    FROM {{ source('reporting','googleads_campaign_performance') }}
    GROUP BY 1,2,3
    UNION ALL
    SELECT 'Bing' as channel, date::date, date_granularity, 
      COALESCE(SUM(spend),0) as spend, COALESCE(SUM(clicks),0) as clicks, 
      0 as sf_leads, 0 as sf_appointments
    FROM {{ source('reporting','bingads_campaign_performance') }}
    GROUP BY 1,2,3
    UNION ALL
    SELECT 'Yelp' as channel, date::date, date_granularity, 
      COALESCE(SUM(spend),0) as spend, COALESCE(SUM(clicks),0) as clicks, 
      COALESCE(SUM(leads),0) as sf_leads, 0 as sf_appointments
    FROM {{ source('reporting','yelp_performance') }}
    GROUP BY 1,2,3
    UNION ALL
    SELECT 'Facebook' as channel, date::date, date_granularity, 
      0 as spend, 0 as clicks, 
      COALESCE(SUM(leads),0) as sf_leads, COALESCE(SUM(appointments),0) as sf_appointments
    FROM {{ source('reporting','salesforce_performance') }}
    WHERE lead_source ~* 'Facebook'
    GROUP BY 1,2,3
    UNION ALL
    SELECT 'Google Ads' as channel, date::date, date_granularity, 
      0 as spend, 0 as clicks, 
      COALESCE(SUM(leads),0) as sf_leads, COALESCE(SUM(appointments),0) as sf_appointments
    FROM {{ source('reporting','salesforce_performance') }}
    WHERE lead_source ~* 'Google'
    GROUP BY 1,2,3
    UNION ALL
    SELECT 'Nextdoor' as channel, date::date, date_granularity, 
      0 as spend, 0 as clicks, 
      COALESCE(SUM(leads),0) as sf_leads, COALESCE(SUM(appointments),0) as sf_appointments
    FROM {{ source('reporting','salesforce_performance') }}
    WHERE lead_source ~* 'Nextdoor'
    GROUP BY 1,2,3
    UNION ALL
    SELECT 'Bing' as channel, date::date, date_granularity, 
      0 as spend, 0 as clicks, 
      COALESCE(SUM(leads),0) as sf_leads, COALESCE(SUM(appointments),0) as sf_appointments
    FROM {{ source('reporting','salesforce_performance') }}
    WHERE lead_source ~* 'bing'
    GROUP BY 1,2,3
    UNION ALL
    SELECT 'Website' as channel, date::date, date_granularity, 
      0 as spend, 0 as clicks, 
      COALESCE(SUM(leads),0) as sf_leads, COALESCE(SUM(appointments),0) as sf_appointments
    FROM {{ source('reporting','salesforce_performance') }}
    WHERE lead_source ~* 'Website'
    GROUP BY 1,2,3)
    
    
SELECT channel,
  date,
  date_granularity,
  spend,
  clicks,
  sf_leads,
  sf_appointments
FROM data
