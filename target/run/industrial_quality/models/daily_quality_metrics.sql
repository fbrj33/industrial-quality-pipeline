
  create view "quality_db"."analytics"."daily_quality_metrics__dbt_tmp"
    
    
  as (
    SELECT
    CAST(event_timestamp AS DATE) AS production_date,

    COUNT(*) AS total_units,

    SUM(CASE
        WHEN quality_label = -1 THEN 1
        ELSE 0
    END) AS passed_units,

    SUM(CASE
        WHEN quality_label = 1 THEN 1
        ELSE 0
    END) AS failed_units,

    ROUND(
        100.0 * SUM(CASE
            WHEN quality_label = 1 THEN 1
            ELSE 0
        END) / NULLIF(COUNT(*), 0),
        2
    ) AS failure_rate_pct,

    ROUND(
        100.0 * SUM(CASE
            WHEN quality_label = -1 THEN 1
            ELSE 0
        END) / NULLIF(COUNT(*), 0),
        2
    ) AS pass_rate_pct

FROM "quality_db"."analytics"."stg_production_events"

GROUP BY CAST(event_timestamp AS DATE)
  );