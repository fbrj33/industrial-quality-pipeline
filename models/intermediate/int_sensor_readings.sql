
WITH production_events AS (

    SELECT *
    FROM {{ ref('stg_production_events') }}

),

sensor_readings AS (

    SELECT event_id, event_timestamp, unit_id, quality_label,
           quality_status, 'sensor_060' AS sensor_name,
           sensor_060 AS sensor_value
    FROM production_events

    UNION ALL

    SELECT event_id, event_timestamp, unit_id, quality_label,
           quality_status, 'sensor_104', sensor_104
    FROM production_events

    UNION ALL

    SELECT event_id, event_timestamp, unit_id, quality_label,
           quality_status, 'sensor_130', sensor_130
    FROM production_events

    UNION ALL

    SELECT event_id, event_timestamp, unit_id, quality_label,
           quality_status, 'sensor_029', sensor_029
    FROM production_events

    UNION ALL

    SELECT event_id, event_timestamp, unit_id, quality_label,
           quality_status, 'sensor_511', sensor_511
    FROM production_events

    UNION ALL

    SELECT event_id, event_timestamp, unit_id, quality_label,
           quality_status, 'sensor_317', sensor_317
    FROM production_events

    UNION ALL

    SELECT event_id, event_timestamp, unit_id, quality_label,
           quality_status, 'sensor_125', sensor_125
    FROM production_events

    UNION ALL

    SELECT event_id, event_timestamp, unit_id, quality_label,
           quality_status, 'sensor_126', sensor_126
    FROM production_events

)

SELECT
    event_id,
    event_timestamp,
    unit_id,
    quality_label,
    quality_status,
    sensor_name,
    sensor_value
FROM sensor_readings
WHERE sensor_value IS NOT NULL
