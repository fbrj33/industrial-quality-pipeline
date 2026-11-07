
SELECT
    CAST(event_timestamp AS DATE) AS anomaly_date,
    sensor_name,
    anomaly_type,
    deviation_type,

    COUNT(*) AS anomaly_count,

    COUNT(DISTINCT event_id) AS affected_units,

    MIN(event_timestamp) AS first_anomaly_timestamp,

    MAX(event_timestamp) AS last_anomaly_timestamp

FROM {{ ref('stg_sensor_anomalies') }}

GROUP BY
    CAST(event_timestamp AS DATE),
    sensor_name,
    anomaly_type,
    deviation_type
