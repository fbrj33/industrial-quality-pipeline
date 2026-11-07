
SELECT
    anomaly_id,
    event_id,
    event_timestamp,
    unit_id,
    sensor_name,
    sensor_value,
    anomaly_type,
    deviation_type,
    detected_at
FROM public.sensor_anomalies
