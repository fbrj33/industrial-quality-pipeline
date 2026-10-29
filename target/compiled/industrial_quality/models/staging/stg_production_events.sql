SELECT
event_id,
event_timestamp,
unit_id,

sensor_060,
sensor_104,
sensor_130,
sensor_029,
sensor_511,
sensor_317,
sensor_125,
sensor_126,

quality_label,

CASE
   WHEN quality_label = -1 THEN 'pass'
    WHEN quality_label = 1 THEN 'fail'
    ELSE 'unknown'
 END AS quality_status,

ingested_at
FROM public.production_events