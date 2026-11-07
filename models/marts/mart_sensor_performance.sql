
SELECT
    sensor_name,

    COUNT(*) AS total_readings,

    ROUND(AVG(sensor_value)::numeric, 4) AS average_reading,

    ROUND(STDDEV(sensor_value)::numeric, 4) AS standard_deviation,

    ROUND(MIN(sensor_value)::numeric, 4) AS minimum_reading,

    ROUND(MAX(sensor_value)::numeric, 4) AS maximum_reading,

    COUNT(*) FILTER (
        WHERE quality_status = 'pass'
    ) AS passed_unit_readings,

    COUNT(*) FILTER (
        WHERE quality_status = 'fail'
    ) AS failed_unit_readings,

    ROUND(
        AVG(sensor_value) FILTER (
            WHERE quality_status = 'pass'
        )::numeric,
        4
    ) AS average_pass_reading,

    ROUND(
        AVG(sensor_value) FILTER (
            WHERE quality_status = 'fail'
        )::numeric,
        4
    ) AS average_fail_reading

FROM {{ ref('int_sensor_readings') }}

GROUP BY sensor_name
