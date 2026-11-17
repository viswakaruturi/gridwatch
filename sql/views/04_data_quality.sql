-- 1
CREATE OR REPLACE VIEW vw_measurement_coverage AS
SELECT
    (period AT TIME ZONE 'UTC')::date AS measurement_date,
    respondent,
    respondent_name,
    COUNT(*) FILTER (
        WHERE
            type = 'D'
    ) AS demand_hours,
    COUNT(*) FILTER (
        WHERE
            type = 'DF'
    ) AS forecast_hours,
    COUNT(*) FILTER (
        WHERE
            type = 'NG'
    ) AS generation_hours,
    COUNT(*) FILTER (
        WHERE
            type = 'TI'
    ) AS interchange_hours,
    COUNT(*) AS total_measurements
FROM
    electricity
WHERE
    type IN ('D', 'DF', 'NG', 'TI')
GROUP BY
    (period AT TIME ZONE 'UTC')::date,
    respondent,
    respondent_name;

-- 2
CREATE OR REPLACE VIEW vw_missing_hourly_records AS
WITH
    measurement_ranges AS (
        SELECT
            respondent,
            MAX(respondent_name) AS respondent_name,
            type,
            MIN(period) AS first_period,
            MAX(period) AS last_period
        FROM
            electricity
        WHERE
            type IN ('D', 'DF', 'NG', 'TI')
        GROUP BY
            respondent,
            type
    ),
    expected_hours AS (
        SELECT
            ranges.respondent,
            ranges.respondent_name,
            ranges.type,
            hours.expected_period
        FROM
            measurement_ranges AS ranges
            CROSS JOIN LATERAL generate_series(
                ranges.first_period,
                ranges.last_period,
                INTERVAL '1 hour'
            ) AS hours (expected_period)
    )
SELECT
    expected.respondent,
    expected.respondent_name,
    expected.type AS measurement_type,
    expected.expected_period AS missing_period
FROM
    expected_hours AS expected
    LEFT JOIN electricity AS actual ON expected.respondent = actual.respondent
    AND expected.type = actual.type
    AND expected.expected_period = actual.period
WHERE
    actual.period IS NULL;

-- 3
CREATE OR REPLACE VIEW vw_data_freshness AS
WITH
    latest_measurements AS (
        SELECT
            respondent,
            MAX(respondent_name) AS respondent_name,
            type AS measurement_type,
            MAX(period) AS latest_period,
            COUNT(*) AS total_records
        FROM
            electricity
        WHERE
            type IN ('D', 'DF', 'NG', 'TI')
        GROUP BY
            respondent,
            type
    ),
    freshness AS (
        SELECT
            respondent,
            respondent_name,
            measurement_type,
            latest_period,
            total_records,
            MAX(latest_period) OVER () AS database_latest_period
        FROM
            latest_measurements
    )
SELECT
    respondent,
    respondent_name,
    measurement_type,
    latest_period,
    database_latest_period,
    ROUND(
        (
            EXTRACT(
                EPOCH
                FROM
                    (database_latest_period - latest_period)
            ) / 3600.0
        )::numeric,
        2
    ) AS lag_hours,
    total_records,
    CASE
        WHEN latest_period >= database_latest_period - INTERVAL '3 hours' THEN 'Current'
        WHEN latest_period >= database_latest_period - INTERVAL '24 hours' THEN 'Delayed'
        ELSE 'Stale'
    END AS freshness_status
FROM
    freshness;
