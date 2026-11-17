-- 1
CREATE OR REPLACE VIEW vw_generation_demand_balance AS
SELECT
    actual.period,
    actual.respondent,
    actual.respondent_name,
    actual.value AS demand_mwh,
    generation.value AS generation_mwh,
    interchange.value AS interchange_mwh,
    (generation.value - actual.value) AS generation_minus_demand_mwh,
    (
        generation.value - interchange.value - actual.value
    ) AS balance_residual_mwh
FROM
    electricity AS actual
    LEFT JOIN electricity AS generation ON actual.period = generation.period
    AND actual.respondent = generation.respondent
    AND generation.type = 'NG'
    LEFT JOIN electricity AS interchange ON actual.period = interchange.period
    AND actual.respondent = interchange.respondent
    AND interchange.type = 'TI'
WHERE
    actual.type = 'D';

-- 2
CREATE OR REPLACE VIEW vw_interchange_patterns AS
WITH
    interchange_stats AS (
        SELECT
            period,
            respondent,
            respondent_name,
            value AS interchange_mwh,
            CASE
                WHEN value > 0 THEN 'Net Export'
                WHEN value < 0 THEN 'Net Import'
                ELSE 'Balanced'
            END AS flow_direction,
            LAG(value) OVER (
                PARTITION BY
                    respondent
                ORDER BY
                    period
            ) AS previous_interchange_mwh,
            LAG(period) OVER (
                PARTITION BY
                    respondent
                ORDER BY
                    period
            ) AS previous_period,
            AVG(value) OVER (
                PARTITION BY
                    respondent
                ORDER BY
                    period ROWS BETWEEN 23 PRECEDING
                    AND CURRENT ROW
            ) AS rolling_avg,
            COUNT(*) OVER (
                PARTITION BY
                    respondent
                ORDER BY
                    period ROWS BETWEEN 23 PRECEDING
                    AND CURRENT ROW
            ) AS rolling_count,
            MIN(period) OVER (
                PARTITION BY
                    respondent
                ORDER BY
                    period ROWS BETWEEN 23 PRECEDING
                    AND CURRENT ROW
            ) AS rolling_start
        FROM
            electricity
        WHERE
            type = 'TI'
    )
SELECT
    period,
    respondent,
    respondent_name,
    interchange_mwh,
    flow_direction,
    CASE
        WHEN period - previous_period = INTERVAL '1 hour' THEN previous_interchange_mwh
        ELSE NULL
    END AS previous_interchange_mwh,
    CASE
        WHEN period - previous_period = INTERVAL '1 hour' THEN interchange_mwh - previous_interchange_mwh
        ELSE NULL
    END AS interchange_change_mwh,
    CASE
        WHEN rolling_count = 24
        AND period - rolling_start = INTERVAL '23 hours' THEN ROUND(rolling_avg, 2)
        ELSE NULL
    END AS rolling_24h_avg_mwh
FROM
    interchange_stats;

-- 3
CREATE OR REPLACE VIEW vw_latest_grid_snapshot AS
WITH
    latest_demand AS (
        SELECT
            period,
            respondent,
            respondent_name,
            value AS demand_mwh,
            ROW_NUMBER() OVER (
                PARTITION BY
                    respondent
                ORDER BY
                    period DESC
            ) AS row_num
        FROM
            electricity
        WHERE
            type = 'D'
    )
SELECT
    latest.respondent,
    latest.respondent_name,
    latest.period,
    latest.demand_mwh,
    balance.generation_mwh,
    balance.interchange_mwh,
    forecast.value AS forecast_demand_mwh,
    balance.balance_residual_mwh
FROM
    latest_demand AS latest
    LEFT JOIN vw_generation_demand_balance AS balance ON latest.respondent = balance.respondent
    AND latest.period = balance.period
    LEFT JOIN electricity AS forecast ON latest.respondent = forecast.respondent
    AND latest.period = forecast.period
    AND forecast.type = 'DF'
WHERE
    latest.row_num = 1;
