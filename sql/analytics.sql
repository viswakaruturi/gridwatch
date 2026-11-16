SELECT
    *
FROM
    electricity
LIMIT
    10;

SELECT
    value,
    value_units,
    period
FROM
    electricity
WHERE
    type = 'D'
ORDER BY
    period DESC
LIMIT
    20;

SELECT DISTINCT
    respondent,
    respondent_name
FROM
    electricity
ORDER BY
    respondent;

SELECT
    type,
    COUNT(*) AS record_count
FROM
    electricity
GROUP BY
    type;

* /
SELECT
    AVG(value) AS average_demand_mwh
FROM
    electricity
WHERE
    type = 'D';

SELECT
    respondent,
    respondent_name,
    AVG(value) AS average_demand
FROM
    electricity
WHERE
    type = 'D'
GROUP BY
    respondent,
    respondent_name
ORDER BY
    average_demand DESC;

SELECT
    respondent,
    respondent_name,
    AVG(value) AS average_demand
FROM
    electricity
WHERE
    type = 'D'
GROUP BY
    respondent,
    respondent_name
ORDER BY
    average_demand DESC
LIMIT
    10;

SELECT
    respondent,
    respondent_name,
    AVG(value) AS average_demand
FROM
    electricity
WHERE
    type = 'D'
GROUP BY
    respondent,
    respondent_name
HAVING
    AVG(value) > 10000
ORDER BY
    average_demand DESC;

* /
SELECT
    actual.period,
    actual.respondent,
    actual.value AS actual_demand,
    forecast.value AS forecast_demand
FROM
    electricity AS actual
    INNER JOIN electricity AS forecast ON actual.period = forecast.period
    AND actual.respondent = forecast.respondent
WHERE
    actual.type = 'D'
    AND forecast.type = 'DF'
LIMIT
    10;

SELECT
    actual.period,
    actual.respondent,
    actual.value AS demand_mwh,
    generation.value AS generation_mwh
FROM
    electricity AS actual
    INNER JOIN electricity AS generation ON actual.period = generation.period
    AND actual.respondent = generation.respondent
WHERE
    actual.type = 'D'
    AND generation.type = 'NG'
ORDER BY
    actual.period DESC
LIMIT
    10;

SELECT
    actual.respondent,
    COUNT(*) AS missing_forecast_count
FROM
    electricity AS actual
    LEFT JOIN electricity AS forecast ON actual.period = forecast.period
    AND actual.respondent = forecast.respondent
    AND forecast.type = 'DF'
WHERE
    actual.type = 'D'
    AND forecast.period IS NULL
GROUP BY
    actual.respondent
ORDER BY
    missing_forecast_count DESC
LIMIT
    10;

WITH
    missing_forecasts AS (
        SELECT
            actual.respondent,
            COUNT(*) AS missing_forecast_count
        FROM
            electricity AS actual
            LEFT JOIN electricity AS forecast ON actual.period = forecast.period
            AND actual.respondent = forecast.respondent
            AND forecast.type = 'DF'
        WHERE
            actual.type = 'D'
            AND forecast.period IS NULL
        GROUP BY
            actual.respondent
    )
SELECT
    respondent,
    missing_forecast_count
FROM
    missing_forecasts
WHERE
    missing_forecast_count > 20
ORDER BY
    missing_forecast_count DESC;
