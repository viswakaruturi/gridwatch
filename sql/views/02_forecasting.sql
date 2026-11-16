CREATE OR REPLACE VIEW vw_forecast_vs_actual AS
SELECT
    actual.respondent,
    actual.respondent_name,
    actual.period,
    actual.value AS actual_demand,
    forecast.value AS forecast_demand,
    (actual.value - forecast.value) AS signed_error,
    ABS(actual.value - forecast.value) AS absolute_error,
    ROUND(
        ABS(actual.value - forecast.value) * 100.0 / NULLIF(actual.value, 0),
        2
    ) AS percentage_error
FROM
    electricity AS actual
    INNER JOIN electricity AS forecast ON actual.period = forecast.period
    AND actual.respondent = forecast.respondent
WHERE
    actual.type = 'D'
    AND forecast.type = 'DF';

CREATE OR REPLACE VIEW vw_forecast_accuracy_summary AS
SELECT
    respondent,
    respondent_name,
    COUNT(*) AS total_forecasts,
    ROUND(AVG(absolute_error), 2) AS mae_mwh,
    ROUND(SQRT(AVG(POWER(signed_error, 2))), 2) AS rmse_mwh,
    ROUND(AVG(percentage_error), 2) AS mape_percentage,
    ROUND(AVG(signed_error), 2) AS mean_signed_error_mwh
FROM
    vw_forecast_vs_actual
GROUP BY
    respondent,
    respondent_name;
