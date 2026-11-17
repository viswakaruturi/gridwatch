-- 1
CREATE OR REPLACE VIEW vw_daily_demand AS
SELECT
       (period AT TIME ZONE 'UTC')::date AS demand_date,
       respondent,
       respondent_name,
       SUM(value) AS total_demand_mwh,
       ROUND(AVG(value), 2) AS average_demand_mwh,
       MAX(value) AS peak_demand_mwh,
       MIN(value) AS minimum_demand_mwh,
       COUNT(*) AS reported_hours
FROM
       electricity
WHERE
       type = 'D'
GROUP BY
       demand_date,
       respondent,
       respondent_name;

-- 2
CREATE OR REPLACE VIEW vw_hourly_demand_patterns AS
SELECT
       respondent,
       respondent_name,
       EXTRACT(
              HOUR
              FROM
                     period AT TIME ZONE 'UTC'
       ) AS hour_of_day,
       ROUND(AVG(value), 2) AS average_demand_mwh,
       MIN(value) AS minimum_demand_mwh,
       MAX(value) AS maximum_demand_mwh,
       COUNT(*) AS observation_count
FROM
       electricity
WHERE
       type = 'D'
GROUP BY
       respondent,
       respondent_name,
       hour_of_day;

-- 3
CREATE OR REPLACE VIEW vw_peak_demand_events AS
WITH
       ranked_demand AS (
              SELECT
                     respondent,
                     respondent_name,
                     period,
                     value AS demand_mwh,
                     ROW_NUMBER() OVER (
                            PARTITION BY
                                   respondent
                            ORDER BY
                                   value DESC,
                                   period DESC
                     ) AS demand_rank
              FROM
                     electricity
              WHERE
                     type = 'D'
       )
SELECT
       respondent,
       respondent_name,
       period,
       demand_mwh,
       demand_rank
FROM
       ranked_demand
WHERE
       demand_rank <= 5
ORDER BY
       respondent,
       demand_rank;

-- 4
CREATE OR REPLACE VIEW vw_demand_trends AS
WITH
       hourly_difference AS (
              SELECT
                     respondent,
                     respondent_name,
                     period,
                     value AS demand_mwh,
                     LAG(value) OVER (
                            PARTITION BY
                                   respondent
                            ORDER BY
                                   period
                     ) AS previous_hour_demand_mwh,
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
                     ) AS rolling_average,
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
                     type = 'D'
       )
SELECT
       respondent,
       respondent_name,
       period,
       demand_mwh,
       CASE
              WHEN period - previous_period = INTERVAL '1 hour' THEN previous_hour_demand_mwh
              ELSE NULL
       END AS previous_hour_demand_mwh,
       CASE
              WHEN period - previous_period = INTERVAL '1 hour' THEN demand_mwh - previous_hour_demand_mwh
              ELSE NULL
       END AS hourly_change_mwh,
       CASE
              WHEN period - previous_period = INTERVAL '1 hour' THEN ROUND(
                     (demand_mwh - previous_hour_demand_mwh) * 100.0 / NULLIF(previous_hour_demand_mwh, 0),
                     2
              )
              ELSE NULL
       END AS hourly_change_percent,
       CASE
              WHEN rolling_count = 24
              AND period - rolling_start = INTERVAL '23 hours' THEN ROUND(rolling_average, 2)
              ELSE NULL
       END AS rolling_24h_avg_mwh
FROM
       hourly_difference;

-- 5
CREATE OR REPLACE VIEW vw_weekday_weekend_demand AS
SELECT
       respondent,
       respondent_name,
       CASE
              WHEN EXTRACT(
                     DOW
                     FROM
                            period AT TIME ZONE 'UTC'
              ) IN (0, 6) THEN 'Weekend'
              ELSE 'Weekday'
       END AS day_type,
       ROUND(AVG(value), 2) AS average_demand_mwh,
       MIN(value) AS minimum_demand_mwh,
       MAX(value) AS maximum_demand_mwh,
       COUNT(*) AS observation_count
FROM
       electricity
WHERE
       type = 'D'
GROUP BY
       respondent,
       respondent_name,
       day_type;

-- 6
CREATE OR REPLACE VIEW vw_demand_anomalies AS
WITH
       demand_stats AS (
              SELECT
                     respondent,
                     respondent_name,
                     period,
                     value AS demand_mwh,
                     AVG(value) OVER (
                            PARTITION BY
                                   respondent,
                                   EXTRACT(
                                          HOUR
                                          FROM
                                                 period AT TIME ZONE 'UTC'
                                   )
                     ) AS mean_demand_mwh,
                     STDDEV_SAMP(value) OVER (
                            PARTITION BY
                                   respondent,
                                   EXTRACT(
                                          HOUR
                                          FROM
                                                 period AT TIME ZONE 'UTC'
                                   )
                     ) AS stddev_demand_mwh
              FROM
                     electricity
              WHERE
                     type = 'D'
       ),
       demand_zscores AS (
              SELECT
                     respondent,
                     respondent_name,
                     period,
                     demand_mwh,
                     ROUND(mean_demand_mwh, 2) AS mean_demand_mwh,
                     ROUND(stddev_demand_mwh, 2) AS stddev_demand_mwh,
                     ROUND(
                            (demand_mwh - mean_demand_mwh) / NULLIF(stddev_demand_mwh, 0),
                            2
                     ) AS z_score
              FROM
                     demand_stats
       )
SELECT
       respondent,
       respondent_name,
       period,
       demand_mwh,
       mean_demand_mwh,
       stddev_demand_mwh,
       z_score,
       CASE
              WHEN z_score > 2 THEN 'High Demand'
              WHEN z_score < -2 THEN 'Low Demand'
              WHEN z_score IS NULL THEN 'Insufficient Variation'
              ELSE 'Normal'
       END AS anomaly_type
FROM
       demand_zscores;