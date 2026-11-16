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
