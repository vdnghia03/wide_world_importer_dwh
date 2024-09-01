With dim_date__source AS (
  SELECT
    *
  FROM
    UNNEST(GENERATE_DATE_ARRAY('2010-01-01', '2030-01-31', INTERVAL 1 DAY)) AS date
)

SELECT
  *
  , FORMAT_DATE('%A', date) AS day_of_week
  , FORMAT_DATE('%a', date) AS day_of_week_short
  , CASE
      WHEN FORMAT_DATE('%A', date) IN ('Saturday', 'Sunday') THEN 'Weekend'
      ELSE 'Weekday'
    END AS is_weekday_or_weekend
  , FORMAT_DATE('%B', date) as month
  , DATE_TRUNC(date, MONTH) as year_month
  , DATE_TRUNC(date, YEAR) as year
  , EXTRACT(YEAR FROM date) AS year_number
FROM dim_date__source