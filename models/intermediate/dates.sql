{{ 
    config(
		materialized='table',
		tags=["fact"]
    ) 
}}
WITH dates AS (
    {{ 
        dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2015-01-01' as date)",
        end_date="cast('2025-01-01' as date)" )
    }}
)

SELECT
    date_day,
    YEAR(date_day) AS year_actual,
    MONTH(date_day) AS month_actual,
    CASE WHEN month_actual >= 7
        THEN year_actual
        ELSE (year_actual - 1)
    END AS fiscal_year,
    CASE
        WHEN month_actual BETWEEN 7 AND 9 THEN 'Q1'
        WHEN month_actual BETWEEN 10 AND 12 THEN 'Q2'
        WHEN month_actual BETWEEN 1 AND 3 THEN 'Q3'
        WHEN month_actual BETWEEN 4 AND 6 THEN 'Q4'
    END AS fiscal_quarter,
    CASE WHEN month_actual >= 7
        THEN CONCAT(year_actual, '-', (year_actual + 1))
        ELSE CONCAT((year_actual - 1), '-', year_actual)
    END AS fiscal_period
FROM dates
