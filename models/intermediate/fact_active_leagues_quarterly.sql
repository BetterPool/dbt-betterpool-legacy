{{ 
    config(
		materialized='table',
        	alias='FACT_ACTIVE_LEAGUES_QUARTERLY',
		tags=["fact"]
    ) 
}}

WITH FINAL AS (
    SELECT
        FISCAL_PERIOD,
        FISCAL_QUARTER,
        LEAGUE_CATEGORY,
	SOURCE,
        COUNT(DISTINCT LEAGUE_ID) AS TOTAL_ACTIVE_LEAGUES
    FROM {{ ref('fact_active_entries') }}
    GROUP BY 1, 2, 3, 4

)

SELECT * FROM FINAL
