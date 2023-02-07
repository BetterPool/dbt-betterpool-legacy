{{ 
    config(
		materialized='table',
        	alias='FACT_ACTIVE_LEAGUES_DAILY',
		tags=["fact"]
    ) 
}}
WITH FINAL AS(
    SELECT
        DATE_TRUNC('DAY', PICK_DATE::DATE) AS ACTIVE_DAY,
        LEAGUE_ID,
        LEAGUE_CATEGORY,
        LEAGUE_TYPE,
        FISCAL_PERIOD,
        FISCAL_QUARTER,
        COUNT(*) AS PICKS,
        COUNT(DISTINCT MEMBER_EMAIL) AS ACTIVE_USERS,
        SOURCE
    FROM {{ ref('fact_active_entries') }} AS P
    GROUP BY 1, 2, 3, 4, 5, 6, 9

)    

SELECT * FROM FINAL
