{{ 
    config(
        materialized='table', 
        alias='FACT_MEMBER_LEAGUE_JOINED',
	tags=["fact"]
    ) 
}}

WITH FINAL_RYP AS(

    SELECT 
        M.MEMBER_EMAIL,
        L.LEAGUE_ID,
        L.LEAGUE_CATEGORY,
        L.LEAGUE_TYPE,
        T.DATE_JOINED::DATE AS DATE_JOINED,
	ROW_NUMBER() OVER (PARTITION BY M.MEMBER_EMAIL ORDER BY T.DATE_JOINED) AS LEAGUE_JOINED_SEQUENCE,
	'RYP' AS SOURCE
    FROM {{ ref('stg_ryp_members') }} M
            JOIN {{ ref('stg_ryp_teams') }} T ON M.MEMBER_ID = T.MEMBER_ID
            JOIN {{ ref('stg_ryp_leagues') }} L ON T.LEAGUE_ID = L.LEAGUE_ID
            JOIN {{ ref('stg_ryp_league_types') }} LT ON L.LEAGUE_TYPE_ID = LT.LEAGUE_TYPE_ID
),

FINAL_OFP AS (


    SELECT 
        M.MEMBER_EMAIL,
        MJ.LEAGUE_ID,
        L.LEAGUE_CATEGORY,
        L.LEAGUE_TYPE,
        MJ.DATE_JOINED::DATE AS DATE_JOINED,
	ROW_NUMBER() OVER (PARTITION BY M.MEMBER_EMAIL ORDER BY MJ.DATE_JOINED) AS LEAGUE_JOINED_SEQUENCE,
	'OFP' AS SOURCE
    FROM {{ ref('stg_ofp_pool_members') }} MJ
	     JOIN {{ ref('stg_ofp_members') }} M ON MJ.MEMBER_ID = M.MEMBER_ID
	     JOIN {{ ref('stg_ofp_pools') }} L ON MJ.LEAGUE_ID = L.POOL_ID

)

SELECT * FROM FINAL_RYP
UNION 
SELECT * FROM FINAL_OFP
