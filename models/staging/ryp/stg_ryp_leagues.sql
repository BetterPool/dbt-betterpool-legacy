{{ 
    config(
        materialized='table', 
        alias='STG_RYP_LEAGUES',
	tags=["stg-ryp"]
    ) 
}}

WITH LEAGUES AS (
    SELECT
        l.LEAGUE_ID,
        l.LEAGUE_TYPE_ID,
        lt.DESCRIPTION AS LEAGUE_TYPE,
        l.LEAGUE_NAME,
	lc.DATASOURCE_NAME AS LEAGUE_CATEGORY,
        l.LEAGUE_START_SEASON,
        l.LEAGUE_START_DATE,
        l.MESSAGEBOARD_ACTIVE,
        l.PAID,
        l.PAID_TIER,
        l.PAID_ENTRIES,
        l.PAID_ADDITIONAL_MEMBERS,
        l.IS_VIP,
	l.SOURCE_CODE,
        'RYP' AS SOURCE
    FROM
        {{ source('RYP_RUNYOURPOOL_DBO', 'LEAGUES') }} l
    	JOIN {{ source('RYP_RUNYOURPOOL_DBO', 'LEAGUE_TYPES') }} lt ON l.league_type_id = lt.league_type_id
    	JOIN {{ source('RYP_RUNYOURPOOL_DBO', 'LEAGUE_CATEGORIES') }} lc ON lt.league_category_id = lc.league_category_id
)

SELECT * FROM LEAGUES
