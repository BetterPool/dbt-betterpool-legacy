{{ 
    config(
        materialized='table', 
        alias='STG_OFP_DELETED_POOLS',
	tags=["stg-ofp"]
    ) 
}}

WITH DELETED AS (
    SELECT
		POOLID AS POOL_ID,
		POOLTYPEID AS POOL_TYPE_ID,
        	PT.POOL_TYPE AS LEAGUE_TYPE,
		SPORTID AS SPORT_ID,
		S.DESCRIPTION AS LEAGUE_CATEGORY,
		DATEDELETED AS DATE_DELETED,
		'OFP' AS SOURCE
    FROM
        {{ source('OFP_DBO', 'OFPDELETEDPOOLS') }} M
	LEFT JOIN {{ ref('stg_ofp_allsports') }} S ON M.SPORTID = S.SPORT_ID
	LEFT JOIN {{ ref('stg_ofp_pool_types') }} PT ON M.POOLTYPEID = PT.POOL_TYPE_ID
)

SELECT * FROM DELETED