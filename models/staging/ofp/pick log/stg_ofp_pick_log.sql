{{ 
    config(
        materialized='table', 
        alias='STG_OFP_AUDIT_PICKS',
	tags=["stg-ofp"]
    ) 
}}

WITH FINAL_PICKS AS (
    SELECT DISTINCT
		IFNULL(B.ALIAS_OF_ID,A.MEMBERID) AS USER_ID,
		A.MEMBERID AS MEMBER_ID,
	        D.POOL_TYPE,
		A.POOLID AS POOL_ID,	
		PICKTIME AS PICK_DATE,
		CA.DESCRIPTION AS LEAGUE_CATEGORY,
		'OFP' AS SOURCE
    FROM
        {{ source('OFP_DBO', 'OFPAUDITPICKS') }} A
        	JOIN {{ ref('stg_ofp_members') }} B on A.MEMBERID = B.MEMBER_ID
        	JOIN {{ ref('stg_ofp_pools') }} C on A.POOLID = C.POOL_ID
        	JOIN {{ ref('stg_ofp_allsports') }} CA on C.SPORT_ID = CA.SPORT_ID
        	JOIN {{ ref('stg_ofp_pool_types') }} D on C.POOL_TYPE_ID = D.POOL_TYPE_ID
UNION

    SELECT DISTINCT
		IFNULL(B.ALIAS_OF_ID,A.MEMBERID) AS USER_ID,
		A.MEMBERID AS MEMBER_ID,
	        D.POOL_TYPE,
		A.POOLID AS POOL_ID,	
		PICKTIME AS PICK_DATE,
		CA.DESCRIPTION AS LEAGUE_CATEGORY,
		'OFP' AS SOURCE
    FROM
        {{ source('OFP_DBO', 'OFPAUDITPICKS') }} A
        	JOIN {{ ref('stg_ofp_members') }} B on A.MEMBERID = B.MEMBER_ID
        	JOIN {{ ref('stg_ofp_pools') }} C on A.POOLID = C.POOL_ID
        	JOIN {{ ref('stg_ofp_allsports') }} CA on C.SPORT_ID = CA.SPORT_ID
        	JOIN {{ ref('stg_ofp_pool_types') }} D on C.POOL_TYPE_ID = D.POOL_TYPE_ID
UNION

    SELECT DISTINCT
		IFNULL(B.ALIAS_OF_ID,A.MEMBERID) AS USER_ID,
		A.MEMBERID AS MEMBER_ID,
	        D.POOL_TYPE,
		A.POOLID AS POOL_ID,	
		PICKTIME AS PICK_DATE,
		CA.DESCRIPTION AS LEAGUE_CATEGORY,
		'OFP' AS SOURCE
    FROM
        {{ source('OFPARCHIVE_DBO', 'OFPAUDITPICKS') }} A
        	JOIN {{ ref('stg_ofp_members') }} B on A.MEMBERID = B.MEMBER_ID
        	JOIN {{ ref('stg_ofp_pools') }} C on A.POOLID = C.POOL_ID
        	JOIN {{ ref('stg_ofp_allsports') }} CA on C.SPORT_ID = CA.SPORT_ID
        	JOIN {{ ref('stg_ofp_pool_types') }} D on C.POOL_TYPE_ID = D.POOL_TYPE_ID
),

FINAL AS (
    SELECT EMAIL AS MEMBER_EMAIL, F.* 
    FROM FINAL_PICKS F
    JOIN {{ source('OFP_DBO', 'ALLMEMBERS') }} B on F.USER_ID = B.MEMBERID
)

SELECT * FROM FINAL