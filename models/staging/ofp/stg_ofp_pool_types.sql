{{ 
    config(
        materialized='table', 
        alias='STG_OFP_POOLTYPES',
	tags=["stg-ofp"]
    ) 
}}

WITH FINAL AS (
    SELECT
		CASE WHEN POOLTYPEID IN (1,2,3,4,20,31) THEN 'PICK ''EM' ELSE
		POOLTYPE END AS POOL_TYPE,
		POOLTYPEID AS POOL_TYPE_ID,
		m.SPORTID AS SPORT_ID,
		AVAILABLE AS AVAILABLE,
		LONGDESC AS LONG_DESC,
		ORDERBY AS ORDER_BY
    FROM
        {{ source('OFP_DBO', 'OFPPOOLTYPES') }} m
	LEFT JOIN {{ ref('stg_ofp_allsports') }} S ON M.SPORTID = S.SPORT_ID
)

SELECT * FROM FINAL


