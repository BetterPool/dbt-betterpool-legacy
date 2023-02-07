{{ 
    config(
        materialized='table', 
        alias='STG_OFP_POOL_MEMBERS',
	tags=["stg-ofp"]
    ) 
}}

WITH FINAL AS (
    SELECT
		MEMBERID AS MEMBER_ID,
		POOLID AS LEAGUE_ID,
		DATE_JOINED::DATE AS DATE_JOINED,
		ACTIVE AS ACTIVE
    FROM
        {{ source('OFP_DBO', 'OFPPOOLMEMBERS') }} m
)

SELECT * FROM FINAL


