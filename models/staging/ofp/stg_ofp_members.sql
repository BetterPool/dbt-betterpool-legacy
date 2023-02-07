{{ 
    config(
        materialized='table', 
        alias='STG_OFP_MEMBERS',
	tags=["stg-ofp"]
    ) 
}}

WITH FINAL AS (
    SELECT
		MEMBERID AS MEMBER_ID,
		EMAIL AS MEMBER_EMAIL,
		EMAILCONFIRMED AS EMAIL_CONFIRMED,
		JOINDATE::DATE AS DATE_TIME_CREATED,
		PHONE AS PHONE,
		CELLPHONE AS CELL_PHONE,
		USERNAME AS USERNAME,
		LASTVISIT::DATE AS LAST_VISIT_DATE,
		ALIASOFID AS ALIAS_OF_ID,
		'OFP' AS SOURCE
    FROM
        {{ source('OFP_DBO', 'ALLMEMBERS') }}
--	WHERE ALIASOFID IS NULL
)

SELECT * FROM FINAL