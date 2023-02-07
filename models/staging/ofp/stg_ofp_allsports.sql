{{ 
    config(
        materialized='table', 
        alias='STG_OFP_ALLSPORTS',
	tags=["stg-ofp"]
    ) 
}}

WITH FINAL AS (
    SELECT
		PRO AS PRO,
		CASE 
			WHEN DESCRIPTION LIKE 'College Football' THEN 'CFB' 
			WHEN DESCRIPTION LIKE 'NCAA Basketball' THEN 'CBB' 
			WHEN DESCRIPTION LIKE 'SOCCER' THEN 'Soccer' 
		ELSE DESCRIPTION
		END
		AS DESCRIPTION,
		SPORTID AS SPORT_ID
    FROM
        {{ source('OFP_DBO', 'ALLSPORTS') }}
)

SELECT * FROM FINAL