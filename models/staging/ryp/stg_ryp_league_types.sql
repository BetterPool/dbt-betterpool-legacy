{{ 
    config(
        materialized='table', 
        alias='STG_RYP_LEAGUE_TYPES',
	tags=["stg-ryp"]
    ) 
}}

WITH LEAGUE_TYPES AS (
    SELECT
        LEAGUE_TYPE_ID,
        LEAGUE_CATEGORY_ID,
        TABLE_NAME_ENTRIES,
	DESCRIPTION,
        ACTIVE,
        STARTDATE,
        'RYP' AS SOURCE

    FROM
        {{ source('RYP_RUNYOURPOOL_DBO', 'LEAGUE_TYPES') }}
)

SELECT * FROM LEAGUE_TYPES
