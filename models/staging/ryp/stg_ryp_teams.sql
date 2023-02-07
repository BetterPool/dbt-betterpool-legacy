{{ 
    config(
        materialized='table', 
        alias='STG_RYP_TEAMS',
	tags=["stg-ryp"]
    ) 
}}

WITH TEAMS AS (
    SELECT
        ACTIVE,
        ARCHIVED,
        CUSTOM_FIELD_VALUE,
        DATE_DELETED,
        DATE_JOINED,
        DATE_LAST_LOGIN,
        DATE_LAST_LOGIN_SUPPRESS,
        DATE_LAST_MESSAGEBOARD_VIEW,
        LEAGUE_COMMISH,
        LEAGUE_ID,
        MEMBER_ID AS MEMBER_ID,
        NOTES,
        TEAM_ID,
        TEAM_NAME,
        'RYP' AS SOURCE
    FROM
        {{ source('RYP_RUNYOURPOOL_DBO', 'TEAMS') }}
)

SELECT * FROM TEAMS
