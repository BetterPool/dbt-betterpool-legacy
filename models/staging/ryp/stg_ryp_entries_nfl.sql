{{ 
    config(
		materialized='table',
       		alias='STG_RYP_ENTRIES_NFL',
		tags=["stg-ryp"]
    ) 
}}

WITH NFL_CONFIDENCE_ENTRIES AS (
    SELECT
        ENTRY_ID,
        ACTIVE,
        LEAGUE_ID,
        TEAM_ID,
        ENTRY_CREATION_DATE,
        'NFL' AS LEAGUE_CATEGORY,
        'RYP' AS SOURCE,
        'NFL Confidence' AS LEAGUE_TYPE

    FROM {{ source('RYP_NFL_DBO', 'NFL_CONFIDENCE_ENTRIES') }}
),

NFL_FANTASY_ENTRIES AS (
    SELECT
        ENTRY_ID,
        ACTIVE,
        LEAGUE_ID,
        TEAM_ID,
        ENTRY_CREATION_DATE,
        'NFL' AS LEAGUE_CATEGORY,
        'RYP' AS SOURCE,
        'NFL Fantasy' AS LEAGUE_TYPE

    FROM {{ source('RYP_NFL_DBO', 'NFL_FANTASY_ENTRIES') }}
),

NFL_PICKEM_ENTRIES AS (
    SELECT
        ENTRY_ID,
        ACTIVE,
        LEAGUE_ID,
        TEAM_ID,
        ENTRY_CREATION_DATE,
        'NFL' AS LEAGUE_CATEGORY,
        'RYP' AS SOURCE,
        'NFL Pickem' AS LEAGUE_TYPE

    FROM {{ source('RYP_NFL_DBO', 'NFL_PICKEM_ENTRIES') }}
),

NFL_SURVIVOR_ENTRIES AS (
    SELECT
        ENTRY_ID,
        ACTIVE,
        LEAGUE_ID,
        TEAM_ID,
        ENTRY_CREATION_DATE,
        'NFL' AS LEAGUE_CATEGORY,
        'RYP' AS SOURCE,
        'NFL Survivor' AS LEAGUE_TYPE

    FROM {{ source('RYP_NFL_DBO', 'NFL_SURVIVOR_ENTRIES') }}
),

NFL_PLAYOFF_BRACKET_ENTRIES AS (
    SELECT
        ENTRY_ID,
        ACTIVE,
        LEAGUE_ID,
        TEAM_ID,
        ENTRY_CREATION_DATE,
        'NFL' AS LEAGUE_CATEGORY,
        'RYP' AS SOURCE,
        'NFL Playoff Bracket' AS LEAGUE_TYPE

    FROM {{ source('RYP_NFL_DBO', 'NFL_PLAYOFF_BRACKET_ENTRIES') }}
)

SELECT *
FROM NFL_CONFIDENCE_ENTRIES

UNION

SELECT *
FROM NFL_FANTASY_ENTRIES

UNION

SELECT *
FROM NFL_PICKEM_ENTRIES

UNION

SELECT *
FROM NFL_SURVIVOR_ENTRIES

UNION

SELECT *
FROM NFL_PLAYOFF_BRACKET_ENTRIES