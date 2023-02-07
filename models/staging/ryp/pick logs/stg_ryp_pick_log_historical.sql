{{ 
    config(
		materialized='table',
		unique_key='pick_id',
		alias='STG_RYP_PICK_LOG_HISTORICAL',
		tags=["stg-ryp"]
	) 
}}

SELECT
    {{ dbt_utils.surrogate_key(['P.TEAM_ID', 'P.TIMESTAMP']) }} as PICK_ID,
    m.member_id,
    m.member_email,
    t.team_id,
    t.league_id,
    p.ip_address,
    p.league_type_id,
    p.entry_id,
    p.sheet AS sheet_id,
    p.pick_period,
    'RYP' AS product,
    lc.datasource_name AS league_category,
    lt.description AS league_type,
    DATEADD('MS', p.timestamp, '1970-01-01') AS pick_timestamp,
    prod.public.parse_useragent(TO_ARRAY(p.user_agent)) AS user_agent,
    CURRENT_TIMESTAMP AS etl_at
FROM {{ source('RYP_HISTORICAL_PICK_LOGS', 'PICK_LOG_2016') }} AS p
INNER JOIN
    {{ source('RYP_RUNYOURPOOL_DBO', 'TEAMS') }} AS t ON p.team_id = t.team_id
INNER JOIN
    {{ source('RYP_RUNYOURPOOL_DBO', 'MEMBERS') }} AS m ON
        t.member_id = m.member_id
INNER JOIN
    {{ source('RYP_RUNYOURPOOL_DBO', 'LEAGUE_TYPES') }} AS lt ON
        lt.league_type_id = p.league_type_id
INNER JOIN
    {{ source('RYP_RUNYOURPOOL_DBO', 'LEAGUE_CATEGORIES') }} AS lc ON
        lt.league_category_id = lc.league_category_id
{% if is_incremental() %}
    WHERE pick_id NOT IN (SELECT pick_id FROM {{ this }})
{% endif %}
