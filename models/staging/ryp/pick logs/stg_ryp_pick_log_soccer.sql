{{ 
    config(
        materialized='table', 
        alias='STG_RYP_PICK_LOG_SOCCER',
	tags=["stg-ryp"]
    ) 
}}

WITH PICK_LOG_SOCCER AS (
	SELECT 
	    {{ dbt_utils.surrogate_key(['p.team_id', 'p.timestamp']) }} as pick_id,
        m.member_id,
	    m.member_email,
	    t.team_id,
	    t.league_id,
	    p.ip_address,
	    DATEADD('MS',p.timestamp,'1970-01-01') AS pick_timestamp,
	    p.league_type_id,
	    prod.public.parse_useragent(TO_ARRAY(p.user_agent)) as user_agent,
	    p.entry_id,
	    p.sheet as sheet_id,
	    p.pick_period,
	    'RYP' AS product,
	    lc.datasource_name AS league_category,
	    lt.description AS league_type,
	    current_timestamp AS etl_at

	FROM {{ source('RYP_PICK_LOGS', 'PICK_LOG_SOCCER') }} p
	    JOIN {{ source('RYP_RUNYOURPOOL_DBO', 'TEAMS') }} t ON p.team_id = t.team_id
	    JOIN {{ source('RYP_RUNYOURPOOL_DBO', 'MEMBERS') }} m ON t.member_id = m.member_id
	    JOIN {{ source('RYP_RUNYOURPOOL_DBO', 'LEAGUE_TYPES') }} lt ON lt.league_type_id = 	p.league_type_id
	    JOIN {{ source('RYP_RUNYOURPOOL_DBO', 'LEAGUE_CATEGORIES') }} lc ON lt.league_category_id = lc.league_category_id
),
PICK_LOG_SOCCER_BACKUP AS (
    SELECT DISTINCT
	    m.member_id,
	    m.member_email,
	    t.team_id,
	    t.league_id,
	    null  as ip_address,
	    DATEADD('MS',0,'2022-11-20') AS pick_timestamp,
	    21 as league_type_id,
	    null as user_agent,
	    null entry_id,
	    p.sheet_id as sheet_id,
	    1 as pick_period,
	    'RYP' AS product,
	    'Soccer' AS league_category,
	    'World Cup Challenge' AS league_type,
	    current_timestamp AS etl_at

	FROM {{ source('RYP_SOCCER_DBO', 'WORLDCUP_PICKS_GROUPSTAGE') }} p
	    JOIN {{ source('RYP_RUNYOURPOOL_DBO', 'TEAMS') }} t ON p.team_id = t.team_id
	    JOIN {{ source('RYP_RUNYOURPOOL_DBO', 'MEMBERS') }} m ON t.member_id = m.member_id
    
    WHERE not exists (
    select 1 from PICK_LOG_SOCCER l where p.team_id = l.team_id and p.sheet_id = l.sheet_id)
)

SELECT 
    * 
FROM PICK_LOG_SOCCER
UNION 
SELECT 
    {{ dbt_utils.surrogate_key(['p.team_id', 'pick_timestamp']) }} as pick_id,
    * 
FROM PICK_LOG_SOCCER_BACKUP p 