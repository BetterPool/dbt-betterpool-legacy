{{ 
    config(
        materialized='table', 
        alias='FACT_REVENUE',
	tags=["fact"]
    ) 
}}


WITH ORDERS AS (
    SELECT *
    FROM {{ ref('stg_ryp_orders') }}
    WHERE STATUS_ID = 1
),

ORDER_LEAGUES AS (
    SELECT DISTINCT
        ORDER_ID,
        LEAGUE_TYPE_ID,
        LEAGUE_ID
    FROM {{ ref('stg_ryp_order_item') }}
),

ORDER_TRANSACTIONS AS (
    SELECT *
    FROM {{ ref('stg_ryp_order_transaction') }}
),

LEAGUE_TYPES AS (
    SELECT *
    FROM {{ ref('stg_ryp_league_types') }}
),

LEAGUE_CATEGORIES AS (
    SELECT *
    FROM {{ source('RYP_RUNYOURPOOL_DBO', 'LEAGUE_CATEGORIES') }}
),

MEMBERS AS (
    SELECT *
    FROM {{ ref('stg_ryp_members') }}
),

FINAL_RYP AS (
    SELECT
        ORDERS.ORDER_ID AS ORDER_ID,
        ORDER_LEAGUES.LEAGUE_ID AS LEAGUE_ID,
        ORDER_TRANSACTIONS.TRANSACTION_DATE_TIME::DATE AS TRANSACTION_DATE,
        ORDERS.SUB_TOTAL_AMOUNT - ORDERS.COUPON_AMOUNT AS TOTAL_AMOUNT,
        LEAGUE_TYPES.DESCRIPTION AS LEAGUE_TYPE,
        LEAGUE_CATEGORIES.DATASOURCE_NAME AS LEAGUE_CATEGORY,
        MEMBERS.MEMBER_ID,
        MEMBERS.MEMBER_EMAIL,
        'RYP' AS SOURCE

    FROM ORDERS
    INNER JOIN ORDER_LEAGUES ON ORDERS.ORDER_ID = ORDER_LEAGUES.ORDER_ID
    INNER JOIN
        ORDER_TRANSACTIONS ON
            ORDERS.PAID_RYP_TRANSACTION_ID = ORDER_TRANSACTIONS.RYP_TRANSACTION_ID
    LEFT JOIN MEMBERS ON ORDERS.MEMBER_ID = MEMBERS.MEMBER_ID
    LEFT JOIN
        LEAGUE_TYPES ON
            ORDER_LEAGUES.LEAGUE_TYPE_ID = LEAGUE_TYPES.LEAGUE_TYPE_ID
    LEFT JOIN
        LEAGUE_CATEGORIES ON
            LEAGUE_TYPES.LEAGUE_CATEGORY_ID = LEAGUE_CATEGORIES.LEAGUE_CATEGORY_ID
),

FINAL_OFP AS (
    SELECT *
    FROM {{ ref('stg_ofp_transactions_final') }}
),

FINAL_BOTH AS (
	SELECT * FROM FINAL_RYP
	UNION
	SELECT * FROM FINAL_OFP
)

SELECT DISTINCT 
	ORDER_ID,
	LEAGUE_ID,
	TRANSACTION_DATE,
	MAX(TOTAL_AMOUNT) AS TOTAL_AMOUNT,
	LEAGUE_TYPE,
	LEAGUE_CATEGORY,
	MEMBER_ID,
	MEMBER_EMAIL,
	SOURCE
FROM FINAL_BOTH
GROUP BY 1,2,3,5,6,7,8,9
	