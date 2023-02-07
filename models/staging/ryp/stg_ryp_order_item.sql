{{ 
    config(
        materialized='table', 
        alias='STG_RYP_ORDER_ITEM',
	tags=["stg-ryp"]
    ) 
}}

WITH ORDER_ITEM AS (
    SELECT
        ID,
        LEAGUETIER AS LEAGUE_TIER,
        LEAGUEID AS LEAGUE_ID,
        ITEMAMOUNT AS ITEM_AMOUNT,
        LINENUMBER AS LINE_NUMBER,
        LEAGUETYPEID AS LEAGUE_TYPE_ID,
        DESCRIPTION AS DESCRIPTION,
        ORDERID AS ORDER_ID,
        QUANTITY AS QUANTITY,
        'RYP' AS SOURCE

    FROM
        {{ source('RYP_RUNYOURPOOL_DBO', 'ORDERITEM') }}
)

SELECT * FROM ORDER_ITEM
