{{ config(
      alias='HUB_ORDERS'
    , materialized='incremental')
}}


WITH ORDERS AS (
      SELECT 
            MD5(O_ORDERKEY) HUB_ID_ORDERS
            , O_ORDERKEY AS ORDER_KEY
            , '{{ var('cod_tenant') }}' AS TENANT
            , CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS DT_LOAD   
            , '{{ source('SF_SAMPLE', 'ORDERS') }}' AS RECORD_SOURCE
      FROM {{ source('SF_SAMPLE', 'ORDERS') }} 
) 
SELECT 
        ORDERS.HUB_ID_ORDERS
      , ORDERS.ORDER_KEY
      , ORDERS.TENANT
      , ORDERS.DT_LOAD
      , ORDERS.RECORD_SOURCE
FROM ORDERS ORDERS
LEFT JOIN {{ this }} HUB
	ON HUB.HUB_ID_ORDERS =  ORDERS.HUB_ID_ORDERS
WHERE HUB.HUB_ID_ORDERS IS NULL