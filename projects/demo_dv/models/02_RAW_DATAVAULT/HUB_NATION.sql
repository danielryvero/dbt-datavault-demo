{{ config(
      alias='HUB_NATION'
    , materialized='incremental')
}}


WITH NATION AS (
      SELECT 
            MD5(N_NATIONKEY) HUB_ID_NATION
            , N_NATIONKEY AS NATION_KEY
            , '{{ var('cod_tenant') }}' AS TENANT
            , CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS DT_LOAD   
            , '{{ source('SF_SAMPLE', 'NATION') }}' AS RECORD_SOURCE
      FROM {{ source('SF_SAMPLE', 'NATION') }} 
) 
SELECT 
        NATION.HUB_ID_NATION
      , NATION.NATION_KEY
      , NATION.TENANT
      , NATION.DT_LOAD
      , NATION.RECORD_SOURCE
FROM NATION NATION
LEFT JOIN {{ this }} HUB
	ON HUB.HUB_ID_NATION =  NATION.HUB_ID_NATION
WHERE HUB.HUB_ID_NATION IS NULL