{% snapshot customer_snapshot_ts %}

{{
    config(
      target_schema='LANDING',
      unique_key='C_CUSTKEY',
      strategy='timestamp',
      updated_at='DT_LOAD'
    )
}}

select * 
FROM
    {{ source('SF_SAMPLE', 'CUSTOMER') }} 

{% endsnapshot %}