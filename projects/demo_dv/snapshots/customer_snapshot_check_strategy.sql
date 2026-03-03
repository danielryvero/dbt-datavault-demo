{% snapshot customer_snapshot_chk %}

{{
    config(
      target_schema='LANDING',
      unique_key='C_CUSTKEY',
      strategy='check',
      check_cols=[ 'C_NAME', 'C_ADDRESS', 'C_NATIONKEY', 'C_PHONE' ],
    )
}}

select * 
FROM
    {{ source('SF_SAMPLE', 'CUSTOMER') }} 

{% endsnapshot %}