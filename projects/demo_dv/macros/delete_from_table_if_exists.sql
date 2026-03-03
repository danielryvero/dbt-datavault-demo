{% macro delete_from_table_if_exists(database, schema, table, condition) -%}

{%- if not execute -%}
    {{ return('') }}
{% endif %}

{% set existing = adapter.get_relation(database, schema, table) %}

{% if existing %}
    {% set delete_query %}
        delete from {{existing.schema}}.{{existing.table}} where {{condition}}
    {% endset %}
    
    {% do run_query(delete_query) %}
{% endif %}

{% endmacro %}