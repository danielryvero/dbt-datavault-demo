{% macro set_query_tag() -%}

{%- if not execute or target.type != "snowflake" or var("query_tagging") == "FALSE" -%}
    {{ return('') }}
{% endif %}

{% set new_query_tag = "DBT_" ~ project_name|upper ~ "_" ~ var("cod_tenant")|upper ~ "_" ~ model.name|upper %} 

{% if new_query_tag %}
    {{ log("Setting query_tag to '{0}'".format(new_query_tag)) }}
    {% do run_query("ALTER SESSION SET QUERY_TAG = '{}'".format(new_query_tag)) %}
{% endif %}

{{ return('')}}

{% endmacro %}