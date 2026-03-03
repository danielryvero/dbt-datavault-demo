{% macro log_results(results) -%}


{%- if not execute or var("sw_debug") == false -%}
{{ return('') }}
{% endif %}

{% set result_type = ['error'] %}
{% if var("sw_debug") == "TRUE" %}
  {% do result_type.append('skipped') %}
{% endif %}

{%- if execute -%}
{%- set model_results = [] %}

  {% set column_arg = { 'results':[] } %}

{% for result in results %}
    {% set exec_status = result.status %}
    {{ log ('--> result.status ' ~ result.status) }}
    {{ log ('--> result.node ' ~ result.node) }}
    {{ log ('--> result.node.alias ' ~ result.node.alias) }}
    {{ log ('--> result.message ' ~ result.message) }}
    {%- if exec_status in result_type -%}
    {%- if "ERROR" in result.message|upper() -%}
      {% set model_result = {
            'status': exec_status,
            'message': result.message|replace("'","''"),
            'unique_id': result.node.unique_id.split('.')[-1],
            'alias': result.node.alias|upper()
            } %}
    {% do model_results.append(model_result) %}
    {% endif %}
    {%- endif -%}
{% endfor %}

{% if model_results|length == 0 %}
{{ log ('--> Execution completed without any error') }}
{{ return('') }} 
{% else %}
{{ log ('--> Execution completed with errors') }}
{% endif %}

{% for result in model_results %}

    {% set run_id = invocation_id %}
    {% set run_started = run_started_at.strftime("%Y-%m-%d %H:%M:%S") %}
    {% set run_started_ts = "TO_TIMESTAMP('" ~ run_started_at.strftime("%Y-%m-%d %H:%M:%S.%f") ~ "', 'YYYY-MM-DD HH:MI:SS.FF3')" %}
    {% set dbt_version = dbt_version %}
    {% set project_name = project_name|upper %}

    {% set my_model = result.alias|upper %}
    {% set my_database = target.database|upper %}
    {% set my_schema = target.schema|upper %}
    {% set my_environment = target.name|upper %}
    {% set my_message = result.message %}
    {% set my_status = 'ERROR' %}
    
    {{log('--> run_id ' ~ run_id)}}
    {{log('--> run_started '~ run_started)}}
    {{log('--> run_started_ts '~ run_started_ts)}}
    {{log('--> dbt_version '~ dbt_version)}}
    {{log('--> project_name '~project_name )}}
    {{log('--> my_model '~ my_model)}}
    {{log('--> my_database '~ my_database)}}
    {{log('--> my_schema '~ my_schema)}}
    {{log('--> my_environment '~ my_environment)}}
    {{log('--> my_status ' ~ my_status)}}
    {{log('--> log_table '~ source('CUSTOM_LOG','EXECUTION_LOG_TABLE')) }}


    {% set sql_command %}

        INSERT INTO {{ source('CUSTOM_LOG','EXECUTION_LOG_TABLE') }}
        VALUES (  '{{ run_id }}', {{run_started_ts}}, '{{ dbt_version }}', '{{ project_name }}', '{{ my_model }}', '{{ my_status }}', '{{ my_message }}', '{{ my_environment }}', '{{ my_database }}', CURRENT_TIMESTAMP ) ;
        COMMIT;

    {% endset %}

    {% do run_query(sql_command) %}

{% endfor %}
{% endif %}

{%- endmacro %}