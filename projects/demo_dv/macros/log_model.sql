{% macro log_model() -%}

{%- if not execute or var("sw_debug") == false -%}
{{ return('') }}
{% endif %}

{%- if execute -%}

  {% set column_arg = {  'status' : ['STATUS'] } %}

  {% set ms = namespace(my_status='') %}


  {% for karg, warg in kwargs.items() %}
    {% if karg not in column_arg.keys()%}
      {{ exceptions.raise_compiler_error("ERROR. log_model() UNKNOWN ARGUMENT. AVAILABLE ARGUMENT IS 'status'") }}
    {% elif karg == 'status' %}
      {% set ms.my_status = warg %}
    {% endif %}
  {% endfor %}


  {% set run_id = invocation_id %}
  {% set run_started = run_started_at.strftime("%Y-%m-%d %H:%M:%S") %}
  {% set run_started_ts = "TO_TIMESTAMP('" ~ run_started_at.strftime("%Y-%m-%d %H:%M:%S.%f") ~ "', 'YYYY-MM-DD HH:MI:SS.FF3')" %}
  {% set dbt_version = dbt_version %}
  {% set project_name = project_name|upper %}

  {% set my_model = model.name|upper %}
  {% set my_database = target.database|upper %}
  {% set my_schema = target.schema|upper %}
  {% set my_environment = target.name|upper %}
  {% set my_message = '' %}

  {{log('--> run_id ' ~ run_id)}}
  {{log('--> run_started '~ run_started)}}
  {{log('--> run_started_ts '~ run_started_ts)}}
  {{log('--> dbt_version '~ dbt_version)}}
  {{log('--> project_name '~project_name )}}
  {{log('--> my_model '~ my_model)}}
  {{log('--> my_database '~ my_database)}}
  {{log('--> my_schema '~ my_schema)}}
  {{log('--> my_environment '~ my_environment)}}
  {{log('--> ms.my_status ' ~ ms.my_status)}}
  {{log('--> log_table '~ source('CUSTOM_LOG','EXECUTION_LOG_TABLE')) }}

  {% set sql_command %}
    INSERT INTO {{ source('CUSTOM_LOG','EXECUTION_LOG_TABLE') }}
    VALUES (  '{{ run_id }}', {{run_started_ts}}, '{{ dbt_version }}', '{{ project_name }}', '{{ my_model }}', '{{ ms.my_status }}', '{{ my_message }}' , '{{ my_environment }}', '{{ my_database }}', CURRENT_TIMESTAMP ) ;
    COMMIT;
  {% endset %}

  {% do run_query(sql_command) %}

  {% endif %}
{%- endmacro %}

