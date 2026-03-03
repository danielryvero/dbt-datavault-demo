{% macro string_to_list(env_var_name) %}
    {% set roles_str = env_var(env_var_name, "") %}
    {% if roles_str == "" %}
        {% set roles = [] %}
    {% else %}
        {#
          Partimos por comas y quitamos espacios extra
          Ej: "analyst_role , bi_role , etl_role"
          → ['analyst_role','bi_role','etl_role']
        #}
        {% set raw_roles = roles_str.split(",") %}
        {% set roles = [] %}
        {% for r in raw_roles %}
            {% do roles.append(r.strip()) %}
        {% endfor %}
    {% endif %}
    {{ return(roles) }}
{% endmacro %}
