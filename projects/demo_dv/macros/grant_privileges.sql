{% macro grant_privileges(roles, privilege="SELECT", object_name=none, object_type="table") %}
    {#
      roles: lista de roles o string con un rol
      privilege: tipo de permiso (SELECT, INSERT, etc.)
      object_name: nombre del objeto (se recomienda pasar `this` cuando se usa como post-hook)
      object_type: table, view, schema...
    #}

    {% if object_name is none %}
        {% set target_object = this %}
    {% else %}
        {% set target_object = object_name %}
    {% endif %}

    {% for role in roles %}
        grant {{ privilege }} on {{ object_type }} {{ target_object }} to role {{ role }};
    {% endfor %}
{% endmacro %}
