{% macro cast_datatypes_athena(columns) %}
    {% for col in columns %}
        {% if col.data_type == 'STRING' %}
            CASE
                WHEN TRIM(CAST("{{ col.column }}" AS VARCHAR)) = '' THEN NULL
                ELSE TRIM(CAST("{{ col.column }}" AS VARCHAR))
            END AS {{ col.alias }}
        {% elif col.data_type == 'N/A' %}
            "{{ col.column }}" AS {{ col.alias }}
        {% elif col.data_type == 'CUSTOM' %}
            {{ col.custom_expr }}
        {% else %}
            CAST("{{ col.column }}" AS {{ col.data_type }}) AS {{ col.alias }}
        {% endif %}
            {% if not loop.last %},{% endif %}
    {% endfor %}
{% endmacro %}