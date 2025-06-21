{% macro delete_insert_iceberg(columns) %}
{%- if is_incremental() -%}
DELETE FROM {{ this }} WHERE {% for column in columns -%} {{ column }} = \'{{ var(column) }}\' {% if not loop.last -%} AND {% endif -%} {%- endfor -%}
{% else %}
-- No action needed for non-incremental runs
{%- endif -%}
{% endmacro %}