{# Por padrão o dbt monta "<schema do profile>_<schema custom>" (ex.: gold_staging).
   Aqui o schema custom vale exatamente como escrito: staging -> staging, gold -> gold. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
