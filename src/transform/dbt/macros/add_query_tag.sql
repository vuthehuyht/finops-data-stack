{%- macro add_query_tag() -%}
{% set query_tag = {} %}
{% set materialized = config.get('materialized') %}
{{ update_query_tag(query_tag, 'dagster_job_name', var('dagster_job_name') ) }}
{{ update_query_tag(query_tag, 'materialized', materialized) }}
{{ update_query_tag(query_tag, 'full_refresh', flags.FULL_REFRESH ) }}
{{ update_query_tag(query_tag, 'database', this.database ) }}
{{ update_query_tag(query_tag, 'schema', this.schema ) }}
{{ update_query_tag(query_tag, 'identifier', this.identifier ) }}
{% do run_query("ALTER SESSION SET QUERY_TAG = '" ~ query_tag | tojson | replace("'", "''") ~ "'") %}
{%- endmacro -%}


{%- macro update_query_tag(query_tag, key, value) -%}
{% if value is not none %}
  {% do query_tag.update({key: value}) %}
{% endif %}
{%- endmacro -%}
