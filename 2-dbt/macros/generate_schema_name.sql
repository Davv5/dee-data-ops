{#
    Custom generate_schema_name — routes models to schemas based on target.

    prod:
      Use +schema: config from dbt_project.yml as-is.
      Yields staging / warehouse / marts inside the consolidated
      project-41542e21-470f-4589-96d (post-U2 retarget 2026-04-23).

    dev, ci, anything else:
      Ignore +schema: config entirely. Consolidate all layers into the
      profile's default schema (dev_<user> or ci) so development and
      CI runs don't fan out across per-layer schemas.

    Source: "DBT Project Environment Setup", Data Ops notebook.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}

    {%- if target.name == 'prod' -%}
        {%- if custom_schema_name is none -%}
            {{ default_schema }}
        {%- else -%}
            {{ custom_schema_name | trim }}
        {%- endif -%}

    {%- else -%}
        {{ default_schema }}

    {%- endif -%}
{%- endmacro %}
