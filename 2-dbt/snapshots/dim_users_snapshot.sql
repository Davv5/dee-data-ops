{% snapshot dim_users_snapshot %}

{{
    config(
        unique_key='user_id',
        strategy='check',
        check_cols=['name', 'role', 'email', 'is_active']
    )
}}

{# target_schema removed in Y1 follow-up (2026-04-29) — snapshot now
   inherits the project-level `+target_schema: Core` block in
   dbt_project.yml. Previously this model-level override forced the
   snapshot into a lowercase `snapshots` dataset that is slated for
   decommission as part of the Y1 schema cutover. #}

select user_id, name, role, email, is_active from {{ ref('dim_users') }}

{% endsnapshot %}
