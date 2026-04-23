{% snapshot dim_users_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='user_id',
        strategy='check',
        check_cols=['name', 'role', 'email', 'is_active']
    )
}}

select user_id, name, role, email, is_active from {{ ref('dim_users') }}

{% endsnapshot %}
