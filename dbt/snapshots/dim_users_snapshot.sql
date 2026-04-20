{% snapshot dim_users_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='user_id',
        strategy='check',
        check_cols=['role', 'email']
    )
}}

select * from {{ ref('dim_users') }}

{% endsnapshot %}
