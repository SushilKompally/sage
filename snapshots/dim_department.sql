{% snapshot dim_department %}
{{
    config(
        unique_key="department_id",
        strategy="timestamp",
        updated_at="last_modified_date"
    )
}}

select
    departmentid                 as department_id,
    title                        as department_name,
    status                       as is_inactive,
    parentid                     as parent,
    whenmodified  as last_modified_date
select * from {{ ref("department") }}

{% endsnapshot %}
