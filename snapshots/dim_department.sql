
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
    try_cast(whenmodified as timestamp_ntz) as last_modified_date
from {{ ref("department") }}

{% endsnapshot %}
