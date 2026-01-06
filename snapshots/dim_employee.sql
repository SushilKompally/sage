
{% snapshot dim_employee %}
{{
    config(
        unique_key="employee_id",
        strategy="timestamp",
        updated_at="last_modified_date"
    )
}}

SELECT
    -- PRIMARY KEY
    EMPLOYEEID   AS employee_id,

    -- FOREIGN KEYS
    DEPARTMENTID AS department_id,
    LOCATIONID   AS location_id,
    ENTITY       AS entity_id,         -- if you maintain a dim_entity
    MEGAENTITYID AS subsidiary,        -- maps to subsidiary_id in dim_subsidiary
    EMPTYPEKEY   AS employee_type_id,

    -- DETAILS
    TITLE                    AS title,
    PERSONALINFO_EMAIL_1     AS email,
    STATUS                   AS is_inactive,

    -- DATES / TIMESTAMPS
    WHENCREATED              AS date_created,

    -- AUDIT / METADATA
    WHENMODIFIED             AS last_modified_date

FROM {{ ref("employee") }}

{% endsnapshot %}
