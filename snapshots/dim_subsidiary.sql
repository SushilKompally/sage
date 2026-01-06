
{% snapshot dim_subsidiary %}
{{
    config(
        unique_key="subsidiary_id",
        strategy="timestamp",
        updated_at="last_modified_date"
    )
}}

SELECT
    -- PRIMARY KEY
    LOCATIONID     AS subsidiary_id,

    -- DETAILS
    FEDERALID      AS subsidiary_number,
    ENTITY         AS subsidiary_title,
    STATUS         AS is_inactive,

    -- DATES / TIMESTAMPS
    WHENCREATED    AS date_created,

    -- AUDIT / METADATA
    WHENMODIFIED   AS last_modified_date

FROM {{ ref("location_entity") }}

{% endsnapshot %}
``
