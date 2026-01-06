
{% snapshot dim_location %}
{{
    config(
        unique_key="location_id",
        strategy="timestamp",
        updated_at="last_modified_date"
    )
}}

SELECT
    -- PRIMARY KEY
    LOCATIONID                AS location_id,

    -- FOREIGN KEYS
    ENTITY                    AS subsidiary_id,  -- maps to dim_subsidiary.subsidiary_id

    -- DETAILS
    NAME                      AS location_full_name,
    STATUS                    AS is_inactive,
    ADDRESSCOUNTRYDEFAULT     AS country,
    LOCATIONTYPE              AS location_type,
    PARENTNAME                AS parent,

    -- AUDIT / METADATA
    WHENMODIFIED              AS last_modified_date

FROM {{ ref("location") }}

{% endsnapshot %}
