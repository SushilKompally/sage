
{% snapshot dim_item %}
{{
    config(
        unique_key="item_id",
        strategy="timestamp",
        updated_at="last_modified_date"
    )
}}

SELECT
    -- PRIMARY KEY
    ITEMID                AS item_id,

    
    -- DETAILS
    NAME                  AS item_name,
    NAME                  AS display_name,
    EXTENDED_DESCRIPTION  AS description,
    STANDARD_COST         AS cost,
    COST_METHOD           AS costing_method,
    ITEMTYPE              AS item_type,
    STATUS                AS is_inactive,

    -- DATES / TIMESTAMPS
    WHENCREATED           AS created_date,

    -- AUDIT / METADATA
    WHENMODIFIED          AS last_modified_date

FROM {{ ref("item") }}

{% endsnapshot %}
