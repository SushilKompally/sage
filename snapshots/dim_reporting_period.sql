
{% snapshot dim_reporting_period %}
{{
    config(
        unique_key="reporting_period_id",
        strategy="timestamp",
        updated_at="last_modified_date"
    )
}}

SELECT
    -- PRIMARY KEY
    RECORDNO       AS reporting_period_id,

    -- DETAILS
    NAME           AS period_name,

    -- DATES / TIMESTAMPS
    START_DATE     AS start_date,
    END_DATE       AS end_date,

    -- AUDIT / METADATA
    WHENMODIFIED   AS last_modified_date

FROM {{ ref("reporting_period") }}

{% endsnapshot %}

