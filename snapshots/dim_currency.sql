
{% snapshot dim_currency %}
{{
    config(
        unique_key="currency_id",
        strategy="timestamp",
        updated_at="last_modified_date"
    )
}}

select
    currency_id,
    currency_name,
    symbol as display_symbol,
    updated_at as last_modified_date
from {{ ref("currency") }}

{% endsnapshot %}
