
{% snapshot dim_chartofaccounts %}
{{
  config(
    unique_key='account_id',
    strategy='timestamp',
    updated_at='updated_at_ts',
  )
}}

with base as (
  select
    A.RECORDNO                                         as account_id,
    A.TITLE                                            as account_title,
    A.ACCOUNTNO                                        as account_number,
    A.CATEGORYKEY                                      as account_subsidiary_id,
    A.ACCOUNTTYPE                                      as account_type,
    LE.ENTITY                                          as subsidiary_full_name,
    LE.RECORDNO                                        as subsidiary_id,
    LE.NAME                                            as subsidiary_name,

    -- derive a robust updated_at using the most recent change across joined tables
    greatest(
      try_cast(A.WHENMODIFIED as timestamp_ntz),
      try_cast(LE.WHENMODIFIED as timestamp_ntz)
    )                                                  as updated_at_ts

  from {{ ref('account') }} A
  left join {{ ref ('location_entity') }} LE
    on A.RECORDNO = LE.RECORDNO
)

select
  account_id,
  account_title,
  account_number,
  account_subsidiary_id,
  account_type,
  subsidiary_full_name,
  subsidiary_id,
  subsidiary_name,
  updated_at_ts
from base

{% endsnapshot %}
