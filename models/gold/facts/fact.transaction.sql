
{{ 
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='TRANSACTIONS_UNIQUE_ID',
    on_schema_change='append_new_columns'
  ) 
}}

WITH source_rows AS (

    SELECT
        -- PRIMARY KEY
        {{ dbt_utils.surrogate_key(['ge.RECORDNO', 'gb.RECORDNO']) }} AS TRANSACTIONS_UNIQUE_ID,

        -- FOREIGN KEYS
        gd.BATCHKEY AS TRANSACTION_ID,
        gb.RECORDNO AS TRAN_ID,
        ge.RECORDNO AS TRANSACTION_LINE_ID,
        ge.ACCOUNTKEY AS ACCOUNT_ID,
        ge.ITEMID AS ITEM_ID,
        ge.CLASSID AS CLASS_ID,
        ge.LOCATIONKEY AS LOCATION_ID,
        ge.DEPARTMENTKEY AS DEPARTMENT_ID,
        ge.CURRENCY AS CURRENCY_ID,
        gb.BATCH_DATE AS REPORTING_PERIOD_ID,

        -- DATES
        gb.BATCH_DATE AS POSTING_PERIOD_DATE,
        ge.ENTRY_DATE AS TRAN_DATE,
        rp.START_DATE AS START_DATE,
        rp.END_DATE   AS END_DATE,

        -- DETAILS
        ga.ACCOUNTNO     AS ACCOUNT_NUMBER,
        ga.ACCOUNTTYPE   AS ACCOUNT_TYPE,
        ga.TITLE         AS ACCOUNT_NAME,
        ge.TR_TYPE       AS TRANSACTION_TYPE,
        ge.LINE_NO       AS TRANSACTION_LINE_NO,
        ge.ITEMNAME      AS ITEM_TYPE,
        gd.LINE_NO       AS ACCOUNTING_LINE_NO,
        ge.BATCHTITLE    AS TITLE,
        gb.BATCH_TITLE   AS MEMO,
        ge.STATE         AS STATE,

        -- MEASURES
        COALESCE(ge.TRX_AMOUNT, gd.TRX_AMOUNT) AS NET_AMOUNT,
        COALESCE(ge.AMOUNT,     gd.AMOUNT)     AS AMOUNT,

        -- DERIVED / HELPER KEYS
        {{ dbt_utils.surrogate_key(['ge.ACCOUNTKEY', 'ge.CLASSID']) }} AS CHART_OF_ACCOUNTS_UNIQUE_ID,

        -- AUDIT / METADATA
        gb.WHENMODIFIED AS LAST_MODIFIED_DATE

    FROM {{ ref('entry') }} ge
    LEFT JOIN {{ ref('batch') }} gb 
        ON ge.LINE_NO = gb.RECORDNO
    LEFT JOIN {{ ref('detail') }} gd 
        ON ge.RECORDNO = gd.RECORDNO
    LEFT JOIN {{ ref('account') }} ga
        ON ge.LINE_NO = ga.RECORDNO
    LEFT JOIN {{ ref('reporting_period') }} rp
        ON gb.BATCH_DATE = rp.START_DATE

    {% if is_incremental() %}
      -- Only process changes since the max LAST_MODIFIED_DATE in target
      WHERE CAST(gb.WHENMODIFIED AS TIMESTAMP_NTZ) > (
        SELECT COALESCE(MAX(LAST_MODIFIED_DATE), '1900-01-01'::TIMESTAMP_NTZ)
        FROM {{ this }}
      )
    {% endif %}
)

-- dbt uses this SELECT as the "USING" subquery for MERGE
SELECT
    -- PRIMARY KEY
    TRANSACTIONS_UNIQUE_ID,

    -- FOREIGN KEYS
    TRANSACTION_ID,
    TRAN_ID,
    TRANSACTION_LINE_ID,
    ACCOUNT_ID,
    ITEM_ID,
    CLASS_ID,
    LOCATION_ID,
    DEPARTMENT_ID,
    CURRENCY_ID,
    REPORTING_PERIOD_ID,

    -- DATES
    POSTING_PERIOD_DATE,
    TRAN_DATE,
    START_DATE,
    END_DATE,

    -- DETAILS
    ACCOUNT_NUMBER,
    ACCOUNT_TYPE,
    ACCOUNT_NAME,
    TRANSACTION_TYPE,
    TRANSACTION_LINE_NO,
    ITEM_TYPE,
    ACCOUNTING_LINE_NO,
    TITLE,
    MEMO,
    STATE,

    -- MEASURES
    NET_AMOUNT,
    AMOUNT,

    -- DERIVED / HELPER KEYS
    CHART_OF_ACCOUNTS_UNIQUE_ID,

    -- AUDIT / METADATA
    LAST_MODIFIED_DATE
FROM source_rows
