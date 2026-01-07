
/*
-- Description: Incremental Load Script for Silver Layer - currency Table
-- Script Name: silver_currency.sql
-- Created on: 05-Jan-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incrementally load and standardize Sage 'currency' from Bronze to Silver.
--     Uses explicit TRY_CAST/TRIM and appends source metadata.
-- Change History:
--     05-Jan-2025 - Initial creation - Sushil Kompally
*/

{{ config(
    unique_key='    currency_ID',                
    incremental_strategy='merge'
) }}

WITH raw AS (
    SELECT
        *,
        {{ source_metadata() }}
    FROM {{ source('sage_bronze', 'currency') }}
    WHERE 1 = 1
    --{{ incremental_filter() }}
),

cleaned AS (
    SELECT
        -- ===========================
        -- PRIMARY KEY
        -- ===========================
        TRY_CAST(ID AS INT)                          AS CURRENCY_ID,

        -- ===========================
        -- DETAILS (strings)
        -- ===========================
        TRIM(NAME)                                   AS CURRENCY_NAME,
        TRIM(SYMBOL)                                 AS SYMBOL,
       -- whenmodified,
        -- ===========================
        -- DATES / TIMESTAMPS FROM SOURCE
        -- ===========================
        TRY_CAST(UPDATED_AT AS TIMESTAMP_NTZ)        AS UPDATED_AT,

        -- ===========================
        -- SILVER / LOAD AUDIT
        -- ===========================
        CURRENT_TIMESTAMP()                          AS SILVER_LOAD_DATE,


    FROM raw
)

SELECT *
FROM cleaned
